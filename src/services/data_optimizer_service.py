import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Callable, Awaitable, List, Dict, Any, Optional

from src.config import config
from src.database.db_service import db_service


class DataOptimizerService:
    """Service responsible for periodic cleanup of stale data and files."""

    def __init__(self) -> None:
        self.logger = logging.getLogger("DataOptimizer")
        self.retention_days: int = getattr(config, 'DATA_RETENTION_DAYS', 60)
        self.batch_size: int = getattr(config, 'CLEANUP_BATCH_SIZE', 50)
        self.batch_delay_seconds: int = getattr(config, 'CLEANUP_BATCH_DELAY_SECONDS', 10)
        self.dry_run: bool = getattr(config, 'CLEANUP_DRY_RUN', False)

    async def run_cleanup_cycle(self) -> None:
        """Entry point for scheduled cleanup cycle."""
        self._refresh_config()

        # Calculate cutoff timestamp based on retention days
        cutoff_dt = datetime.utcnow() - timedelta(days=int(self.retention_days))
        cutoff_iso = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

        self.logger.info(
            f"Starting cleanup cycle (cutoff<{cutoff_iso}, retention_days={self.retention_days}, "
            f"batch_size={self.batch_size}, delay={self.batch_delay_seconds}s, dry_run={self.dry_run})"
        )

        total_summary = {"orders": 0, "payments": 0, "reports": 0, "media": 0, "transactions": 0, "collections": 0}

        try:
            # Orders first (will cascade order_files or we delete files explicitly via file_paths)
            total_summary["orders"] += await self._cleanup_entity_batches(
                fetch_batch=lambda: db_service.get_stale_orders_batch(cutoff_iso, self.batch_size),
                table_name="orders",
            )

            # Product media (videos/images/thumbnails)
            total_summary["media"] += await self._cleanup_entity_batches(
                fetch_batch=lambda: db_service.get_stale_product_media_batch(cutoff_iso, self.batch_size),
                table_name="product_media",
            )

            total_summary["payments"] += await self._cleanup_entity_batches(
                fetch_batch=lambda: db_service.get_stale_payments_batch(cutoff_iso, self.batch_size),
                table_name="payment",
            )

            total_summary["reports"] += await self._cleanup_entity_batches(
                fetch_batch=lambda: db_service.get_stale_reports_batch(cutoff_iso, self.batch_size),
                table_name="user_reports",
            )

            # Transactions (unlinked only per query)
            total_summary["transactions"] += await self._cleanup_entity_batches(
                fetch_batch=lambda: db_service.get_stale_transactions_batch(cutoff_iso, self.batch_size),
                table_name="transactions",
            )

            # Collections without dependencies
            total_summary["collections"] += await self._cleanup_entity_batches(
                fetch_batch=lambda: db_service.get_stale_collections_batch(cutoff_iso, self.batch_size),
                table_name="collections",
            )
        except Exception as e:
            self.logger.error(f"Cleanup cycle error: {e}")

        self.logger.info(
            f"Cleanup cycle finished. Deleted counts (dry_run={self.dry_run}): "
            f"orders={total_summary['orders']}, media={total_summary['media']}, payments={total_summary['payments']}, "
            f"reports={total_summary['reports']}, transactions={total_summary['transactions']}, collections={total_summary['collections']}"
        )

    def _refresh_config(self) -> None:
        """Refresh runtime config values from .env and database."""
        try:
            # Reload .env to pick up any configuration changes without restart
            try:
                from dotenv import load_dotenv  # type: ignore
                load_dotenv(override=True)
            except Exception:
                pass

            # Optionally reload DB-based settings
            try:
                config.load_from_db()
            except Exception:
                pass

            # Update service settings from config/env
            self.retention_days = getattr(config, 'DATA_RETENTION_DAYS', self.retention_days)
            self.batch_size = getattr(config, 'CLEANUP_BATCH_SIZE', self.batch_size)
            self.batch_delay_seconds = getattr(config, 'CLEANUP_BATCH_DELAY_SECONDS', self.batch_delay_seconds)
            self.dry_run = getattr(config, 'CLEANUP_DRY_RUN', self.dry_run)
        except Exception:
            pass

    async def _cleanup_entity_batches(self, fetch_batch: Callable[[], List[Dict[str, Any]]], table_name: str) -> int:
        """Generic cleanup loop over batches for a given entity/table.

        Returns number of records deleted (or would delete in dry_run).
        """
        total_deleted = 0

        while True:
            batch = fetch_batch()
            if not batch:
                break

            ids_to_delete: List[int] = []
            files_to_delete: List[str] = []

            for item in batch:
                item_id = int(item.get("id"))
                ids_to_delete.append(item_id)
                for f in item.get("file_paths", []) or []:
                    if isinstance(f, str) and f.strip():
                        files_to_delete.append(f)

            # Delete files first (best-effort)
            for fpath in files_to_delete:
                await self._delete_file_safe(fpath)

            # Delete DB rows
            if self.dry_run:
                self.logger.info(f"[DRY RUN] Would delete {len(ids_to_delete)} rows from {table_name}: {ids_to_delete[:10]}{'...' if len(ids_to_delete)>10 else ''}")
                deleted_count = len(ids_to_delete)
            else:
                ok = db_service.delete_records_by_ids(table_name, ids_to_delete)
                deleted_count = len(ids_to_delete) if ok else 0

            total_deleted += deleted_count

            # Rate-limit between batches
            try:
                await asyncio.sleep(float(self.batch_delay_seconds))
            except Exception:
                await asyncio.sleep(1)

        return total_deleted

    async def _delete_file_safe(self, file_path: str) -> None:
        """Delete a file from filesystem with logging; respect dry-run."""
        try:
            # Only attempt within project directories we expect
            norm = os.path.normpath(file_path)
            if not (norm.startswith('uploads') or norm.startswith('receipts') or os.path.join('uploads', '') in norm):
                # Allow absolute or relative paths but log unexpected
                self.logger.debug(f"Skipping non-uploads file path: {file_path}")
            if self.dry_run:
                self.logger.info(f"[DRY RUN] Would delete file: {file_path}")
                return
            if os.path.exists(file_path):
                os.remove(file_path)
                self.logger.debug(f"Deleted file: {file_path}")
            else:
                self.logger.debug(f"File not found (skip): {file_path}")
        except Exception as e:
            self.logger.error(f"Failed deleting file {file_path}: {e}")


# Global instance
data_optimizer_service = DataOptimizerService()
