"""
Async Database Service Wrapper
Provides non-blocking access to database operations by running them in the TaskOrchestrator.
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional
from src.database.db_service import db_service
from src.services.task_orchestrator import get_task_orchestrator

logger = logging.getLogger(__name__)


class AsyncDatabaseService:
    """Async wrapper for database service operations."""

    def __init__(self):
        """Initialize the async database service."""
        self._orchestrator = get_task_orchestrator()

    # User operations
    async def get_user_by_telegram_id(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Get user by telegram ID (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_user_by_telegram_id, telegram_id)

    async def get_user_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get user by code (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_user_by_code, code)

    async def create_user(self, user_data: Dict[str, Any]) -> Optional[int]:
        """Create user (non-blocking)."""
        return await self._orchestrator.submit(db_service.create_user, user_data)

    async def update_user(self, telegram_id: int, user_data: Dict[str, Any]) -> bool:
        """Update user (non-blocking)."""
        return await self._orchestrator.submit(db_service.update_user, telegram_id, user_data)

    # Product media operations
    async def get_product_media_by_message_id(self, channel_id: int, message_id: int) -> Optional[Dict[str, Any]]:
        """Get product media by message ID (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_product_media_by_message_id, channel_id, message_id)

    async def get_product_media_by_file_id(self, file_unique_id: str) -> Optional[Dict[str, Any]]:
        """Get product media by file unique ID (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_product_media_by_file_id, file_unique_id)

    async def create_product_media(self, media_data: Dict[str, Any]) -> Optional[int]:
        """Create product media entry (non-blocking)."""
        return await self._orchestrator.submit(db_service.create_product_media, media_data)

    async def update_product_media_status(self, file_unique_id: str, status: str) -> bool:
        """Update product media status (non-blocking)."""
        return await self._orchestrator.submit(db_service.update_product_media_status, file_unique_id, status)

    async def get_pending_product_media(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get pending product media entries (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_pending_product_media, limit)

    # Order operations
    async def create_order(self, user_id: int, collection_id: int, amount: Optional[int] = None,
                          original_message_id: Optional[int] = None, original_channel_id: Optional[int] = None) -> Optional[int]:
        """Create order (non-blocking)."""
        return await self._orchestrator.submit(
            db_service.create_order,
            user_id,
            collection_id,
            amount,
            original_message_id,
            original_channel_id
        )

    async def find_existing_order(self, user_id: int, collection_id: int, original_message_id: int, original_channel_id: int) -> Optional[Dict[str, Any]]:
        """Find an existing, active order (non-blocking)."""
        return await self._orchestrator.submit(
            db_service.find_existing_order,
            user_id,
            collection_id,
            original_message_id,
            original_channel_id
        )

    async def get_order_by_id(self, order_id: int) -> Optional[Dict[str, Any]]:
        """Get order by ID (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_order_by_id, order_id)

    async def update_order(self, order_id: int, order_data: Dict[str, Any]) -> bool:
        """Update order (non-blocking)."""
        return await self._orchestrator.submit(db_service.update_order, order_id, order_data)

    async def get_user_orders(self, user_id: int) -> List[Dict[str, Any]]:
        """Get user orders (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_user_orders, user_id)

    async def get_user_order_count(self, telegram_id: int) -> int:
        """Get user order count (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_user_order_count, telegram_id)

    # Collection operations
    async def get_active_collection(self) -> Optional[Dict[str, Any]]:
        """Get active collection (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_active_collection)

    async def create_collection(self, collection_data: Dict[str, Any]) -> Optional[int]:
        """Create collection (non-blocking)."""
        return await self._orchestrator.submit(db_service.create_collection, collection_data)

    async def update_collection_status(self, collection_id: int, status: str) -> bool:
        """Update collection status (non-blocking)."""
        return await self._orchestrator.submit(db_service.update_collection_status, collection_id, status)

    # File operations
    async def add_order_file_placeholder(self, order_id: int, file_unique_id: str) -> bool:
        """Create a pending placeholder row for an order file (non-blocking)."""
        try:
            return await self._orchestrator.submit(db_service.add_order_file, order_id, file_unique_id)
        except Exception:
            return False

    async def add_order_file(self, order_id: int, file_path: str, file_unique_id: str = None) -> bool:
        """Add or update an order file entry (non-blocking).

        Compatibility note: the underlying DB service expects creating a placeholder
        first (order_id, file_unique_id) and then updating it with the actual
        file path/status. Callers pass a file_path here after a successful
        download, so we perform both steps safely.
        """
        try:
            # Create placeholder row (idempotent across duplicates)
            await self._orchestrator.submit(db_service.add_order_file, order_id, file_unique_id)

            # Update with real file path and mark as downloaded
            updated: bool = await self._orchestrator.submit(
                db_service.update_order_file,
                file_unique_id,
                "downloaded",
                file_path,
                order_id,
            )
            return updated
        except Exception:
            # Surface False on any failure; caller logs appropriately
            return False

    async def complete_order_file(self, order_id: int, file_unique_id: str, file_path: str) -> bool:
        """Mark a specific file as downloaded with its real path (non-blocking)."""
        try:
            return await self._orchestrator.submit(
                db_service.update_order_file,
                file_unique_id,
                "downloaded",
                file_path,
                order_id,
            )
        except Exception:
            return False

    async def get_order_files(self, order_id: int) -> List[str]:
        """Get order files (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_order_files, order_id)

    # Payment operations
    async def create_payment(self, payment_data: Dict[str, Any]) -> Optional[int]:
        """Create payment (non-blocking)."""
        return await self._orchestrator.submit(db_service.create_payment, payment_data)

    async def update_payment_status(self, payment_id: int, status: str, admin_id: int = None) -> bool:
        """Update payment status (non-blocking)."""
        return await self._orchestrator.submit(db_service.update_payment_status, payment_id, status, admin_id)

    # Card operations
    async def get_all_cards(self) -> List[Dict[str, Any]]:
        """Get all cards (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_all_cards)

    async def get_active_cards(self) -> List[Dict[str, Any]]:
        """Get active cards (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_active_cards)

    async def create_card(self, card_data: Dict[str, Any]) -> Optional[int]:
        """Create card (non-blocking)."""
        return await self._orchestrator.submit(db_service.create_card, card_data)

    # Statistics operations
    async def get_user_stats(self, telegram_id: int) -> Dict[str, Any]:
        """Get user statistics (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_user_stats, telegram_id)

    async def mark_final_notification_sent(self, order_id: int) -> bool:
        """Mark that the final notification has been sent for this order (non-blocking)."""
        return await self._orchestrator.submit(db_service.mark_final_notification_sent, order_id)

    async def has_final_notification_been_sent(self, order_id: int) -> bool:
        """Check if the final notification has already been sent for this order (non-blocking)."""
        return await self._orchestrator.submit(db_service.has_final_notification_been_sent, order_id)

    # Admin operations
    async def get_user_orders_by_collection(self, user_id: int, collection_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get user orders by collection (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_user_orders_by_collection, user_id, collection_id, limit)

    # Report operations
    async def get_collections_for_reporting(self) -> List[Dict[str, Any]]:
        """Get collections for reporting (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_collections_for_reporting)

    async def create_or_update_user_report(self, user_id: int, collection_id: int, file_path: str) -> Optional[int]:
        """Create or update a user report (non-blocking)."""
        return await self._orchestrator.submit(db_service.create_or_update_user_report, user_id, collection_id, file_path)

    async def get_user_report_status_for_collection(self, collection_id: int) -> Dict[int, bool]:
        """Get user report status for a collection (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_user_report_status_for_collection, collection_id)

    async def check_and_set_all_reports_sent(self, collection_id: int) -> bool:
        """Check and set all_reports_sent flag for a collection (non-blocking)."""
        return await self._orchestrator.submit(db_service.check_and_set_all_reports_sent, collection_id)

    async def get_reports_for_user(self, user_id: int) -> List[Dict[str, Any]]:
        """Get reports for a user (non-blocking)."""
        return await self._orchestrator.submit(db_service.get_reports_for_user, user_id)

    # Utility operations
    async def execute_raw_query(self, query: str, params: tuple = ()) -> Any:
        """Execute raw SQL query (non-blocking) - use with caution."""
        return await self._orchestrator.submit(db_service._execute_query, query, params)

    # Health check
    async def health_check(self) -> bool:
        """Check database health (non-blocking)."""
        try:
            # Simple query to check if database is responsive
            await self._orchestrator.submit(lambda: db_service._get_connection().execute("SELECT 1").fetchone())
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False


# Global instance
async_db_service = AsyncDatabaseService()