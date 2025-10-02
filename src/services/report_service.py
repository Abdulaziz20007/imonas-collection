import os
import logging
from typing import List, Dict, Any, IO
from fastapi import UploadFile
from telegram import Bot
from telegram.error import Forbidden

# Compatibility import for python-telegram-bot versions without FSInputFile
try:
    from telegram import FSInputFile  # type: ignore
    _HAS_FSINPUTFILE = True
except Exception:  # pragma: no cover - runtime compatibility
    try:
        from telegram import InputFile  # type: ignore
        _HAS_FSINPUTFILE = False
    except Exception:  # Fallback if even InputFile import path differs
        InputFile = None  # type: ignore
        _HAS_FSINPUTFILE = False

from src.database.db_service import db_service
from src.config import config

logger = logging.getLogger(__name__)


class ReportService:
    def __init__(self, bot: Bot = None):
        self.bot = bot
        self.reports_dir = os.path.join("uploads", "reports")
        os.makedirs(self.reports_dir, exist_ok=True)

    def set_bot(self, bot: Bot):
        self.bot = bot

    async def process_bulk_upload(self, collection_id: int, uploaded_files: List[UploadFile]) -> Dict[str, Any]:
        """
        Processes a bulk upload of PDF reports for a collection.
        Filenames are expected to be `{user_code}.pdf`.
        """
        from src.services.task_orchestrator import get_task_orchestrator
        orchestrator = get_task_orchestrator()

        summary = {"success": 0, "failed": 0, "errors": []}
        
        for file in uploaded_files:
            if not file.filename or not file.filename.lower().endswith('.pdf'):
                summary['failed'] += 1
                summary['errors'].append(f"{file.filename}: Not a PDF file.")
                continue

            user_code = os.path.splitext(file.filename)[0]
            user = await orchestrator.submit(db_service.get_user_by_code, user_code)

            if not user:
                summary['failed'] += 1
                summary['errors'].append(f"{file.filename}: User with code '{user_code}' not found.")
                continue
            
            user_orders_in_collection = await orchestrator.submit(db_service.get_user_orders_by_collection, user['id'], collection_id, 1)
            if not user_orders_in_collection:
                summary['failed'] += 1
                summary['errors'].append(f"{file.filename}: User '{user_code}' is not part of collection #{collection_id}.")
                continue

            try:
                await self._save_and_send_report(user['id'], user['telegram_id'], collection_id, user['code'], file.file, "Hisobotingiz tayyor!")
                summary['success'] += 1
            except Exception as e:
                logger.error(f"Failed to process report for user {user_code}: {e}")
                summary['failed'] += 1
                summary['errors'].append(f"{file.filename}: {str(e)}")

        await orchestrator.submit(db_service.check_and_set_all_reports_sent, collection_id)
        
        return summary

    async def process_individual_upload(self, user_id: int, collection_id: int, uploaded_file: IO) -> None:
        """Processes an individual report upload for a specific user."""
        from src.services.task_orchestrator import get_task_orchestrator
        orchestrator = get_task_orchestrator()

        user = await orchestrator.submit(db_service.get_user_by_id, user_id)
        if not user or not user.get('code'):
            raise ValueError(f"User with ID {user_id} not found or has no code.")
        
        await self._save_and_send_report(user['id'], user['telegram_id'], collection_id, user['code'], uploaded_file, "Hisobotingiz tayyor!")
        await orchestrator.submit(db_service.check_and_set_all_reports_sent, collection_id)

    async def process_report_update(self, user_id: int, collection_id: int, new_file: IO) -> None:
        """Processes an update for an existing report."""
        from src.services.task_orchestrator import get_task_orchestrator
        orchestrator = get_task_orchestrator()

        user = await orchestrator.submit(db_service.get_user_by_id, user_id)
        if not user or not user.get('code'):
            raise ValueError(f"User with ID {user_id} not found or has no code.")
            
        await self._save_and_send_report(user['id'], user['telegram_id'], collection_id, user['code'], new_file, "Hisobotingiz yangilandi!")
    
    async def _save_and_send_report(self, user_id: int, telegram_id: int, collection_id: int, user_code: str, file_stream: IO, message: str):
        """Helper to save the report file and send it to the user."""
        from src.services.task_orchestrator import get_task_orchestrator
        orchestrator = get_task_orchestrator()

        file_name = f"{collection_id}-{user_code}.pdf"
        file_path = os.path.join(self.reports_dir, file_name)
        
        def _save_file_sync():
            with open(file_path, "wb") as buffer:
                file_stream.seek(0)
                buffer.write(file_stream.read())
        
        await orchestrator.submit(_save_file_sync)
        
        await orchestrator.submit(db_service.create_or_update_user_report, user_id, collection_id, file_path)
        
        if not self.bot:
            logger.warning("Bot not set in ReportService. Cannot send notification.")
            return
            
        # Determine the caption based on the message
        if message == "Hisobotingiz tayyor!":
            caption = f"Kolleksiya #{collection_id} uchun hisobotingiz tayyor."
        else:
            caption = message  # For updates: "Hisobotingiz yangilandi!"

        try:
            # Ensure file is not empty before sending
            if os.path.getsize(file_path) <= 0:
                raise ValueError("Generated report file is empty")

            if _HAS_FSINPUTFILE:
                await self.bot.send_document(
                    chat_id=telegram_id,
                    document=FSInputFile(file_path, filename=f"Hisobot-Kolleksiya-{collection_id}.pdf"),
                    caption=caption
                )
            else:
                if InputFile is None:
                    raise RuntimeError("python-telegram-bot InputFile not available")
                f = open(file_path, "rb")
                try:
                    await self.bot.send_document(
                        chat_id=telegram_id,
                        document=InputFile(f, filename=f"Hisobot-Kolleksiya-{collection_id}.pdf"),
                        caption=caption
                    )
                finally:
                    try:
                        f.close()
                    except Exception:
                        pass
            logger.info(f"Report sent to user {telegram_id} for collection {collection_id}")
        except Forbidden:
            logger.warning(f"Failed to send report to user {telegram_id}: Bot was blocked by the user.")
            # Do not re-raise, as this is a non-critical error for bulk uploads.
        except Exception as e:
            logger.error(f"Failed to send report to user {telegram_id}: {e}")
            raise


# Global instance
report_service = ReportService()


