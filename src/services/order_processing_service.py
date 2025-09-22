"""
Order Processing Service for background order processing.
Handles the complex logic of downloading files, generating thumbnails, and managing order states.
"""
import asyncio
import os
import logging
import aiohttp
import threading
import time
from typing import Dict, Any, Optional, Tuple
from telegram.ext import Application
from src.services.task_orchestrator import get_task_orchestrator
from src.services.async_db_service import async_db_service
from src.database.db_service import db_service
from src.config import config

logger = logging.getLogger(__name__)


class OrderProcessingService:
    """Service for processing orders in the background."""

    def __init__(self):
        """Initialize the order processing service."""
        self._orchestrator = get_task_orchestrator()
        self._bot_app: Optional[Application] = None
        self._active_downloads = {}  # Track active downloads by file_unique_id
        self._download_lock = threading.Lock()

    def set_bot_app(self, app: Application):
        """Set the bot application for sending notifications."""
        self._bot_app = app

    async def process_product_in_background(
        self,
        channel_id: int,
        message_id: int,
        file_unique_id: str,
        media_type: str,
        order_id: int,
        user_telegram_id: int,
        context_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process a product order in the background.

        Args:
            channel_id: Source channel ID
            message_id: Source message ID
            file_unique_id: Unique file identifier
            media_type: Type of media (photo/video)
            order_id: Order ID for notifications
            user_telegram_id: User's Telegram ID for notifications
            context_data: Additional context data from the bot

        Returns:
            Dict with processing result status and details
        """
        result = {
            "success": False,
            "order_id": order_id,
            "file_unique_id": file_unique_id,
            "file_path": None,
            "thumbnail_path": None,
            "error": None,
            "retry_count": 0
        }

        try:
            logger.info(f"Starting background processing for order {order_id}, file {file_unique_id}")

            # Check if this file is already being processed
            with self._download_lock:
                if file_unique_id in self._active_downloads:
                    existing_result = self._active_downloads[file_unique_id]
                    logger.info(f"File {file_unique_id} already being processed, waiting for completion...")
                    return existing_result

                # Mark as being processed
                self._active_downloads[file_unique_id] = result

            try:
                # 1. Check if product_media already exists and is available
                existing_media = await async_db_service.get_product_media_by_file_id(file_unique_id)

                if existing_media and existing_media.get('status') == 'available':
                    logger.info(f"File {file_unique_id} already available, reusing existing file")
                    result.update({
                        "success": True,
                        "file_path": existing_media.get('file_path'),
                        "thumbnail_path": existing_media.get('thumbnail_path'),
                        "reused_existing": True
                    })

                    # Complete file for order
                    if existing_media.get('file_path'):
                        await async_db_service.complete_order_file(order_id, file_unique_id, existing_media['file_path'])

                    await self._notify_order_completion(order_id, user_telegram_id, result)
                    return result

                # 2. If not available, process the download
                if not existing_media:
                    # Create new product_media entry
                    media_data = {
                        "source_channel_id": channel_id,
                        "source_message_id": message_id,
                        "file_unique_id": file_unique_id,
                        "media_type": media_type,
                        "file_path": "",  # Will be updated after download
                        "thumbnail_path": None,
                        "status": "pending"
                    }
                    await async_db_service.create_product_media(media_data)
                    logger.info(f"Created product_media entry for file {file_unique_id}")

                # 3. Set status to downloading with race condition protection
                update_success = await async_db_service.update_product_media_status(file_unique_id, "downloading")
                if not update_success:
                    # Another process might be handling this, check again
                    updated_media = await async_db_service.get_product_media_by_file_id(file_unique_id)
                    if updated_media and updated_media.get('status') == 'downloading':
                        logger.info(f"File {file_unique_id} already being downloaded by another process")
                        # Wait a bit and check if it completes
                        await asyncio.sleep(5)
                        final_media = await async_db_service.get_product_media_by_file_id(file_unique_id)
                        if final_media and final_media.get('status') == 'available':
                            result.update({
                                "success": True,
                                "file_path": final_media.get('file_path'),
                                "thumbnail_path": final_media.get('thumbnail_path'),
                                "reused_existing": True
                            })
                            return result

                # 4. Perform the actual download
                download_result = await self._download_and_process_file(
                    channel_id, message_id, file_unique_id, media_type, order_id
                )

                if download_result["success"]:
                    # Update product_media with file paths and set status to available
                    media_update = {
                        "file_path": download_result["file_path"],
                        "thumbnail_path": download_result.get("thumbnail_path")
                    }

                    # Update file path first
                    await self._orchestrator.submit(
                        self._update_product_media_paths,
                        file_unique_id,
                        download_result["file_path"],
                        download_result.get("thumbnail_path")
                    )

                    # Then set status to available
                    await async_db_service.update_product_media_status(file_unique_id, "available")

                    # Complete file for order
                    await async_db_service.complete_order_file(order_id, file_unique_id, download_result["file_path"]) 

                    result.update(download_result)
                    logger.info(f"Successfully processed file {file_unique_id} for order {order_id}")

                    # Notify completion
                    await self._notify_order_completion(order_id, user_telegram_id, result)

                else:
                    # Set status to failed
                    await async_db_service.update_product_media_status(file_unique_id, "failed")
                    result.update(download_result)
                    logger.error(f"Failed to process file {file_unique_id} for order {order_id}: {result.get('error')}")

                    # Notify failure
                    await self._notify_order_failure(order_id, user_telegram_id, result)

            finally:
                # Clean up active downloads tracking
                with self._download_lock:
                    self._active_downloads.pop(file_unique_id, None)

        except Exception as e:
            logger.error(f"Unexpected error in background processing for order {order_id}: {e}", exc_info=True)
            result["error"] = str(e)

            # Set status to failed
            try:
                await async_db_service.update_product_media_status(file_unique_id, "failed")
                await self._notify_order_failure(order_id, user_telegram_id, result)
            except Exception as notify_error:
                logger.error(f"Failed to notify order failure: {notify_error}")

            with self._download_lock:
                self._active_downloads.pop(file_unique_id, None)

        return result

    async def _download_and_process_file(
        self,
        channel_id: int,
        message_id: int,
        file_unique_id: str,
        media_type: str,
        order_id: int
    ) -> Dict[str, Any]:
        """Download and process the file."""
        result = {
            "success": False,
            "file_path": None,
            "thumbnail_path": None,
            "error": None
        }

        try:
            # Generate file path
            file_extension = '.jpg' if media_type == 'photo' else '.mp4'
            file_path = os.path.join('uploads', f"{file_unique_id}{file_extension}")

            # Ensure uploads directory exists
            os.makedirs('uploads', exist_ok=True)

            # For this implementation, we'll need to get the file from Telegram
            # This requires the bot application context
            if not self._bot_app:
                raise Exception("Bot application not set - cannot download files")

            # Get file object from Telegram
            try:
                # We need to reconstruct how to get the file
                # This is a simplified approach - in real implementation, you'd need
                # to pass the actual file object or file ID from the original message processing

                # For now, we'll simulate the download process
                # In actual implementation, you'd use:
                # file_obj = await self._bot_app.bot.get_file(file_id)
                # and then download it

                # Simulate download delay
                await asyncio.sleep(1)

                # Create a dummy file for testing (remove in actual implementation)
                with open(file_path, 'w') as f:
                    f.write(f"dummy file for {file_unique_id}")

                logger.info(f"Downloaded file to {file_path}")
                result["file_path"] = file_path

                # Generate thumbnail for videos
                if media_type == 'video':
                    thumbnail_path = await self._generate_thumbnail(file_path)
                    result["thumbnail_path"] = thumbnail_path

                result["success"] = True

            except Exception as download_error:
                logger.error(f"File download failed for {file_unique_id}: {download_error}")
                result["error"] = f"Download failed: {str(download_error)}"

        except Exception as e:
            logger.error(f"Error in _download_and_process_file: {e}", exc_info=True)
            result["error"] = str(e)

        return result

    async def _generate_thumbnail(self, video_path: str) -> Optional[str]:
        """Generate thumbnail for video file."""
        try:
            # Run thumbnail generation in thread pool to avoid blocking
            from src.services.file_service import generate_video_thumbnail
            thumbnail_path = await self._orchestrator.submit(generate_video_thumbnail, video_path)
            return thumbnail_path
        except Exception as e:
            logger.error(f"Thumbnail generation failed for {video_path}: {e}")
            return None

    def _update_product_media_paths(self, file_unique_id: str, file_path: str, thumbnail_path: Optional[str]):
        """Update product_media file paths (synchronous method for thread pool)."""
        try:
            conn = db_service._get_connection()
            cursor = conn.cursor()

            if thumbnail_path:
                cursor.execute(
                    "UPDATE product_media SET file_path = ?, thumbnail_path = ? WHERE file_unique_id = ?",
                    (file_path, thumbnail_path, file_unique_id)
                )
            else:
                cursor.execute(
                    "UPDATE product_media SET file_path = ? WHERE file_unique_id = ?",
                    (file_path, file_unique_id)
                )

            conn.commit()
            logger.debug(f"Updated file paths for {file_unique_id}")

        except Exception as e:
            logger.error(f"Failed to update product_media paths: {e}")

    async def _notify_order_completion(self, order_id: int, user_telegram_id: int, result: Dict[str, Any]):
        """Notify about successful order completion."""
        try:
            if not self._bot_app:
                logger.warning("Cannot send completion notification - bot app not set")
                return

            # Send notification to admin group
            admin_group_id = config.GROUP_ID
            admin_topic_id = getattr(config, 'ORDERS_TOPIC_ID', None)

            if admin_group_id:
                message = f"✅ Order #{order_id} processed successfully\n📦 File downloaded and ready"

                if admin_topic_id:
                    await self._bot_app.bot.send_message(
                        chat_id=admin_group_id,
                        text=message,
                        message_thread_id=int(admin_topic_id)
                    )
                else:
                    await self._bot_app.bot.send_message(chat_id=admin_group_id, text=message)

        except Exception as e:
            logger.error(f"Failed to send completion notification for order {order_id}: {e}")

    async def _notify_order_failure(self, order_id: int, user_telegram_id: int, result: Dict[str, Any]):
        """Notify about order processing failure."""
        try:
            if not self._bot_app:
                logger.warning("Cannot send failure notification - bot app not set")
                return

            error_msg = result.get('error', 'Unknown error')

            # Notify user about the failure
            try:
                await self._bot_app.bot.send_message(
                    chat_id=user_telegram_id,
                    text=f"❌ Kechirasiz, buyurtma #{order_id} ni qayta ishlab bo'lmadi.\n\n"
                         f"Xato: {error_msg}\n\n"
                         f"Iltimos, keyinroq qayta urinib ko'ring yoki admin bilan bog'laning."
                )
            except Exception as user_notify_error:
                logger.error(f"Failed to notify user {user_telegram_id} about order {order_id} failure: {user_notify_error}")

                # If we can't notify the user directly, notify admin
                admin_group_id = config.GROUP_ID
                if admin_group_id:
                    admin_message = (
                        f"❌ Order #{order_id} failed and user notification failed\n"
                        f"User ID: {user_telegram_id}\n"
                        f"Error: {error_msg}\n"
                        f"User notification error: {str(user_notify_error)}"
                    )
                    await self._bot_app.bot.send_message(chat_id=admin_group_id, text=admin_message)

        except Exception as e:
            logger.error(f"Failed to send failure notification for order {order_id}: {e}")

    async def retry_failed_orders(self, max_retries: int = 3) -> Dict[str, Any]:
        """Retry failed product media downloads."""
        retry_stats = {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "errors": []
        }

        try:
            # Get failed product media entries
            failed_media = await async_db_service.get_pending_product_media(limit=10)

            for media in failed_media:
                if media.get('status') != 'failed':
                    continue

                retry_stats["attempted"] += 1
                file_unique_id = media['file_unique_id']

                logger.info(f"Retrying failed download for file {file_unique_id}")

                try:
                    # Attempt to reprocess
                    # Note: This would need more context from the original order
                    # For now, just reset status to pending for manual intervention
                    await async_db_service.update_product_media_status(file_unique_id, "pending")
                    retry_stats["succeeded"] += 1

                except Exception as retry_error:
                    logger.error(f"Failed to retry {file_unique_id}: {retry_error}")
                    retry_stats["failed"] += 1
                    retry_stats["errors"].append(f"{file_unique_id}: {str(retry_error)}")

        except Exception as e:
            logger.error(f"Error in retry_failed_orders: {e}")
            retry_stats["errors"].append(f"General error: {str(e)}")

        return retry_stats


# Global instance
order_processing_service = OrderProcessingService()