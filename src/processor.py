"""
Message processor containing the core business logic for the Telegram bot.
Handles validation, data extraction, and coordination with the database service.
"""
import asyncio
import random
import string
import logging
import ffmpeg
import os
import concurrent.futures
import threading
import aiohttp
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from telegram import Message, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, KeyboardButton
from telegram.ext import ContextTypes
from src.database.db_service import db_service
from src.config import config
from src.database.config_db_service import config_db_service
from src.utils.keyboards import get_region_keyboard
from src.receipt_processor import receipt_processor
from src.services.file_service import generate_video_thumbnail as _generate_video_thumbnail
from src.services.concurrency_manager import concurrency_manager
from src.services.file_service import generate_thumbnails_for_all_videos as _generate_thumbnails_for_all_videos
from src.services.file_service import generate_video_thumbnail_to_dir as _generate_video_thumbnail_to_dir
from telegram import Update
from src.ai_payment_confirmator import ai_payment_confirmator

logger = logging.getLogger(__name__)

def _generate_video_thumbnail(video_path: str) -> Optional[str]:
    """
    Generates a thumbnail for a video file using ffmpeg.
    Saves it as a .webp file in the uploads/thumbnail directory.
    
    Args:
        video_path: The path to the video file.
        
    Returns:
        The path to the generated thumbnail, or None on failure.
    """
    try:
        if not os.path.exists(video_path):
            logger.error(f"Video file not found for thumbnail generation: {video_path}")
            return None

        # Create thumbnail path
        thumbnail_dir = os.path.join('uploads', 'thumbnail')
        # This directory should be created on startup, but exist_ok=True is safe.
        os.makedirs(thumbnail_dir, exist_ok=True)
        
        base_filename = os.path.splitext(os.path.basename(video_path))[0]
        thumbnail_filename = f"{base_filename}.webp"
        thumbnail_path = os.path.join(thumbnail_dir, thumbnail_filename)
        
        # Skip if thumbnail already exists and is valid
        if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
            logger.debug(f"Thumbnail already exists for {video_path} at {thumbnail_path}")
            return thumbnail_path
        
        # Extract thumbnail using ffmpeg at 5 second mark (or beginning if video is shorter)
        (
            ffmpeg
            .input(video_path, ss='00:00:05')
            .output(thumbnail_path, vframes=1, **{
                'c:v': 'libwebp',
                'quality': '80',
                'lossless': '0'
            })
            .overwrite_output()
            .run(quiet=True, capture_stdout=True)
        )
        
        # Verify that the thumbnail was created
        if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
            logger.info(f"Thumbnail generated for {video_path} at {thumbnail_path}")
            return thumbnail_path
        else:
            logger.error(f"Failed to generate thumbnail for {video_path}")
            return None
            
    except ffmpeg.Error as e:
        logger.error(f"FFmpeg error generating thumbnail for {video_path}: {e.stderr.decode() if e.stderr else 'Unknown error'}")
        return None
    except Exception as e:
        logger.error(f"Error generating thumbnail for {video_path}: {e}", exc_info=True)
        return None

def generate_thumbnails_for_all_videos():
    """
    Generate thumbnails for all existing video files that don't have thumbnails yet.
    This is useful for batch processing existing videos.
    """
    try:
        uploads_dir = 'uploads'
        if not os.path.exists(uploads_dir):
            logger.warning("Uploads directory not found")
            return
            
        video_extensions = ('.mp4', '.webm', '.mov', '.avi', '.mkv')
        generated_count = 0
        
        # Find all video files in uploads directory
        for filename in os.listdir(uploads_dir):
            if filename.lower().endswith(video_extensions):
                video_path = os.path.join(uploads_dir, filename)
                
                # Check if thumbnail already exists
                base_filename = os.path.splitext(filename)[0]
                thumbnail_path = os.path.join('uploads', 'thumbnail', f"{base_filename}.webp")
                
                if not os.path.exists(thumbnail_path):
                    logger.info(f"Generating missing thumbnail for {filename}")
                    if _generate_video_thumbnail(video_path):
                        generated_count += 1
                        
        logger.info(f"Batch thumbnail generation completed. Generated {generated_count} thumbnails.")
        
    except Exception as e:
        logger.error(f"Error in batch thumbnail generation: {e}", exc_info=True)

class MessageProcessor:
    """Handles the processing of incoming Telegram messages."""
    
    def __init__(self):
        """Initialize the message processor."""
        self.db_service = db_service
        self.bot_app = None
        self.userbot = None
        # Thread pool for concurrent file operations - optimized for 20-core system
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=15, thread_name_prefix="FileProcessor")
        # Semaphore to limit concurrent downloads - increased for high-speed internet
        self.download_semaphore = asyncio.Semaphore(10)
        # Track active tasks per user to prevent spam
        self.user_active_tasks = {}  # user_id -> set of task names
        self.user_task_lock = threading.Lock()
        # Locks for per-product processing to avoid duplicate downloads
        self.product_locks: dict[tuple[int, int], asyncio.Lock] = {}
        self.product_locks_lock = threading.Lock()
        logger.info("MessageProcessor initialized with concurrent file processing support")
    
    def set_bot_app(self, bot_application):
        """Set the bot application instance for sending notifications."""
        self.bot_app = bot_application
    
    def set_userbot(self, userbot_client):
        """Set the userbot client instance for advanced operations."""
        self.userbot = userbot_client
    
    def __del__(self):
        """Cleanup resources when the processor is destroyed."""
        try:
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
                logger.info("File processor thread pool shutdown completed")
            if hasattr(self, 'user_active_tasks'):
                with self.user_task_lock:
                    self.user_active_tasks.clear()
                logger.info("Cleared active task tracking")
        except Exception as e:
            logger.warning(f"Error during processor cleanup: {e}")
    
    def get_user_task_count(self, user_id: int) -> int:
        """Get the number of active tasks for a specific user."""
        with self.user_task_lock:
            return len(self.user_active_tasks.get(user_id, set()))
    
    async def _download_file_optimized(self, file_url: str, file_path: str, timeout: int = 30) -> bool:
        """
        Optimized file download using aiohttp with streaming and proper error handling.
        Returns True if successful, False otherwise.
        """
        try:
            # Configure optimized HTTP client for high concurrency
            timeout_config = aiohttp.ClientTimeout(total=timeout)
            connector = aiohttp.TCPConnector(
                limit=100,          # Increased total connection pool size
                limit_per_host=20,  # Increased per-host limit for Telegram API
                keepalive_timeout=60,
                enable_cleanup_closed=True,
                ttl_dns_cache=300,  # DNS cache for 5 minutes
                use_dns_cache=True,
                family=0            # Use both IPv4 and IPv6
            )
            
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=timeout_config,
                headers={'User-Agent': 'Telegram Bot File Downloader/1.0'}
            ) as session:
                logger.info(f"Starting optimized download from {file_url}")
                
                async with session.get(file_url) as response:
                    if response.status != 200:
                        logger.error(f"Download failed with status {response.status}")
                        return False
                    
                    # Stream download in chunks for better performance
                    with open(file_path, 'wb') as file:
                        chunk_size = 8192  # 8KB chunks for optimal performance
                        downloaded = 0
                        async for chunk in response.content.iter_chunked(chunk_size):
                            file.write(chunk)
                            downloaded += len(chunk)
                        
                        logger.info(f"Successfully downloaded {downloaded} bytes to {file_path}")
                        return True
                        
        except asyncio.TimeoutError:
            logger.error(f"Download timeout for {file_url}")
            return False
        except aiohttp.ClientError as e:
            logger.error(f"HTTP client error downloading {file_url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {file_url}: {e}")
            return False
    
    async def _process_file_in_background(self, bot, user_telegram_id: int, order_id: int, file_to_download, file_unique_id: str, file_extension: str):
        """
        Downloads a file in the background with concurrency control,
        updates its status in the DB, and triggers order finalization if all conditions are met.
        """
        # Use semaphore to limit concurrent downloads
        async with self.download_semaphore:
            try:
                logger.info(f"Starting file download for order {order_id}, file {file_unique_id}")
                
                # 1. Download the file
                os.makedirs("uploads", exist_ok=True)
                
                # Generate a unique file path for this order (handles duplicate files)
                import time
                timestamp = int(time.time() * 1000)  # milliseconds for uniqueness
                file_path = os.path.join("uploads", f"order_{user_telegram_id}_{order_id}_{timestamp}_{file_unique_id}{file_extension}")
                
                # If file already exists (shouldn't happen with timestamp, but just in case), add counter
                counter = 1
                original_file_path = file_path
                while os.path.exists(file_path):
                    file_path = original_file_path.replace(file_extension, f"_{counter}{file_extension}")
                    counter += 1
                
                # Download with optimized method and retry logic
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        # Get file object to obtain the download URL
                        file_object = await file_to_download.get_file()
                        file_url = file_object.file_path
                        
                        # Use optimized download method with streaming
                        download_success = await self._download_file_optimized(
                            file_url=file_url,
                            file_path=file_path,
                            timeout=30  # Reduced timeout since optimized method is faster
                        )
                        
                        if download_success:
                            logger.info(f"File downloaded successfully to {file_path} for order {order_id} (attempt {attempt + 1})")
                            break
                        else:
                            raise Exception("Optimized download failed")
                            
                    except Exception as download_error:
                        logger.warning(f"Download failed for file {file_unique_id}, attempt {attempt + 1}: {download_error}")
                        if attempt == max_retries - 1:
                            # Fallback to original method if all optimized attempts fail
                            try:
                                logger.info(f"Falling back to original download method for {file_unique_id}")
                                file_object = await file_to_download.get_file()
                                await asyncio.wait_for(
                                    file_object.download_to_drive(file_path),
                                    timeout=60.0
                                )
                                logger.info(f"Fallback download successful for {file_unique_id}")
                                break
                            except Exception as fallback_error:
                                logger.error(f"Both optimized and fallback download failed for {file_unique_id}: {fallback_error}")
                                raise

                # 2. Generate thumbnail if it's a video (run in thread pool to avoid blocking)
                if file_extension in ['.mp4', '.mov', '.avi']:
                    try:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(self.executor, _generate_video_thumbnail, file_path)
                        logger.info(f"Generated thumbnail for video {file_path}")
                    except Exception as thumb_error:
                        logger.warning(f"Failed to generate thumbnail for {file_path}: {thumb_error}")
                        # Continue processing even if thumbnail generation fails

                # 3. Update file status in DB to 'downloaded' (run in thread pool to avoid blocking)
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self.executor, 
                    self.db_service.update_order_file, file_unique_id, 'downloaded', file_path, order_id)
                logger.info(f"Updated file status to 'downloaded' for file {file_unique_id}")

                # 4. Check if all files for the order are now complete (run in thread pool)
                statuses = await loop.run_in_executor(self.executor, 
                    self.db_service.get_order_file_statuses, order_id)
                if statuses and all(s == 'downloaded' for s in statuses):
                    logger.info(f"All files for order {order_id} are downloaded.")
                    # 5. Check if the order amount (series) has been set (run in thread pool)
                    order = await loop.run_in_executor(self.executor, 
                        self.db_service.get_order_by_id, order_id)
                    if order and order.get('amount') is not None:
                        logger.info(f"Order {order_id} has amount set. Finalizing and sending notification.")
                        # 6. Finalize order by sending notification
                        # We need a context-like object to pass to the notification function
                        class MockContext:
                            def __init__(self, bot_instance):
                                self.bot = bot_instance
                        
                        mock_context = MockContext(bot)
                        finalized = await self.attempt_to_finalize_order(order_id, mock_context)
                        if finalized:
                            logger.info(f"Finalized order {order_id} during media processing")
                        else:
                            logger.debug(f"Order {order_id} not ready for finalization during media processing")
                        
                        # Send completion notification to user
                        try:
                            user = self.db_service.get_user_by_telegram_id(user_telegram_id)
                            user_name = user.get('name', 'Foydalanuvchi') if user else 'Foydalanuvchi'
                            
                            # Completion notification removed per user request - only send simple success message once
                            logger.info(f"Order {order_id} processing completed for user {user_telegram_id}")
                        except Exception as notify_error:
                            logger.error(f"Failed to send completion notification to user {user_telegram_id}: {notify_error}")
                    else:
                        logger.info(f"Order {order_id} is waiting for series amount.")
                
            except Exception as e:
                logger.error(f"Background file processing failed for order {order_id}, file {file_unique_id}: {e}", exc_info=True)
                # Update status to 'failed'
                self.db_service.update_order_file(file_unique_id, 'failed', None, order_id)
                # Notify the user about the failure
                try:
                    user = self.db_service.get_user_by_telegram_id(user_telegram_id)
                    user_name = user.get('name', 'Foydalanuvchi') if user else 'Foydalanuvchi'
                    
                    await bot.send_message(
                        chat_id=user_telegram_id,
                        text=f"❌ Kechirasiz {user_name}, faylni yuklab bo'lmadi. Buyurtma #{order_id} uchun qaytadan urining."
                    )
                except Exception as notify_error:
                    logger.error(f"Failed to notify user {user_telegram_id} about download failure: {notify_error}")
        
    async def _ai_confirmation_job(self, context: ContextTypes.DEFAULT_TYPE):
        """JobQueue callback to run AI confirmation after delay."""
        try:
            data = context.job.data if context and context.job else {}
            payment_id = data.get('payment_id') if isinstance(data, dict) else None
            receipt_data = data.get('receipt_data') if isinstance(data, dict) else None
            if not payment_id or receipt_data is None:
                logger.error("AI confirmation job missing data")
                return
            await self._trigger_ai_payment_confirmation(payment_id, receipt_data)
        except Exception as e:
            logger.error(f"Error in AI confirmation job: {e}")

    async def _delayed_ai_confirmation(self, payment_id: int, receipt_data: Dict[str, Any], delay_seconds: int = 60):
        """Fallback: delay via asyncio when JobQueue is unavailable."""
        try:
            import asyncio as _asyncio
            await _asyncio.sleep(delay_seconds)
            await self._trigger_ai_payment_confirmation(payment_id, receipt_data)
        except Exception as e:
            logger.error(f"Error in delayed AI confirmation for payment {payment_id}: {e}")

    async def get_group_topics(self):
        """Get all topics from the configured group using userbot."""
        if not self.userbot or not config.GROUP_ID:
            logger.warning("Userbot or group ID not configured for topic retrieval")
            return []
        
        try:
            from telethon.tl.functions.channels import GetForumTopicsRequest
            from telethon.tl.types import Channel
            
            # Get the group entity
            group_id = int(config.GROUP_ID)
            if group_id > 0:
                group_id = -group_id  # Convert to negative for groups
                
            group = await self.userbot.get_entity(group_id)
            
            # Check if it's a supergroup/channel with topics
            if not isinstance(group, Channel):
                logger.warning(f"Group {group_id} is not a supergroup/channel")
                return []
            
            # Get forum topics
            result = await self.userbot(GetForumTopicsRequest(
                channel=group,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=100
            ))
            
            topics = []
            for topic in result.topics:
                if hasattr(topic, 'title') and hasattr(topic, 'id'):
                    topics.append({
                        'id': topic.id,
                        'title': topic.title,
                        'icon_color': getattr(topic, 'icon_color', None),
                        'icon_emoji_id': getattr(topic, 'icon_emoji_id', None),
                        'closed': getattr(topic, 'closed', False),
                        'hidden': getattr(topic, 'hidden', False)
                    })
            
            logger.info(f"Retrieved {len(topics)} topics from group {group_id}")
            return topics
            
        except Exception as e:
            logger.error(f"Error retrieving group topics: {e}")
            return []
    
    async def process_media_submission(self, update: "Update", context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, Optional[str]]:
        """
        Process a media submission. This can be a payment receipt or a forwarded product.
        For products, it uses a reference-based system.
        Returns a tuple (success, error_message). On success, error_message is None.
        """
        message = update.message
        try:
            message_dict = message.to_dict()

            # Extract user information
            user_data = self._extract_user_data(message_dict)
            if not user_data:
                return False, "❌ Xato: Foydalanuvchi ma'lumotlarini olib bo'lmadi."

            # Check if user is fully registered
            user = self.db_service.get_user_by_telegram_id(user_data['telegram_id'])
            if not user or user['reg_step'] != 'done':
                return False, "Buyurtma yuborish uchun avval /start buyrug'i orqali ro'yxatdan o'ting."

            # Handle payment workflow (remains unchanged)
            # Test account bypass for user config.TEST_ACCOUNT_ID
            is_test_account = config.TEST_ACCOUNT_ID and user['telegram_id'] == config.TEST_ACCOUNT_ID
            if user.get('payment_step') != 'confirmed' and not is_test_account:
                logger.info(f"User {user['telegram_id']} payment step: {user.get('payment_step')}")
                if user.get('payment_step') == 'awaiting_receipt':
                    if not message.photo:
                        return False, "Iltimos, faqat to'lov kvitansiyasining RASMINI yuboring."

                    photo = message.photo[-1]
                    try:
                        receipt_file = await context.bot.get_file(photo.file_id)
                        os.makedirs("receipts", exist_ok=True)
                        receipt_path = os.path.join("receipts", f"receipt_{user['telegram_id']}_{photo.file_unique_id}.jpg")
                        
                        async with concurrency_manager.limit():
                            # Use optimized download for receipts too
                            receipt_download_success = await self._download_file_optimized(
                                file_url=receipt_file.file_path,
                                file_path=receipt_path,
                                timeout=15  # Shorter timeout for receipts
                            )

                            if not receipt_download_success:
                                # Fallback to original method for receipt if optimized fails
                                await receipt_file.download_to_drive(receipt_path)

                        # Immediately acknowledge to the user (without wait time)
                        try:
                            await message.reply_text(
                                "📨 To'lov kvitansiyangiz qabul qilindi.\n\n✅ Tasdiqlash uchun yuborildi. Iltimos, kuting."
                            )
                        except Exception:
                            pass

                        receipt_data = None
                        if receipt_processor.is_available():
                            logger.info(f"Processing receipt with AI for user {user['telegram_id']}")
                            receipt_data = await receipt_processor.process_receipt_image(receipt_path)

                            if receipt_data:
                                logger.info(f"Receipt data extracted: {receipt_data}")
                                payment_id = self.db_service.add_payment(
                                    user_id=user['id'],
                                    amount=receipt_data.get('amount', 0),
                                    receipt_url=receipt_path
                                )

                                if payment_id:
                                    # Schedule AI confirmation with a 1-minute delay to wait for bank notification
                                    try:
                                        wait_secs = int(getattr(config, 'PAYMENT_CHECKING_WAIT_TIME', 60) or 60)
                                        if context.job_queue is not None:
                                            context.job_queue.run_once(
                                                self._ai_confirmation_job,
                                                when=wait_secs,
                                                data={
                                                    'payment_id': payment_id,
                                                    'receipt_data': receipt_data
                                                },
                                                name=f"ai_confirm_payment_{payment_id}"
                                            )
                                        else:
                                            import asyncio as _asyncio
                                            _asyncio.create_task(self._delayed_ai_confirmation(payment_id, receipt_data, wait_secs))
                                    except Exception as _sched_err:
                                        logger.error(f"Failed to schedule AI confirmation for payment {payment_id}: {_sched_err}")
                                    return False, None
                                else:
                                    logger.error(f"Failed to create payment record for user {user['telegram_id']}")
                            else:
                                logger.warning(f"Failed to extract data from receipt for user {user['telegram_id']}")

                        keyboard = InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"confirm_payment_{user['telegram_id']}"),
                                InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_payment_{user['telegram_id']}")
                            ]
                        ])

                        caption_text = f"Foydalanuvchi {user.get('name', '')} ({user['telegram_id']}) to'lov kvitansiyasini yubordi."
                        if receipt_data:
                            caption_text += f"\n\n🤖 AI Tahlili:\n💰 Miqdor: {receipt_data.get('amount', 'N/A')} so'm\n⏰ Vaqt: {receipt_data.get('transaction_time', 'N/A')}\n💳 Karta: {receipt_data.get('card_number', 'N/A')}"
                        else:
                            caption_text += "\n\n⚠️ AI tahlil amalga oshmadi - qo'lda tekshirish kerak"

                        await context.bot.send_photo(
                            chat_id=config.GROUP_ID,
                            photo=photo.file_id,
                            caption=caption_text,
                            reply_markup=keyboard,
                            message_thread_id=int(config.CONFIRMATION_TOPIC_ID)
                        )
                        return False, None

                    except Exception as payment_error:
                        logger.error(f"Failed to process payment receipt for user {user['telegram_id']}: {payment_error}")
                        return False, "❌ To'lov kvitansiyasini qayta ishlashda xatolik. Iltimos, qaytadan urining."
                else:
                    return False, "Buyurtma yuborishdan oldin kanalga a'zo bo'lish uchun to'lovni amalga oshiring. /start"

            # --- Product Order Submission Logic (Reference-Based) ---
            if not message.forward_origin or message.forward_origin.type != 'channel' or str(message.forward_origin.chat.id) != config.PRIVATE_CHANNEL_ID:
                await message.reply_text("Iltimos, mahsulotlarni faqat bizning kanalimizdan yuboring.")
                return False, None

            # Get active collection
            active_collection = self.db_service.get_active_collection()
            if not active_collection:
                await message.reply_text("❌ Hozirda buyurtmalar qabul qilinmayapti. Iltimos, keyinroq qayta urining.")
                return False, None

            original_channel_id = message.forward_origin.chat.id
            original_message_id = message.forward_origin.message_id

            # Validate message existence using userbot
            if not self.userbot or not self.userbot.is_connected():
                logger.error("Userbot is not connected. Cannot validate product.")
                await message.reply_text("❌ Tizim xatosi: Mahsulotni tekshirib bo'lmadi. Admin bilan bog'laning.")
                return False, None

            try:
                # Telethon's get_messages returns None if message doesn't exist
                message_check = await self.userbot.get_messages(entity=original_channel_id, ids=original_message_id)
                if not message_check or getattr(message_check, 'deleted', False):
                    await message.reply_text("Bu mahsulot sotuvda yo'q.")
                    return False, None
            except Exception as e:
                logger.error(f"Userbot validation failed for msg {original_message_id} in channel {original_channel_id}: {e}")
                await message.reply_text("Bu mahsulot sotuvda yo'q.")
                return False, None

            # Create or reuse order (first media per submission)
            if 'order_id' not in context.user_data:
                order_id = self.db_service.create_order(
                    user_id=user['id'],
                    collection_id=active_collection['id'],
                    original_message_id=original_message_id,
                    original_channel_id=original_channel_id
                )
                if not order_id:
                    return False, "❌ Buyurtma yaratishda xatolik."
                context.user_data['order_id'] = order_id
            else:
                order_id = context.user_data['order_id']

            # Determine media
            file_to_download = None
            media_type = ''
            file_extension = ''
            file_unique_id = ''
            if message.photo:
                file_to_download = message.photo[-1]
                media_type = 'photo'
                file_extension = '.jpg'
                file_unique_id = file_to_download.file_unique_id
            elif message.video:
                file_to_download = message.video
                media_type = 'video'
                file_extension = '.mp4'
                file_unique_id = file_to_download.file_unique_id
            if not file_to_download:
                return False, "❌ Fayl topilmadi."

            # Download-once: check product_media cache
            existing_pm = self.db_service.get_product_media_by_message_id(original_channel_id, original_message_id)
            if existing_pm and existing_pm.get('file_path') and os.path.exists(existing_pm['file_path']):
                logger.info(f"Using cached product media for {original_channel_id}:{original_message_id}")
                # No order_files write; just proceed success
                return True, None

            # Ensure per-product lock
            key = (int(original_channel_id), int(original_message_id))
            with self.product_locks_lock:
                lock = self.product_locks.get(key)
                if lock is None:
                    lock = asyncio.Lock()
                    self.product_locks[key] = lock

            async with lock:
                # Re-check inside lock to avoid race
                existing_pm = self.db_service.get_product_media_by_message_id(original_channel_id, original_message_id)
                if existing_pm and existing_pm.get('file_path') and os.path.exists(existing_pm['file_path']):
                    return True, None

                # Prepare directories
                products_dir = os.path.join('uploads', 'products')
                thumbs_dir = os.path.join(products_dir, 'thumbnails')
                os.makedirs(products_dir, exist_ok=True)
                os.makedirs(thumbs_dir, exist_ok=True)

                # Build canonical filename
                base_name = f"{original_message_id}-{file_unique_id}{file_extension}"
                file_path = os.path.join(products_dir, base_name)

                # Download
                try:
                    file_object = await file_to_download.get_file()
                    file_url = file_object.file_path
                    ok = await self._download_file_optimized(file_url=file_url, file_path=file_path, timeout=60)
                    if not ok:
                        await file_object.download_to_drive(file_path)
                except Exception as d_err:
                    logger.error(f"Product media download failed: {d_err}")
                    return False, "❌ Faylni yuklab bo'lmadi. Keyinroq urinib ko'ring."

                # Thumbnail for videos
                thumbnail_path = None
                if media_type == 'video':
                    try:
                        thumbnail_path = _generate_video_thumbnail_to_dir(file_path, thumbs_dir)
                    except Exception as t_err:
                        logger.warning(f"Thumbnail generation failed: {t_err}")

                # Create product_media record
                self.db_service.create_product_media({
                    'source_channel_id': int(original_channel_id),
                    'source_message_id': int(original_message_id),
                    'file_unique_id': str(file_unique_id),
                    'media_type': media_type,
                    'file_path': file_path,
                    'thumbnail_path': thumbnail_path
                })

                return True, None

        except Exception as e:
            logger.error(f"Error processing media submission: {str(e)}", exc_info=True)
            return False, "❌ Buyurtmani qayta ishlashda kutilmagan xatolik yuz berdi. Iltimos, qaytadan urining."

    async def process_series_amount_reply(self, message: Message, context: ContextTypes.DEFAULT_TYPE) -> str:
        """
        Process the user's reply with the series amount to finalize the order.
        """
        try:
            amount_text = message.text.strip() if message.text else ""
            # Prefer awaiting_order_id (set by prompt) over transient order_id
            order_id = context.user_data.get('awaiting_order_id') or context.user_data.get('order_id')
            user_id = message.from_user.id
            is_editing = context.user_data.get('is_editing_order', False)

            logger.info(f"SERIES_UPDATE: START - Order ID {order_id}, User ID {user_id}, Amount: '{amount_text}', Is Editing: {is_editing}")

            if not order_id:
                logger.warning(f"SERIES_UPDATE: No order_id in context for user {user_id}")
                return "❌ Xato: Buyurtma ma'lumotlari topilmadi. Iltimos, avval mahsulotni yuboring."

            # Strict validation: only accept pure numeric text with no extra characters
            if not amount_text:
                return "📝 Iltimos, seriya raqamini kiriting (masalan: 5 yoki 10)."

            # Check if the text contains only digits (no spaces, letters, or other characters)
            if not amount_text.isdigit():
                return "❌ Faqat raqam kiriting. Boshqa belgilar yoki matnlar qo'shmang.\n\n📝 Masalan: 5 yoki 10"

            amount = int(amount_text)
            if amount <= 0:
                return "❌ Seriya raqami 0 dan katta bo'lishi kerak.\n\n📝 Masalan: 5 yoki 10"

            # Check if order still exists before attempting update (prevents race condition)
            loop = asyncio.get_event_loop()
            order_exists = await loop.run_in_executor(self.executor,
                self.db_service.get_order_by_id, order_id)

            if not order_exists:
                logger.warning(f"SERIES_UPDATE: Order {order_id} no longer exists, likely cancelled")
                return "❌ Bu buyurtma bekor qilingan. Yangi buyurtma berish uchun mahsulotni qaytadan yuboring."

            # Verify order ownership to prevent security issues
            user = self.db_service.get_user_by_telegram_id(user_id)
            if not user or order_exists['user_id'] != user['id']:
                logger.warning(f"SERIES_UPDATE: Order {order_id} ownership mismatch for user {user_id}")
                return "❌ Bu buyurtma sizga tegishli emas."

            # Update order amount, merging if this is an edit
            update_success = await loop.run_in_executor(
                self.executor,
                self.db_service.update_order_amount,
                order_id,
                amount,
                is_editing,
            )
            
            if is_editing:
                context.user_data.pop('is_editing_order', None) # Clean up the flag

            if not update_success:
                logger.error(f"SERIES_UPDATE: Failed to update order {order_id} with amount")
                return "❌ Buyurtma seriyasini saqlashda xatolik."

            logger.info(f"SERIES_UPDATE: SUCCESS - Order {order_id} updated.")

            # Dispatch finalization in background (non-blocking for user response)
            try:
                asyncio.create_task(self._finalize_order_in_background(order_id, context))
                logger.info(f"FINALIZATION_TASK: Dispatched background finalization for order {order_id}")
            except Exception as finalization_error:
                logger.error(f"Failed to dispatch background finalization for order {order_id}: {finalization_error}")

            # Clean up only the awaiting markers for this flow
            context.user_data.pop('awaiting_order_id', None)
            # Keep other context like registration state intact

            logger.info(f"Order {order_id} confirmed with series {amount}. Files processing in background.")
            
            # Always send immediate success confirmation
            return "✅ Buyurtma qabul qilindi"
            
        except Exception as e:
            logger.error(f"Error processing series amount reply: {str(e)}")
            return "❌ Buyurtmani yakunlashda kutilmagan xatolik yuz berdi. Iltimos, qaytadan urining."
    

    
    def _extract_user_data(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract user information from the Telegram message.
        
        Args:
            message: Telegram message object
        
        Returns:
            Dict containing user data or None if extraction failed
        """
        try:
            from_user = message.get('from')
            if not from_user:
                logger.error("No 'from' field in message")
                return None
            
            return {
                'telegram_id': from_user.get('id'),
                'name': from_user.get('first_name', ''),
                'username': from_user.get('username', '')
            }
            
        except Exception as e:
            logger.error(f"Error extracting user data: {str(e)}")
            return None

    def _generate_unique_code(self, length=4) -> str:
        """Generates a random alphanumeric code."""
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length)).upper()
    
    def _get_phone_contact_keyboard(self) -> ReplyKeyboardMarkup:
        """Creates a keyboard with contact sharing button."""
        keyboard = [[KeyboardButton("📱 Telefon raqamni yuborish", request_contact=True)]]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
    
    def _normalize_phone(self, phone: str) -> Tuple[bool, str]:
        """
        Normalizes a phone number to international format.
        Accepts any valid phone number format provided by Telegram.
        """
        cleaned_phone = ''.join(filter(lambda char: char.isdigit() or char == '+', phone))
        if not cleaned_phone.startswith('+'):
            return False, "❌ Telefon raqami '+' belgisi bilan boshlanishi kerak."
        if len(cleaned_phone) < 8:
            return False, "❌ Telefon raqami juda qisqa."
        return True, cleaned_phone

    async def _finalize_order_in_background(self, order_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        Fire-and-forget wrapper that safely attempts to finalize an order
        and logs the outcome without blocking the user-facing flow.
        """
        try:
            logger.info(f"FINALIZATION_TASK: Starting background finalization for order {order_id}")
            finalized = await self.attempt_to_finalize_order(order_id, context)
            if finalized:
                logger.info(f"FINALIZATION_TASK: Final notification sent for order {order_id}")
            else:
                logger.info(f"FINALIZATION_TASK: Order {order_id} not ready yet or already finalized")
        except Exception as e:
            logger.error(f"FINALIZATION_TASK: Exception while finalizing order {order_id}: {e}", exc_info=True)

    async def attempt_to_finalize_order(self, order_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """
        Centralized function to check if an order is complete and send final notification.
        This function is idempotent and prevents duplicate notifications.

        Returns True if the final notification was sent, False otherwise.
        """
        try:
            # Check if final notification has already been sent to prevent duplicates
            if self.db_service.has_final_notification_been_sent(order_id):
                logger.debug(f"Final notification already sent for order {order_id}, skipping")
                return False

            # Check if order has both amount (series) and all files downloaded
            order = self.db_service.get_order_by_id(order_id)
            if not order or order.get('amount') is None:
                logger.debug(f"Order {order_id} not ready for finalization - no amount set")
                return False

            # Check if all files are downloaded
            statuses = self.db_service.get_order_file_statuses(order_id)
            if not statuses or not all(s == 'downloaded' for s in statuses):
                logger.debug(f"Order {order_id} not ready for finalization - files still downloading. Statuses: {statuses}")
                return False

            # Mark notification as sent BEFORE sending to prevent race condition
            if not self.db_service.mark_final_notification_sent(order_id):
                logger.error(f"Failed to mark final notification as sent for order {order_id}")
                return False

            # Send the final notification
            logger.info(f"FINALIZATION: Sending final notification for order {order_id}")
            await self._send_realtime_order_notification(context, order_id, None)
            logger.info(f"FINALIZATION: Successfully sent final notification for order {order_id}")
            return True

        except Exception as e:
            logger.error(f"Error in attempt_to_finalize_order for order {order_id}: {e}", exc_info=True)
            return False

    async def _send_realtime_order_notification(self, context: ContextTypes.DEFAULT_TYPE, order_id: int, message: Message) -> None:
        """Send a notification about a new order to the Telegram group topic."""
        try:
            logger.info(f"NOTIFICATION_TRIGGER: Sending realtime notification for order {order_id}")
            # Get order details
            order = self.db_service.get_order_by_id(order_id)
            if not order or not context:
                logger.error(f"Could not find order {order_id} for real-time notification")
                return
            
            # Get user details
            user = self.db_service.get_user_by_id(order['user_id'])
            if not user:
                logger.error(f"Could not find user {order['user_id']} for real-time notification")
                return
            
            # Construct user's code for this collection
            user_code = f"{order['collection_id']}-{user.get('code', 'N/A')}"
            
            # Get current timestamp
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Format the notification message
            notification_text = (
                f"✅ Yangi buyurtma!\n\n"
                f"👤 Foydalanuvchi: {user.get('name', 'N/A')} {user.get('surname', '')}\n"
                f"📞 Telefon: {user.get('phone', 'N/A')}\n"
                f"🏷 Kod: {user_code}\n"
                f"📦 Buyurtma ID: #{order_id}\n"
                f"🏷 Kolleksiya: #{order['collection_id']}\n"
                f"🔢 Serya: {order.get('amount', 'N/A')}\n"
                f"📷 Fayllar soni: {len(order.get('file_urls', []))}\n"
                f"⏰ Vaqt: {current_time}"
            )
            
            # Get all files for this order
            file_paths = order.get('file_urls', [])
            logger.info(f"Realtime notification for order {order_id}: Found {len(file_paths)} files: {file_paths}")
            
            # Thumbnails are generated at media ingestion time and stored under
            # uploads/products/thumbnails. Avoid regenerating here to prevent
            # duplicates and extra I/O.

            # Send the notification to the realtime orders topic
            if config.GROUP_ID and config.REALTIME_ORDERS_TOPIC_ID:
                if not file_paths:
                    # No files - do not send text-only fallback; retries will handle media
                    logger.info(f"Skipping text-only notification for order {order_id} (no files)")
                elif len(file_paths) == 1:
                    # Single file - send as photo or video with caption
                    file_path = file_paths[0]
                    if os.path.exists(file_path):
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                # Check file size and use appropriate timeout
                                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                                timeout_seconds = min(300, max(60, file_size // 100000))  # 60s-300s based on file size
                                
                                if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.ico', '.svg')):
                                    await asyncio.wait_for(
                                        context.bot.send_photo(
                                            chat_id=config.GROUP_ID,
                                            photo=file_path,
                                            caption=notification_text,
                                            message_thread_id=int(config.REALTIME_ORDERS_TOPIC_ID)
                                        ),
                                        timeout=timeout_seconds
                                    )
                                elif file_path.lower().endswith(('.mp4', '.webm', '.mov')):
                                    await asyncio.wait_for(
                                        context.bot.send_video(
                                            chat_id=config.GROUP_ID,
                                            video=file_path,
                                            caption=notification_text,
                                            message_thread_id=int(config.REALTIME_ORDERS_TOPIC_ID)
                                        ),
                                        timeout=timeout_seconds
                                    )
                                logger.info(f"Real-time notification sent for order {order_id}")
                                break  # Success, exit retry loop
                            except Exception as e:
                                logger.error(f"Error sending file for order {order_id} (attempt {attempt + 1}/{max_retries}): {e}")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(5 * (attempt + 1))  # Exponential backoff
                                else:
                                    # Final attempt failed, log and continue
                                    logger.error(f"Final attempt failed to send file for order {order_id}")
                    else:
                        # Suppress text-only missing file note
                        logger.warning(f"Media file missing for order {order_id}: {file_path}")
                else:
                    # Multiple files - send as media group
                    max_retries = 3
                    for attempt in range(max_retries):
                        media_group = []
                        valid_files = []
                        opened_files = []
                        try:
                            # Prepare media group
                            for j, file_path in enumerate(file_paths):
                                if os.path.exists(file_path):
                                    file_obj = open(file_path, 'rb')
                                    opened_files.append(file_obj)
                                    caption = notification_text if j == 0 else None
                                    if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.ico', '.svg')):
                                        media_group.append(InputMediaPhoto(media=file_obj, caption=caption))
                                    elif file_path.lower().endswith(('.mp4', '.webm', '.mov')):
                                        media_group.append(InputMediaVideo(media=file_obj, caption=caption))
                                    else:
                                        media_group.append(InputMediaPhoto(media=file_obj, caption=caption))
                                    valid_files.append(file_path)

                            if not media_group:
                                logger.warning(f"No valid files to send for order {order_id}")
                                break # No point retrying

                            # Send media group
                            total_size = sum(os.path.getsize(f) for f in valid_files)
                            timeout_seconds = min(600, max(120, total_size // 50000))
                            await asyncio.wait_for(
                                context.bot.send_media_group(
                                    chat_id=config.GROUP_ID,
                                    media=media_group,
                                    message_thread_id=int(config.REALTIME_ORDERS_TOPIC_ID)
                                ),
                                timeout=timeout_seconds
                            )
                            logger.info(f"Real-time notification sent for order {order_id}")
                            break # Success
                        except Exception as e:
                            logger.error(f"Error sending media group for order {order_id} (attempt {attempt + 1}/{max_retries}): {e}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(5 * (attempt + 1))
                            else:
                                logger.error(f"Final attempt failed to send media group for order {order_id}")
                        finally:
                            for file_obj in opened_files:
                                try:
                                    file_obj.close()
                                except:
                                    pass
            else:
                logger.warning("GROUP_ID or REALTIME_ORDERS_TOPIC_ID not configured")
            
                
        except Exception as e:
            logger.error(f"Error sending real-time order notification: {str(e)}")
    
    def _format_collection_message(self, collection) -> Tuple[str, InlineKeyboardMarkup]:
        """Format a collection message with appropriate buttons based on status."""
        # Get stats for the collection
        stats = self.db_service.get_collection_stats(collection['id'])
        
        # Format status with emoji and Uzbek text
        status_info = {
            'open': {'emoji': '🟢', 'text': 'OCHIQ'},
            'close': {'emoji': '🔴', 'text': 'YOPIQ'}, 
            'finish': {'emoji': '✅', 'text': 'YAKUNLANGAN'}
        }
        
        status_data = status_info.get(collection['status'], {'emoji': '⚪', 'text': 'NOMA\'LUM'})
        
        # Format dates
        created_date = collection['created_at'][:19] if collection['created_at'] else 'N/A'
        
        collection_info = (
            f"{status_data['emoji']} Kolleksiya #{collection['id']}\n"
            f"📊 Holati: {status_data['text']}\n"
            f"📦 Mahsulotlar: {stats['item_count']}\n"
            f"👥 Foydalanuvchilar: {stats['user_count']}\n"
            f"📅 Ochildi: {created_date}\n"
        )
        
        if collection['status'] in ['close', 'finish']:
            if collection.get('close_at'):
                close_date = collection['close_at'].split('.')[0] if '.' in collection['close_at'] else collection['close_at'][:19]
                collection_info += f"🔒 Yopildi: {close_date}\n"
            if collection.get('finish_at'):
                finish_date = collection['finish_at'].split('.')[0] if '.' in collection['finish_at'] else collection['finish_at'][:19]
                collection_info += f"✅ Tugadi: {finish_date}\n"
        
        # Create inline keyboard with logical workflow buttons only
        buttons = []
        
        # Stricter lifecycle: do not allow manual closing for open collections
        if collection['status'] == 'close':
            # Closed collections can be reopened or finished
            buttons.extend([
                InlineKeyboardButton("🟢 Ochish", callback_data=f"status_open_{collection['id']}"),
                InlineKeyboardButton("✅ Yakunlash", callback_data=f"status_finish_{collection['id']}")
            ])
        # Finished collections cannot be reopened - no buttons for finished status
        
        keyboard = InlineKeyboardMarkup([buttons]) if buttons else InlineKeyboardMarkup([])
        
        return collection_info, keyboard

    async def send_collections_list(self, update, collections_data) -> None:
        """Send last 10 collections as separate messages with inline buttons."""
        if not collections_data:
            await update.message.reply_text("📋 Hech qanday kolleksiya topilmadi.")
            return
            
        # Send header message
        await update.message.reply_text("📋 Oxirgi 10 ta kolleksiya:")
        
        for i, collection in enumerate(collections_data, 1):
            # Use the new helper method for consistent formatting
            collection_info, keyboard = self._format_collection_message(collection)
            
            # Send individual collection message
            await update.message.reply_text(collection_info, reply_markup=keyboard)

    def _format_card_message(self, card) -> Tuple[str, InlineKeyboardMarkup]:
        """Format a single card message with control buttons (activate/edit/delete)."""
        # Format card information
        status = "🟢 Faol" if card['is_active'] else "⚪ Faol emas"
        card_info = (
            f"💳 Karta #{card['id']}\n\n"
            f"📝 Nomi: {card['name']}\n"
            f"💳 Raqami: `{card['number']}`\n"
            f"⚙️ Holati: {status}"
        )
        
        # Create inline keyboard with control buttons
        rows = []
        
        # Show "aktiv qilish" button only for inactive cards
        if not card['is_active']:
            rows.append([InlineKeyboardButton("🟢 Aktiv qilish", callback_data=f"activate_card_{card['id']}")])
        
        # Provide edit button always; include delete only if not active
        if card['is_active']:
            rows.append([
                InlineKeyboardButton("✏️ Tahrirlash", callback_data=f"edit_card_{card['id']}")
            ])
        else:
            rows.append([
                InlineKeyboardButton("✏️ Tahrirlash", callback_data=f"edit_card_{card['id']}") , InlineKeyboardButton("🗑 O'chirish", callback_data=f"delete_card_{card['id']}")
            ])
        
        keyboard = InlineKeyboardMarkup(rows)
        return card_info, keyboard

    async def send_cards_list(self, update, cards_data) -> None:
        """Send cards section header with management keyboard, then each card with control buttons."""
        # Send section management keyboard
        from telegram import ReplyKeyboardMarkup
        section_keyboard = ReplyKeyboardMarkup(
            [["➕ Karta qo'shish", "⬅️ Asosiy menyu"]],
            resize_keyboard=True
        )
        await update.message.reply_text("💳 Kartalar bo'limi:", reply_markup=section_keyboard)

        if not cards_data:
            await update.message.reply_text("💳 Hech qanday karta topilmadi.")
            return
        
        # Send header message
        await update.message.reply_text("💳 Barcha kartalar:")
        
        for card in cards_data:
            # Use the helper method for consistent formatting
            card_info, keyboard = self._format_card_message(card)
            
            # Send individual card message
            await update.message.reply_text(card_info, reply_markup=keyboard, parse_mode="Markdown")

    async def send_user_orders(self, update, user_data) -> None:
        """Send user information and all their orders as separate messages with images."""
        try:
            # First send user information
            await update.message.reply_text(user_data['user_info'])
            
            # Get user orders for the specific collection
            # Note: user_data should include collection_id from the code search
            if 'collection_id' in user_data:
                orders = self.db_service.get_user_orders_by_collection(user_data['user_id'], user_data['collection_id'])
            else:
                # Fallback to all orders if collection_id not available
                orders = self.db_service.get_user_orders(user_data['user_id'])
            
            if not orders:
                await update.message.reply_text("📦 Bu foydalanuvchining hech qanday buyurtmasi topilmadi.")
                return
            
            # Send header for orders
            await update.message.reply_text(f"📦 Buyurtmalar ({len(orders)} ta):")
            
            # Send each order as separate message with image(s)
            for i, order in enumerate(orders, 1):
                # Format collection status
                status_info = {
                    'open': {'emoji': '🟢', 'text': 'OCHIQ'},
                    'close': {'emoji': '🔴', 'text': 'YOPIQ'}, 
                    'finish': {'emoji': '✅', 'text': 'YAKUNLANGAN'}
                }
                
                status_data = status_info.get(order.get('collection_status', ''), {'emoji': '⚪', 'text': 'NOMA\'LUM'})
                
                # Get all files for this order
                file_paths = self.db_service.get_order_files(order['id'])
                
                order_text = (
                    f"📦 Buyurtma #{order['id']}\n"
                    f"🏷 Kolleksiya: #{order['collection_id']}\n"
                    f"📊 Kolleksiya holati: {status_data['emoji']} {status_data['text']}\n"
                    f"🔢 Seryasi: {order.get('amount', 'N/A')}\n"
                    f"📷 Fayllar soni: {len(file_paths)}\n"
                )
                
                if not file_paths:
                    # No files for this order
                    order_text += "\n❌ Fayllar topilmadi"
                    await update.message.reply_text(order_text)
                elif len(file_paths) == 1:
                    # Single file - use existing method
                    file_path = file_paths[0]
                    if os.path.exists(file_path):
                        try:
                            if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.ico', '.svg')):
                                await update.message.reply_photo(
                                    photo=file_path,
                                    caption=order_text
                                )
                            elif file_path.lower().endswith(('.mp4', '.webm', '.mov')):
                                await update.message.reply_video(
                                    video=file_path,
                                    caption=order_text
                                )
                            else:
                                await update.message.reply_photo(
                                    photo=file_path,
                                    caption=order_text
                                )
                        except Exception as e:
                            logger.error(f"Error sending file for order {order['id']}: {e}")
                            order_text += f"\n📄 Fayl: {file_path} (yuklashda xatolik)"
                            await update.message.reply_text(order_text)
                    else:
                        order_text += f"\n📄 Fayl: {file_path} (topilmadi)"
                        await update.message.reply_text(order_text)
                else:
                    # Multiple files - send as media group
                    media_group = []
                    valid_files = []
                    
                    for j, file_path in enumerate(file_paths):
                        if os.path.exists(file_path):
                            try:
                                # Add caption only to the first file
                                caption = order_text if j == 0 else None
                                if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.ico', '.svg')):
                                    media_group.append(InputMediaPhoto(media=file_path, caption=caption))
                                elif file_path.lower().endswith(('.mp4', '.webm', '.mov')):
                                    media_group.append(InputMediaVideo(media=file_path, caption=caption))
                                else:
                                    media_group.append(InputMediaPhoto(media=file_path, caption=caption))
                                valid_files.append(file_path)
                            except Exception as e:
                                logger.error(f"Error preparing file {file_path} for order {order['id']}: {e}")
                    
                    if media_group:
                        try:
                            await update.message.reply_media_group(media=media_group)
                        except Exception as e:
                            logger.error(f"Error sending media group for order {order['id']}: {e}")
                            # Fallback to text message with file info
                            order_text += f"\n📄 Fayllar: {', '.join(valid_files)} (yuklashda xatolik)"
                            await update.message.reply_text(order_text)
                    else:
                        # No valid files found
                        order_text += f"\n📄 Fayllar: {', '.join(file_paths)} (hech biri topilmadi)"
                        await update.message.reply_text(order_text)
                    
        except Exception as e:
            logger.error(f"Error sending user orders: {e}")
            await update.message.reply_text("❌ Buyurtmalarni ko'rsatishda xatolik yuz berdi.")

    async def process_admin_command(self, message: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> Tuple[Optional[str], Optional[Any]]:
        """Process a text message from an admin."""
        text = message.get('text', '').strip()

        if text == '🆕 Yangi kolleksiya':
            # Check if there are any collections with 'close' status
            if self.db_service.has_close_collections():
                close_collections = self.db_service.get_close_collections()
                close_collections_text = ", ".join([f"#{c['id']}" for c in close_collections])
                return (
                    "❌ Yangi kolleksiya yarata olmaysiz! Avval yopilgan kolleksiyalarni tugatish kerak.\n\n"
                    f"🔒 Yopilgan kolleksiyalar: {close_collections_text}",
                    None,
                )

            # Atomically close current and open a new collection
            new_collection_id = self.db_service.atomically_open_new_collection()
            if new_collection_id:
                # Notify all registered users and give them a new code
                registered_users = self.db_service.get_users_by_registration_step('done')
                sent_count = 0
                for user in registered_users:
                    try:
                        if not user.get('code'):
                            continue # Skip users without a static code

                        unique_code = f"{new_collection_id}-{user['code']}"

                        # Send message to user
                        message_text = (
                            "🎉 Yangi kolleksiya ochildi!\n\n"
                            f"🌟 Sizning yangi unikal kodingiz: `{unique_code}`"
                        )
                        sent_message = await context.bot.send_message(
                            chat_id=user['telegram_id'],
                            text=message_text,
                            parse_mode="Markdown"
                        )

                        # Pin the message
                        await context.bot.pin_chat_message(
                            chat_id=user['telegram_id'],
                            message_id=sent_message.message_id,
                            disable_notification=True
                        )
                        sent_count += 1
                    except Exception as e:
                        logger.error(f"Failed to send/pin code for user {user['telegram_id']}: {e}")

                return (
                    f"✅ Yangi kolleksiya #{new_collection_id} ochildi. Eskisi yopildi. 📧 {sent_count} ta foydalanuvchiga kod yuborildi.",
                    None,
                )
            else:
                return "❌ Xatolik: Yangi kolleksiya yaratib bo'lmadi.", None

        elif text == '📀 Aktiv kolleksiyani ko\'rish':
            active_collection = self.db_service.get_active_collection()
            if not active_collection:
                return "🚫 Hozirda ochiq kolleksiya mavjud emas.", None

            stats = self.db_service.get_collection_stats(active_collection['id'])

            # Format status with Uzbek text
            status_info = {
                'open': 'OCHIQ',
                'close': 'YOPIQ', 
                'finish': 'YAKUNLANGAN'
            }

            unknown_status = "NOMA'LUM"
            response_text = (
                f"📀 Aktiv Kolleksiya #{active_collection['id']}\n\n"
                f"📦 Mahsulotlar soni: {stats['item_count']}\n"
                f"👥 Foydalanuvchilar soni: {stats['user_count']}\n"
                f"🗺 Ochilgan vaqti: {active_collection['created_at']}\n"
                f"⚙️ Holati: {status_info.get(active_collection['status'], unknown_status)}"
            )

            # Stricter workflow: no manual close button shown
            keyboard = None

            return response_text, keyboard

        elif text == '📋 Oxirgi 10 ta kolleksiya':
            collections = self.db_service.get_last_collections(10)
            if not collections:
                return "📋 Hech qanday kolleksiya topilmadi.", None
            
            # This will be handled differently - we'll return a special format
            # that tells the main handler to send multiple messages
            return "SHOW_COLLECTIONS_LIST", collections

        elif text == '🔍 Mahsulotlarni qidirish':
            return "🔍 Qidirish uchun foydalanuvchining unikal kodini yuboring.", None
        
        elif text == '💳 Kartalar':
            # Enter cards section: show section keyboard and list cards
            cards = self.db_service.get_all_cards()
            return "SHOW_CARDS_LIST", cards

        elif text == '🔗 Get link':
            if not config.PRIVATE_CHANNEL_ID:
                return "❌ Maxfiy kanal ID sozlanmagan.", None
            try:
                link = await context.bot.create_chat_invite_link(
                    chat_id=config.PRIVATE_CHANNEL_ID,
                    member_limit=1,
                    name=f"Admin link for {message.get('from', {}).get('id')}"
                )
                return f"✅ Bir martalik taklifnoma havolasi:\n\n{link.invite_link}", None
            except Exception as e:
                logger.error(f"Error creating invite link for admin: {e}")
                return "❌ Havola yaratishda xatolik yuz berdi.", None

        elif text == "➕ Karta qo'shish":
            # Begin add card flow
            context.user_data['cards_flow'] = {'state': 'awaiting_new_card_name'}
            return "🆕 Yangi karta qo'shish\n\n📝 Karta nomini kiriting:", None

        elif text == "⬅️ Asosiy menyu":
            # Exit cards section and restore admin menu keyboard
            # Clear ongoing admin flows
            context.user_data.pop('cards_flow', None)
            context.user_data.pop('subscription_price_flow', None)
            admin_keyboard = [
                ['🆕 Yangi kolleksiya', '📀 Aktiv kolleksiyani ko\'rish'], 
                ['📋 Oxirgi 10 ta kolleksiya','🔍 Mahsulotlarni qidirish'],
                ['💳 Kartalar','💰 Obuna narxini o\'zgartirish'],
                ['🔗 Link olish']
            ]
            admin_markup = ReplyKeyboardMarkup(admin_keyboard, resize_keyboard=True)
            return "🏠 Asosiy menyu.", admin_markup

        elif text == "💰 Obuna narxini o\'zgartirish":
            # Start subscription price setting flow
            settings = config_db_service.get_all_settings()
            try:
                current_price = float(settings.get('subscription_price', 0))
            except Exception:
                current_price = 0.0
            context.user_data['subscription_price_flow'] = {
                'state': 'awaiting_new_amount',
                'current_price': current_price
            }
            price_formatted = f"{current_price:,.0f}".replace(',', ' ')
            back_keyboard = ReplyKeyboardMarkup([["⬅️ Asosiy menyu"]], resize_keyboard=True)
            return (f"🧾 Hozirgi obuna narxi: {price_formatted} UZS\n\n"
                    f"Yangi narxni kiriting (0 yoki kamida 1 000 UZS):"), back_keyboard
        
        
        else:
            # Handle cards add/edit flows
            cards_flow = context.user_data.get('cards_flow')
            if cards_flow:
                state = cards_flow.get('state')
                if state == 'awaiting_new_card_name':
                    name = text.strip()
                    if not name:
                        return "❌ Noto'g'ri nom. Iltimos, karta nomini kiriting:", None
                    context.user_data['cards_flow'] = {'state': 'awaiting_new_card_number', 'name': name}
                    return "💳 Karta raqamini kiriting (16 raqam):", None
                elif state == 'awaiting_new_card_number':
                    number = text.strip().replace(' ', '')
                    if not number.isdigit() or len(number) < 16:
                        return "❌ Noto'g'ri karta raqami. 16 ta raqam kiriting:", None
                    name = cards_flow.get('name')
                    new_id = self.db_service.add_card(name, number)
                    # Clear flow
                    context.user_data.pop('cards_flow', None)
                    if not new_id:
                        return "❌ Karta qo'shilmadi. Raqam allaqachon mavjud bo'lishi mumkin.", None
                    # Ask whether to activate if it's not first card (db auto-activates first)
                    cards = self.db_service.get_all_cards()
                    card = next((c for c in cards if c['id'] == new_id), None)
                    if card and not card['is_active']:
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🟢 Aktiv qilish", callback_data=f"activate_card_{new_id}")]])
                        return f"✅ Karta qo'shildi!\n\n📝 Nomi: {name}\n💳 Raqami: `{number}`", kb
                    return f"✅ Karta qo'shildi!\n\n📝 Nomi: {name}\n💳 Raqami: `{number}`", None
                elif state == 'awaiting_edit_card_name':
                    name = text.strip()
                    if not name:
                        return "❌ Noto'g'ri nom. Iltimos, karta nomini kiriting:", None
                    cards_flow['name'] = name
                    cards_flow['state'] = 'awaiting_edit_card_number'
                    return "💳 Yangi karta raqamini kiriting (16 raqam):", None
                elif state == 'awaiting_edit_card_number':
                    number = text.strip().replace(' ', '')
                    if not number.isdigit() or len(number) < 16:
                        return "❌ Noto'g'ri karta raqami. 16 ta raqam kiriting:", None
                    card_id = cards_flow.get('card_id')
                    name = cards_flow.get('name')
                    ok = self.db_service.update_card(card_id, name, number)
                    context.user_data.pop('cards_flow', None)
                    if ok:
                        return f"✅ Karta yangilandi!\n\n🆔 ID: {card_id}\n📝 Nomi: {name}\n💳 Raqami: `{number}`", None
                    else:
                        return "❌ Karta yangilashda xatolik. Ehtimol, bu raqam allaqachon mavjud.", None

                elif state == 'awaiting_edit_name_only':
                    name = text.strip()
                    if not name:
                        return "❌ Noto'g'ri nom. Iltimos, karta nomini kiriting:", None
                    card_id = cards_flow.get('card_id')
                    ok = self.db_service.update_card_name(card_id, name)
                    context.user_data.pop('cards_flow', None)
                    if ok:
                        # Get updated card info to display
                        cards = self.db_service.get_all_cards()
                        card = next((c for c in cards if c['id'] == card_id), None)
                        if card:
                            return f"✅ Karta nomi yangilandi!\n\n🆔 ID: {card_id}\n📝 Yangi nom: {name} \n💳 Raqam: `{card['number']}`", None
                        else:
                            return f"✅ Karta nomi yangilandi!\n\n🆔 ID: {card_id}\n📝 Yangi nom: {name}", None
                    else:
                        return "❌ Karta nomini yangilashda xatolik.", None

                elif state == 'awaiting_edit_number_only':
                    number = text.strip().replace(' ', '')
                    if not number.isdigit() or len(number) < 16:
                        return "❌ Noto'g'ri karta raqami. 16 ta raqam kiriting:", None
                    card_id = cards_flow.get('card_id')
                    ok = self.db_service.update_card_number(card_id, number)
                    context.user_data.pop('cards_flow', None)
                    if ok:
                        # Get updated card info to display
                        cards = self.db_service.get_all_cards()
                        card = next((c for c in cards if c['id'] == card_id), None)
                        if card:
                            return f"✅ Karta raqami yangilandi!\n\n🆔 ID: {card_id} \n📝 Nomi: {card['name']} \n💳 Yangi raqam: `{number}`", None
                        else:
                            return f"✅ Karta raqami yangilandi!\n\n🆔 ID: {card_id} \n💳 Yangi raqam: `{number}`", None
                    else:
                        return "❌ Karta raqamini yangilashda xatolik. Ehtimol, bu raqam allaqachon mavjud.", None

            # Handle subscription price flow
            price_flow = context.user_data.get('subscription_price_flow')
            if price_flow:
                text_input = text.replace(' ', '')
                if not text_input.isdigit():
                    back_keyboard = ReplyKeyboardMarkup([["⬅️ Asosiy menyu"]], resize_keyboard=True)
                    return "❌ Noto'g'ri qiymat. Butun son kiriting (UZS).", back_keyboard
                try:
                    new_price = float(text_input)
                except Exception:
                    back_keyboard = ReplyKeyboardMarkup([["⬅️ Asosiy menyu"]], resize_keyboard=True)
                    return "❌ Qiymatni o'qishda xatolik. Iltimos, qaytadan kiriting.", back_keyboard
                if new_price != 0 and new_price < 1000:
                    back_keyboard = ReplyKeyboardMarkup([["⬅️ Asosiy menyu"]], resize_keyboard=True)
                    return "❌ Obuna narxi 1 000 UZS dan kam bo'lmasligi kerak (yoki 0).", back_keyboard
                # Ask policy
                price_formatted = f"{new_price:,.0f}".replace(',', ' ')
                buttons = [
                    [InlineKeyboardButton("🆕 Faqat yangi foydalanuvchilar", callback_data=f"apply_price_new:{int(new_price)}")],
                    [InlineKeyboardButton("👥 Hamma to'lov qilmaganlarga", callback_data=f"apply_price_all:{int(new_price)}")],
                    [InlineKeyboardButton("❌ Bekor qilish", callback_data="apply_price_cancel")]
                ]
                return f"Yangi narx: {price_formatted} UZS\n\nQaysi foydalanuvchilarga qo'llaymiz?", InlineKeyboardMarkup(buttons)

            # Check if the text might be a user code for search
            if '-' in text and len(text.split('-')) == 2:
                try:
                    collection_id_str, user_code = text.upper().split('-')
                    collection_id = int(collection_id_str)
                except ValueError:
                    return f"❌ Noto'g'ri kod formati: `{text.upper()}`. Format: `KolleksiyaID-FoydalanuvchiKodi`", None

                user = self.db_service.get_user_by_code(user_code)
                if user:
                    
                    if user:
                        # Get collection-specific order count
                        collection_orders = self.db_service.get_user_orders_by_collection(user['id'], collection_id)
                        collection_orders_count = len(collection_orders)
                        
                        # Get total order count for reference
                        total_orders_count = self.db_service.get_user_order_count(user['telegram_id'])
                        
                        active_status = "Ha" if user.get('is_active') else "Yo'q"
                        response_text = (
                            f"👤 Kod bo'yicha ma'lumotlar: `{text.upper()}`\n\n"
                            f"📛 Ism: {user.get('name', 'N/A')}\n"
                            f"📛 Familiya: {user.get('surname', 'N/A')}\n"
                            f"📞 Telefon: {user.get('phone', 'N/A')}\n"
                            f"📧 Username: @{user.get('username', 'N/A')}\n"
                            f"🆔 Telegram ID: {user['telegram_id']}\n"
                            f"✅ Faol: {active_status}\n"
                            f"🏷 Kolleksiya: #{collection_id}\n"
                            f"📦 Bu kolleksiyada: {collection_orders_count} ta buyurtma\n"
                            f"📊 Jami buyurtmalar: {total_orders_count} ta"
                        )
                        
                        # Return a special format to indicate we need to send orders separately
                        return "SHOW_USER_ORDERS", {'user_info': response_text, 'user_id': user['id'], 'collection_id': collection_id}
                    else:
                        return "❌ Foydalanuvchi ma'lumotlarini topib bo'lmadi.", None
                else:
                    return "❌ Ushbu kod bilan foydalanuvchi topilmadi.", None
            
            # Default response for admin if no button is matched
            return "❓ Noma'lum buyruq. Iltimos, quyidagi tugmalardan foydalaning yoki unikal kodni yuboring.", None

    async def process_callback_query(self, query, context: ContextTypes.DEFAULT_TYPE = None) -> Optional[Dict[str, Any]]:
        """Processes an inline keyboard button press."""
        callback_data = query.data

        if callback_data.startswith("select_region:"):
            try:
                region = callback_data.split(":", 1)[1]
                telegram_id = query.from_user.id

                self.db_service.update_user_info(telegram_id, 'region', region)
                self.db_service.update_user_registration_step(telegram_id, 'done')

                # Activate the user and generate a unique code
                self.db_service.update_user_active_status(telegram_id, True)
                unique_code = self._generate_unique_code()
                # Ensure code is unique
                while self.db_service.get_user_by_code(unique_code):
                    unique_code = self._generate_unique_code()
                
                self.db_service.update_user_info(telegram_id, 'code', unique_code)

                # Check if user is already in the private channel
                if self.bot_app and config.PRIVATE_CHANNEL_ID:
                    try:
                        chat_member = await self.bot_app.bot.get_chat_member(
                            chat_id=config.PRIVATE_CHANNEL_ID,
                            user_id=telegram_id
                        )
                        if chat_member.status in ['creator', 'administrator', 'member']:
                            self.db_service.update_user_subscription_status(telegram_id, 'active')
                            logger.info(f"User {telegram_id} is already in the channel. Activating subscription.")
                            return {"text": (f"✅ Tabriklaymiz, ro'yxatdan muvaffaqiyatli o'tdingiz!\n\n"
                                    f"🔑 Sizning shaxsiy kodingiz: `{unique_code}`\n\n"
                                    f"Siz allaqachon maxfiy kanal a'zosisiz. Botdan foydalanishni boshlashingiz mumkin!\n\n"
                                    f"📷 Buyurtma berish uchun mahsulotni yuboring."), "keyboard": None, "parse_mode": "Markdown"}
                    except Exception as e:
                        logger.error(f"Could not check channel membership for user {telegram_id}: {e}")
                        # Fallback to normal payment flow if check fails

                self.db_service.update_user_payment_step(telegram_id, 'pending')
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Kanalga qo'shilish", callback_data="join_channel")]
                ])
                
                response_text = (f"✅ Tabriklaymiz, ro'yxatdan muvaffaqiyatli o'tdingiz!\n\n"
                                 f"🔑 Sizning shaxsiy kodingiz: `{unique_code}`\n\n"
                                 f"Botdan foydalanish uchun, iltimos, maxfiy kanalga qo'shiling.")
                return {"text": response_text, "keyboard": keyboard, "parse_mode": "Markdown"}
            except Exception as e:
                logger.error(f"Error processing region selection: {e}")
                return {"text": "❌ Viloyatni saqlashda xatolik yuz berdi.", "keyboard": None}

        # No add_card_type callbacks anymore

        # Handle order cancellation
        if callback_data.startswith("cancel_order_"):
            try:
                order_id_str = callback_data.replace("cancel_order_", "")
                order_id = int(order_id_str)
                user_id = query.from_user.id

                logger.info(f"CANCEL_ORDER: START - Order ID {order_id}, User ID {user_id}")

                # Get order details to verify ownership
                order = self.db_service.get_order_by_id(order_id)
                if not order:
                    logger.warning(f"CANCEL_ORDER: Order {order_id} not found for user {user_id}")
                    return {"text": "❌ Buyurtma topilmadi.", "keyboard": None}

                # Check if the user owns this order
                user = self.db_service.get_user_by_telegram_id(user_id)
                if not user or user['id'] != order['user_id']:
                    logger.warning(f"CANCEL_ORDER: Order {order_id} ownership mismatch for user {user_id}")
                    return {"text": "❌ Bu buyurtma sizga tegishli emas.", "keyboard": None}

                # Cancel the order (delete it)
                if self.db_service.delete_order(order_id, user['id']):
                    # Clean up file collection state if necessary
                    if context:
                        # Cancel any pending finalization timer
                        job_name = context.user_data.get('finalization_job_name')
                        if job_name and context.job_queue:
                            existing_jobs = context.job_queue.get_jobs_by_name(job_name)
                            for job in existing_jobs:
                                job.schedule_removal()
                                logger.debug(f"Cancelled finalization job {job_name} during order cancellation")

                        # Cancel any asyncio finalization task
                        finalization_task = context.user_data.get('finalization_task')
                        if finalization_task and not finalization_task.done():
                            finalization_task.cancel()
                            logger.debug(f"Cancelled finalization task during order cancellation")

                        # Clear user state
                        context.user_data.clear()

                    logger.info(f"CANCEL_ORDER: SUCCESS - Order {order_id} cancelled by user {user_id}")
                    return {"text": "❌ Buyurtma bekor qilindi.", "keyboard": None}
                else:
                    logger.error(f"CANCEL_ORDER: FAILED - Could not delete order {order_id} for user {user_id}")
                    return {"text": "❌ Buyurtmani bekor qilishda xatolik.", "keyboard": None}
                    
            except (ValueError, Exception) as e:
                logger.error(f"Error cancelling order: {e}")
                return {"text": "❌ Buyurtmani bekor qilishda xatolik.", "keyboard": None}

        if callback_data == "join_channel":
            user_id = query.from_user.id
            
            # Test account bypass for user config.TEST_ACCOUNT_ID
            if config.TEST_ACCOUNT_ID and user_id == config.TEST_ACCOUNT_ID:
                try:
                    # Create invite link directly for test account
                    link = await context.bot.create_chat_invite_link(
                        chat_id=config.PRIVATE_CHANNEL_ID,
                        member_limit=1,
                        name=f'Test User {user_id}'
                    )
                    self.db_service.update_user_payment_step(user_id, 'confirmed')
                    text = f"✅ Test akkaunti uchun kanal havolasi:\n\n{link.invite_link}"
                    return {"text": text, "keyboard": None}
                except Exception as e:
                    logger.error(f"Error creating invite link for test account {user_id}: {e}")
                    text = "❌ Kanal havolasini yaratishda xatolik yuz berdi."
                    return {"text": text, "keyboard": None}
            
            # Set user to awaiting receipt and initialize target/paid amounts
            self.db_service.update_user_payment_step(user_id, 'awaiting_receipt')

            # Initialize subscription target and reset paid amount
            try:
                settings = config_db_service.get_all_settings()
                price_val = float(settings.get('subscription_price', 0))
            except Exception:
                price_val = 0.0
            try:
                self.db_service.update_user_subscription_amounts(
                    telegram_id=user_id,
                    target_amount=price_val,
                    paid_amount=0.0
                )
            except Exception:
                pass

            # Get subscription price from settings
            try:
                settings = config_db_service.get_all_settings()
                price_val = float(settings.get('subscription_price', 0))
            except Exception:
                price_val = 0.0
            price_formatted = f"{price_val:,.0f}".replace(',', ' ')

            # Get active card from database and format number with spaces
            active_card = self.db_service.get_active_card()
            if active_card and active_card.get('number'):
                raw_num = active_card['number']
                grouped = ' '.join([raw_num[i:i+4] for i in range(0, len(raw_num), 4)])
                owner_line = f"👤 Karta {active_card.get('name','')} nomida\n" if active_card.get('name') else ""
                card_block = f"{owner_line}💳 {grouped}"
            else:
                card_block = "Karta ma'lumotlari topilmadi. Admin bilan bog'laning."

            text = (f"💳 Maxfiy kanalga qo'shilish uchun, iltimos, ushbu kartaga {price_formatted} to'lov qiling:\n\n"
                    f"{card_block}\n\n"
                    "📸 To'lovdan so'ng, to'lov kvitansiyasining rasmini yuboring.")
            return {"text": text, "keyboard": None}

        elif callback_data.startswith("apply_price_"):
            # Handle policy selection after entering new price
            try:
                if callback_data == 'apply_price_cancel':
                    await query.answer("Bekor qilindi")
                    admin_keyboard = [
                        ['🆕 Yangi kolleksiya', '📀 Aktiv kolleksiya ni ko\'rish'], 
                        ['📋 Oxirgi 10 ta kolleksiya','🔍 Mahsulotlarni qidirish'],
                        ['💳 Kartalar','💰 Obuna narxini o\'zgartirish'],
                        ['🔗 Link olish']
                    ]
                    return {"text": "Bekor qilindi.", "keyboard": ReplyKeyboardMarkup(admin_keyboard, resize_keyboard=True)}

                policy, amount_str = callback_data.split(':', 1)
                new_price = float(amount_str)
                # Persist price
                config_db_service.update_settings({'subscription_price': str(new_price)}, 'bot-admin')
                try:
                    config.load_from_db()
                except Exception:
                    pass

                msg = f"✅ Obuna narxi yangilandi: {int(new_price):,} UZS".replace(',', ' ')
                if policy == 'apply_price_new':
                    msg += "\n\n✔️ Faqat yangi foydalanuvchilar uchun qo'llanadi."

                admin_keyboard = [
                    ['🆕 Yangi kolleksiya', '📀 Aktiv kolleksiya ni ko\'rish'], 
                    ['📋 Oxirgi 10 ta kolleksiya','🔍 Mahsulotlarni qidirish'],
                    ['💳 Kartalar','💰 Obuna narxini o\'zgartirish'],
                    ['🔗 Link olish']
                ]
                await query.answer("Saqlandi")
                return {"text": msg, "keyboard": ReplyKeyboardMarkup(admin_keyboard, resize_keyboard=True)}
            except Exception as e:
                await query.answer("Xatolik")
                return {"text": f"❌ Xatolik: {e}", "keyboard": None}

        elif callback_data.startswith("confirm_payment_"):
            user_telegram_id = int(callback_data.split("_")[-1])
            
            user = self.db_service.get_user_by_telegram_id(user_telegram_id)
            if not user:
                await query.answer("Foydalanuvchi topilmadi.", show_alert=True)
                return {"text": f"❌ Xatolik: Foydalanuvchi (ID: {user_telegram_id}) topilmadi.", "keyboard": None}

            payment_to_process = self.db_service.get_latest_payment_for_user(user_id=user['id'])
            receipt_path_to_delete = payment_to_process.get('receipt_url') if payment_to_process else None

            # Defer receipt deletion to the end of the function
            async def delete_receipt():
                if receipt_path_to_delete and os.path.exists(receipt_path_to_delete):
                    try:
                        os.remove(receipt_path_to_delete)
                        logger.info(f"Deleted receipt file: {receipt_path_to_delete}")
                    except Exception as e:
                        logger.error(f"Failed to delete receipt file {receipt_path_to_delete}: {e}")

            # Read target price from settings (fallback to user's target_amount or 0)
            try:
                settings = config_db_service.get_all_settings()
                target_amount = float(settings.get('subscription_price', user.get('target_amount') or 0) or 0)
            except Exception:
                target_amount = float(user.get('target_amount') or 0)

            payment = payment_to_process

            # If we have a detectable amount, apply it; otherwise grant access directly
            try:
                if payment and float(payment.get('amount') or 0) > 0:
                    # Mark payment as verified and add to user's paid amount
                    self.db_service.update_payment_status(payment['id'], 'verified')

                    current_paid = float(user.get('paid_amount') or 0)
                    new_paid = current_paid + float(payment.get('amount') or 0)
                    self.db_service.update_user_subscription_amounts(
                        telegram_id=user_telegram_id,
                        paid_amount=new_paid
                    )

                    remaining = max(0.0, target_amount - new_paid)

                    if remaining <= 0.001:
                        # Fully paid: confirm and invite
                        self.db_service.update_user_payment_step(user_telegram_id, 'confirmed')
                        link = await context.bot.create_chat_invite_link(
                            chat_id=config.PRIVATE_CHANNEL_ID,
                            member_limit=1,
                            name=user.get('name', f'User {user_telegram_id}')
                        )
                        await context.bot.send_message(
                            chat_id=user_telegram_id,
                            text=(
                                "✅ To'lovingiz qabul qilindi va tasdiqlandi!\n\n"
                                f"Jami to'langan: {int(new_paid):,} UZS\n\n".replace(',', ' ')
                                + f"Kanalga qo'shilish havolasi:\n{link.invite_link}"
                            )
                        )
                        await query.answer("✅ Tasdiqlandi")
                        await delete_receipt()
                        return {"text": f"✅ QABUL QILINDI VA TASDIQLANDI\nFoydalanuvchi: {user.get('name', '')} ({user_telegram_id})", "keyboard": None}
                    else:
                        # Partial: notify remaining amount
                        remaining_str = f"{int(round(remaining)):,}".replace(',', ' ')
                        amount_str = f"{int(round(float(payment.get('amount') or 0))):,}".replace(',', ' ')
                        new_paid_str = f"{int(round(new_paid)):,}".replace(',', ' ')
                        target_str = f"{int(round(target_amount)):,}".replace(',', ' ')
                        self.db_service.update_user_payment_step(user_telegram_id, 'awaiting_receipt')
                        # Include active card details for remaining payment
                        active_card2 = self.db_service.get_active_card()
                        if active_card2 and active_card2.get('number'):
                            raw2 = active_card2['number']
                            grouped2 = ' '.join([raw2[i:i+4] for i in range(0, len(raw2), 4)])
                            owner2 = f"👤 Karta {active_card2.get('name','')} nomida\n" if active_card2.get('name') else ""
                            card_info2 = f"{owner2}💳 {grouped2}\n\n"
                        else:
                            card_info2 = ""

                        await context.bot.send_message(
                            chat_id=user_telegram_id,
                            text=(
                                f"✅ {amount_str} UZS qabul qilindi!\n\n"
                                f"Jami to'langan: {new_paid_str} UZS\n"
                                f"Qo'shilish narxi: {target_str} UZS\n\n"
                                f"💡 Qolgan summa: {remaining_str} UZS. Iltimos, quyidagi kartaga to'lovni amalga oshiring:\n\n"
                                f"{card_info2}"
                                "📸 To'lovdan so'ng, kvitansiya rasmini yuboring."
                            )
                        )
                        await query.answer("✅ Qabul qilindi")
                        return {"text": (
                            f"✅ Qisman to'lov qabul qilindi. Qolgan: {remaining_str} UZS\n\n"
                            f"👤 {user.get('name', '')} ({user_telegram_id})"
                        ), "keyboard": None}
                else:
                    # No detectable amount, but admin confirmed manually.
                    # Assume full payment is made and activate the user.
                    self.db_service.update_user_subscription_amounts(
                        telegram_id=user_telegram_id,
                        paid_amount=target_amount,  # Mark as fully paid
                        target_amount=target_amount # Also set target amount for clarity
                    )
                    self.db_service.update_user_payment_step(user_telegram_id, 'confirmed') # set status to active

                    # Create invite link
                    link = await context.bot.create_chat_invite_link(
                        chat_id=config.PRIVATE_CHANNEL_ID,
                        member_limit=1,
                        name=user.get('name', f'User {user_telegram_id}')
                    )

                    # Send success message to user
                    target_amount_str = f"{int(round(target_amount)):,}".replace(',', ' ')
                    await context.bot.send_message(
                        chat_id=user_telegram_id,
                        text=(
                            "✅ To'lovingiz admin tomonidan tasdiqlandi!\n\n"
                            f"To'langan summa: {target_amount_str} UZS\n\n"
                            f"Kanalga qo'shilish havolasi:\n{link.invite_link}"
                        )
                    )
                    await query.answer("✅ Tasdiqlandi")
                    await delete_receipt()
                    return {"text": f"✅ QO'LDA TASDIQLANDI\nFoydalanuvchi: {user.get('name', '')} ({user_telegram_id})", "keyboard": None}

            except Exception as e:
                logger.error(f"Error finalizing admin confirmation for {user_telegram_id}: {e}")
                await query.answer("Xatolik yuz berdi.", show_alert=True)
                await delete_receipt() # Attempt deletion even on error
                return {"text": f"❌ Xatolik: {e}", "keyboard": None}

        elif callback_data.startswith("cancel_payment_"):
            user_telegram_id = int(callback_data.split("_")[-1])

            user = self.db_service.get_user_by_telegram_id(user_telegram_id)
            if not user:
                await query.answer("Foydalanuvchi topilmadi.", show_alert=True)
                return {"text": f"❌ Xatolik: Foydalanuvchi (ID: {user_telegram_id}) topilmadi.", "keyboard": None}

            payment_to_process = self.db_service.get_latest_payment_for_user(user_id=user['id'])

            # Check if payment is already processed
            current_payment_status = user.get('payment_step')
            if current_payment_status == 'confirmed':
                await query.answer("❌ Tasdiqlangan to'lovni bekor qilib bo'lmaydi!", show_alert=True)
                return {"text": f"❌ TASDIQLANGAN TO'LOVNI BEKOR QILIB BO'LMAYDI\nFoydalanuvchi: {user.get('name', '')} ({user_telegram_id})", "keyboard": None}
            
            if current_payment_status == 'rejected':
                await query.answer("❌ Bu to'lov allaqachon bekor qilingan!", show_alert=True)
                return {"text": f"❌ TO'LOV ALLAQACHON BEKOR QILINGAN\nFoydalanuvchi: {user.get('name', '')} ({user_telegram_id})", "keyboard": None}
            
            if current_payment_status not in ['pending', 'awaiting_receipt']:
                await query.answer("❌ Noto'g'ri to'lov holati!", show_alert=True)
                return {"text": f"❌ NOTO'G'RI TO'LOV HOLATI: {current_payment_status}\nFoydalanuvchi: {user.get('name', '')} ({user_telegram_id})", "keyboard": None}

            try:
                await context.bot.send_message(
                    chat_id=user_telegram_id,
                    text="❌ Sizning to'lovingiz tasdiqlanmadi."
                )
                
                # Update payment status to failed in payment table
                if payment_to_process:
                    self.db_service.update_payment_status(payment_to_process['id'], 'failed')
                
                # Delete receipt
                if payment_to_process and payment_to_process.get('receipt_url'):
                    receipt_path = payment_to_process.get('receipt_url')
                    if os.path.exists(receipt_path):
                        os.remove(receipt_path)
                        logger.info(f"Deleted receipt file for cancelled payment: {receipt_path}")
                
                await query.answer("❌ To'lov bekor qilindi.")
                return {"text": f"❌ TO'LOV BEKOR QILINDI\nFoydalanuvchi: {user.get('name', '')} ({user_telegram_id})", "keyboard": None}
            except Exception as e:
                logger.error(f"Error cancelling payment for {user_telegram_id}: {e}")
                if payment_to_process and payment_to_process.get('receipt_url') and os.path.exists(payment_to_process.get('receipt_url')):
                    os.remove(payment_to_process.get('receipt_url')) # Attempt deletion on error too
                await query.answer("Xatolik yuz berdi.", show_alert=True)
                return {"text": f"❌ Xatolik: {e}", "keyboard": None}

        # Handle card management callbacks
        elif callback_data.startswith("show_card_"):
            card_id = int(callback_data.split("_")[-1])
            cards = self.db_service.get_all_cards()
            card = next((c for c in cards if c['id'] == card_id), None)
            if card:
                text, keyboard = self._format_card_message(card)
                return {"text": text, "keyboard": keyboard, "parse_mode": "Markdown"}
            else:
                return {"text": "❌ Karta topilmadi.", "keyboard": None}

        elif callback_data.startswith("edit_card_name_"):
            card_id = int(callback_data.split("_")[-1])
            if context:
                context.user_data['cards_flow'] = {'state': 'awaiting_edit_name_only', 'card_id': card_id}
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_card_edit")]])
            return {"text": "📛 Yangi kartaning nomini kiriting:", "keyboard": kb}

        elif callback_data.startswith("edit_card_number_"):
            card_id = int(callback_data.split("_")[-1])
            if context:
                context.user_data['cards_flow'] = {'state': 'awaiting_edit_number_only', 'card_id': card_id}
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel_card_edit")]])
            return {"text": "💳 Yangi kartaning raqamini kiriting (16 raqam):", "keyboard": kb}

        elif callback_data.startswith("activate_card_"):
            card_id = int(callback_data.split("_")[-1])
            
            # Set the card as active
            success = self.db_service.set_active_card(card_id)
            
            if success:
                # Get card info for confirmation
                cards = self.db_service.get_all_cards()
                active_card = next((c for c in cards if c['id'] == card_id), None)
                
                if active_card:
                    # Notify users of card change if bot context is available
                    if context and hasattr(context, 'bot'):
                        try:
                            await self.notify_users_of_card_change(context)
                        except Exception as e:
                            logger.error(f"Error notifying users of card change: {e}")
                    
                    return {"text": f"✅ Karta faollashtirildi!\n\n📝 Nomi: {active_card['name']}\n💳 Raqami: `{active_card['number']}`", "keyboard": None}
                else:
                    return {"text": "❌ Xatolik: Karta ma'lumotlarini topib bo'lmadi.", "keyboard": None}
            else:
                return {"text": "❌ Xatolik: Kartani faollashtrishda muammo yuz berdi.", "keyboard": None}
        
        elif callback_data.startswith("delete_card_"):
            card_id = int(callback_data.split("_")[-1])
            # Ask for confirmation
            cards = self.db_service.get_all_cards()
            card = next((c for c in cards if c['id'] == card_id), None)
            if not card:
                return {"text": "❌ Xatolik: Karta topilmadi.", "keyboard": None}
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Ha, o'chirilsin", callback_data=f"confirm_delete_card_{card_id}")],
                [InlineKeyboardButton("❌ Yo'q, bekor qilish", callback_data=f"cancel_delete_card_{card_id}")]
            ])
            return {"text": f"❓ Kartani o'chirishga ishonchingiz komilmi?\n\n🆔 ID: {card['id']}\n📝 Nomi: {card['name']}\n💳 Raqami: `{card['number']}`", "keyboard": kb}

        elif callback_data.startswith("confirm_delete_card_"):
            card_id = int(callback_data.split("_")[-1])
            cards = self.db_service.get_all_cards()
            if len(cards) <= 1:
                return {"text": "❌ Oxirgi kartani o'chirib bo'lmaydi.", "keyboard": None}
            # Prevent deleting currently active card
            active = self.db_service.get_active_card()
            if active and active.get('id') == card_id:
                return {"text": "❌ Faol kartani o'chirib bo'lmaydi. Avval boshqa kartani faollashtiring, so'ng eski kartani o'chiring.", "keyboard": None}
            card_to_delete = next((c for c in cards if c['id'] == card_id), None)
            if not card_to_delete:
                return {"text": "❌ Xatolik: Karta topilmadi.", "keyboard": None}
            success = self.db_service.delete_card(card_id)
            if success:
                if card_to_delete['is_active']:
                    return {"text": f"✅ Faol karta o'chirildi va boshqa karta avtomatik faollashtirildi!\n\n📝 O'chirilgan: {card_to_delete['name']}\n💳 Raqami: `{card_to_delete['number']}`", "keyboard": None}
                return {"text": f"✅ Karta o'chirildi!\n\n📝 Nomi: {card_to_delete['name']}\n💳 Raqami: `{card_to_delete['number']}`", "keyboard": None}
            return {"text": "❌ Xatolik: Kartani o'chirishda muammo yuz berdi.", "keyboard": None}

        elif callback_data.startswith("cancel_delete_card_"):
            card_id = int(callback_data.split("_")[-1])
            cards = self.db_service.get_all_cards()
            card = next((c for c in cards if c['id'] == card_id), None)
            if card:
                text, keyboard = self._format_card_message(card)
                return {"text": text, "keyboard": keyboard}
            return {"text": "❌ Karta topilmadi.", "keyboard": None}

        elif callback_data.startswith("edit_card_"):
            card_id = int(callback_data.split("_")[-1])
            cards = self.db_service.get_all_cards()
            card = next((c for c in cards if c['id'] == card_id), None)
            if not card:
                return {"text": "❌ Xatolik: Karta topilmadi.", "keyboard": None}
            # Offer choice: edit name or number
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📛 Nomni o'zgartirish", callback_data=f"edit_card_name_{card_id}")],
                [InlineKeyboardButton("💳 Raqamni o'zgartirish", callback_data=f"edit_card_number_{card_id}")],
                [InlineKeyboardButton("⬅️ Orqaga", callback_data=f"show_card_{card_id}")]
            ])
            return {"text": f"✏️ Tahrirlash (ID: {card['id']})\n\nHozirgi nom: {card['name']}\nHozirgi raqam: `{card['number']}`\n\nQaysi qismni tahrirlaysiz?", "keyboard": kb, "parse_mode": "Markdown"}

        elif callback_data == "cancel_card_edit":
            if context and 'cards_flow' in context.user_data:
                context.user_data.pop('cards_flow', None)
            return {"text": "❌ Karta tahrirlash bekor qilindi.", "keyboard": None}

        elif callback_data.startswith("noop_activate_"):
            card_id = int(callback_data.split("_")[-1])
            # Just edit the message to confirm the card was added but not activated.
            cards = self.db_service.get_all_cards()
            card = next((c for c in cards if c['id'] == card_id), None)
            if card:
                text = f"✅ Karta qo'shildi, lekin faollashtirilmadi.\n\n📝 Nomi: {card['name']}\n💳 Raqami: `{card['number']}`"
                return {"text": text, "keyboard": None}
            else:
                return {"text": "✅ Karta qo'shildi, lekin faollashtirilmadi.", "keyboard": None}

        # Handle edit profile callbacks
        if callback_data.startswith("edit_"):
            if context:
                return self.process_edit_callback(query, context)
            else:
                return {"text": "❌ Xatolik: Kontekst mavjud emas.", "keyboard": None}

        # Handle new status change buttons
        if callback_data.startswith("status_"):
            parts = callback_data.split("_")
            if len(parts) == 3:
                action = parts[1]  # open, close, finish
                collection_id = int(parts[2])
                
                # Get current collection info
                collections = self.db_service.get_last_collections(50)  # Get more to find this one
                target_collection = next((c for c in collections if c['id'] == collection_id), None)
                
                if not target_collection:
                    return {"text": "❌ Xatolik: Kolleksiya topilmadi.", "keyboard": None}
                
                current_status = target_collection['status']
                
                # Special logic for opening a collection
                if action == 'open':
                    # Check if we're reopening a closed collection
                    if current_status == 'close':
                        # Get active collection to merge with
                        active_collection = self.db_service.get_active_collection()
                        if active_collection:
                            # Merge the active collection into the collection being reopened
                            merge_success = self.db_service.merge_collections(
                                from_collection_id=active_collection['id'], 
                                to_collection_id=collection_id
                            )
                            
                            if merge_success:
                                # Delete the old active collection after successful merge
                                self.db_service.delete_collection(active_collection['id'])
                                logger.info(f"Successfully merged collection #{active_collection['id']} into #{collection_id}")
                                # User codes are static, no need to merge them.
                            else:
                                return {"text": "❌ Xatolik: Kolleksiyalarni birlashtirish amalga oshmadi.", "keyboard": None}
                    
                    elif current_status == 'finish':
                        # Prevent reopening finished collections
                        return {"text": f"❌ Yakunlangan kolleksiyalarni qayta ochib bo'lmaydi! Kolleksiya #{collection_id} yakunlangan.", "keyboard": None}
                    
                    else:
                        # For other cases, check if there's already an active collection
                        active_collection = self.db_service.get_active_collection()
                        if active_collection and active_collection['id'] != collection_id:
                            return {"text": f"❌ Boshqa kolleksiya allaqachon ochiq! Kolleksiya #{active_collection['id']} ni avval yoping.", "keyboard": None}
                
                # Special logic for closing a collection
                elif action == 'close':
                    # Check if there are any collections with 'close' status
                    if self.db_service.has_close_collections():
                        close_collections = self.db_service.get_close_collections()
                        close_collections_text = ", ".join([f"#{c['id']}" for c in close_collections])
                        return {"text": f"❌ Kolleksiyani yopa olmaysiz! Avval yopilgan kolleksiyalarni tugatish kerak.\n\n🔒 Yopilgan kolleksiyalar: {close_collections_text}", "keyboard": None}
                
                # Update the collection status
                success = self.db_service.update_collection_status(collection_id, action)
                
                # If closing a collection, we might need to open a new one
                if success and action == 'close':
                    # Check if there are other 'close' status collections
                    if not self.db_service.has_close_collections():
                        new_collection_id = self.db_service.create_collection('open')
                        if new_collection_id:
                            # Notify users about the new collection
                            registered_users = self.db_service.get_users_by_registration_step('done')
                            sent_count = 0
                            for user in registered_users:
                                if not user.get('code'):
                                    continue
                                try:
                                    unique_code = f"{new_collection_id}-{user['code']}"
                                    message_text = f"🎉 Yangi kolleksiya ochildi!\n\n🌟 Sizning yangi unikal kodingiz: `{unique_code}`"
                                    await query.bot.send_message(chat_id=user['telegram_id'], text=message_text, parse_mode="Markdown")
                                    sent_count += 1
                                except Exception as e:
                                    logger.error(f"Failed to send new collection code to user {user['telegram_id']}: {e}")
                            logger.info(f"Sent new collection #{new_collection_id} codes to {sent_count} users.")
                        else:
                            logger.error("Failed to create a new collection after closing one.")

                if success:
                    # If opening a collection, notify users
                    if action == 'open':
                        try:
                            registered_users = self.db_service.get_users_by_registration_step('done')
                            sent_count = 0
                            for user in registered_users:
                                if not user.get('code'):
                                    continue
                                try:
                                    unique_code = f"{collection_id}-{user['code']}"
                                    # Send message to user
                                    message_text = f"🎉 Kolleksiya #{collection_id} qayta ochildi!\n\n🌟 Sizning yangi unikal kodingiz: `{unique_code}`"
                                    try:
                                        sent_message = await query.bot.send_message(
                                            chat_id=user['telegram_id'],
                                            text=message_text,
                                            parse_mode="Markdown"
                                        )
                                        # Pin the message
                                        await query.bot.pin_chat_message(
                                            chat_id=user['telegram_id'],
                                            message_id=sent_message.message_id,
                                            disable_notification=True
                                        )
                                    except Exception as e:
                                        logger.warning(f"Could not send/pin message to user {user['telegram_id']}: {e}")

                                    sent_count += 1
                                except Exception as e:
                                    logger.error(f"Failed to send/pin code for user {user['telegram_id']}: {e}")
                        except Exception as e:
                            logger.error(f"Error notifying users about reopened collection: {e}")
                    
                    # Get updated collection info and format the message
                    updated_collections = self.db_service.get_last_collections(50)
                    updated_collection = next((c for c in updated_collections if c['id'] == collection_id), None)
                    
                    if updated_collection:
                        text, keyboard = self._format_collection_message(updated_collection)
                        return {"text": text, "keyboard": keyboard}
                    else:
                        return {"text": f"❌ Xatolik: Yangilangan kolleksiya ma'lumotlarini olib bo'lmadi.", "keyboard": None}
                else:
                    return {"text": f"❌ Xatolik: Kolleksiya #{collection_id} holatini o'zgartirishda xatolik yuz berdi.", "keyboard": None}

        # Manual close removed: instruct admin to use New Collection rotation
        elif callback_data.startswith("close_collection_"):
            return {"text": "❌ Ushbu amal endi qo'llab-quvvatlanmaydi. Yangi kolleksiya yaratish orqali yopiladi.", "keyboard": None}

        # Handle view_orders callback
        elif callback_data.startswith("view_orders:"):
            try:
                _, params = callback_data.split(":", 1)
                user_id, collection_scope = params.split(":", 1)
                user_id = int(user_id)

                # Get user data for context
                user = self.db_service.get_user_by_id(user_id)
                if not user:
                    return {"text": "❌ Foydalanuvchi topilmadi.", "keyboard": None}

                # Edit the original message to show confirmation
                try:
                    await query.edit_message_text(
                        text=query.message.text + "\n\n✅ Buyurtmalar yuborilmoqda...",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass  # Ignore if editing fails

                # Fetch orders based on scope
                if collection_scope == "all":
                    # Get all orders for user
                    orders = self.db_service.get_user_orders_with_files(user_id, limit=50)
                    scope_text = "Barcha kolleksiyalar"
                else:
                    # Get orders for specific collection
                    collection_id = int(collection_scope)
                    orders = self.db_service.get_user_orders_by_collection(user_id, collection_id, limit=50)
                    scope_text = f"Kolleksiya #{collection_id}"

                if not orders:
                    await query.message.reply_text(f"❌ {scope_text} uchun buyurtmalar topilmadi.")
                    return None

                # Send orders. If admin is viewing their own orders, try DM first with fallback to the current chat
                original_chat_id = query.message.chat_id
                target_chat_id = original_chat_id

                try:
                    if query.from_user and int(query.from_user.id) == int(user.get('telegram_id')):
                        # Attempt to send to the admin's private chat
                        target_chat_id = int(query.from_user.id)
                        try:
                            await self._send_orders_to_chat(target_chat_id, user, orders, scope_text, context)
                            # Optionally inform in group that orders were sent privately
                            if target_chat_id != original_chat_id:
                                try:
                                    await context.bot.send_message(chat_id=original_chat_id, text="✅ Buyurtmalar shaxsiy chatga yuborildi.")
                                except Exception:
                                    pass
                            return None
                        except Exception as dm_error:
                            logger.warning(f"Failed to send orders via DM to {target_chat_id}: {dm_error}. Falling back to current chat {original_chat_id}.")
                            try:
                                await context.bot.send_message(chat_id=original_chat_id, text="⚠️ Shaxsiy chatga yuborib bo'lmadi. Buyurtmalar shu yerga yuborilmoqda.")
                            except Exception:
                                pass

                    # Default: send to the chat where the callback was triggered, but within the Find Orders topic if configured
                    find_orders_topic_id = int(config.FIND_ORDERS_TOPIC_ID) if config.FIND_ORDERS_TOPIC_ID else None
                    await self._send_orders_to_chat(original_chat_id, user, orders, scope_text, context, message_thread_id=find_orders_topic_id)
                    return None
                except Exception as e:
                    logger.error(f"Error sending orders after view_orders callback: {e}")
                    return {"text": "❌ Buyurtmalarni yuklashda xatolik.", "keyboard": None}

                return None

            except Exception as e:
                logger.error(f"Error processing view_orders callback: {e}")
                return {"text": "❌ Buyurtmalarni yuklashda xatolik.", "keyboard": None}

        return None

    async def _send_orders_to_chat(self, chat_id: int, user: dict, orders: list, scope_text: str, context: ContextTypes.DEFAULT_TYPE, message_thread_id: Optional[int] = None) -> None:
        """Helper method to send orders to a specific chat using context.bot."""
        try:
            logger.info(f"Attempting to send {len(orders)} orders for user {user.get('id')} to chat_id {chat_id} (scope: {scope_text}), topic_id={message_thread_id}")
            # Send header for orders
            await context.bot.send_message(chat_id=chat_id, text=f"📦 {scope_text} buyurtmalari ({len(orders)} ta):", message_thread_id=message_thread_id)

            # Send each order as separate message with image(s)
            for i, order in enumerate(orders, 1):
                # Format collection status
                status_info = {
                    'open': {'emoji': '🟢', 'text': 'OCHIQ'},
                    'close': {'emoji': '🔴', 'text': 'YOPIQ'},
                    'finish': {'emoji': '✅', 'text': 'YAKUNLANGAN'}
                }

                status_data = status_info.get(order.get('collection_status', ''), {'emoji': '⚪', 'text': 'NOMA\'LUM'})

                # Get files from order (should already be included from optimized query)
                file_paths = order.get('files', [])

                order_text = (
                    f"📦 Buyurtma #{order['id']}\n"
                    f"🏷 Kolleksiya: #{order['collection_id']}\n"
                    f"📊 Kolleksiya holati: {status_data['emoji']} {status_data['text']}\n"
                    f"🔢 Seryasi: {order.get('amount', 'N/A')}\n"
                    f"📷 Fayllar soni: {len(file_paths)}\n"
                )

                await self._send_order_with_files_to_chat(chat_id, order_text, file_paths, order['id'], context, message_thread_id=message_thread_id)

        except Exception as e:
            logger.error(f"Error sending orders to chat {chat_id}: {e}")
            await context.bot.send_message(chat_id=chat_id, text="❌ Buyurtmalarni yuklashda xatolik yuz berdi.")

    async def _send_order_with_files_to_chat(self, chat_id: int, order_text: str, file_paths: list, order_id: int, context: ContextTypes.DEFAULT_TYPE, message_thread_id: Optional[int] = None) -> None:
        """Helper to send a single order with files to a chat."""
        try:
            if not file_paths:
                # No files for this order
                order_text += "\n❌ Fayllar topilmadi"
                await context.bot.send_message(chat_id=chat_id, text=order_text, message_thread_id=message_thread_id)
            elif len(file_paths) == 1:
                # Single file
                file_path = file_paths[0]
                exists = os.path.exists(file_path)
                logger.info(f"Order {order_id}: preparing single file '{file_path}', exists={exists}")
                if exists:
                    try:
                        if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.ico', '.svg')):
                            await context.bot.send_photo(chat_id=chat_id, photo=file_path, caption=order_text, message_thread_id=message_thread_id)
                        elif file_path.lower().endswith(('.mp4', '.webm', '.mov')):
                            await context.bot.send_video(chat_id=chat_id, video=file_path, caption=order_text, message_thread_id=message_thread_id)
                        else:
                            order_text += f"\n📄 Fayl: {os.path.basename(file_path)} (qo'llab-quvvatlanmaydigan format)"
                            await context.bot.send_message(chat_id=chat_id, text=order_text, message_thread_id=message_thread_id)
                    except Exception as e:
                        logger.error(f"Error sending file for order {order_id}: {e}")
                        order_text += f"\n📄 Fayl: {os.path.basename(file_path)} (yuklashda xatolik)"
                        await context.bot.send_message(chat_id=chat_id, text=order_text, message_thread_id=message_thread_id)
                else:
                    order_text += f"\n📄 Fayl: {os.path.basename(file_path)} (topilmadi)"
                    await context.bot.send_message(chat_id=chat_id, text=order_text, message_thread_id=message_thread_id)
            else:
                # Multiple files - send as media group
                media_group = []
                valid_files = []

                for j, file_path in enumerate(file_paths):
                    exists = os.path.exists(file_path)
                    logger.info(f"Order {order_id}: preparing file '{file_path}', exists={exists}")
                    if exists:
                        try:
                            # Add caption only to the first file
                            caption = order_text if j == 0 else None

                            if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.ico', '.svg')):
                                media_group.append(InputMediaPhoto(media=file_path, caption=caption))
                            elif file_path.lower().endswith(('.mp4', '.webm', '.mov')):
                                media_group.append(InputMediaVideo(media=file_path, caption=caption))

                            valid_files.append(os.path.basename(file_path))
                        except Exception as e:
                            logger.error(f"Error preparing file {file_path} for order {order_id}: {e}")

                if media_group:
                    try:
                        await context.bot.send_media_group(chat_id=chat_id, media=media_group, message_thread_id=message_thread_id)
                    except Exception as e:
                        logger.error(f"Error sending media group for order {order_id}: {e}")
                        # Fallback to text message with file info
                        order_text += f"\n📄 Fayllar: {', '.join(valid_files)} (yuklashda xatolik)"
                        await context.bot.send_message(chat_id=chat_id, text=order_text, message_thread_id=message_thread_id)
                else:
                    # No valid files found
                    order_text += f"\n📄 Fayllar: {', '.join([os.path.basename(p) for p in file_paths])} (hech biri topilmadi)"
                    await context.bot.send_message(chat_id=chat_id, text=order_text, message_thread_id=message_thread_id)
        except Exception as e:
            logger.error(f"Error in _send_order_with_files_to_chat for order {order_id}: {e}")
            await context.bot.send_message(chat_id=chat_id, text=order_text + "\n❌ Fayllarni yuklashda xatolik", message_thread_id=message_thread_id)

    def process_registration_message(self, message: Dict[str, Any]) -> Tuple[str, Optional[ReplyKeyboardMarkup]]:
        """Process a text message during the registration flow."""
        user_data = self._extract_user_data(message)
        if not user_data:
            return "Foydalanuvchi ma'lumotlarini olib bo'lmadi."

        telegram_id = user_data['telegram_id']
        text = message.get('text', '').strip()
        user = self.db_service.get_user_by_telegram_id(telegram_id)

        if not user or user['reg_step'] == 'done':
            return "", None  # Not in registration process, so do nothing.

        if user['reg_step'] == 'name':
            self.db_service.update_user_info(telegram_id, 'name', text)
            self.db_service.update_user_registration_step(telegram_id, 'surname')
            return "🌟 Familiyangizni kiriting:", None

        elif user['reg_step'] == 'surname':
            self.db_service.update_user_info(telegram_id, 'surname', text)
            self.db_service.update_user_registration_step(telegram_id, 'phone')
            return "🎉 Ajoyib! Endi telefon raqamingizni yuboring.\n\n📱 Quyidagi tugmani bosing:", self._get_phone_contact_keyboard()

        elif user['reg_step'] == 'phone':
            # Phone step should not accept text input anymore, only contact messages
            return "❌ Iltimos, telefon raqamingizni yuborish uchun 📱 tugmasini bosing.", self._get_phone_contact_keyboard()
        
        elif user['reg_step'] == 'region':
            return "Viloyatingizni tanlang:", get_region_keyboard()

        return "", None  # Should not be reached

    async def process_contact_message(self, message: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, Optional[Any]]:
        """
        Process a contact message during registration or editing.
        
        Args:
            message: Telegram message object containing contact
            
        Returns:
            Tuple[str, Optional[Any]]: Response message and keyboard
        """
        try:
            user_data = self._extract_user_data(message)
            if not user_data:
                return "❌ Foydalanuvchi ma'lumotlarini olib bo'lmadi.", None
            
            telegram_id = user_data['telegram_id']
            
            # Check if message contains contact
            if 'contact' not in message:
                return "❌ Kontakt ma'lumotlari topilmadi.", None
            
            contact = message['contact']
            
            # Validate that user is sharing their own contact
            if contact.get('user_id') != telegram_id:
                return "❌ Iltimos, faqat o'zingizning telefon raqamingizni yuboring.", self._get_phone_contact_keyboard()
            
            # Get phone number from contact
            phone_number = contact.get('phone_number', '')
            if not phone_number:
                return "❌ Telefon raqami topilmadi. Qaytadan urinib ko'ring.", self._get_phone_contact_keyboard()
            
            # Normalize phone number to international format
            if not phone_number.startswith('+'):
                phone_number = '+' + phone_number
            is_valid, result = self._normalize_phone(phone_number)
            if not is_valid:
                return result, self._get_phone_contact_keyboard()
            normalized_phone = result
            
            user = self.db_service.get_user_by_telegram_id(telegram_id)
            if not user:
                return "❌ Foydalanuvchi topilmadi.", None
            
            # Handle registration flow
            if user['reg_step'] == 'phone':
                self.db_service.update_user_info(telegram_id, 'phone', normalized_phone)
                self.db_service.update_user_registration_step(telegram_id, 'region')
                return "Viloyatingizni tanlang:", get_region_keyboard()
            
            # If user is fully registered, this might be for editing
            elif user['reg_step'] == 'done':
                # This could be an edit phone operation - we need context to know
                return "✅ Telefon raqami qabul qilindi.", None
            
            # Other registration steps shouldn't receive contact messages
            return "❌ Hozirda telefon raqami talab qilinmaydi.", None
            
        except Exception as e:
            logger.error(f"Error processing contact message: {str(e)}")
            return "❌ Kontaktni qayta ishlashda xatolik yuz berdi.", None

    def process_contact_edit(self, message: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, Optional[Any]]:
        """
        Process a contact message during profile editing.
        
        Args:
            message: Telegram message object containing contact
            context: Bot context
            
        Returns:
            Tuple[str, Optional[ReplyKeyboardMarkup]]: Response message and keyboard
        """
        try:
            user_data = self._extract_user_data(message)
            if not user_data:
                return "❌ Foydalanuvchi ma'lumotlarini olib bo'lmadi.", None
            
            telegram_id = user_data['telegram_id']
            edit_state = context.user_data.get('edit_state')
            
            if edit_state != 'edit_phone':
                return "❌ Hozirda telefon tahrirlash rejimida emassiz.", None
            
            # Check if message contains contact
            if 'contact' not in message:
                return "❌ Kontakt ma'lumotlari topilmadi.", None
            
            contact = message['contact']
            
            # Validate that user is sharing their own contact
            if contact.get('user_id') != telegram_id:
                return "❌ Iltimos, faqat o'zingizning telefon raqamingizni yuboring.", None
            
            # Get phone number from contact
            phone_number = contact.get('phone_number', '')
            if not phone_number:
                return "❌ Telefon raqami topilmadi. Qaytadan urinib ko'ring.", None
            
            # Normalize phone number to international format
            if not phone_number.startswith('+'):
                phone_number = '+' + phone_number
            is_valid, result = self._normalize_phone(phone_number)
            if not is_valid:
                return result, None
            normalized_phone = result
            
            user = self.db_service.get_user_by_telegram_id(telegram_id)
            if not user:
                context.user_data.pop('edit_state', None)
                return "❌ Foydalanuvchi topilmadi.", None
            
            old_value = user.get('phone', 'Kiritilmagan')
            success = self.db_service.update_user_info(telegram_id, 'phone', normalized_phone)
            
            # Clear edit state
            context.user_data.pop('edit_state', None)
            
            if success:
                # Create home menu keyboard after successful phone edit
                from telegram import ReplyKeyboardMarkup
                keyboard = ReplyKeyboardMarkup([
                    ["📦 Mening buyurtmalarim", "👤 Mening profilim"]
                ], resize_keyboard=True)
                
                response_text = (f"✅ Telefon muvaffaqiyatli o'zgartirildi!\n\n"
                               f"Eski: {old_value}\n"
                               f"Yangi: {normalized_phone}\n\n"
                               "🏠 Asosiy menyuga qaytdingiz.")
                return response_text, keyboard
            else:
                return "❌ Telefonni o'zgartirishda xatolik yuz berdi. Qaytadan urining.", None
            
        except Exception as e:
            logger.error(f"Error processing contact edit: {str(e)}")
            context.user_data.pop('edit_state', None)
            return "❌ Kontaktni qayta ishlashda xatolik yuz berdi.", None

    def process_start_command(self, message: Dict[str, Any]) -> Tuple[str, Optional[ReplyKeyboardMarkup]]:
        """
        Process the /start command.
        
        Args:
            message: Telegram message object
        
        Returns:
            Tuple[str, Optional[ReplyKeyboardMarkup]]: A tuple of (response_text, keyboard_markup)
        """
        user_data = self._extract_user_data(message)
        if not user_data:
            return "Foydalanuvchi ma'lumotlarini olib bo'lmadi.", None

        telegram_id = user_data['telegram_id']

        # Check if the user is an admin
        if str(telegram_id) in getattr(config, 'ADMIN_IDS', []):
            domain = os.getenv('ADMIN_DOMAIN', 'http://localhost:4040').rstrip('/')
            web_url = f"{domain}/"
            web_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🌐 Web panelni ochish", url=web_url)]
            ])
            return "👨‍💼 Salom, Admin! Boshqaruv paneliga xush kelibsiz.", web_keyboard

        # Regular user flow
        user = self.db_service.get_user_by_telegram_id(telegram_id)

        if not user:
            self.db_service.create_user(user_data)
            return "🌟 Assalomu alaykum! Botimizga xush kelibsiz.\n\nRo'yxatdan o'tish uchun ismingizni kiriting:", None

        if user['reg_step'] == 'done':
            # Test account bypass for user config.TEST_ACCOUNT_ID
            is_test_account = config.TEST_ACCOUNT_ID and user['telegram_id'] == config.TEST_ACCOUNT_ID
            if user.get('payment_step') != 'confirmed' and not is_test_account:
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Kanalga qo'shilish", callback_data="join_channel")]
                ])
                return "Botdan foydalanish uchun, iltimos, maxfiy kanalga qo'shiling.", keyboard
            else:
                user_name = user.get('name') or user_data.get('name', 'foydalanuvchi')
                user_code_text = f"🔑 Sizning shaxsiy kodingiz: `{user.get('code', 'N/A')}`\n\n"
                welcome_message = (f"👋 Salom {user_name}!\n\n"
                    f"{user_code_text}"
                    "🚀 Buyurtma Kuzatish Botiga xush kelibsiz!\n\n"
                    "📋 Buyurtma berish uchun rasm yoki video yuboring.\n\n"
                    "⚡ Buyruqlar:\n"
                    "🔄 /start - Ushbu xush kelibsiz xabarini ko'rsatish\n"
                    "📊 /mystats - O'z statistikangizni ko'rish\n"
                    "✏️ /edit - Shaxsiy ma'lumotlarni tahrirlash\n\n"
                    "✨ Boshladik! Menga rasm yoki video yuboring."
                )
                # Create keyboard with My Orders and My Profile buttons
                keyboard = ReplyKeyboardMarkup([
                    ["📦 Mening buyurtmalarim", "👤 Mening profilim"]
                ], resize_keyboard=True)
                return welcome_message, keyboard
        else:
            # Continue registration
            if user['reg_step'] == 'name':
                return "👤 Ro'yxatdan o'tishni davom ettirish uchun ismingizni kiriting:", None
            elif user['reg_step'] == 'surname':
                return "📝 Familiyangizni kiriting:", None
            elif user['reg_step'] == 'phone':
                return ("📞 Telefon raqamingizni yuboring:\n\n"
                       "📱 Quyidagi tugmani bosing:"), self._get_phone_contact_keyboard()
            elif user['reg_step'] == 'region':
                return "Viloyatingizni tanlang:", get_region_keyboard()
    
    def process_mystats_command(self, message: Dict[str, Any]) -> str:
        """
        Process the /mystats command to show user's order count.
        
        Args:
            message: Telegram message object
        
        Returns:
            str: Statistics message
        """
        try:
            user_data = self._extract_user_data(message)
            if not user_data:
                return "❌ Xato: Sizning ma'lumotlaringizni olib bo'lmadi."
            
            telegram_id = user_data['telegram_id']
            order_count = self.db_service.get_user_order_count(telegram_id)
            
            user_name = user_data.get('name', 'Foydalanuvchi')
            
            return (f"📊 {user_name} uchun statistika\n\n"
                   f"📦 Jami buyurtmalar soni: {order_count}")
            
        except Exception as e:
            logger.error(f"Error processing mystats command: {str(e)}")
            return "❌ Statistikangizni olishda xatolik yuz berdi."

    def process_edit_command(self, message: Dict[str, Any]) -> Tuple[str, Optional[Any]]:
        """
        Process the /edit command to show user profile and editing options.
        
        Args:
            message: Telegram message object
        
        Returns:
            Tuple[str, Optional[InlineKeyboardMarkup]]: Response text and keyboard
        """
        try:
            user_data = self._extract_user_data(message)
            if not user_data:
                return "❌ Xato: Sizning ma'lumotlaringizni olib bo'lmadi.", None
            
            telegram_id = user_data['telegram_id']
            user = self.db_service.get_user_by_telegram_id(telegram_id)
            
            if not user:
                return "❌ Foydalanuvchi topilmadi. Avval /start buyrug'i orqali ro'yxatdan o'ting.", None
            
            if user['reg_step'] != 'done':
                return "❌ Avval ro'yxatdan o'tishni yakunlang.", None
            
            # Display current profile information
            profile_text = (
                "👤 Sizning profilingiz:\n\n"
                f"🔑 Sizning kodingiz: `{user.get('code', 'N/A')}` (O'zgartirilmaydi)\n"
                f"📛 Ism: {user.get('name', 'Kiritilmagan')}\n"
                f"👨‍💼 Familiya: {user.get('surname', 'Kiritilmagan')}\n"
                f"📞 Telefon: {user.get('phone', 'Kiritilmagan')}\n\n"
                "Qaysi ma'lumotni o'zgartirmoqchisiz?"
            )
            
            # Create inline keyboard for editing options
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            keyboard = [
                [InlineKeyboardButton("📛 Ismni o'zgartirish", callback_data="edit_name")],
                [InlineKeyboardButton("👨‍💼 Familiyani o'zgartirish", callback_data="edit_surname")],
                [InlineKeyboardButton("📞 Telefonni o'zgartirish", callback_data="edit_phone")],
                [InlineKeyboardButton("❌ Bekor qilish", callback_data="edit_cancel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            return profile_text, reply_markup
            
        except Exception as e:
            logger.error(f"Error processing edit command: {str(e)}")
            return "❌ Profil ma'lumotlarini olishda xatolik yuz berdi.", None

    def process_edit_callback(self, query, context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, Any]]:
        """
        Process callback queries for profile editing.
        
        Args:
            query: CallbackQuery object
            context: Bot context
            
        Returns:
            Dict with response or None
        """
        try:
            callback_data = query.data
            user_id = query.from_user.id
            
            user = self.db_service.get_user_by_telegram_id(user_id)
            if not user or user['reg_step'] != 'done':
                return {"text": "❌ Foydalanuvchi topilmadi yoki ro'yxatdan o'tmagan.", "keyboard": None}
            
            if callback_data == "edit_name":
                context.user_data['edit_state'] = 'edit_name'
                return {"text": f"📛 Hozirgi ismingiz: {user.get('name', 'Kiritilmagan')}\n\nYangi ismingizni kiriting:", "keyboard": None}
            
            elif callback_data == "edit_surname":
                context.user_data['edit_state'] = 'edit_surname'
                return {"text": f"👨‍💼 Hozirgi familiyangiz: {user.get('surname', 'Kiritilmagan')}\n\nYangi familiyangizni kiriting:", "keyboard": None}
            
            elif callback_data == "edit_phone":
                context.user_data['edit_state'] = 'edit_phone'
                return {"text": f"📞 Hozirgi telefoningiz: {user.get('phone', 'Kiritilmagan')}\n\nYangi telefon raqamingizni yuboring:\n\n📱 Quyidagi tugmani bosing:", "keyboard": self._get_phone_contact_keyboard()}
            
            elif callback_data == "edit_cancel":
                context.user_data.pop('edit_state', None)
                return {"text": "❌ Tahrirlash bekor qilindi.", "keyboard": None}
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing edit callback: {str(e)}")
            return {"text": "❌ Xatolik yuz berdi.", "keyboard": None}

    def process_edit_input(self, message: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, Optional[Any]]:
        """
        Process text input during profile editing.
        
        Args:
            message: Telegram message object
            context: Bot context
            
        Returns:
            Tuple[str, Optional[ReplyKeyboardMarkup]]: Response message and keyboard
        """
        try:
            user_data = self._extract_user_data(message)
            if not user_data:
                return "❌ Xato: Foydalanuvchi ma'lumotlarini olib bo'lmadi."
            
            telegram_id = user_data['telegram_id']
            text = message.get('text', '').strip()
            edit_state = context.user_data.get('edit_state')
            
            if not edit_state:
                return "", None  # Not in edit mode
            
            # Prevent saving known menu button labels as user data
            disallowed_menu_labels = {
                "📦 Mening buyurtmalarim",
                "👤 Mening profilim",
                "⬅️ Asosiy menyu",
            }
            if text in disallowed_menu_labels:
                # Exit edit mode and return to home
                context.user_data.pop('edit_state', None)
                from telegram import ReplyKeyboardMarkup
                keyboard = ReplyKeyboardMarkup([["📦 Mening buyurtmalarim", "👤 Mening profilim"]], resize_keyboard=True)
                return "❌ Tahrirlash bekor qilindi.\n\n🏠 Asosiy menyuga qaytdingiz.", keyboard

            user = self.db_service.get_user_by_telegram_id(telegram_id)
            if not user:
                context.user_data.pop('edit_state', None)
                return "❌ Foydalanuvchi topilmadi.", None
            
            # Validate input
            if not text:
                return "❌ Bo'sh qiymat kiritib bo'lmaydi. Iltimos, to'g'ri ma'lumot kiriting.", None
            
            if len(text) > 100:
                return "❌ Juda uzun matn. Iltimos, qisqaroq kiriting.", None
            
            # Update based on edit state
            success = False
            field_name = ""
            old_value = ""
            
            if edit_state == 'edit_name':
                field_name = "ism"
                old_value = user.get('name', 'Kiritilmagan')
                success = self.db_service.update_user_info(telegram_id, 'name', text)
            
            elif edit_state == 'edit_surname':
                field_name = "familiya"
                old_value = user.get('surname', 'Kiritilmagan')
                success = self.db_service.update_user_info(telegram_id, 'surname', text)
            
            elif edit_state == 'edit_phone':
                # Phone editing should only accept contact messages, not text
                return "❌ Telefon raqamini o'zgartirish uchun 📱 tugmasini bosib kontaktingizni yuboring.", None
            
            # Clear edit state
            context.user_data.pop('edit_state', None)
            
            if success:
                # Create home menu keyboard after successful edit
                from telegram import ReplyKeyboardMarkup
                keyboard = ReplyKeyboardMarkup([
                    ["📦 Mening buyurtmalarim", "👤 Mening profilim"]
                ], resize_keyboard=True)
                
                response_text = (f"✅ {field_name.capitalize()} muvaffaqiyatli o'zgartirildi!\n\n"
                               f"Eski: {old_value}\n"
                               f"Yangi: {text}\n\n"
                               "🏠 Asosiy menyuga qaytdingiz.")
                return response_text, keyboard
            else:
                return f"❌ {field_name.capitalize()}ni o'zgartirishda xatolik yuz berdi. Qaytadan urining.", None
            
        except Exception as e:
            logger.error(f"Error processing edit input: {str(e)}")
            context.user_data.pop('edit_state', None)
            return "❌ Ma'lumotni o'zgartirishda xatolik yuz berdi.", None
    
    async def handle_bank_notification(self, message):
        """
        Handle bank notification messages from the userbot.
        Parse bank messages and create transaction records.
        """
        try:
            message_text = message.raw_text
            logger.info(f"Processing bank notification: {message_text[:100]}...")
            
            # Optionally filter by allowed bank bot sender IDs (keep broad but safe)
            try:
                sender_id = getattr(getattr(message, 'sender', None), 'id', None) or getattr(message, 'sender_id', None)
                from src.config import config as _cfg
                allowed_bot_ids = getattr(_cfg, 'ALLOWED_BANK_BOT_IDS', set())
                if sender_id and sender_id not in allowed_bot_ids:
                    logger.debug(f"Ignoring message from non-bank sender: {sender_id}")
                    return
            except Exception:
                pass

            # Parse different types of bank notifications
            parsed_data = self._parse_bank_message(message_text)
            
            if parsed_data:
                # Type-agnostic: attach transaction to active card (single active enforced)
                active_card = self.db_service.get_active_card()

                if active_card:
                    # Create transaction record
                    transaction_id = self.db_service.add_transaction(
                        card_id=active_card['id'],
                        amount=parsed_data['amount'],
                        transaction_time=parsed_data['transaction_time'],
                        card_balance=parsed_data.get('card_balance'),
                        raw_message=message_text
                    )
                    
                    if transaction_id:
                        logger.info(f"Transaction {transaction_id} created for card {active_card['id']}: {parsed_data['amount']} UZS")
                        
                        # Try to match with existing pending payments
                        await self._trigger_payment_verification_for_new_transaction(transaction_id)
                        
                    else:
                        logger.error("Failed to create transaction record")
                else:
                    logger.warning("No active card found for transaction")
            else:
                logger.debug("Bank message doesn't match transaction patterns")
            
            logger.info("Bank notification processed successfully")
            
        except Exception as e:
            logger.error(f"Error handling bank notification: {e}")
    
    def _parse_bank_message(self, message_text: str) -> Optional[Dict[str, Any]]:
        """
        Dispatcher that tries source-specific parsers in order.
        Returns the first successful parse, otherwise None.
        """
        parsed = self._parse_humo_card_message(message_text)
        if parsed:
            return parsed
        return self._parse_cardxabar_message(message_text)

    def _parse_humo_card_message(self, message_text: str) -> Optional[Dict[str, Any]]:
        """
        Parse HUMO Card bot top-up (income) messages.
        Expected markers:
        - Header often includes "🎉 Пополнение"
        - Amount formatted like '5.000,00' UZS ('.' thousands, ',' decimal)
        - Time formatted like 'HH:mm DD.MM.YYYY'
        - Card shown as '*1234'
        Returns None for non-top-up messages (e.g., with '💸' or '🔴').
        """
        import re

        # Quick rejection for obvious outgoing/payment indicators
        if re.search(r'(?:🔴|💸)', message_text):
            return None

        # Primary pattern targeting HUMO layout with 🎉 and HH:mm DD.MM.YYYY
        pattern = (
            r'🎉[\s\S]*?'                                         # Celebration emoji
            r'(?:Пополнение|To\'ldirish|Perevod[^\n\r]*)?[\s\S]*?'  # Optional keyword
            r'[➕\+]\s*([\d\.]+,\d{2})\s*UZS[\s\S]*?'       # Amount (HUMO style)
            r'\*(\d{4})[\s\S]*?'                               # Card suffix
            r'(\d{2}:\d{2}\s+\d{2}\.\d{2}\.\d{4})'         # Time HH:mm DD.MM.YYYY
        )
        match = re.search(pattern, message_text, re.DOTALL | re.IGNORECASE)
        if not match:
            return None

        amount_str, card_suffix, time_str = match.groups()
        # Convert '5.000,00' -> 5000.00
        normalized_amount = amount_str.replace('.', '').replace(',', '.')
        try:
            amount = float(normalized_amount)
        except Exception:
            return None

        parsed_time = self._parse_transaction_time(time_str)

        # Optional balance extraction
        balance = None
        balance_match = re.search(r'[💵💰]\s*([\d\.]+,\d{2}|[\d\s\.]+)\s*UZS?', message_text)
        if balance_match:
            raw_balance = balance_match.group(1)
            if ',' in raw_balance and '.' in raw_balance:
                raw_balance = raw_balance.replace('.', '').replace(',', '.')
            balance = float(re.sub(r'[^\d\.]', '', raw_balance.replace(' ', '')))

        return {
            'amount': amount,
            'card_suffix': card_suffix,
            'transaction_time': parsed_time,
            'card_balance': balance,
            'transaction_type': 'incoming'
        }

    def _parse_cardxabar_message(self, message_text: str) -> Optional[Dict[str, Any]]:
        """
        Parse CardXabar bot top-up (income) messages and ignore outgoing.
        Typical layout:
        - 🟢 Perevod na kartu / To'ldirish
        - ➕ 5 000.00 UZS
        - 💳 ***9605
        - 🕓 07.09.25 14:15
        """
        import re

        # Ignore explicit outgoing markers
        if re.search(r'(?:🔴|💸)', message_text):
            return None

        incoming_pattern = r'(?:🟢|To\'ldirish|Perevod)[\s\S]*?[➕\+]\s*([\d\s,\.]+)\s*UZS[\s\S]*?💳[\s\S]*?\*(\d{4})[\s\S]*?(?:🕓|⏰|🕒)?\s*([\d\.\:\s]+)'
        match = re.search(incoming_pattern, message_text, re.DOTALL | re.IGNORECASE)
        if not match:
            return None

        amount_str, card_suffix, time_str = match.groups()

        cleaned_amount_str = amount_str.replace(' ', '')
        if ',' in cleaned_amount_str and '.' in cleaned_amount_str:
            final_amount_str = cleaned_amount_str.replace('.', '').replace(',', '.')
        else:
            final_amount_str = cleaned_amount_str.replace(',', '.')
        try:
            amount = float(final_amount_str)
        except Exception:
            return None

        parsed_time = self._parse_transaction_time(time_str)

        balance = None
        balance_match = re.search(r'[💵💰]\s*([\d\s,\.]+)\s*UZS?', message_text)
        if balance_match:
            balance_str = balance_match.group(1)
            balance = float(re.sub(r'[^\d.]', '', balance_str.replace(',', '.').replace(' ', '')))

        return {
            'amount': amount,
            'card_suffix': card_suffix,
            'transaction_time': parsed_time,
            'card_balance': balance,
            'transaction_type': 'incoming'
        }
    
    def _parse_transaction_time(self, time_str: str) -> str:
        """
        Parse transaction time string and convert to database format.
        """
        import re
        from datetime import datetime, timedelta, timezone
        try:
            # Prefer Python 3.9+ zoneinfo if available
            from zoneinfo import ZoneInfo  # type: ignore
        except Exception:
            ZoneInfo = None  # Fallback handled below
        
        try:
            # Clean up the time string
            time_str = time_str.strip()
            
            # Load configured timezone (default Asia/Tashkent); fallback to UTC if zoneinfo unavailable
            try:
                from src.config import config as _cfg
                tz_name = getattr(_cfg, 'TIMEZONE', 'Asia/Tashkent')
            except Exception:
                tz_name = 'Asia/Tashkent'

            tzinfo = None
            if ZoneInfo is not None:
                try:
                    tzinfo = ZoneInfo(tz_name)
                except Exception:
                    tzinfo = ZoneInfo('Asia/Tashkent')

            # Pattern: "07.09.25 14:15"
            match1 = re.match(r'(\d{2})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})', time_str)
            if match1:
                day, month, year, hour, minute = match1.groups()
                # Assume 20xx for 2-digit years
                full_year = f"20{year}"
                naive = datetime(int(full_year), int(month), int(day), int(hour), int(minute))
                if tzinfo:
                    local_dt = naive.replace(tzinfo=tzinfo)
                    utc_dt = local_dt.astimezone(timezone.utc)
                    return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
                # Fallback: treat as local and return naive string
                return naive.strftime('%Y-%m-%d %H:%M:%S')
            
            # Pattern: "14:15 07.09.2025"
            match2 = re.match(r'(\d{2}):(\d{2})\s+(\d{2})\.(\d{2})\.(\d{4})', time_str)
            if match2:
                hour, minute, day, month, year = match2.groups()
                naive = datetime(int(year), int(month), int(day), int(hour), int(minute))
                if tzinfo:
                    local_dt = naive.replace(tzinfo=tzinfo)
                    utc_dt = local_dt.astimezone(timezone.utc)
                    return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
                return naive.strftime('%Y-%m-%d %H:%M:%S')
            
            # If we can't parse, use current time as fallback
            logger.warning(f"Could not parse transaction time: {time_str}, using current time")
            return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            
        except Exception as e:
            logger.error(f"Error parsing transaction time '{time_str}': {e}")
            return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    async def notify_users_of_card_change(self, context: ContextTypes.DEFAULT_TYPE):
        """
        Notify users when the active payment card changes.
        This can be called from the web interface when admins activate a new card.
        """
        try:
            # Get the active card
            active_card = self.db_service.get_active_card()
            if not active_card:
                logger.warning("No active card found for notification")
                return
            
            # Target users: registration completed AND subscription not confirmed/not fully paid
            users = self.db_service.get_users_with_incomplete_subscription()
            
            if not users:
                logger.info("No users in awaiting_receipt status to notify")
                return
            
            # Create notification message
            notification_text = (
                "🔔 To'lov karta ma'lumotlari yangilandi!\n\n"
                f"👤 Karta {active_card['name']} nomida\n"
                f"💳 Raqam: {active_card['number']}\n\n"
                "📣 Obunangiz hali tasdiqlanmagan. To'lovni shu yangi kartaga amalga oshiring va kvitansiyani yuboring."
            )
            
            # Send notification to all users waiting for payment
            notification_count = 0
            for user in users:
                try:
                    # Ensure only users with completed registration receive the message
                    if user.get('reg_step') != 'done':
                        continue
                    await context.bot.send_message(chat_id=user['telegram_id'], text=notification_text)
                    notification_count += 1
                    logger.info(f"Card change notification sent to user {user['telegram_id']}")
                except Exception as e:
                    logger.error(f"Failed to send card change notification to user {user['telegram_id']}: {e}")
            
            logger.info(f"Card change notifications sent to {notification_count} users")
            
        except Exception as e:
            logger.error(f"Error notifying users of card change: {e}")

    async def _trigger_payment_verification_for_payment(self, payment_id: int):
        """
        Trigger payment verification for a specific payment.
        This looks for matching transactions for the given payment.
        """
        try:
            logger.info(f"Triggering payment verification for payment {payment_id}")
            
            # Try to find a matching transaction for this payment
            matching_transaction = self.db_service.find_matching_transaction_for_payment(
                payment_id, 
                time_window_minutes=15
            )
            
            if matching_transaction:
                # Found a match! Link them
                success = self.db_service.link_payment_and_transaction(
                    payment_id, 
                    matching_transaction['id']
                )
                
                if success:
                    logger.info(f"Successfully linked payment {payment_id} with transaction {matching_transaction['id']}")
                    
                    # Get payment details to notify user
                    payment = self.db_service.get_payment_by_id(payment_id)
                    if payment:
                        await self._notify_payment_verified(payment)
                    
                    return True
                else:
                    logger.error(f"Failed to link payment {payment_id} with transaction {matching_transaction['id']}")
            else:
                logger.info(f"No matching transaction found for payment {payment_id}")
                # Mark payment for manual review if no automatic match found
                self.db_service.update_payment_status(payment_id, 'manual_review')
                
                # Notify admins about manual review needed
                await self._notify_admins_manual_review(payment_id)
            
            return False
            
        except Exception as e:
            logger.error(f"Error in payment verification for payment {payment_id}: {e}")
            return False
    
    async def _trigger_payment_verification_for_new_transaction(self, transaction_id: int):
        """
        When a new transaction is created, check if it matches any pending payments.
        """
        try:
            logger.info(f"Checking for payments matching new transaction {transaction_id}")
            
            # Get all pending payments that might match this transaction
            pending_payments = self.db_service.get_payments_by_status('pending')
            
            if not pending_payments:
                logger.info("No pending payments to match against")
                return
            
            # Get the transaction details
            transaction = self.db_service.get_transaction_by_id(transaction_id)
            if not transaction:
                logger.error(f"Transaction {transaction_id} not found")
                return
            
            # Try to find a matching payment
            matched_payment = None
            for payment in pending_payments:
                if self._payments_match(payment, transaction):
                    matched_payment = payment
                    break
            
            if matched_payment:
                # Link the payment and transaction
                success = self.db_service.link_payment_and_transaction(
                    matched_payment['id'], transaction_id
                )
                
                if success:
                    logger.info(f"Successfully linked payment {matched_payment['id']} with transaction {transaction_id}")
                    await self._notify_payment_verified(matched_payment)
                else:
                    logger.error(f"Failed to link payment {matched_payment['id']} with transaction {transaction_id}")
            else:
                logger.info(f"No pending payment matches transaction {transaction_id}")
                
        except Exception as e:
            logger.error(f"Error in transaction verification for transaction {transaction_id}: {e}")
    
    def _payments_match(self, payment: Dict[str, Any], transaction: Dict[str, Any], time_window_minutes: int = 15) -> bool:
        """
        Check if a payment matches a transaction based on amount and time.
        """
        try:
            # Amount must match exactly
            if abs(payment['amount'] - transaction['amount']) > 0.01:  # Allow for small floating point differences
                return False
            
            # Parse times
            from datetime import datetime, timedelta
            
            payment_time = datetime.fromisoformat(payment['created_at'].replace('Z', '+00:00'))
            transaction_time = datetime.fromisoformat(transaction['transaction_time'])
            
            # Check if times are within the window
            time_diff = abs((payment_time - transaction_time).total_seconds())
            if time_diff > time_window_minutes * 60:
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking payment/transaction match: {e}")
            return False
    
    async def _notify_admins_manual_review(self, payment_id: int):
        """
        Notify admins in the AI confirmations topic when a payment needs manual review.
        """
        try:
            if not self.bot_app or not config.GROUP_ID or not config.AI_CONFIRMATIONS_TOPIC_ID:
                logger.warning("Bot app or admin group not configured for manual review notifications")
                return
            
            # Get payment and user details
            payment = self.db_service.get_payment_by_id(payment_id)
            if not payment:
                logger.error(f"Payment {payment_id} not found for admin notification")
                return
            
            user = self.db_service.get_user_by_id(payment['user_id'])
            if not user:
                logger.error(f"User {payment['user_id']} not found for payment {payment_id}")
                return
            
            # Prepare admin notification captions (text used as caption when sending photos)
            admin_caption = (
                f"⚠️ QOLDA TEKSHIRISH KERAK\n\n"
                f"💰 Miqdor: {payment['amount']} UZS\n"
                f"👤 Foydalanuvchi: {user.get('name', 'N/A')} {user.get('surname', 'N/A')}\n"
                f"📱 Telegram ID: {user['telegram_id']}\n"
                f"🏷 Foydalanuvchi kodi: {user.get('code', 'N/A')}\n\n"
                f"🔍 Muammo: Ma'lumotlar bazasida mos tranzaksiya topilmadi\n"
                f"🕐 To'lov yuborilgan vaqt: {payment['created_at']}\n\n"
                f"Iltimos, bu to'lovni qo'lda tekshiring."
            )
            
            # Send to admin group AI confirmations topic (NO BUTTONS)
            try:
                # Try to attach the receipt image if available; otherwise send plain text
                receipt_url = payment.get('receipt_url')
                if receipt_url and os.path.exists(receipt_url):
                    with open(receipt_url, 'rb') as img_file:
                        await self.bot_app.bot.send_photo(
                            chat_id=config.GROUP_ID,
                            message_thread_id=int(config.AI_CONFIRMATIONS_TOPIC_ID),
                            photo=img_file,
                            caption=admin_caption
                        )
                else:
                    await self.bot_app.bot.send_message(
                        chat_id=config.GROUP_ID,
                        message_thread_id=int(config.AI_CONFIRMATIONS_TOPIC_ID),
                        text=admin_caption
                    )
                logger.info(f"Manual review notification sent to admin group AI topic for payment {payment_id}")
                
                # Also send to Confirmations topic using configured topic ID (WITH BUTTONS)
                try:
                    receipt_url = payment.get('receipt_url')
                    # Build Accept/Reject buttons
                    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"confirm_payment_{user['telegram_id']}") ,
                            InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_payment_{user['telegram_id']}")
                        ]
                    ])
                    if receipt_url and os.path.exists(receipt_url):
                        with open(receipt_url, 'rb') as img_file:
                            await self.bot_app.bot.send_photo(
                                chat_id=config.GROUP_ID,
                                message_thread_id=int(config.CONFIRMATION_TOPIC_ID),
                                photo=img_file,
                                caption=admin_caption,
                                reply_markup=keyboard
                            )
                    else:
                        await self.bot_app.bot.send_message(
                            chat_id=config.GROUP_ID,
                            message_thread_id=int(config.CONFIRMATION_TOPIC_ID),
                            text=admin_caption,
                            reply_markup=keyboard
                        )
                    logger.info(f"Confirmation message sent to Confirmations topic ({config.CONFIRMATION_TOPIC_ID}) for payment {payment_id}")
                except Exception as e:
                    logger.error(f"Failed to send confirmation message to topic {config.CONFIRMATION_TOPIC_ID}: {e}")
            except Exception as e:
                logger.error(f"Failed to send admin notification with Markdown: {e}")
                # Try without markdown if that failed
                try:
                    # Fallback plain text
                    await self.bot_app.bot.send_message(
                        chat_id=config.GROUP_ID,
                        message_thread_id=int(config.AI_CONFIRMATIONS_TOPIC_ID),
                        text=admin_caption
                    )
                    logger.info(f"Simple manual review notification sent to admin group AI topic for payment {payment_id}")
                except Exception as e2:
                    logger.error(f"Failed to send even simple admin notification: {e2}")
                    
        except Exception as e:
            logger.error(f"Error notifying admins of manual review for payment {payment_id}: {e}")
    
    async def _trigger_ai_payment_confirmation(self, payment_id: int, receipt_data: Dict[str, Any]):
        """
        Trigger AI-powered payment confirmation with retry logic.
        """
        try:
            logger.info(f"Starting AI payment confirmation for payment {payment_id}")
            
            # Get payment details
            payment = self.db_service.get_payment_by_id(payment_id)
            if not payment:
                logger.error(f"Payment {payment_id} not found for AI confirmation")
                return
            
            # Get pending transactions within a time window around the payment time
            from src.config import config as _cfg
            time_window = getattr(_cfg, 'AI_CONFIRMATION_TIME_WINDOW_MINUTES', 10)
            pending_transactions = self.db_service.get_pending_transactions_for_payment(payment_id, time_window)
            logger.info(f"Found {len(pending_transactions)} time-windowed pending transactions for AI analysis (±{time_window}m)")
            
            if not pending_transactions:
                logger.warning(f"No pending transactions available for payment {payment_id} - marking for manual review")
                self.db_service.update_payment_status(payment_id, 'manual_review')
                await self._notify_admins_manual_review(payment_id)
                return
            
            # Prepare payment data for AI
            payment_data = {
                'payment_id': payment_id,
                'amount': receipt_data.get('amount', payment.get('amount', 0)),
                'transaction_time': receipt_data.get('transaction_time', ''),
                'card_number': receipt_data.get('card_number', ''),
                'merchant_name': receipt_data.get('merchant_name', ''),
                'transaction_id': receipt_data.get('transaction_id', ''),
                'user_id': payment['user_id']
            }
            
            # Check if AI confirmator is available
            if not ai_payment_confirmator.is_available():
                logger.warning("AI payment confirmator not available - falling back to traditional verification")
                await self._trigger_payment_verification_for_payment(payment_id)
                return
            
            # Use AI confirmation with retry logic
            confirmation_result = await ai_payment_confirmator.confirm_payment_with_retry(
                payment_data, 
                pending_transactions,
                retry_delay_minutes=2
            )
            
            if confirmation_result:
                await self._handle_ai_confirmation_result(payment_id, confirmation_result)
            else:
                logger.error(f"AI confirmation failed for payment {payment_id} - marking for manual review")
                self.db_service.update_payment_status(payment_id, 'manual_review')
                await self._notify_admins_manual_review(payment_id)
                
        except Exception as e:
            logger.error(f"Error in AI payment confirmation for payment {payment_id}: {e}")
            # Fallback to manual review on any error
            self.db_service.update_payment_status(payment_id, 'manual_review')
            await self._notify_admins_manual_review(payment_id)
    
    async def _handle_ai_confirmation_result(self, payment_id: int, confirmation_result: Dict[str, Any]):
        """
        Handle the result from AI payment confirmation.
        """
        try:
            confirm = confirmation_result.get('confirm', False)
            transaction_id = confirmation_result.get('transaction_id')
            reason = confirmation_result.get('reason', 'No reason provided')
            confidence = confirmation_result.get('confidence', 0.0)
            
            logger.info(f"AI confirmation result for payment {payment_id}: confirm={confirm}, reason='{reason}', confidence={confidence}")
            
            if confirm and transaction_id:
                # AI confirmed the payment - finalize it
                logger.info(f"AI confirmed payment {payment_id} matches transaction {transaction_id}")
                
                # Link payment and transaction
                success = self.db_service.link_payment_and_transaction(payment_id, transaction_id)
                
                if success:
                    # Mark transaction as done
                    self.db_service.mark_transaction_done(transaction_id)
                    
                    # Get payment details for notification
                    payment = self.db_service.get_payment_by_id(payment_id)
                    if payment:
                        await self._notify_payment_verified(payment)
                        
                        # Update user's payment step to confirmed
                        user = self.db_service.get_user_by_id(payment['user_id'])
                        if user:
                            self.db_service.update_user_payment_step(user['telegram_id'], 'confirmed')
                        
                        # Send success notification to admin channel
                        await self._notify_admins_ai_success(payment_id, transaction_id, confidence, reason)
                    
                    logger.info(f"Successfully processed AI-confirmed payment {payment_id}")
                else:
                    logger.error(f"Failed to link AI-confirmed payment {payment_id} with transaction {transaction_id}")
                    # Mark for manual review if linking failed
                    self.db_service.update_payment_status(payment_id, 'manual_review')
                    await self._notify_admins_manual_review(payment_id)
            else:
                # AI could not confirm the payment
                logger.info(f"AI could not confirm payment {payment_id}: {reason}")
                self.db_service.update_payment_status(payment_id, 'manual_review')
                await self._notify_admins_manual_review(payment_id)
                
        except Exception as e:
            logger.error(f"Error handling AI confirmation result for payment {payment_id}: {e}")
            self.db_service.update_payment_status(payment_id, 'manual_review')
            await self._notify_admins_manual_review(payment_id)
        finally:
            # Delete the receipt file after processing is complete
            payment = self.db_service.get_payment_by_id(payment_id)
            if payment and payment.get('receipt_url'):
                receipt_path = payment.get('receipt_url')
                if os.path.exists(receipt_path):
                    try:
                        os.remove(receipt_path)
                        logger.info(f"Deleted receipt file after AI confirmation: {receipt_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete receipt file {receipt_path}: {e}")
    
    async def _notify_admins_ai_success(self, payment_id: int, transaction_id: int, confidence: float, reason: str):
        """
        Notify admins when AI successfully confirms a payment.
        """
        try:
            if not self.bot_app or not config.GROUP_ID or not config.AI_CONFIRMATIONS_TOPIC_ID:
                logger.warning("Bot app or admin group not configured for AI success notifications")
                return
            
            # Get payment and user details
            payment = self.db_service.get_payment_by_id(payment_id)
            if not payment:
                logger.error(f"Payment {payment_id} not found for admin AI success notification")
                return
            
            user = self.db_service.get_user_by_id(payment['user_id'])
            if not user:
                logger.error(f"User {payment['user_id']} not found for payment {payment_id}")
                return
            
            # Create AI success notification message in Uzbek
            success_message = (
                f"🤖 **AI TO'LOVNI TASDIQLADI** 🎉\n\n"
                f"💰 Miqdor: {payment['amount']} UZS\n"
                f"👤 Foydalanuvchi: {user.get('name', 'N/A')} {user.get('surname', 'N/A')}\n"
                f"📱 Telegram ID: `{user['telegram_id']}`\n"
                f"🏷 Foydalanuvchi kodi: `{user.get('code', 'N/A')}`\n"
                f"🔗 Tranzaksiya ID: {transaction_id}\n"
                f"🎯 Ishonch darajasi: {confidence:.2f}\n"
                f"🧠 AI sababi: {reason}\n\n"
                f"✅ To'lov avtomatik tasdiqlandi va foydalanuvchi faollashtirildi!"
            )
            
            # Send to admin group AI confirmations topic
            try:
                # Send to AI confirmations topic in admin group
                await self.bot_app.bot.send_message(
                    chat_id=config.GROUP_ID,
                    message_thread_id=int(config.AI_CONFIRMATIONS_TOPIC_ID),
                    text=success_message,
                    parse_mode='Markdown'
                )
                logger.info(f"AI success notification sent to admin group AI topic for payment {payment_id}")
            except Exception as e:
                logger.error(f"Failed to send AI success notification with Markdown: {e}")
                # Try without markdown if that failed
                try:
                    simple_success_message = (
                        f"🤖 AI TO'LOVNI TASDIQLADI 🎉\n\n"
                        f"💰 Miqdor: {payment['amount']} UZS\n"
                        f"👤 Foydalanuvchi: {user.get('name', 'N/A')} {user.get('surname', 'N/A')}\n"
                        f"📱 Telegram ID: {user['telegram_id']}\n"
                        f"🏷 Foydalanuvchi kodi: {user.get('code', 'N/A')}\n"
                        f"🔗 Tranzaksiya ID: {transaction_id}\n"
                        f"🎯 Ishonch darajasi: {confidence:.2f}\n"
                        f"🧠 AI sababi: {reason}\n\n"
                        f"✅ To'lov avtomatik tasdiqlandi!"
                    )
                    await self.bot_app.bot.send_message(
                        chat_id=config.GROUP_ID,
                        message_thread_id=int(config.AI_CONFIRMATIONS_TOPIC_ID),
                        text=simple_success_message
                    )
                    logger.info(f"Simple AI success notification sent to admin group AI topic for payment {payment_id}")
                except Exception as e2:
                    logger.error(f"Failed to send even simple AI success notification: {e2}")
                    
        except Exception as e:
            logger.error(f"Error notifying admins of AI success for payment {payment_id}: {e}")
    
    async def _notify_payment_verified(self, payment: Dict[str, Any]):
        """
        Notify user when their payment is verified. If fully paid, send invite link; otherwise show remaining balance.
        """
        try:
            if not self.bot_app:
                logger.error("Bot app not set - cannot notify user of payment verification")
                return
            
            # Get user details
            user = self.db_service.get_user_by_id(payment['user_id'])
            if not user:
                logger.error(f"User {payment['user_id']} not found for payment verification notification")
                return
            
            user_telegram_id = user['telegram_id']
            
            # Determine target amount: prefer user's target_amount, fallback to global settings
            try:
                settings = config_db_service.get_all_settings()
                settings_price = float(settings.get('subscription_price', 0) or 0)
            except Exception:
                settings_price = 0.0

            user_target_amount_raw = user.get('target_amount')
            try:
                user_target_amount = float(user_target_amount_raw) if user_target_amount_raw is not None else 0.0
            except Exception:
                user_target_amount = 0.0

            target_amount = user_target_amount if user_target_amount > 0 else settings_price

            try:
                # Increase paid_amount by this verified payment
                current_paid = user.get('paid_amount') or 0.0
                new_paid = float(current_paid) + float(payment.get('amount', 0))
                self.db_service.update_user_subscription_amounts(
                    telegram_id=user_telegram_id,
                    paid_amount=new_paid
                )
            except Exception:
                new_paid = float(payment.get('amount', 0))

            remaining = max(0.0, float(target_amount) - float(new_paid))

            # Only activate when target_amount is positive and fully covered
            if target_amount > 0 and remaining <= 0.001:
                # Mark payment and user as confirmed
                self.db_service.update_user_payment_step(user_telegram_id, 'confirmed')

                try:
                    link = await self.bot_app.bot.create_chat_invite_link(
                        chat_id=config.PRIVATE_CHANNEL_ID,
                        member_limit=1,
                        name=user.get('name', f'User {user_telegram_id}')
                    )
                    new_paid_str = f"{int(new_paid):,}".replace(',', ' ')
                    success_text = (
                        "\U0001F389 To'lovingiz tasdiqlandi!\n\n"
                        f"\U0001F4B0 Jami to'langan: {new_paid_str} UZS\n"
                        "\u2705 Status: Tasdiqlangan\n\n"
                        "Kanalga qo'shilish uchun havoladan foydalaning:\n\n"
                        f"{link.invite_link}"
                    )
                    await self.bot_app.bot.send_message(chat_id=user_telegram_id, text=success_text)
                except Exception as e:
                    logger.error(f"Error creating invite link or sending full-payment notification to user {user_telegram_id}: {e}")
                    try:
                        new_paid_str = f"{int(new_paid):,}".replace(',', ' ')
                        fallback_text = (
                            "\U0001F389 To'lovingiz tasdiqlandi!\n\n"
                            f"\U0001F4B0 Jami to'langan: {new_paid_str} UZS\n"
                            "\u2705 Status: Tasdiqlangan\n\n"
                            "Tez orada siz kanalga qo'shilasiz."
                        )
                        await self.bot_app.bot.send_message(chat_id=user_telegram_id, text=fallback_text)
                    except Exception as e2:
                        logger.error(f"Failed to send fallback full-payment notification to user {user_telegram_id}: {e2}")
            else:
                # Partial payment: keep awaiting_receipt and instruct remaining amount
                self.db_service.update_user_payment_step(user_telegram_id, 'awaiting_receipt')
                remaining_int = int(round(remaining))
                paid_int = int(round(new_paid))
                target_int = int(round(target_amount))
                amount_str = f"{int(payment.get('amount', 0)):,}".replace(',', ' ')
                paid_str = f"{paid_int:,}".replace(',', ' ')
                target_str = f"{target_int:,}".replace(',', ' ')
                remaining_str = f"{remaining_int:,}".replace(',', ' ')
                partial_text = (
                    f"\u2705 {amount_str} UZS qabul qilindi!\n\n"
                    f"Jami to'langan: {paid_str} UZS\n"
                    f"Qo'shilish narxi: {target_str} UZS\n\n"
                    f"💡 Qolgan summa: {remaining_str} UZS\n\n"
                    f"Kanalga qo'shilish uchun, iltimos, yana {remaining_str} UZS to'lov qiling va kvitansiya rasmini yuboring."
                )
                await self.bot_app.bot.send_message(chat_id=user_telegram_id, text=partial_text)
                    
        except Exception as e:
            logger.error(f"Error in _notify_payment_verified for payment {payment.get('id', 'unknown')}: {e}")

    def process_my_orders_button(self, message: Dict[str, Any]) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
        """
        Process the 'My Orders' button press to show user's orders.
        
        Args:
            message: Telegram message object
        
        Returns:
            Tuple[str, Optional[InlineKeyboardMarkup]]: Orders information message and keyboard
        """
        try:
            user_data = self._extract_user_data(message)
            if not user_data:
                return "❌ Xato: Sizning ma'lumotlaringizni olib bo'lmadi.", None
            
            telegram_id = user_data['telegram_id']
            user = self.db_service.get_user_by_telegram_id(telegram_id)
            
            if not user:
                return "❌ Foydalanuvchi topilmadi. Avval /start buyrug'i orqali ro'yxatdan o'ting.", None
            
            if user['reg_step'] != 'done':
                return "❌ Avval ro'yxatdan o'tishni yakunlang.", None
            
            # Get user's orders
            orders = self.db_service.get_user_orders(user['id'])
            
            if not orders:
                return "📦 Sizning buyurtmalaringiz yo'q.\n\n📷 Buyurtma berish uchun mahsulotni yuboring.", None
            
            # Format orders information
            orders_text = f"📦 Sizning buyurtmalaringiz ({len(orders)} ta):\n\n"
            
            for order in orders[:10]:  # Show last 10 orders
                status_emoji = {
                    'open': '🟢',
                    'close': '🔴', 
                    'finish': '✅'
                }.get(order.get('collection_status', ''), '⚪')
                
                status_text = {
                    'open': 'OCHIQ',
                    'close': 'YOPIQ', 
                    'finish': 'YAKUNLANGAN'
                }.get(order.get('collection_status', ''), 'NOMA\'LUM')
                
                orders_text += (
                    f"🏷 Buyurtma #{order['id']}\n"
                    f"📊 Kolleksiya: #{order['collection_id']}\n"
                    f"{status_emoji} Holat: {status_text}\n"
                    f"🔢 Seriya: {order.get('amount', 'N/A')}\n"
                    f"📅 Sana: {order.get('created_at', 'N/A')[:10]}\n\n"
                )
            
            if len(orders) > 10:
                orders_text += f"... va yana {len(orders) - 10} ta buyurtma\n\n"
            
            orders_text += "📋 Batafsil ma'lumot uchun profilingizga o'ting.\n\n"
            orders_text += "📊 Batafsil statistika uchun: /mystats"
            
            # Create inline button for user profile
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            domain = os.getenv('USER_DOMAIN', 'http://localhost:3030').rstrip('/')
            user_code = user.get('code', '')
            web_url = f"{domain}/{user_code}"
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 Profilga o'tish", url=web_url)]
            ])
            
            return orders_text, keyboard
            
        except Exception as e:
            logger.error(f"Error processing my orders button: {str(e)}")
            return "❌ Buyurtmalarni olishda xatolik yuz berdi.", None

    def process_my_profile_button(self, message: Dict[str, Any]) -> Tuple[str, Optional[Any]]:
        """
        Process the 'My Profile' button press to show user's profile.
        
        Args:
            message: Telegram message object
        
        Returns:
            Tuple[str, Optional[InlineKeyboardMarkup]]: Profile information message and inline keyboard
        """
        try:
            user_data = self._extract_user_data(message)
            if not user_data:
                return "❌ Xato: Sizning ma'lumotlaringizni olib bo'lmadi.", None
            
            telegram_id = user_data['telegram_id']
            user = self.db_service.get_user_by_telegram_id(telegram_id)
            
            if not user:
                return "❌ Foydalanuvchi topilmadi. Avval /start buyrug'i orqali ro'yxatdan o'ting.", None
            
            if user['reg_step'] != 'done':
                return "❌ Avval ro'yxatdan o'tishni yakunlang.", None
            
            # Get additional stats
            order_count = self.db_service.get_user_order_count(telegram_id)
            
            # Format profile information
            profile_text = (
                f"👤 {user.get('name', 'N/A')} {user.get('surname', '')}\n\n"
                f"🔑 Shaxsiy kodingiz: `{user.get('code', 'N/A')}`\n"
                f"📞 Telefon: {user.get('phone', 'Kiritilmagan')}\n"
                f"📧 Username: @{user.get('username', 'N/A')}\n"
                f"🆔 Telegram ID: {user.get('telegram_id', 'N/A')}\n\n"
                f"📊 Statistika:\n"
                f"📦 Jami buyurtmalar: {order_count}\n"
                f"📅 Ro'yxatga olingan: {user.get('created_at', 'N/A')[:10]}\n\n"
                f"✏️ Profil ma'lumotlarini tahrirlash: /edit"
            )
            
            # Create inline keyboard with "open profile" button
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            domain = os.getenv('USER_DOMAIN', 'http://localhost:3030').rstrip('/')
            user_code = user.get('code', '')
            web_url = f"{domain}/{user_code}"
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 Profilni ochish", url=web_url)]
            ])
            
            return profile_text, keyboard
            
        except Exception as e:
            logger.error(f"Error processing my profile button: {str(e)}")
            return "❌ Profil ma'lumotlarini olishda xatolik yuz berdi.", None

# Create a global processor instance
message_processor = MessageProcessor()
