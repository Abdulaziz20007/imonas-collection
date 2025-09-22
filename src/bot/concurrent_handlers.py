"""
Concurrent Telegram bot handlers using the new non-blocking architecture.
These handlers implement the fully concurrent, non-blocking design.
"""
import asyncio
import logging
from typing import Dict, Any, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from src.database.db_service import db_service
from src.services.async_db_service import async_db_service
from src.services.order_processing_service import order_processing_service
from src.services.task_orchestrator import get_task_orchestrator
from src.config import config

logger = logging.getLogger(__name__)


def is_private_chat(update: Update) -> bool:
    """Check if the update is from a private chat."""
    return update.effective_chat and update.effective_chat.type == "private"


async def check_user_blocked(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is blocked (non-blocking version)."""
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return False

    # Use async database service
    user = await async_db_service.get_user_by_telegram_id(user_id)

    if user and not user.get('is_active', True):
        if str(user_id) in getattr(config, 'ADMIN_IDS', []):
            return False

        message_to_reply = update.effective_message or (update.callback_query.message if update.callback_query else None)
        if message_to_reply:
            try:
                await message_to_reply.reply_text(
                    "🚫 *Kechirasiz, sizning hisobingiz vaqtincha cheklangan*\n\n"
                    "📞 Murojaat uchun admin bilan bog'laning.",
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.warning(f"Could not reply to blocked user {user_id}: {e}")
        return True

    return False


async def handle_media_concurrent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Concurrent media handler with instant user feedback.
    Implements the new fully non-blocking architecture.
    """
    if not is_private_chat(update):
        return

    if await check_user_blocked(update, context):
        return

    if not update.message:
        return

    # Don't process messages from bots
    if update.message.from_user and update.message.from_user.is_bot:
        return

    user_id = update.message.from_user.id if update.message.from_user else None
    if not user_id:
        return

    # Ignore media from admins
    if str(user_id) in getattr(config, 'ADMIN_IDS', []):
        return

    # Check if user is awaiting series amount
    if context.user_data.get('state') == 'awaiting_series_amount':
        order_id = context.user_data.get('awaiting_order_id') or context.user_data.get('order_id')
        if order_id:
            caption = update.message.caption
            if caption and caption.strip().isdigit():
                await update.message.reply_text(
                    "❌ Seriya raqamini rasm yoki video bilan birga yuborish mumkin emas.\n\n"
                    "📝 Iltimos, faqat matn ko'rinishida raqam yuboring (masalan: 5 yoki 10)"
                )
            else:
                await update.message.reply_text(
                    "❌ Avval yuborgan mahsulot uchun seriyani kiriting.\n\n"
                    "📝 Iltimos, faqat raqam yuboring (masalan: 5 yoki 10)"
                )
            return

    try:
        # Extract message information
        message = update.message

        # Check user registration and state (non-blocking) early to handle payment receipts first
        user = await async_db_service.get_user_by_telegram_id(user_id)
        if user:
            try:
                logger.info(f"User {user_id} entered handle_media_concurrent with payment_step: {user.get('payment_step')}")
            except Exception:
                pass

        # If user is expected to send a payment receipt, delegate to legacy processor which has correct flow
        if user and user.get('payment_step') == 'awaiting_receipt':
            try:
                from src.processor import message_processor
                await message_processor.process_media_submission(update, context)
            except Exception as receipt_error:
                logger.error(f"Error delegating receipt processing for user {user_id}: {receipt_error}")
                await message.reply_text("❌ To'lov kvitansiyasini qayta ishlashda xatolik. Iltimos, qayta yuboring.")
            return

        # For product submissions, require forwarded media
        is_forwarded = bool(message.forward_origin)

        if not is_forwarded:
            await message.reply_text(
                "❌ Iltimos, mahsulotni faqat bizning kanalimizdan yuboring."
            )
            return

        # Extract forwarded message details
        try:
            forward_origin = message.forward_origin
            if hasattr(forward_origin, 'chat') and hasattr(forward_origin, 'message_id'):
                channel_id = forward_origin.chat.id
                forward_message_id = forward_origin.message_id
            else:
                await message.reply_text("❌ Mahsulotni faqat bizning kanalimizdan yuboring.")
                return
        except Exception as e:
            logger.error(f"Error extracting forward origin: {e}")
            await message.reply_text("❌ Mahsulotni faqat bizning kanalimizdan yuboring.")
            return

        # Ensure user is registered (non-blocking)
        if not user or user['reg_step'] != 'done':
            await message.reply_text(
                "Buyurtma yuborish uchun avval /start buyrug'i orqali ro'yxatdan o'ting."
            )
            return

        # Handle payment workflow (test account bypass)
        is_test_account = config.TEST_ACCOUNT_ID and user['telegram_id'] == config.TEST_ACCOUNT_ID
        if user.get('payment_step') != 'confirmed' and not is_test_account:
            if user.get('payment_step') == 'awaiting_receipt':
                await message.reply_text("Iltimos, faqat to'lov kvitansiyasining RASMINI yuboring.")
            else:
                await message.reply_text(
                    "Buyurtma berish uchun avval obuna to'lovini amalga oshiring.\n"
                    "📞 Admin bilan bog'laning: @your_admin"
                )
            return

        # Get file information
        if message.photo:
            file_to_download = message.photo[-1]
            media_type = 'photo'
        elif message.video:
            file_to_download = message.video
            media_type = 'video'
        else:
            await message.reply_text("❌ Faqat rasm yoki video yuborishingiz mumkin.")
            return

        file_unique_id = file_to_download.file_unique_id

        # INSTANT VALIDATION: Check if product exists using userbot (non-blocking)
        try:
            # Import userbot validation here to avoid circular imports
            from src.web_app import validate_product_exists_async

            product_exists = await validate_product_exists_async(channel_id, forward_message_id)

            if not product_exists:
                await message.reply_text("❌ Bu mahsulot bazada topilmadi.")
                return

        except Exception as validation_error:
            logger.error(f"Product validation error: {validation_error}")
            await message.reply_text(
                "❌ Mahsulotni tekshirishda xatolik yuz berdi. Keyinroq urinib ko'ring."
            )
            return

        # Get active collection (non-blocking)
        active_collection = await async_db_service.get_active_collection()
        if not active_collection:
            await message.reply_text(
                "❌ Hozirda faol kolleksiya mavjud emas. Admin bilan bog'laning."
            )
            return

        # Check current user state for file collection
        user_state = context.user_data.get('state')

        if user_state == 'collecting_files':
            # User is in file collection mode - add to existing order
            order_id = context.user_data.get('collecting_order_id')
            if not order_id:
                # State inconsistency - reset and create new order
                logger.warning(f"User {user_id} in collecting_files state but no collecting_order_id found")
                context.user_data.clear()
                order_id = await async_db_service.create_order(
                    user_id=user['id'],
                    collection_id=active_collection['id'],
                    amount=None,
                    original_message_id=forward_message_id,
                    original_channel_id=channel_id
                )
                if not order_id:
                    await message.reply_text("❌ Buyurtma yaratishda xatolik yuz berdi.")
                    return
                context.user_data['collecting_order_id'] = order_id
                context.user_data['state'] = 'collecting_files'

            logger.info(f"Adding file to collecting order {order_id}")


        else:
            # First file - create new order and enter collection state
            order_id = await async_db_service.create_order(
                user_id=user['id'],
                collection_id=active_collection['id'],
                amount=None,  # Will be set when user provides series
                original_message_id=forward_message_id,
                original_channel_id=channel_id
            )
            if not order_id:
                await message.reply_text("❌ Buyurtma yaratishda xatolik yuz berdi.")
                return

            # Set file collection state
            context.user_data['state'] = 'collecting_files'
            context.user_data['collecting_order_id'] = order_id

            logger.info(f"Created new order {order_id} and entered file collection mode")


        # ADD FILE TO ORDER: Immediately add file placeholder to order
        # Use placeholder entry (status=pending) to avoid premature notifications
        await async_db_service.add_order_file_placeholder(order_id, file_unique_id)
        logger.info(f"Added file placeholder {file_unique_id} to order {order_id}")

        # MANAGE FINALIZATION TIMER: Schedule or reschedule 2-second timer
        await _schedule_file_collection_finalization(context, order_id, user_id)

        # REMOVE IMMEDIATE NOTIFICATION: Only send final notification after series is provided
        # This prevents the confusing "Kutilmoqda..." and "Yuklanmoqda..." messages
        logger.info(f"Skipping immediate notification for order {order_id} - will send final notification after series is provided")

        # BACKGROUND PROCESSING: Start file processing in background
        # This does not block the user interface
        orchestrator = get_task_orchestrator()

        # Extract bot token for background processing (context may not be thread-safe)
        bot_token = context.bot.token if context.bot else None

        # Submit background task without waiting for it
        background_task = orchestrator.submit_nowait(
            _process_order_background_sync,
            channel_id,
            forward_message_id,
            file_unique_id,
            media_type,
            order_id,
            user_id,
            file_to_download.file_id,  # Pass file_id for actual download
            bot_token  # Pass bot token instead of context
        )

        # Log the background task submission
        logger.info(f"Submitted background processing for order {order_id}, file {file_unique_id}")

        # Store background task reference for potential cancellation
        context.user_data['background_task'] = background_task

    except Exception as e:
        logger.error(f"Error in handle_media_concurrent: {e}", exc_info=True)
        await message.reply_text(
            "❌ Buyurtma qayta ishlanishida xatolik yuz berdi. Keyinroq urinib ko'ring."
        )


def _process_order_background_sync(
    channel_id: int,
    message_id: int,
    file_unique_id: str,
    media_type: str,
    order_id: int,
    user_telegram_id: int,
    file_id: str,
    bot_token: str
) -> Dict[str, Any]:
    """
    Synchronous wrapper for background order processing.
    This runs in the thread pool and handles async calls internally.
    """
    try:
        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Run the async processing
            result = loop.run_until_complete(
                _process_order_background_async(
                    channel_id, message_id, file_unique_id,
                    media_type, order_id, user_telegram_id, file_id, bot_token
                )
            )
            return result
        finally:
            loop.close()

    except Exception as e:
        logger.error(f"Error in background sync wrapper: {e}", exc_info=True)
        return {
            "success": False,
            "order_id": order_id,
            "error": str(e)
        }


async def _process_order_background_async(
    channel_id: int,
    message_id: int,
    file_unique_id: str,
    media_type: str,
    order_id: int,
    user_telegram_id: int,
    file_id: str,
    bot_token: str
) -> Dict[str, Any]:
    """Async background processing of the order."""
    try:
        # Check if product_media already exists
        existing_media = await async_db_service.get_product_media_by_file_id(file_unique_id)

        if existing_media and existing_media.get('status') == 'available':
            logger.info(f"File {file_unique_id} already available, reusing")

            # Complete file for order
            if existing_media.get('file_path'):
                await async_db_service.complete_order_file(order_id, file_unique_id, existing_media['file_path'])

            await _notify_admin_order_ready(order_id, bot_token)

            # Check if order is complete and send realtime notification
            await _check_and_send_realtime_notification(order_id, bot_token)
            return {
                "success": True,
                "order_id": order_id,
                "file_path": existing_media.get('file_path'),
                "reused_existing": True
            }

        # Create or update product_media entry
        if not existing_media:
            media_data = {
                "source_channel_id": channel_id,
                "source_message_id": message_id,
                "file_unique_id": file_unique_id,
                "media_type": media_type,
                "file_path": "",
                "thumbnail_path": None,
                "status": "pending"
            }
            await async_db_service.create_product_media(media_data)

        # Set status to downloading
        await async_db_service.update_product_media_status(file_unique_id, "downloading")

        # Perform actual download
        download_result = await _download_file_async(file_id, file_unique_id, media_type, bot_token)

        if download_result["success"]:
            # Update product_media with paths
            orchestrator = get_task_orchestrator()
            await orchestrator.submit(
                _update_product_media_paths_sync,
                file_unique_id,
                download_result["file_path"],
                download_result.get("thumbnail_path")
            )

            # Set status to available
            await async_db_service.update_product_media_status(file_unique_id, "available")

            # Complete file for order
            await async_db_service.complete_order_file(order_id, file_unique_id, download_result["file_path"]) 

            # Notify admin
            await _notify_admin_order_ready(order_id, bot_token)

            # Check if order is complete and send realtime notification
            await _check_and_send_realtime_notification(order_id, bot_token)

            logger.info(f"Successfully processed order {order_id}")
            return download_result

        else:
            # Set status to failed
            await async_db_service.update_product_media_status(file_unique_id, "failed")
            await _notify_user_order_failed(user_telegram_id, order_id, download_result.get("error"), bot_token)

            logger.error(f"Failed to process order {order_id}: {download_result.get('error')}")
            return download_result

    except Exception as e:
        logger.error(f"Error in background async processing: {e}", exc_info=True)

        # Set status to failed
        try:
            await async_db_service.update_product_media_status(file_unique_id, "failed")
            await _notify_user_order_failed(user_telegram_id, order_id, str(e), bot_token)
        except Exception as notify_error:
            logger.error(f"Failed to notify about error: {notify_error}")

        return {
            "success": False,
            "order_id": order_id,
            "error": str(e)
        }


async def _download_file_async(file_id: str, file_unique_id: str, media_type: str, bot_token: str) -> Dict[str, Any]:
    """Download file using bot API."""
    result = {
        "success": False,
        "file_path": None,
        "thumbnail_path": None,
        "error": None
    }

    try:
        # Import required modules
        import os
        from telegram import Bot

        # Create a bot instance from the token
        bot = Bot(token=bot_token)

        # Generate file path
        file_extension = '.jpg' if media_type == 'photo' else '.mp4'
        file_path = os.path.join('uploads', f"{file_unique_id}{file_extension}")

        # Ensure uploads directory exists
        os.makedirs('uploads', exist_ok=True)

        # Download file
        file_obj = await bot.get_file(file_id)
        await file_obj.download_to_drive(file_path)

        result["file_path"] = file_path
        result["success"] = True

        # Generate thumbnail for videos
        if media_type == 'video':
            try:
                from src.services.file_service import generate_video_thumbnail_to_dir
                thumbnail_dir = os.path.join('uploads', 'thumbnail')
                os.makedirs(thumbnail_dir, exist_ok=True)

                orchestrator = get_task_orchestrator()
                thumbnail_path = await orchestrator.submit(
                    generate_video_thumbnail_to_dir, file_path, thumbnail_dir
                )
                result["thumbnail_path"] = thumbnail_path

            except Exception as thumb_error:
                logger.warning(f"Thumbnail generation failed: {thumb_error}")
                # Don't fail the whole process for thumbnail errors

        logger.info(f"Successfully downloaded file {file_unique_id} to {file_path}")

    except Exception as e:
        logger.error(f"File download failed for {file_unique_id}: {e}")
        result["error"] = str(e)

    return result


def _update_product_media_paths_sync(file_unique_id: str, file_path: str, thumbnail_path: Optional[str]):
    """Update product_media file paths (sync version for thread pool)."""
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


async def _notify_admin_order_ready(order_id: int, bot_token: str):
    """Notify admin that order is ready."""
    try:
        from telegram import Bot
        bot = Bot(token=bot_token)

        admin_group_id = config.GROUP_ID
        admin_topic_id = getattr(config, 'ORDERS_TOPIC_ID', None)

        if admin_group_id:
            message = f"✅ Order #{order_id} processed successfully\n📦 File downloaded and ready"

            if admin_topic_id:
                await bot.send_message(
                    chat_id=admin_group_id,
                    text=message,
                    message_thread_id=int(admin_topic_id)
                )
            else:
                await bot.send_message(chat_id=admin_group_id, text=message)

    except Exception as e:
        logger.error(f"Failed to notify admin about order {order_id}: {e}")


async def _notify_user_order_failed(user_telegram_id: int, order_id: int, error: str, bot_token: str):
    """Notify user that order processing failed."""
    try:
        from telegram import Bot
        bot = Bot(token=bot_token)

        await bot.send_message(
            chat_id=user_telegram_id,
            text=f"❌ Kechirasiz, buyurtma #{order_id} ni qayta ishlab bo'lmadi.\n\n"
                 f"Iltimos, keyinroq qayta urinib ko'ring yoki admin bilan bog'laning."
        )

    except Exception as e:
        logger.error(f"Failed to notify user {user_telegram_id} about order {order_id} failure: {e}")

        # If user notification fails, notify admin
        try:
            admin_group_id = config.GROUP_ID
            if admin_group_id:
                admin_message = (
                    f"❌ Order #{order_id} failed and user notification failed\n"
                    f"User ID: {user_telegram_id}\n"
                    f"Error: {error}"
                )
                await bot.send_message(chat_id=admin_group_id, text=admin_message)
        except Exception as admin_notify_error:
            logger.error(f"Failed to notify admin about notification failure: {admin_notify_error}")


async def _check_and_send_realtime_notification(order_id: int, bot_token: str):
    """Check if all files for an order are downloaded and send realtime notification if ready."""
    try:
        from telegram import Bot
        from src.database.db_service import db_service

        bot = Bot(token=bot_token)

        # Get order details to check if it has an amount (series) set
        order = db_service.get_order_by_id(order_id)
        if not order or order.get('amount') is None:
            logger.debug(f"Order {order_id} not ready for notification - no amount set")
            return

        # Check if all files are downloaded
        statuses = db_service.get_order_file_statuses(order_id)
        if not statuses or not all(s == 'downloaded' for s in statuses):
            logger.debug(f"Order {order_id} not ready for notification - files still downloading. Statuses: {statuses}")
            return

        # All files are downloaded and order has amount - send realtime notification
        logger.info(f"Order {order_id} is complete - sending realtime notification")

        # Create a mock context for the notification
        class MockContext:
            def __init__(self, bot_instance):
                self.bot = bot_instance

        mock_context = MockContext(bot)

        # Import here to avoid circular imports
        from src.processor import message_processor
        finalized = await message_processor.attempt_to_finalize_order(order_id, mock_context)
        if finalized:
            logger.info(f"Successfully sent final notification for completed order {order_id}")
        else:
            logger.debug(f"Order {order_id} not ready for final notification or already sent")

    except Exception as e:
        logger.error(f"Failed to check/send realtime notification for order {order_id}: {e}")


async def _send_immediate_realtime_notification(
    order_id: int,
    user_telegram_id: int,
    collection_id: int,
    file_to_download,
    media_type: str,
    channel_id: int,
    message_id: int,
    bot_token: str,
    is_additional_file: bool = False
):
    """Send realtime notification immediately when order is created, including the original file."""
    try:
        from telegram import Bot
        from src.database.db_service import db_service
        from datetime import datetime
        import asyncio

        bot = Bot(token=bot_token)

        # Get user details
        user = db_service.get_user_by_telegram_id(user_telegram_id)
        if not user:
            logger.error(f"Could not find user {user_telegram_id} for immediate notification")
            return

        # Construct user's code for this collection
        user_code = f"{collection_id}-{user.get('code', 'N/A')}"

        # Get current timestamp
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Format the notification message
        if is_additional_file:
            notification_text = (
                f"➕ Qo'shimcha fayl!\n\n"
                f"👤 Foydalanuvchi: {user.get('name', 'N/A')} {user.get('surname', '')}\n"
                f"📞 Telefon: {user.get('phone', 'N/A')}\n"
                f"🏷 Kod: {user_code}\n"
                f"📦 Buyurtma ID: #{order_id}\n"
                f"🏷 Kolleksiya: #{collection_id}\n"
                f"🔢 Serya: Kutilmoqda...\n"
                f"📷 Qo'shimcha fayl yuklanmoqda...\n"
                f"⏰ Vaqt: {current_time}"
            )
        else:
            notification_text = (
                f"🆕 Yangi buyurtma!\n\n"
                f"👤 Foydalanuvchi: {user.get('name', 'N/A')} {user.get('surname', '')}\n"
                f"📞 Telefon: {user.get('phone', 'N/A')}\n"
                f"🏷 Kod: {user_code}\n"
                f"📦 Buyurtma ID: #{order_id}\n"
                f"🏷 Kolleksiya: #{collection_id}\n"
                f"🔢 Serya: Kutilmoqda...\n"
                f"📷 Fayl: Yuklanmoqda...\n"
                f"⏰ Vaqt: {current_time}"
            )

        # Send the notification to the realtime orders topic with retries and timeout
        if config.GROUP_ID and config.REALTIME_ORDERS_TOPIC_ID:
            max_retries = 3
            timeout_seconds = 60  # 1 minute timeout for large files

            for attempt in range(max_retries):
                try:
                    # Send original file with notification using timeout
                    if media_type == 'photo':
                        await asyncio.wait_for(
                            bot.send_photo(
                                chat_id=config.GROUP_ID,
                                photo=file_to_download.file_id,
                                caption=notification_text,
                                message_thread_id=int(config.REALTIME_ORDERS_TOPIC_ID)
                            ),
                            timeout=timeout_seconds
                        )
                    elif media_type == 'video':
                        await asyncio.wait_for(
                            bot.send_video(
                                chat_id=config.GROUP_ID,
                                video=file_to_download.file_id,
                                caption=notification_text,
                                message_thread_id=int(config.REALTIME_ORDERS_TOPIC_ID)
                            ),
                            timeout=timeout_seconds
                        )
                    else:
                        # Fallback to text message
                        await asyncio.wait_for(
                            bot.send_message(
                                chat_id=config.GROUP_ID,
                                text=notification_text,
                                message_thread_id=int(config.REALTIME_ORDERS_TOPIC_ID)
                            ),
                            timeout=30  # Shorter timeout for text messages
                        )

                    logger.info(f"Successfully sent immediate realtime notification for order {order_id} (attempt {attempt + 1})")
                    break  # Success, exit retry loop

                except asyncio.TimeoutError:
                    logger.warning(f"Timeout sending immediate notification for order {order_id} (attempt {attempt + 1})")
                    if attempt == max_retries - 1:
                        # Last attempt failed, send fallback text message
                        try:
                            await asyncio.wait_for(
                                bot.send_message(
                                    chat_id=config.GROUP_ID,
                                    text=notification_text + f"\n⏱ Media yuklanishda timeout (fayl juda katta)",
                                    message_thread_id=int(config.REALTIME_ORDERS_TOPIC_ID)
                                ),
                                timeout=30
                            )
                            logger.info(f"Sent fallback text notification for order {order_id} after timeout")
                        except Exception as fallback_error:
                            logger.error(f"Fallback notification after timeout also failed for order {order_id}: {fallback_error}")
                    else:
                        # Wait before retry
                        await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff

                except Exception as e:
                    logger.error(f"Error sending immediate realtime notification for order {order_id} (attempt {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        # Last attempt failed, send fallback text message
                        try:
                            await asyncio.wait_for(
                                bot.send_message(
                                    chat_id=config.GROUP_ID,
                                    text=notification_text + f"\n❌ Media yuklanmadi: {str(e)[:50]}",
                                    message_thread_id=int(config.REALTIME_ORDERS_TOPIC_ID)
                                ),
                                timeout=30
                            )
                            logger.info(f"Sent fallback text notification for order {order_id} after error")
                        except Exception as fallback_error:
                            logger.error(f"Fallback notification also failed for order {order_id}: {fallback_error}")
                    else:
                        # Wait before retry
                        await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
        else:
            logger.warning("GROUP_ID or REALTIME_ORDERS_TOPIC_ID not configured for immediate notification")

    except Exception as e:
        logger.error(f"Failed to send immediate realtime notification for order {order_id}: {e}", exc_info=True)


async def _schedule_file_collection_finalization(context: ContextTypes.DEFAULT_TYPE, order_id: int, user_id: int) -> None:
    """Schedule or reschedule the file collection finalization timer."""
    try:
        # Create unique job name for this user
        job_name = f"finalize_order_{user_id}"

        # Cancel any existing finalization job for this user
        if context.job_queue:
            existing_jobs = context.job_queue.get_jobs_by_name(job_name)
            for job in existing_jobs:
                job.schedule_removal()
                logger.debug(f"Cancelled existing finalization job {job_name}")

        # Store job name in user_data for cleanup purposes
        context.user_data['finalization_job_name'] = job_name

        # Schedule new finalization job (configurable delay)
        delay_time = int(getattr(config, 'ORDER_DELAY_TIME', 2) or 2)  # Default to 2 seconds if not set
        if context.job_queue:
            context.job_queue.run_once(
                finalize_file_collection,
                when=delay_time,  # Use configurable delay from ORDER_DELAY_TIME
                chat_id=context._chat_id,
                user_id=user_id,
                name=job_name,
                data={'order_id': order_id, 'user_id': user_id}
            )
            logger.info(f"Scheduled file collection finalization for order {order_id} in {delay_time} seconds")
        else:
            # Fallback using asyncio if JobQueue is not available
            logger.warning("JobQueue not available, using asyncio fallback for finalization")

            # Cancel existing task if any
            existing_task = context.user_data.get('finalization_task')
            if existing_task and not existing_task.done():
                existing_task.cancel()

            # Create new delayed task
            async def delayed_finalization():
                try:
                    await asyncio.sleep(delay_time)
                    await _finalize_file_collection_direct(context, order_id, user_id)
                except asyncio.CancelledError:
                    logger.debug(f"Finalization task cancelled for order {order_id}")
                except Exception as e:
                    logger.error(f"Error in delayed finalization for order {order_id}: {e}")

            task = asyncio.create_task(delayed_finalization())
            context.user_data['finalization_task'] = task

    except Exception as e:
        logger.error(f"Error scheduling file collection finalization: {e}")


async def finalize_file_collection(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    JobQueue callback to finalize file collection and prompt for series.
    This function is called when the 2-second timer expires.
    """
    try:
        job = context.job
        if not job or not job.data:
            logger.error("finalize_file_collection called without job data")
            return

        order_id = job.data.get('order_id')
        user_id = job.data.get('user_id')

        if not order_id or not user_id:
            logger.error("finalize_file_collection missing order_id or user_id")
            return

        await _finalize_file_collection_direct(context, order_id, user_id)

    except Exception as e:
        logger.error(f"Error in finalize_file_collection callback: {e}")


async def _finalize_file_collection_direct(context: ContextTypes.DEFAULT_TYPE, order_id: int, user_id: int) -> None:
    """Direct implementation of file collection finalization."""
    try:
        # Check if user is still in collecting_files state
        if context.user_data.get('state') != 'collecting_files':
            logger.debug(f"User {user_id} no longer in collecting_files state, skipping finalization")
            return

        # Check if the order_id matches
        if context.user_data.get('collecting_order_id') != order_id:
            logger.debug(f"Order ID mismatch for user {user_id}, skipping finalization")
            return

        # Transition user state to awaiting series amount
        context.user_data['state'] = 'awaiting_series_amount'
        context.user_data['awaiting_order_id'] = order_id

        # Clean up collection-specific state
        context.user_data.pop('collecting_order_id', None)
        context.user_data.pop('finalization_job_name', None)
        context.user_data.pop('finalization_task', None)

        # Send series prompt to user
        cancel_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_order_{order_id}")]
        ])

        await context.bot.send_message(
            chat_id=user_id,
            text="📝 Seriyani kiriting",
            reply_markup=cancel_keyboard
        )

        logger.info(f"Successfully finalized file collection for order {order_id} and prompted for series")

    except Exception as e:
        logger.error(f"Error in _finalize_file_collection_direct for order {order_id}: {e}")
