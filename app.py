"""
Main application for the Telegram Product Submission Tracker Bot and Web Interface.
Runs both the Telegram bot and web application simultaneously.
"""
import logging
import os
import asyncio
from datetime import time as dt_time
import uvicorn
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
try:
    # Optional: PTB specific timeout/network error types
    from telegram.error import TimedOut as PTBTimedOut, NetworkError as PTBNetworkError
except Exception:  # pragma: no cover - if not available, we'll fallback to string checks
    PTBTimedOut = tuple()
    PTBNetworkError = tuple()
from telegram import Update, InputMediaPhoto, InputMediaVideo, ReplyKeyboardRemove, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, ConversationHandler
from telegram.ext.filters import MessageFilter
from src.database.db_service import db_service
from src.services.report_service import report_service
from src.database.config_db_service import config_db_service
from src.config import config
from src.processor import message_processor
from src.bot.handlers import (
    start as start_handler,
    mystats as mystats_handler,
    edit_profile as edit_profile_handler,
    handle_media as handle_media_handler,
    myreports,
    send_reports_start, select_collection_for_report, handle_report_document, send_reports_cancel,
    SELECTING_COLLECTION, AWAITING_REPORTS
)
from src.bot.callbacks import button_callback as callbacks_button_callback
from src.services.data_optimizer_service import data_optimizer_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# In-memory registry for debounce tasks when JobQueue is unavailable
debounce_tasks = {}

async def check_user_blocked(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Checks if the user is blocked. If so, sends a message and returns True.
    This should be called at the beginning of user-facing handlers.
    """
    user_id = None
    if update.effective_user:
        user_id = update.effective_user.id
    
    if not user_id:
        return False # Cannot check if no user

    user = db_service.get_user_by_telegram_id(user_id)
    
    # If user exists and is_active is False (or 0)
    if user and not user.get('is_active', True):
        # Admins should not be blocked from using the bot
        if str(user_id) in getattr(config, 'ADMIN_IDS', []):
            return False
            
        message_to_reply = update.effective_message
        if not message_to_reply:
            # For callback queries, the message is under query.message
            if update.callback_query:
                message_to_reply = update.callback_query.message
        
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

def is_private_chat(update: Update) -> bool:
    """Check if the update is from a private chat."""
    return update.effective_chat.type == "private"


def is_target_group_and_topic(update: Update, target_topic_id: str) -> bool:
    """Check if the update is from the target group and specific topic."""
    if not update.effective_chat or not update.message:
        return False
    
    # Check if it's the target group
    if str(update.effective_chat.id) != config.GROUP_ID:
        return False
    
    # Check if it's the specific topic
    if update.message.message_thread_id != int(target_topic_id):
        return False
    
    return True


class FindOrdersFilter(MessageFilter):
    """Custom filter for Find orders topic messages."""
    
    def filter(self, message) -> bool:
        # Create a fake update object to use existing function
        class FakeUpdate:
            def __init__(self, message):
                self.message = message
                self.effective_chat = message.chat
        
        fake_update = FakeUpdate(message)
        return is_target_group_and_topic(fake_update, config.FIND_ORDERS_TOPIC_ID)


async def send_order_with_files(update: Update, order_text: str, file_paths: list, order_id: int) -> None:
    """Helper function to send order with files (photos/videos) efficiently."""
    try:
        if not file_paths:
            # No files for this order
            order_text += "\n❌ Fayllar topilmadi"
            await update.message.reply_text(order_text)
        elif len(file_paths) == 1:
            # Single file - send as photo or video with caption
            file_path = file_paths[0]
            if os.path.exists(file_path):
                try:
                    if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.ico', '.svg')):
                        await update.message.reply_photo(photo=file_path, caption=order_text)
                    elif file_path.lower().endswith(('.mp4', '.webm', '.mov')):
                        await update.message.reply_video(video=file_path, caption=order_text)
                    else:
                        order_text += f"\n📄 Fayl: {os.path.basename(file_path)} (qo'llab-quvvatlanmaydigan format)"
                        await update.message.reply_text(order_text)
                except Exception as e:
                    logger.error(f"Error sending file for order {order_id}: {e}")
                    order_text += f"\n📄 Fayl: {os.path.basename(file_path)} (yuklashda xatolik)"
                    await update.message.reply_text(order_text)
            else:
                order_text += f"\n📄 Fayl: {os.path.basename(file_path)} (topilmadi)"
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
                        
                        valid_files.append(os.path.basename(file_path))
                    except Exception as e:
                        logger.error(f"Error preparing file {file_path} for order {order_id}: {e}")
            
            if media_group:
                try:
                    await update.message.reply_media_group(media=media_group)
                except Exception as e:
                    logger.error(f"Error sending media group for order {order_id}: {e}")
                    # Fallback to text message with file info
                    order_text += f"\n📄 Fayllar: {', '.join(valid_files)} (yuklashda xatolik)"
                    await update.message.reply_text(order_text)
            else:
                # No valid files found
                order_text += f"\n📄 Fayllar: {', '.join([os.path.basename(p) for p in file_paths])} (hech biri topilmadi)"
                await update.message.reply_text(order_text)
    except Exception as e:
        logger.error(f"Error in send_order_with_files for order {order_id}: {e}")
        await update.message.reply_text(order_text + "\n❌ Fayllarni yuklashda xatolik")


async def prompt_for_series(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback function for the job queue to ask for the series amount after media submission."""
    job = context.job
    logger.info(f"prompt_for_series triggered for chat {job.chat_id}")
    
    # Get user_data from job data if available
    job_data = job.data if job.data else {}
    saved_user_data = job_data.get('user_data', {})
    
    logger.info(f"Job data: {job_data}")
    logger.info(f"Saved user data: {saved_user_data}")
    
    if 'order_id' in saved_user_data:
        # Non-destructive state set: track which order we're awaiting a series for
        awaiting_order_id = saved_user_data['order_id']
        context.user_data['awaiting_order_id'] = awaiting_order_id
        context.user_data['state'] = 'awaiting_series_amount'
        logger.info(f"Sending series prompt to chat {job.chat_id} for order {awaiting_order_id}")
        # Add cancel button for series prompt
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_order_{awaiting_order_id}")]])
        await context.bot.send_message(chat_id=job.chat_id, text="📝 Seriyani kiriting", reply_markup=cancel_keyboard)
        # Clear the acknowledged flag to allow new acknowledgements for the next submission
        context.user_data.pop('acknowledged', None)
    else:
        logger.info(f"No order_id found in job data for chat {job.chat_id}, skipping prompt")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the /start command."""
    if not is_private_chat(update):
        return  # Ignore messages from groups and channels

    if await check_user_blocked(update, context):
        return
        
    if update.message:
        # Don't process commands from the bot itself
        if update.message.from_user and update.message.from_user.is_bot:
            return
            
        message_dict = update.message.to_dict()
        response_text, keyboard = message_processor.process_start_command(message_dict)
        await update.message.reply_text(response_text, reply_markup=keyboard)
        
        # Send admin command keyboard as a separate message if user is admin
        user_id = message_dict.get('from', {}).get('id')
        if str(user_id) in getattr(config, 'ADMIN_IDS', []):
            from telegram import ReplyKeyboardMarkup
            admin_keyboard = [
                ['🆕 Yangi kolleksiya', '📀 Aktiv kolleksiyani ko\'rish'], 
                ['📋 Oxirgi 10 ta kolleksiya','🔍 Mahsulotlarni qidirish'],
                ['💳 Kartalar','💰 Obuna narxini o\'zgartirish'],
                ['🔗 Link olish']
            ]
            admin_markup = ReplyKeyboardMarkup(admin_keyboard, resize_keyboard=True)
            await update.message.reply_text("Admin buyruqlar menyusi:", reply_markup=admin_markup)


async def mystats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the /mystats command."""
    if not is_private_chat(update):
        return  # Ignore messages from groups and channels

    if await check_user_blocked(update, context):
        return
        
    if update.message:
        # Don't process commands from the bot itself
        if update.message.from_user and update.message.from_user.is_bot:
            return
            
        message_dict = update.message.to_dict()
        response_text = message_processor.process_mystats_command(message_dict)
        await update.message.reply_text(response_text)


async def edit_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for the /edit command."""
    if not is_private_chat(update):
        return  # Ignore messages from groups and channels

    if await check_user_blocked(update, context):
        return
        
    if update.message:
        # Don't process commands from the bot itself
        if update.message.from_user and update.message.from_user.is_bot:
            return
            
        message_dict = update.message.to_dict()
        response_text, keyboard = message_processor.process_edit_command(message_dict)
        await update.message.reply_text(response_text, reply_markup=keyboard)


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Parses the CallbackQuery and updates the message text and keyboard."""
    query = update.callback_query

    # Don't process callback queries from the bot itself
    if query.from_user and query.from_user.is_bot:
        return

    if await check_user_blocked(update, context):
        await query.answer()
        return

    await query.answer()

    # Handle admin payment confirmations from the group
    if query.data.startswith(("confirm_payment_", "cancel_payment_")):
        response = await message_processor.process_callback_query(query, context)
        if response and response.get('text'):
            # Check if the message has a photo (payment receipt) and caption
            if query.message.photo:
                # Edit the caption and remove keyboard for photo messages
                current_caption = query.message.caption or ""
                status_text = response.get('text')
                new_caption = f"{current_caption}\n\n{status_text}"
                try:
                    await query.edit_message_caption(caption=new_caption, reply_markup=None)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        logger.info("Payment message caption unchanged, skipping update")
                        pass  # Caption is identical, no update needed
                    else:
                        raise e
            else:
                # Edit text for text messages
                try:
                    await query.edit_message_text(text=response.get('text'), reply_markup=response.get('keyboard'))
                except Exception as e:
                    if "Message is not modified" in str(e):
                        logger.info("Payment message text unchanged, skipping update")
                        pass  # Text is identical, no update needed
                    else:
                        raise e
        return

    response = await message_processor.process_callback_query(query, context)
    if response:
        # Handle new format that returns both text and keyboard
        if isinstance(response, dict):
            text = response.get('text')
            keyboard = response.get('keyboard')
            if text is not None:
                # If a ReplyKeyboardMarkup is returned, we cannot attach it to an edited message.
                # Remove inline keyboard from the original message, then send a new message with the reply keyboard.
                if isinstance(keyboard, ReplyKeyboardMarkup):
                    try:
                        await query.edit_message_reply_markup(reply_markup=None)
                    except Exception:
                        pass
                    await context.bot.send_message(chat_id=query.message.chat_id, text=text, reply_markup=keyboard)
                else:
                    try:
                        parse_mode = response.get('parse_mode')
                        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode=parse_mode)
                    except Exception as e:
                        # Handle the case where message content is identical
                        if "Message is not modified" in str(e):
                            logger.info("Message content unchanged, skipping update")
                            pass  # Content is identical, no update needed
                        else:
                            # Re-raise other exceptions
                            raise e
        else:
            # Fallback for any old format (just in case)
            try:
                await query.edit_message_text(text=response)
            except Exception as e:
                # Handle the case where message content is identical
                if "Message is not modified" in str(e):
                    logger.info("Message content unchanged in fallback, skipping update")
                    pass  # Content is identical, no update needed
                else:
                    # Re-raise other exceptions
                    raise e


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for photo submissions."""
    if not is_private_chat(update):
        return  # Ignore media messages from groups and channels

    if await check_user_blocked(update, context):
        return
        
    if update.message:
        # Log forwarded message handling for debugging (safe for different origins)
        is_forwarded = bool(update.message.forward_origin)
        try:
            forward_msg_id = getattr(update.message.forward_origin, 'message_id', None) if is_forwarded else None
        except Exception:
            forward_msg_id = None
        media_group_id = getattr(update.message, 'media_group_id', None)
        logger.info(f"handle_media triggered. Update ID: {update.update_id}, Is Forwarded: {is_forwarded}, Forward Msg ID: {forward_msg_id}, Media Group ID: {media_group_id}")
        # Don't process messages from the bot itself
        if update.message.from_user and update.message.from_user.is_bot:
            return
            
        # Check if the sender is admin and ignore photos from admin
        user_id = update.message.from_user.id if update.message.from_user else None
        if str(user_id) in getattr(config, 'ADMIN_IDS', []):
            return  # Ignore photos from admins
        
        # Check if user is already awaiting series amount for a previous product
        if context.user_data.get('state') == 'awaiting_series_amount':
            order_id = context.user_data.get('awaiting_order_id') or context.user_data.get('order_id')
            if order_id:
                # Check if this media has a caption that might be intended as a series number
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
            
        success, response_text = await message_processor.process_media_submission(update, context)
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Check if this forwarded message is part of a media group
        media_group_id = getattr(update.message, 'media_group_id', None)
        
        if not success:
            if response_text:
                await update.message.reply_text(response_text)
            # If it's a media group, we might have already started an order. Clear it.
            if media_group_id and 'order_id' in context.user_data:
                context.user_data.clear()
            return

        # If successful, schedule a job to send the prompt after a delay.
        # This handles media groups by debouncing the prompt.
        # When a media group contains multiple items, each item triggers this handler,
        # but we only want to send one reply per group.
        if media_group_id:
            job_name = f"product_order_prompt_{user_id}_group_{media_group_id}"
        elif update.message.forward_origin:
            # Single forwarded media item
            forward_message_id = update.message.forward_origin.message_id
            job_name = f"product_order_prompt_{user_id}_single_{forward_message_id}"
        else:
            job_name = f"product_order_prompt_{user_id}"
        
        # Check if job_queue is available before using it
        if context.job_queue is not None:
            # Remove existing job to reset the timer
            existing_jobs = context.job_queue.get_jobs_by_name(job_name)
            for job in existing_jobs:
                job.schedule_removal()
            
            # Schedule a new job to run after a short delay
            # Pass user_data to the job context to maintain state
            job_context = {"user_data": context.user_data.copy() if context.user_data else {}}
            context.job_queue.run_once(
                prompt_for_series,
                when=int(getattr(config, 'ORDER_DELAY_TIME', 1) or 1),
                chat_id=chat_id,
                user_id=user_id,
                name=job_name,
                data=job_context,
            )
        else:
            # Fallback: debounce via asyncio to mimic 1s delay and deduplicate prompts
            try:
                import asyncio as _asyncio
                global debounce_tasks

                # Cancel any existing debounce task for this job name
                existing_task = debounce_tasks.get(job_name)
                if existing_task and not existing_task.done():
                    existing_task.cancel()

                # Snapshot user_data for restoration in the delayed prompt.
                saved_user_data = context.user_data.copy() if context.user_data else {}

                async def _delayed_prompt_series():
                    try:
                        await _asyncio.sleep(int(getattr(config, 'ORDER_DELAY_TIME', 1) or 1))
                        if 'order_id' in saved_user_data:
                            # Non-destructive set: only mark which order is awaiting series
                            awaiting_order_id = saved_user_data['order_id']
                            context.user_data['awaiting_order_id'] = awaiting_order_id
                            context.user_data['state'] = 'awaiting_series_amount'
                            # Add cancel button for series prompt
                            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                            cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_order_{awaiting_order_id}")]])
                            await context.bot.send_message(chat_id=chat_id, text="📝 Seriyani kiriting", reply_markup=cancel_keyboard)
                        else:
                            logger.info(f"No order_id found in fallback debounce for job {job_name}")
                    except Exception as _e:
                        logger.error(f"Async debounce prompt error for {job_name}: {_e}")
                    finally:
                        # Cleanup task reference
                        debounce_tasks.pop(job_name, None)

                # Schedule new debounce task
                debounce_tasks[job_name] = _asyncio.create_task(_delayed_prompt_series())

            except Exception as _fb_err:
                logger.error(f"Failed fallback debounce for job {job_name}: {_fb_err}")
                # Last resort: prompt immediately
                # Add cancel button for series prompt (fallback case)
                from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                # Use awaiting_order_id if available
                awaiting_order_id = context.user_data.get('awaiting_order_id', context.user_data.get('order_id', 'unknown'))
                cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_order_{awaiting_order_id}")]])
                await update.message.reply_text("📝 Seriyani kiriting", reply_markup=cancel_keyboard)


async def handle_group_find_orders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for text messages in the Find orders topic of the target group."""
    if update.message:
        user_id = update.message.from_user.id

        # Only process messages from admins
        if str(user_id) not in getattr(config, 'ADMIN_IDS', []):
            return

        text = update.message.text.strip()

        # Parse input to extract collection_id (optional) and user_code
        collection_id = None
        user_code = None

        if '-' in text and len(text.split('-')) == 2:
            # Format: collection_id-code
            try:
                collection_id_str, user_code = text.upper().split('-')
                collection_id = int(collection_id_str)
            except ValueError:
                await update.message.reply_text(f"❌ Noto'g'ri kod formati: `{text.upper()}`. Format: `KolleksiyaID-FoydalanuvchiKodi` yoki `FoydalanuvchiKodi`", parse_mode="Markdown")
                return
        else:
            # Format: code only
            user_code = text.upper()

        user = db_service.get_user_by_code(user_code)

        if user:
            # Send immediate response to show processing started
            processing_msg = await update.message.reply_text(f"🔍 Kod `{text.upper()}` uchun ma'lumotlar izlanmoqda...", parse_mode="Markdown")

            # Get total order count for reference
            total_orders_count = db_service.get_user_order_count(user['telegram_id'])

            # Determine order count for specific scope
            if collection_id is not None:
                # Collection-specific search
                collection_orders = db_service.get_user_orders_by_collection(user['id'], collection_id, limit=1)
                collection_order_count = len(db_service.get_user_orders_by_collection(user['id'], collection_id, limit=1000))
                scope_text = f"🏷 Kolleksiya: #{collection_id}\n📦 Bu kolleksiyada: {collection_order_count} ta buyurtma"
                callback_data_suffix = f"{user['id']}:{collection_id}"
            else:
                # All orders search
                collection_order_count = total_orders_count
                scope_text = f"🔍 Barcha kolleksiyalar"
                callback_data_suffix = f"{user['id']}:all"

            # Send user info with stats
            user_info_text = (
                f"👤 Kod bo'yicha ma'lumotlar: `{text.upper()}`\n\n"
                f"📛 Ism: {user.get('name', 'N/A')}\n"
                f"📛 Familiya: {user.get('surname', 'N/A')}\n"
                f"📞 Telefon: {user.get('phone', 'N/A')}\n"
                f"📧 Username: @{user.get('username', 'N/A')}\n"
                f"🆔 Telegram ID: {user['telegram_id']}\n"
                f"{scope_text}\n"
                f"📊 Jami buyurtmalar: {total_orders_count} ta"
            )

            # Create inline keyboard with "View Orders" button if user has orders
            keyboard = None
            if collection_order_count > 0:
                from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton(f"📦 Buyurtmalarni ko'rish ({collection_order_count})",
                                       callback_data=f"view_orders:{callback_data_suffix}")
                ]])

            await update.message.reply_text(user_info_text, parse_mode="Markdown", reply_markup=keyboard)
        else:
            await update.message.reply_text(f"❌ Kod `{user_code}` uchun foydalanuvchi ma'lumotlari topilmadi.", parse_mode="Markdown")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for text-only messages that are not commands."""
    if not is_private_chat(update):
        return  # Ignore text messages from groups and channels (except Find orders topic)

    if await check_user_blocked(update, context):
        return
        
    if update.message:
        message_dict = update.message.to_dict()
        user_id = message_dict.get('from', {}).get('id')
        
        # Don't process messages from the bot itself
        if update.message.from_user and update.message.from_user.is_bot:
            return

        # Log state and text for debugging
        try:
            _msg_text = (update.message.text or "").strip()
        except Exception:
            _msg_text = ""
        logger.info(
            f"handle_text for user {user_id}: text='{_msg_text}', "
            f"state='{context.user_data.get('state')}', edit_state='{context.user_data.get('edit_state')}'"
        )

        # Admin command processing
        if str(user_id) in getattr(config, 'ADMIN_IDS', []):
            response_text, keyboard = await message_processor.process_admin_command(message_dict, context)
            if response_text == "SHOW_COLLECTIONS_LIST":
                # Special case: show collections as separate messages
                await message_processor.send_collections_list(update, keyboard)  # keyboard contains collections data
            elif response_text == "SHOW_CARDS_LIST":
                # Special case: show cards as separate messages with control buttons
                await message_processor.send_cards_list(update, keyboard)  # keyboard contains cards data
            elif response_text == "SHOW_USER_ORDERS":
                # Special case: show user info and orders as separate messages with images
                await message_processor.send_user_orders(update, keyboard)  # keyboard contains user data
            elif response_text:
                await update.message.reply_text(response_text, reply_markup=keyboard)
                # If a new card was added, also show the cards section reply keyboard
                try:
                    if isinstance(response_text, str) and response_text.startswith("✅ Karta qo'shildi!"):
                        section_keyboard = ReplyKeyboardMarkup([["➕ Karta qo'shish", "⬅️ Asosiy menyu"]], resize_keyboard=True)
                        await update.message.reply_text("💳 Kartalar bo'limi:", reply_markup=section_keyboard)
                except Exception:
                    pass
            return

        # Check for button presses (global navigation takes priority over transient states)
        message_text = update.message.text.strip()

        # Handle "My Orders" button
        if message_text == "📦 Mening buyurtmalarim":
            # Clear transient states so navigation works from anywhere
            context.user_data.pop('edit_state', None)
            context.user_data.pop('state', None)
            context.user_data.pop('awaiting_order_id', None)
            response_text, keyboard = message_processor.process_my_orders_button(message_dict)
            await update.message.reply_text(response_text, parse_mode="Markdown", reply_markup=keyboard)
            return

        # Handle "My Profile" button  
        if message_text == "👤 Mening profilim":
            # Clear transient states so navigation works from anywhere
            context.user_data.pop('edit_state', None)
            context.user_data.pop('state', None)
            context.user_data.pop('awaiting_order_id', None)
            response_text, keyboard = message_processor.process_my_profile_button(message_dict)
            await update.message.reply_text(response_text, reply_markup=keyboard, parse_mode="Markdown")
            return

        # Check if user is editing their profile (runs only if no global nav matched)
        if context.user_data.get('edit_state'):
            edit_response, edit_keyboard = message_processor.process_edit_input(message_dict, context)
            if edit_response:
                await update.message.reply_text(edit_response, reply_markup=edit_keyboard, parse_mode="Markdown")
                return

        # Check if user is in file collection mode - ignore text input
        # if context.user_data.get('state') == 'collecting_files':
        #     await update.message.reply_text(
        #         "✅ Fayllarni qabul qilyapman. Iltimos, fayllarni yuborib tugating yoki 2 soniya kuting."
        #     )
            return

        # Check if user is replying with an amount
        if context.user_data.get('state') == 'awaiting_series_amount':
            # Ensure this is a pure text message (no media, files, or other content)
            if (update.message.photo or update.message.video or update.message.document or 
                update.message.audio or update.message.voice or update.message.sticker or 
                update.message.animation or update.message.contact or update.message.location):
                await update.message.reply_text(
                    "❌ Seriya raqamini faqat matn ko'rinishida yuboring.\n\n"
                    "📝 Iltimos, faqat raqam yuboring (masalan: 5 yoki 10)"
                )
                return
            
            response_text = await message_processor.process_series_amount_reply(update.message, context)
            await update.message.reply_text(response_text, parse_mode="Markdown")
            return

        # Check if the user is in the middle of registration
        reg_response, reg_keyboard = message_processor.process_registration_message(message_dict)
        if reg_response:
            await update.message.reply_text(reg_response, reply_markup=reg_keyboard, parse_mode="Markdown")
            return

        # Check for other button presses handled elsewhere (none here)

        # Check if user is registered and provide appropriate response
        user = db_service.get_user_by_telegram_id(user_id)
        if user and user['reg_step'] == 'done':
            # Check if user is awaiting payment receipt
            if user.get('payment_step') == 'awaiting_receipt':
                default_response = "Iltimos, faqat to'lov kvitansiyasining RASMINI yuboring."
            else:
                # Registered user - provide ordering instructions
                default_response = ("📷 Buyurtma berish uchun mahsulotni yuboring.\n\n"
                                  "Buyruqlar:\n"
                                  "/start - Boshlang'ich xabar\n"
                                  "/mystats - O'z statistikangizni ko'rish\n"
                                  "/edit - Shaxsiy ma'lumotlarni tahrirlash")
        else:
            # Unregistered user - guide to registration
            default_response = ("👋 Botdan foydalanish uchun avval ro'yxatdan o'ting.\n\n"
                              "/start buyrug'ini yuboring.")
        
        await update.message.reply_text(default_response)


async def handle_other(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for other message types that are not supported."""
    if not is_private_chat(update):
        return  # Ignore other message types from groups and channels

    if await check_user_blocked(update, context):
        return
        
    if update.message:
        # Don't process messages from the bot itself
        if update.message.from_user and update.message.from_user.is_bot:
            return
        
        # Check if user is awaiting series amount
        if context.user_data.get('state') == 'awaiting_series_amount':
            order_id = context.user_data.get('order_id')
            if order_id:
                await update.message.reply_text(
                    "❌ Seriya raqamini faqat matn ko'rinishida yuboring.\n\n"
                    "📝 Iltimos, faqat raqam yuboring (masalan: 5 yoki 10)"
                )
                return
            
        # Log what type of message triggered this handler for debugging
        message_type = []
        if update.message.sticker:
            message_type.append("sticker")
        if update.message.voice:
            message_type.append("voice")
        if update.message.document:
            message_type.append("document")
        if update.message.audio:
            message_type.append("audio")
        if update.message.animation:
            message_type.append("animation")
        
        logger.info(f"Unsupported message type received: {', '.join(message_type) if message_type else 'unknown'}")
        
        # Check if user is registered before showing the generic message
        message_dict = update.message.to_dict()
        user_id = message_dict.get('from', {}).get('id')
        
        if user_id:
            user = db_service.get_user_by_telegram_id(user_id)
            if user and user['reg_step'] == 'done':
                # Check if user is awaiting payment receipt
                if user.get('payment_step') == 'awaiting_receipt':
                    response_text = "Iltimos, faqat to'lov kvitansiyasining RASMINI yuboring."
                else:
                    # For registered users, provide helpful guidance
                    response_text = ("📷 Buyurtma berish uchun mahsulotni yuboring.\n\n"
                                   "Buyruqlar:\n"
                                   "/start - Boshlang'ich xabar\n"
                                   "/mystats - O'z statistikangizni ko'rish\n"
                                   "/edit - Shaxsiy ma'lumotlarni tahrirlash")
            else:
                # For unregistered users
                response_text = ("👋 Botdan foydalanish uchun avval ro'yxatdan o'ting.\n\n"
                               "/start buyrug'ini yuboring.")
        else:
            # Fallback message
            response_text = ("🤖 Men faqat rasmlar, videolar va matnli buyruqlarni qayta ishlay olaman.\n\n"
                           "Buyurtma berish uchun: Rasm yoki video yuboring.\n"
                           "Yordam uchun: /start buyrug'ini yuboring")
        
        await update.message.reply_text(response_text)


async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for contact messages."""
    if not is_private_chat(update):
        return  # Ignore contact messages from groups and channels

    if await check_user_blocked(update, context):
        return
        
    if update.message:
        # Don't process messages from the bot itself
        if update.message.from_user and update.message.from_user.is_bot:
            return
        
        # Check if user is awaiting series amount
        if context.user_data.get('state') == 'awaiting_series_amount':
            order_id = context.user_data.get('order_id')
            if order_id:
                await update.message.reply_text(
                    "❌ Seriya raqamini faqat matn ko'rinishida yuboring.\n\n"
                    "📝 Iltimos, faqat raqam yuboring (masalan: 5 yoki 10)"
                )
                return
            
        message_dict = update.message.to_dict()
        user_id = message_dict.get('from', {}).get('id')
        
        # Check if user is editing their profile (specifically phone)
        if context.user_data.get('edit_state') == 'edit_phone':
            edit_response, edit_keyboard = message_processor.process_contact_edit(message_dict, context)
            if edit_response:
                # Use the returned keyboard (which will be the home menu after successful edit)
                await update.message.reply_text(edit_response, reply_markup=edit_keyboard, parse_mode="Markdown")
                return
        
        # Handle contact during registration
        reg_response, reg_keyboard = await message_processor.process_contact_message(message_dict, context)
        if reg_response:
            # Check if we moved to region step or completed registration
            user = db_service.get_user_by_telegram_id(user_id)
            if user and user.get('reg_step') in ('region', 'done') and reg_keyboard:
                # Remove the contact keyboard first
                await update.message.reply_text("✅ Telefon raqami qabul qilindi!", reply_markup=ReplyKeyboardRemove(), parse_mode="Markdown")
                # Then send the main response with inline keyboard (region selection or next step)
                await update.message.reply_text(reg_response, reply_markup=reg_keyboard, parse_mode="Markdown")
            else:
                await update.message.reply_text(reg_response, reply_markup=reg_keyboard, parse_mode="Markdown")
            return
        
        # Default response for unexpected contact messages
        await update.message.reply_text("❌ Hozirda kontakt ma'lumotlari talab qilinmaydi.")


async def cleanup_old_order_files(context: ContextTypes.DEFAULT_TYPE):
    """Periodically cleans up old order files from finished collections."""
    logger.info("Running scheduled job: cleanup_old_order_files")
    try:
        expired_collections = db_service.get_expired_finished_collections(days_ago=30)
        if not expired_collections:
            logger.info("No expired collections found to clean up.")
            return

        total_files_deleted = 0
        for collection in expired_collections:
            collection_id = collection['id']
            logger.info(f"Cleaning up files for expired collection #{collection_id}")
            order_ids = db_service.get_order_ids_by_collection(collection_id)
            
            for order_id in order_ids:
                file_paths = db_service.get_order_files(order_id)
                for file_path in file_paths:
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            total_files_deleted += 1
                            logger.debug(f"Deleted old order file: {file_path}")
                        except Exception as e:
                            logger.error(f"Failed to delete file {file_path}: {e}")
        
        if total_files_deleted > 0:
            logger.info(f"Cleanup job finished. Deleted {total_files_deleted} old order files.")
        else:
            logger.info("Cleanup job finished. No files were deleted.")

    except Exception as e:
        logger.error(f"Error during cleanup_old_order_files job: {e}")


async def run_data_cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """JobQueue wrapper to run DataOptimizerService cleanup cycle."""
    logger.info("Running scheduled job: data optimizer cleanup")
    try:
        await data_optimizer_service.run_cleanup_cycle()
    except Exception as e:
        logger.error(f"Error during data optimizer cleanup job: {e}")

def setup_bot_application() -> Application:
    """Set up and configure the Telegram bot application."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    # Add handlers for different commands (delegated to src.bot)
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("mystats", mystats_handler))
    application.add_handler(CommandHandler("edit", edit_profile_handler))
    application.add_handler(CommandHandler("myreports", myreports))
    application.add_handler(CallbackQueryHandler(callbacks_button_callback))

    # Add handlers for different message types in order of priority
    
    # Handle text messages in Find orders topic (specific filter ensures only group topic messages)
    find_orders_filter = FindOrdersFilter()
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & find_orders_filter, handle_group_find_orders))
    
    # Handle photos for product submissions (using concurrent architecture)
    from src.bot.concurrent_handlers import handle_media_concurrent
    application.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, handle_media_concurrent))
    
    # Handle contact messages for registration and phone editing
    application.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    
    # Handle text messages (but not commands) for registration and regular text
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    
    # Handle all other message types (stickers, voice, video, documents, etc.)
    # This catches anything that's not a command, photo, text, or contact
    other_filters = ~filters.COMMAND & ~filters.PHOTO & ~filters.VIDEO & ~filters.TEXT & ~filters.CONTACT
    application.add_handler(MessageHandler(other_filters, handle_other))

    # Add conversation handler for sending reports
    report_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('send_reports', send_reports_start)],
        states={
            SELECTING_COLLECTION: [CallbackQueryHandler(select_collection_for_report, pattern='^report_coll_')],
            AWAITING_REPORTS: [MessageHandler(filters.Document.PDF, handle_report_document)],
        },
        fallbacks=[CommandHandler('cancel', send_reports_cancel)],
        conversation_timeout=300
    )
    application.add_handler(report_conv_handler)

    return application




async def run_bot(application: Application):
    """Run the Telegram bot with conflict handling."""
    bot_started = False
    updater_started = False
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            logger.info(f"🤖 Starting Telegram bot... (attempt {retry_count + 1}/{max_retries})")
            
            # Set bot application in message processor for notifications
            message_processor.set_bot_app(application)
            report_service.set_bot(application.bot)

            # Initialize concurrent services
            from src.services.concurrent_integration import initialize_concurrent_services
            await initialize_concurrent_services(application)
            
            await application.initialize()
            bot_started = True
            
            await application.start()
            
            # Start polling with drop_pending_updates to clear any conflicts
            await application.updater.start_polling(
                drop_pending_updates=True,
                allowed_updates=[]  # Allow all update types
            )
            updater_started = True
            
            if application.job_queue:
                application.job_queue.run_daily(
                    cleanup_old_order_files,
                    time=dt_time(hour=3, minute=0),
                    name="daily_cleanup_job"
                )
                logger.info("Scheduled daily cleanup job for old order files.")

                # Schedule DataOptimizer cleanup at 00:00 Asia/Tashkent daily
                try:
                    tz_name = getattr(config, 'TIMEZONE', 'Asia/Tashkent') or 'Asia/Tashkent'
                except Exception:
                    tz_name = 'Asia/Tashkent'
                tz = None
                try:
                    tz = ZoneInfo(tz_name)
                except Exception:
                    tz = None
                schedule_time = dt_time(hour=0, minute=0, tzinfo=tz)

                application.job_queue.run_daily(
                    run_data_cleanup_job,
                    time=schedule_time,
                    name="daily_data_optimizer_cleanup"
                )
                logger.info("Scheduled DataOptimizer cleanup daily at 00:00 Asia/Tashkent (config TIMEZONE applied if available).")

            logger.info("✅ Telegram bot started successfully")
            
            # Keep the bot running
            while True:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ Bot error (attempt {retry_count + 1}): {e}")
            
            # Check if it's a conflict error
            if "conflict" in str(e).lower() or "terminated by other getUpdates" in str(e):
                retry_count += 1
                logger.warning(f"⚠️ Bot conflict detected. Waiting 10 seconds before retry...")
                
                # Clean up current attempt
                try:
                    if updater_started:
                        await application.updater.stop()
                        updater_started = False
                except:
                    pass
                    
                try:
                    if bot_started:
                        await application.stop()
                        await application.shutdown()
                        bot_started = False
                except:
                    pass
                
                # Wait before retry
                await asyncio.sleep(10)
                
                # Recreate application for retry
                if retry_count < max_retries:
                    application = setup_bot_application()
                    message_processor.set_bot_app(application)
                    
                continue
            else:
                # Handle transient timeouts/network glitches gracefully
                is_timeout = (
                    isinstance(e, (asyncio.TimeoutError, TimeoutError)) or
                    (PTBTimedOut and isinstance(e, PTBTimedOut)) or
                    (PTBNetworkError and isinstance(e, PTBNetworkError)) or
                    "timed out" in str(e).lower()
                )
                if is_timeout:
                    logger.warning("⏳ Bot timed out. Will retry starting in 5 seconds...")
                    try:
                        if updater_started:
                            await application.updater.stop()
                            updater_started = False
                    except Exception:
                        pass
                    try:
                        if bot_started:
                            await application.stop()
                            await application.shutdown()
                            bot_started = False
                    except Exception:
                        pass
                    await asyncio.sleep(5)
                    # Recreate app and retry without incrementing retry_count
                    application = setup_bot_application()
                    message_processor.set_bot_app(application)
                    continue
                # Non-conflict, non-timeout error → escalate
                raise e
                
        break
    
    if retry_count >= max_retries:
        logger.error("❌ Failed to start bot after maximum retries")
        raise Exception("Bot failed to start after multiple attempts")
            
    # Cleanup on exit
    try:
        logger.info("🛑 Stopping Telegram bot...")

        # Shutdown concurrent services first
        try:
            from src.services.concurrent_integration import shutdown_concurrent_services
            await shutdown_concurrent_services()
        except Exception as e:
            logger.warning(f"Warning during concurrent services shutdown: {e}")

        if updater_started:
            await application.updater.stop()
        if bot_started:
            await application.stop()
            await application.shutdown()
    except Exception as e:
        logger.warning(f"Warning during bot shutdown: {e}")


def is_port_available(port, host='0.0.0.0'):
    """Check if a port is available by trying to bind to it."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return True
        except OSError:
            return False

async def run_web_server(app_instance, port, app_name, context=None):
    """Run a web server instance."""
    # Import web app here to avoid circular imports
    from src.web_app import set_bot_app
    
    # Pass bot application to web_app for sending notifications
    bot_application = context.get('bot_application') if context else None
    if bot_application:
        set_bot_app(bot_application)
    
    if not is_port_available(port):
        logger.error(f"❌ {app_name}: Port {port} is already in use")
        raise Exception(f"{app_name}: Port {port} is already in use")
        
    try:
        logger.info(f"🌐 Starting {app_name} on port {port}...")
        
        # Create uvicorn config
        config_uvicorn = uvicorn.Config(
            app=app_instance,
            host="0.0.0.0",
            port=port,
            log_level="info",
            access_log=True
        )
        
        # Create and run server
        server = uvicorn.Server(config_uvicorn)
        logger.info(f"✅ {app_name} started successfully on port {port}")
        
        # Determine which domain to show based on the app name
        if "Admin" in app_name:
            domain = os.getenv('ADMIN_DOMAIN', f'http://localhost:{port}').rstrip('/')
            logger.info(f"🌐 Admin Interface: {domain}")
        elif "User" in app_name:
            domain = os.getenv('USER_DOMAIN', f'http://localhost:{port}').rstrip('/')
            logger.info(f"🌐 User Interface: {domain}")
        
        await server.serve()
        return
        
    except Exception as e:
        logger.error(f"❌ {app_name} error on port {port}: {e}")
        raise e


async def main_async():
    """Main async function that runs both bot and web server."""
    # Initialize the database
    db_service.initialize_database()

    # Ensure at least one open collection exists on first run
    try:
        existing = db_service.get_last_collections(limit=1)
        if not existing:
            db_service.create_collection('open')
            logger.info("Initialized first open collection on startup")
    except Exception as e:
        logger.error(f"Failed ensuring initial collection: {e}")

    # Load dynamic config from database
    config.load_from_db()
    
    # Ensure thumbnail directory exists
    thumbnail_dir = os.path.join('uploads', 'thumbnail')
    if not os.path.exists(thumbnail_dir):
        os.makedirs(thumbnail_dir, exist_ok=True)
        logger.info(f"Created directory: {thumbnail_dir}")
    
    # Generate thumbnails for existing videos that don't have them
    logger.info("🎞️ Generating thumbnails for existing videos...")
    from src.processor import generate_thumbnails_for_all_videos
    generate_thumbnails_for_all_videos()

    # Import startup_userbot here to avoid circular dependency issues at module level
    from src.web_app import admin_app, user_app, startup_userbot
    
    # Set up bot application
    bot_application = setup_bot_application()
    
    logger.info("🚀 Starting Order Management System...")
    logger.info("📱 Telegram Bot: Active")
    logger.info("🤖 Userbot Client: Active")
    admin_port = os.getenv('ADMIN_PORT', '4040')
    user_port = os.getenv('USER_PORT', '3030')
    logger.info(f"🌐 Admin Web Interface on port {admin_port}")
    logger.info(f"🌐 User Web Interface on port {user_port}")
    logger.info("⏹️  Press Ctrl+C to stop all services")
    logger.info("-" * 60)
    
    context = {'bot_application': bot_application}
    
    # Create tasks for all services
    tasks = []
    
    try:
        # Create tasks for bot, web, and persistent userbot startup
        bot_task = asyncio.create_task(run_bot(bot_application))
        admin_web_task = asyncio.create_task(run_web_server(admin_app, int(admin_port), "Admin Web App", context))
        user_web_task = asyncio.create_task(run_web_server(user_app, int(user_port), "User Web App", context))
        userbot_startup_task = asyncio.create_task(startup_userbot())
        
        tasks = [bot_task, admin_web_task, user_web_task, userbot_startup_task]
        
        # Wait for any task to complete (or fail)
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        
        # Check if any task failed with an exception
        for task in done:
            if task.exception():
                logger.error(f"❌ Service failed: {task.exception()}")
                # Cancel all pending tasks
                for pending_task in pending:
                    pending_task.cancel()
                break
        
    except KeyboardInterrupt:
        logger.info("🛑 Received shutdown signal")
    except Exception as e:
        logger.error(f"❌ Application error: {e}")
    finally:
        logger.info("🔄 Shutting down services...")
        
        # Cancel any remaining tasks
        for task in tasks:
            if not task.done():
                task.cancel()
                
        # Wait for all tasks to complete cancellation
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    """Main entry point - starts both bot and web server."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("👋 Application stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        exit(1)


if __name__ == "__main__":
    # Validate configuration before starting
    # Initialize config DB first, as it might create default admin from .env
    config_db_service.initialize_database()
    # Load dynamic config from DB before validation
    config.load_from_db()
    try:
        config.validate()
        logger.info("Configuration validated successfully")
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        exit(1)

    main()