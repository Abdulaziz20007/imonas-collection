"""
CallbackQuery handlers for Telegram bot.
"""
import logging
import os
from telegram import Update, InlineKeyboardMarkup, ReplyKeyboardMarkup

# Compatibility import for FSInputFile/InputFile
try:
    from telegram import FSInputFile  # type: ignore
    _HAS_FSINPUTFILE = True
except Exception:
    try:
        from telegram import InputFile  # type: ignore
        _HAS_FSINPUTFILE = False
    except Exception:
        InputFile = None  # type: ignore
        _HAS_FSINPUTFILE = False
from telegram.ext import ContextTypes

from src.processor import message_processor
from src.config import config
from src.database.db_service import db_service

logger = logging.getLogger(__name__)


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query.from_user and query.from_user.is_bot:
        return

    # If user is blocked, message_processor's check occurs in app layer; add light guard if needed.
    await query.answer()

    # Handle new existing order callbacks
    if query.data.startswith("cancel_existing_order_"):
        try:
            order_id = int(query.data.split("_")[-1])
            user_telegram_id = query.from_user.id

            user = db_service.get_user_by_telegram_id(user_telegram_id)
            if not user:
                await query.edit_message_text(text="❌ Xatolik: Foydalanuvchi topilmadi.")
                return

            if db_service.cancel_order(order_id, user['id']):
                await query.edit_message_text(text=f"✅ Mavjud buyurtma #{order_id} bekor qilindi.")
            else:
                await query.edit_message_text(text="❌ Buyurtmani bekor qilishda xatolik yoki u sizga tegishli emas.")
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing cancel_existing_order callback: {e}")
            await query.edit_message_text(text="❌ Noto'g'ri so'rov.")
        return

    if query.data.startswith("edit_existing_order_"):
        try:
            order_id = int(query.data.split("_")[-1])

            # Set user state to reuse existing series amount flow
            context.user_data['state'] = 'awaiting_series_amount'
            context.user_data['awaiting_order_id'] = order_id
            context.user_data['is_editing_order'] = True

            # Edit the message to remove buttons, then send a new prompt
            await query.edit_message_text(
                text="Bu mahsulot allaqachon buyurtma qilingan.\n\n✏️ Tahrirlash tanlandi..."
            )
            # Use reply_text on the original message to send a new prompt
            await query.message.reply_text("📝 Seriyani kiriting")

        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing edit_existing_order callback: {e}")
            await query.edit_message_text(text="❌ Noto'g'ri so'rov.")
        return

    if query.data == "back_to_main":
        try:
            # Reset any transient user state
            context.user_data.clear()

            # Build home reply keyboard (same as /start)
            from telegram import ReplyKeyboardMarkup
            keyboard = ReplyKeyboardMarkup([
                ["📦 Mening buyurtmalarim", "👤 Mening profilim"]
            ], resize_keyboard=True)

            # Update current message and send a new home message with reply keyboard
            await query.edit_message_text(text="🏠 Asosiy menyuga qaytdingiz.")
            await query.message.reply_text(
                "👋 Xush kelibsiz! Rasm yoki video yuboring yoki menyudan tanlang.",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Error handling back_to_main: {e}")
            await query.edit_message_text(text="❌ Xatolik yuz berdi.")
        return

    if query.data.startswith("get_report_"):
        try:
            report_id = int(query.data.split('_')[-1])
            user_telegram_id = query.from_user.id
            user = db_service.get_user_by_telegram_id(user_telegram_id)
            if not user:
                await query.edit_message_text("Xatolik: Foydalanuvchi topilmadi.")
                return

            # Get report from DB
            conn = db_service._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM user_reports WHERE id = ?", (report_id,))
            report = cursor.fetchone()

            if not report or report['user_id'] != user['id']:
                await query.edit_message_text("Xatolik: Hisobot topilmadi yoki sizga tegishli emas.")
                return
            
            file_path = report['file_path']
            if not os.path.exists(file_path):
                await query.edit_message_text("Xatolik: Hisobot fayli serverda topilmadi.")
                return

            if _HAS_FSINPUTFILE:
                await context.bot.send_document(
                    chat_id=user_telegram_id,
                    document=FSInputFile(file_path, filename=f"Hisobot-{report['collection_id']}.pdf")
                )
            else:
                if InputFile is None:
                    raise RuntimeError("python-telegram-bot InputFile not available")
                with open(file_path, "rb") as f:
                    await context.bot.send_document(
                        chat_id=user_telegram_id,
                        document=InputFile(f, filename=f"Hisobot-{report['collection_id']}.pdf")
                    )
            await query.edit_message_text(f"Kolleksiya #{report['collection_id']} uchun hisobot yuborildi.")
        except Exception as e:
            logger.error(f"Failed to send report document via bot: {e}")
            await query.edit_message_text("Hisobotni yuborishda xatolik yuz berdi.")
        return

    if query.data.startswith(("confirm_payment_", "cancel_payment_")):
        response = await message_processor.process_callback_query(query, context)
        if response and response.get('text'):
            if query.message.photo:
                current_caption = query.message.caption or ""
                status_text = response.get('text')
                new_caption = f"{current_caption}\n\n{status_text}"
                try:
                    await query.edit_message_caption(caption=new_caption, reply_markup=None)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        logger.info("Payment message caption unchanged, skipping update")
                    else:
                        raise e
            else:
                try:
                    keyboard = response.get('keyboard')
                    text = response.get('text')
                    # Only inline keyboards are allowed on edit_message_text
                    if isinstance(keyboard, InlineKeyboardMarkup) or keyboard is None:
                        await query.edit_message_text(text=text, reply_markup=keyboard)
                    else:
                        # Send a new message with reply keyboard instead of editing
                        await query.edit_message_text(text=text, reply_markup=None)
                        await query.message.reply_text(text, reply_markup=keyboard)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        logger.info("Payment message text unchanged, skipping update")
                    else:
                        raise e
        return

    response = await message_processor.process_callback_query(query, context)
    if response:
        if isinstance(response, dict):
            text = response.get('text')
            keyboard = response.get('keyboard')
            if text is not None:
                try:
                    parse_mode = response.get('parse_mode')
                    # Only inline keyboards can be used with edit_message_text
                    if isinstance(keyboard, InlineKeyboardMarkup) or keyboard is None:
                        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode=parse_mode)
                    elif isinstance(keyboard, ReplyKeyboardMarkup):
                        # Edit original message without keyboard, then send new message with reply keyboard
                        await query.edit_message_text(text=text, reply_markup=None, parse_mode=parse_mode)
                        await query.message.reply_text(text, reply_markup=keyboard, parse_mode=parse_mode)
                    else:
                        await query.edit_message_text(text=text, reply_markup=None, parse_mode=parse_mode)
                except Exception as e:
                    if "Message is not modified" in str(e):
                        logger.info("Message content unchanged, skipping update")
                    else:
                        raise e
        else:
            try:
                await query.edit_message_text(text=response)
            except Exception as e:
                if "Message is not modified" in str(e):
                    logger.info("Message content unchanged in fallback, skipping update")
                else:
                    raise e
