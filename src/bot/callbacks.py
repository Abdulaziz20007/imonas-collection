"""
CallbackQuery handlers for Telegram bot.
"""
import logging
from telegram import Update, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import ContextTypes

from src.processor import message_processor
from src.config import config

logger = logging.getLogger(__name__)


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query.from_user and query.from_user.is_bot:
        return

    # If user is blocked, message_processor's check occurs in app layer; add light guard if needed.
    await query.answer()

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
