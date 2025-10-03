"""
Telegram bot handlers layer.
Thin wrappers around service layer and message_processor for gradual migration.
"""
import os
import asyncio
import logging
from datetime import time as dt_time
from typing import Any, Dict, Optional

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, MessageHandler, filters, ConversationHandler

from src.database.db_service import db_service
from src.config import config
from src.processor import message_processor

logger = logging.getLogger(__name__)


def is_private_chat(update: Update) -> bool:
    return update.effective_chat and update.effective_chat.type == "private"


async def check_user_blocked(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return False
    user = db_service.get_user_by_telegram_id(user_id)
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    if await check_user_blocked(update, context):
        return
    if update.message:
        if update.message.from_user and update.message.from_user.is_bot:
            return
        message_dict = update.message.to_dict()
        response_text, keyboard = message_processor.process_start_command(message_dict)
        await update.message.reply_text(response_text, reply_markup=keyboard)

        user_id = message_dict.get('from', {}).get('id')
        if str(user_id) in getattr(config, 'ADMIN_IDS', []):
            admin_keyboard = [
                ['🆕 Yangi kolleksiya', '📀 Aktiv kolleksiyani ko\'rish'],
                ['📋 Oxirgi 10 ta kolleksiya','🔍 Mahsulotlarni qidirish'],
                ['💳 Kartalar','💰 Obuna narxini o\'zgartirish'],
                ['🔗 Link olish']
            ]
            admin_markup = ReplyKeyboardMarkup(admin_keyboard, resize_keyboard=True)
            await update.message.reply_text("Admin buyruqlar menyusi:", reply_markup=admin_markup)


async def mystats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    if await check_user_blocked(update, context):
        return
    if update.message:
        if update.message.from_user and update.message.from_user.is_bot:
            return
        message_dict = update.message.to_dict()
        response_text = message_processor.process_mystats_command(message_dict)
        await update.message.reply_text(response_text)


async def edit_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    if await check_user_blocked(update, context):
        return
    if update.message:
        if update.message.from_user and update.message.from_user.is_bot:
            return
        message_dict = update.message.to_dict()
        response_text, keyboard = message_processor.process_edit_command(message_dict)
        await update.message.reply_text(response_text, reply_markup=keyboard)


async def myreports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows users to view and download their generated reports."""
    if not is_private_chat(update):
        return
    if await check_user_blocked(update, context):
        return
    
    user_telegram_id = update.effective_user.id
    user = db_service.get_user_by_telegram_id(user_telegram_id)
    if not user:
        await update.message.reply_text("❌ Xatolik: Foydalanuvchi topilmadi.")
        return

    reports = db_service.get_reports_for_user(user['id'])
    
    if not reports:
        await update.message.reply_text("Sizda hali hisobotlar mavjud emas.")
        return

    message = "📋 Sizning hisobotlaringiz:\n\nHisobotni yuklab olish uchun bosing."
    buttons = []
    for report in reports:
        collection_date = report['collection_date'][:10] if report.get('collection_date') else 'N/A'
        button_text = f"Kolleksiya #{report['collection_id']} ({collection_date})"
        buttons.append([InlineKeyboardButton(button_text, callback_data=f"get_report_{report['id']}")])

    reply_markup = InlineKeyboardMarkup(buttons)
    await update.message.reply_text(message, reply_markup=reply_markup)


async def prompt_for_series(context: ContextTypes.DEFAULT_TYPE) -> None:
    job = context.job
    logger.info(f"prompt_for_series triggered for chat {job.chat_id}")
    job_data = job.data if job.data else {}
    saved_user_data = job_data.get('user_data', {})
    if 'order_id' in saved_user_data:
        awaiting_order_id = saved_user_data['order_id']
        context.user_data['awaiting_order_id'] = awaiting_order_id
        context.user_data['state'] = 'awaiting_series_amount'
        cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_order_{awaiting_order_id}")]])
        await context.bot.send_message(chat_id=job.chat_id, text="📝 Seriyani kiriting", reply_markup=cancel_keyboard)
        context.user_data.pop('acknowledged', None)


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    if await check_user_blocked(update, context):
        return
    if update.message:
        is_forwarded = bool(update.message.forward_origin)
        try:
            forward_msg_id = getattr(update.message.forward_origin, 'message_id', None) if is_forwarded else None
        except Exception:
            forward_msg_id = None
        media_group_id = getattr(update.message, 'media_group_id', None)
        if update.message.from_user and update.message.from_user.is_bot:
            return
        user_id = update.message.from_user.id if update.message.from_user else None
        if str(user_id) in getattr(config, 'ADMIN_IDS', []):
            return
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
        success, response_text = await message_processor.process_media_submission(update, context)
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        media_group_id = getattr(update.message, 'media_group_id', None)
        if not success:
            if response_text:
                await update.message.reply_text(response_text)
            if media_group_id and 'order_id' in context.user_data:
                context.user_data.clear()
            return

        # Prompt for series immediately, as each forwarded post is a separate order
        order_id = context.user_data.get('order_id')
        if order_id:
            context.user_data['awaiting_order_id'] = order_id
            context.user_data['state'] = 'awaiting_series_amount'
            cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_order_{order_id}")]])
            await update.message.reply_text("📝 Seriyani kiriting", reply_markup=cancel_keyboard)


# States for conversation handler
SELECTING_COLLECTION, AWAITING_REPORTS = range(2)

async def send_reports_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the send reports conversation (placeholder)."""
    if update.message:
        await update.message.reply_text("This feature is not yet implemented.")
    return ConversationHandler.END

async def select_collection_for_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles collection selection for reports (placeholder)."""
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("This feature is not yet implemented.")
    return ConversationHandler.END

async def handle_report_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the report document upload (placeholder)."""
    if update.message:
        await update.message.reply_text("This feature is not yet implemented.")
    return ConversationHandler.END

async def send_reports_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the send reports conversation."""
    if update.message:
        await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END
