"""
Admin-related service wrappers delegating to the existing message_processor.
"""
from typing import Any, Dict, Tuple
from telegram import Update
from telegram.ext import ContextTypes

from src.processor import message_processor


async def process_admin_command(message_dict: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, Any]:
    return await message_processor.process_admin_command(message_dict, context)


async def send_collections_list(update: Update, data: Any) -> None:
    await message_processor.send_collections_list(update, data)


async def send_cards_list(update: Update, data: Any) -> None:
    await message_processor.send_cards_list(update, data)


async def send_user_orders(update: Update, data: Any) -> None:
    await message_processor.send_user_orders(update, data)


async def process_callback_query(query, context: ContextTypes.DEFAULT_TYPE):
    return await message_processor.process_callback_query(query, context)
