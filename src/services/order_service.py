"""
Order-related service wrappers that delegate to the existing message_processor.
"""
from typing import Any
from telegram import Message, Update
from telegram.ext import ContextTypes

from src.processor import message_processor


async def process_media_submission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await message_processor.process_media_submission(update, context)


async def process_series_amount_reply(message: Message, context: ContextTypes.DEFAULT_TYPE) -> str:
    return await message_processor.process_series_amount_reply(message, context)
