"""
Notification service wrappers delegating to the existing message_processor.
"""
from typing import Optional
from telegram.ext import ContextTypes

from src.processor import message_processor


async def send_realtime_order_notification(context: ContextTypes.DEFAULT_TYPE, order_id: int, extra: Optional[object] = None):
    """Send realtime order notification using the centralized finalization logic."""
    return await message_processor.attempt_to_finalize_order(order_id, context)


async def notify_users_of_card_change(bot_app):
    return await message_processor.notify_users_of_card_change(bot_app)
