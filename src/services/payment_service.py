"""
Payment-related service wrappers delegating to the existing message_processor.
"""
from typing import Any

from src.processor import message_processor


async def handle_bank_notification(telethon_message: Any):
    return await message_processor.handle_bank_notification(telethon_message)
