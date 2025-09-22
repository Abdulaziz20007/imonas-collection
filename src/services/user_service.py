"""
User-related service wrappers that delegate to the existing message_processor.
This allows gradual migration while keeping call sites stable.
"""
from typing import Any, Dict, Tuple
from telegram.ext import ContextTypes

from src.processor import message_processor


def process_start_command(message_dict: Dict[str, Any]) -> Tuple[str, Any]:
    return message_processor.process_start_command(message_dict)


def process_mystats_command(message_dict: Dict[str, Any]) -> str:
    return message_processor.process_mystats_command(message_dict)


def process_edit_command(message_dict: Dict[str, Any]) -> Tuple[str, Any]:
    return message_processor.process_edit_command(message_dict)


def process_edit_input(message_dict: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, Any]:
    return message_processor.process_edit_input(message_dict, context)


def process_contact_edit(message_dict: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, Any]:
    return message_processor.process_contact_edit(message_dict, context)


async def process_contact_message(message_dict: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE):
    return await message_processor.process_contact_message(message_dict, context)


def process_registration_message(message_dict: Dict[str, Any]):
    return message_processor.process_registration_message(message_dict)


def process_my_orders_button(message_dict: Dict[str, Any]):
    return message_processor.process_my_orders_button(message_dict)


def process_my_profile_button(message_dict: Dict[str, Any]):
    return message_processor.process_my_profile_button(message_dict)
