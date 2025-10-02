from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from src.utils.constants import REGIONS

def get_region_keyboard() -> InlineKeyboardMarkup:
    """Generates an inline keyboard with regions for selection."""
    keyboard = [
        [InlineKeyboardButton(region, callback_data=f"select_region:{region}")]
        for region in REGIONS
    ]
    return InlineKeyboardMarkup(keyboard)


