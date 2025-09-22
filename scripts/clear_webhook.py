#!/usr/bin/env python3
"""
Script to clear webhook settings for the Telegram bot.
This will allow polling mode to work properly.
"""

import asyncio
import logging
from telegram import Bot
from src.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def clear_webhook():
    """Clear webhook settings to enable polling mode."""
    try:
        bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
        
        # Get current webhook info
        webhook_info = await bot.get_webhook_info()
        logger.info(f"Current webhook URL: {webhook_info.url}")
        logger.info(f"Pending updates: {webhook_info.pending_update_count}")
        
        if webhook_info.url:
            logger.info("🔄 Clearing webhook...")
            # Clear the webhook by setting it to empty
            result = await bot.set_webhook(url="")
            if result:
                logger.info("✅ Webhook cleared successfully")
            else:
                logger.error("❌ Failed to clear webhook")
        else:
            logger.info("✅ No webhook configured - polling should work")
            
        # Verify webhook is cleared
        webhook_info = await bot.get_webhook_info()
        logger.info(f"Webhook URL after clearing: {webhook_info.url or 'None'}")
        
    except Exception as e:
        logger.error(f"❌ Error clearing webhook: {e}")

if __name__ == "__main__":
    asyncio.run(clear_webhook())
