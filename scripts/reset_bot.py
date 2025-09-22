#!/usr/bin/env python3
"""
Script to reset the Telegram bot state and clear any conflicts.
Run this before starting the main application if you encounter bot conflicts.
"""

import asyncio
import logging
import requests
from src.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reset_bot_sync():
    """Reset bot state using synchronous requests."""
    try:
        base_url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
        
        # Clear webhook (if any)
        logger.info("🔄 Clearing webhook...")
        webhook_response = requests.post(f"{base_url}/setWebhook", data={"url": ""})
        if webhook_response.status_code == 200:
            logger.info("✅ Webhook cleared")
        else:
            logger.warning(f"⚠️ Webhook clear response: {webhook_response.status_code}")
            
        # Get pending updates and clear them
        logger.info("🔄 Clearing pending updates...")
        updates_response = requests.post(f"{base_url}/getUpdates", data={
            "offset": -1,  # Get the latest update
            "limit": 1,
            "timeout": 1
        })
        
        if updates_response.status_code == 200:
            updates_data = updates_response.json()
            if updates_data.get("result"):
                # If there are updates, get the highest update_id and clear all
                highest_update_id = max(update["update_id"] for update in updates_data["result"])
                clear_response = requests.post(f"{base_url}/getUpdates", data={
                    "offset": highest_update_id + 1,  # Clear all updates up to this point
                    "limit": 100,
                    "timeout": 1
                })
                logger.info(f"✅ Cleared pending updates (highest ID: {highest_update_id})")
            else:
                logger.info("✅ No pending updates to clear")
        else:
            logger.warning(f"⚠️ Updates response: {updates_response.status_code}")
            
        # Get bot info to verify connection
        logger.info("🔄 Verifying bot connection...")
        me_response = requests.post(f"{base_url}/getMe")
        if me_response.status_code == 200:
            bot_info = me_response.json()
            if bot_info.get("ok"):
                logger.info(f"✅ Bot verified: @{bot_info['result']['username']}")
            else:
                logger.error(f"❌ Bot verification failed: {bot_info}")
        else:
            logger.error(f"❌ Bot connection failed: {me_response.status_code}")
            
        logger.info("🎉 Bot reset complete! You can now start your application.")
        
    except Exception as e:
        logger.error(f"❌ Error resetting bot: {e}")

if __name__ == "__main__":
    reset_bot_sync()
