#!/usr/bin/env python3
"""
Test script to verify the duplicate notification fix.
This script simulates the race condition scenario to ensure only one final notification is sent.
"""

import sys
import os
import asyncio
import logging
from unittest.mock import MagicMock

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_duplicate_prevention():
    """Test that duplicate notifications are prevented."""
    try:
        # Import after path setup
        from database.db_service import db_service
        from processor import message_processor

        # Initialize database
        db_service.initialize_database()

        # Create a test user
        test_user_data = {
            'telegram_id': 999999,
            'username': 'test_user',
            'name': 'Test',
            'surname': 'User',
            'phone': '+1234567890',
            'code': 'TEST',
            'reg_step': 'done'
        }

        user_id = db_service.create_user(test_user_data)
        if not user_id:
            # User might already exist, get existing one
            user = db_service.get_user_by_telegram_id(999999)
            user_id = user['id'] if user else None

        if not user_id:
            logger.error("Failed to create or find test user")
            return False

        # Create a test collection
        collection_data = {
            'name': 'Test Collection',
            'status': 'active'
        }
        collection_id = db_service.create_collection(collection_data)
        if not collection_id:
            # Collection might already exist, get any active one
            active_collection = db_service.get_active_collection()
            collection_id = active_collection['id'] if active_collection else None

        if not collection_id:
            logger.error("Failed to create or find test collection")
            return False

        # Create a test order
        order_id = db_service.create_order(
            user_id=user_id,
            collection_id=collection_id,
            amount=None,  # Will be set later
            original_message_id=12345,
            original_channel_id=-100123456789
        )

        if not order_id:
            logger.error("Failed to create test order")
            return False

        logger.info(f"Created test order {order_id}")

        # Add a test file to the order and mark it as downloaded
        test_file_unique_id = "test_file_123"
        db_service.add_order_file(order_id, test_file_unique_id)
        db_service.update_order_file(test_file_unique_id, "downloaded", "test_file.jpg")

        logger.info(f"Added downloaded file to order {order_id}")

        # Create a mock context
        class MockContext:
            def __init__(self):
                self.bot = MagicMock()

        mock_context = MockContext()

        # Test 1: Verify no notification is sent before amount is set
        logger.info("Test 1: Attempting finalization without amount...")
        result1 = await message_processor.attempt_to_finalize_order(order_id, mock_context)
        logger.info(f"Result 1 (should be False): {result1}")

        # Test 2: Set the amount and attempt finalization - should send notification
        logger.info("Test 2: Setting amount and attempting finalization...")
        db_service.update_order_amount(order_id, 5)
        result2 = await message_processor.attempt_to_finalize_order(order_id, mock_context)
        logger.info(f"Result 2 (should be True): {result2}")

        # Test 3: Attempt finalization again - should NOT send duplicate notification
        logger.info("Test 3: Attempting finalization again (should prevent duplicate)...")
        result3 = await message_processor.attempt_to_finalize_order(order_id, mock_context)
        logger.info(f"Result 3 (should be False): {result3}")

        # Test 4: Verify the notification flag is set
        logger.info("Test 4: Checking notification flag...")
        flag_set = db_service.has_final_notification_been_sent(order_id)
        logger.info(f"Final notification flag set (should be True): {flag_set}")

        # Test 5: Simulate race condition - multiple concurrent attempts
        logger.info("Test 5: Simulating race condition with concurrent attempts...")

        # Reset the order for this test
        db_service.mark_final_notification_sent(order_id)  # Reset flag to False
        await asyncio.sleep(0.1)  # Brief pause

        # Create multiple concurrent tasks
        tasks = [
            message_processor.attempt_to_finalize_order(order_id, mock_context),
            message_processor.attempt_to_finalize_order(order_id, mock_context),
            message_processor.attempt_to_finalize_order(order_id, mock_context),
        ]

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"Race condition results: {results}")

        # Count how many returned True (successfully sent notification)
        successful_notifications = sum(1 for r in results if r is True)
        logger.info(f"Successful notifications in race condition: {successful_notifications}")

        # Clean up
        db_service.delete_order(order_id, user_id)
        logger.info(f"Cleaned up test order {order_id}")

        # Verify results
        if (not result1 and result2 and not result3 and flag_set and successful_notifications <= 1):
            logger.info("✅ All tests passed! Duplicate notification fix is working correctly.")
            return True
        else:
            logger.error("❌ Some tests failed. The fix may not be working correctly.")
            return False

    except Exception as e:
        logger.error(f"Test failed with exception: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = asyncio.run(test_duplicate_prevention())
    sys.exit(0 if success else 1)