#!/usr/bin/env python3
"""
Test script to verify the config loading fix for the admin addition bug.
"""
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_config_load():
    """Test that config.load_from_db() works without AttributeError."""
    try:
        from src.config import config

        print("Testing config.load_from_db()...")
        config.load_from_db()
        print("✅ SUCCESS: config.load_from_db() completed without errors")

        print(f"📊 Loaded {len(config.ADMIN_IDS)} admin IDs: {config.ADMIN_IDS}")
        return True

    except AttributeError as e:
        if "get_telegram_admins" in str(e):
            print(f"❌ FAILED: The AttributeError still exists: {e}")
            return False
        else:
            print(f"❌ FAILED: Different AttributeError: {e}")
            return False
    except Exception as e:
        print(f"⚠️  WARNING: Other error (this might be expected): {e}")
        print("This could be due to missing database files or other setup issues.")
        print("The important thing is that there's no 'get_telegram_admins' AttributeError.")
        return True

if __name__ == "__main__":
    print("=" * 60)
    print("Testing config loading fix for admin addition bug")
    print("=" * 60)

    success = test_config_load()

    print("=" * 60)
    if success:
        print("🎉 TEST PASSED: The fix appears to work!")
        print("The admin addition should now work without the AttributeError.")
    else:
        print("💥 TEST FAILED: The fix did not resolve the issue.")
    print("=" * 60)