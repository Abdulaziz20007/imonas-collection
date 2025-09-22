#!/usr/bin/env python3
"""
Test script to verify the JSON serialization fix for userbot status.
This script helps identify and resolve coroutine serialization issues.
"""

import json
import asyncio
import inspect
from typing import Any, Dict

def test_json_serialization():
    """Test various scenarios that could cause JSON serialization errors."""

    print("🧪 JSON Serialization Error Fix Test")
    print("=" * 50)

    # Test 1: Normal status object (should work)
    print("\n📋 Test 1: Normal status object")
    normal_status = {
        "connected": True,
        "authorized": True,
        "state": "AUTHORIZED",
        "me": {
            "id": 12345,
            "first_name": "Test",
            "phone": "+1234567890"
        },
        "debug": {
            "task_status": "RUNNING",
            "client_exists": True
        }
    }

    try:
        json_str = json.dumps(normal_status)
        print(f"   ✅ Normal status serializes correctly: {len(json_str)} chars")
    except Exception as e:
        print(f"   ❌ ERROR: Normal status failed: {e}")

    # Test 2: Status with coroutine (should fail)
    print("\n📋 Test 2: Status with coroutine object")
    async def dummy_async():
        return "result"

    problematic_status = {
        "connected": True,
        "authorized": True,
        "me": dummy_async(),  # This is a coroutine!
        "debug": {"task_status": "RUNNING"}
    }

    try:
        json_str = json.dumps(problematic_status)
        print(f"   ❌ ERROR: Problematic status should have failed but didn't!")
    except TypeError as e:
        print(f"   ✅ EXPECTED: Coroutine correctly caused TypeError: {e}")
    except Exception as e:
        print(f"   ⚠️  UNEXPECTED: Different error: {e}")

    # Test 3: Coroutine detection function
    print("\n📋 Test 3: Coroutine detection and cleaning")

    def clean_coroutines(obj, path=""):
        """Clean coroutines from objects (same as in web_app.py)."""
        if inspect.iscoroutine(obj):
            print(f"   🚨 Found coroutine at {path}: {obj}")
            return f"<COROUTINE_ERROR: {type(obj).__name__}>"
        elif isinstance(obj, dict):
            cleaned = {}
            for key, value in obj.items():
                cleaned[key] = clean_coroutines(value, f"{path}.{key}" if path else key)
            return cleaned
        elif isinstance(obj, (list, tuple)):
            return [clean_coroutines(value, f"{path}[{i}]" if path else f"[{i}]") for i, value in enumerate(obj)]
        else:
            return obj

    cleaned_status = clean_coroutines(problematic_status)

    try:
        json_str = json.dumps(cleaned_status)
        print(f"   ✅ Cleaned status serializes correctly: {len(json_str)} chars")
        print(f"   📄 Cleaned object: {cleaned_status}")
    except Exception as e:
        print(f"   ❌ ERROR: Cleaned status still fails: {e}")

    # Test 4: Common problematic scenarios
    print("\n📋 Test 4: Common problematic scenarios")

    scenarios = [
        ("Task exception object", Exception("test error")),
        ("None value", None),
        ("Boolean values", True),
        ("Numbers", 42),
        ("Nested dict", {"nested": {"value": "test"}}),
    ]

    for name, value in scenarios:
        test_obj = {"test": value}
        try:
            json.dumps(test_obj)
            print(f"   ✅ {name}: Serializes OK")
        except Exception as e:
            print(f"   ❌ {name}: Failed - {e}")

    print("\n🎯 RECOMMENDATIONS:")
    print("   1. The clean_coroutines() function should prevent serialization errors")
    print("   2. Check server logs for '🚨 COROUTINE FOUND' messages")
    print("   3. Look for any async functions called without 'await'")
    print("   4. Verify all Telethon method calls are properly awaited")

    print("\n🔍 IF ERRORS PERSIST:")
    print("   - Check for async methods in userbot_client.get_*() calls")
    print("   - Verify task.exception() doesn't return coroutines")
    print("   - Look for any other async library calls in get_userbot_status()")

    print("\n✅ Test completed!")

if __name__ == "__main__":
    test_json_serialization()