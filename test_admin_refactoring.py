#!/usr/bin/env python3
"""
Test script to verify the admin refactoring works correctly.
Tests the AdminConfigService functionality and migration.
"""

import os
import sys
import json
import tempfile
import shutil

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_admin_config_service():
    """Test the AdminConfigService functionality."""
    print("Testing AdminConfigService...")

    # Create a temporary directory for testing
    test_dir = tempfile.mkdtemp()
    test_file = os.path.join(test_dir, "test_admins.json")

    try:
        from services.admin_config_service import AdminConfigService

        # Create service with test file
        service = AdminConfigService(test_file)

        # Test 1: Empty file should return empty list
        admins = service.get_admins()
        assert admins == [], f"Expected empty list, got {admins}"
        print("✓ Empty file test passed")

        # Test 2: Add admins
        assert service.add_admin("123456789", "Test Admin 1"), "Failed to add first admin"
        assert service.add_admin("987654321", "Test Admin 2"), "Failed to add second admin"
        print("✓ Add admins test passed")

        # Test 3: Duplicate admin should fail
        assert not service.add_admin("123456789", "Duplicate Admin"), "Duplicate admin should fail"
        print("✓ Duplicate admin test passed")

        # Test 4: Get admins should return 2 admins
        admins = service.get_admins()
        assert len(admins) == 2, f"Expected 2 admins, got {len(admins)}"
        assert admins[0]["id"] == "123456789", f"Expected first admin ID to be '123456789', got {admins[0]['id']}"
        assert admins[0]["name"] == "Test Admin 1", f"Expected first admin name to be 'Test Admin 1', got {admins[0]['name']}"
        print("✓ Get admins test passed")

        # Test 5: Update admin
        assert service.update_admin("123456789", "Updated Admin 1"), "Failed to update admin"
        admins = service.get_admins()
        updated_admin = next((admin for admin in admins if admin["id"] == "123456789"), None)
        assert updated_admin and updated_admin["name"] == "Updated Admin 1", "Admin name not updated correctly"
        print("✓ Update admin test passed")

        # Test 6: Update non-existent admin should fail
        assert not service.update_admin("999999999", "Non-existent"), "Update non-existent admin should fail"
        print("✓ Update non-existent admin test passed")

        # Test 7: Delete admin
        assert service.delete_admin("987654321"), "Failed to delete admin"
        admins = service.get_admins()
        assert len(admins) == 1, f"Expected 1 admin after deletion, got {len(admins)}"
        assert admins[0]["id"] == "123456789", "Wrong admin remained after deletion"
        print("✓ Delete admin test passed")

        # Test 8: Delete non-existent admin should fail
        assert not service.delete_admin("999999999"), "Delete non-existent admin should fail"
        print("✓ Delete non-existent admin test passed")

        print("✅ All AdminConfigService tests passed!")
        return True

    except Exception as e:
        print(f"❌ AdminConfigService test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Clean up
        shutil.rmtree(test_dir, ignore_errors=True)

def test_config_integration():
    """Test that config integration works."""
    print("\nTesting config integration...")

    try:
        from config import Config
        from services.admin_config_service import admin_config_service

        # Test that config can load admins
        Config.load_from_db()
        admins_from_config = Config.get_admins_data()
        admins_from_service = admin_config_service.get_admins()

        assert admins_from_config == admins_from_service, "Config and service should return same data"
        print("✓ Config integration test passed")

        print("✅ Config integration tests passed!")
        return True

    except Exception as e:
        print(f"❌ Config integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_json_format():
    """Test that the existing JSON file format is handled correctly."""
    print("\nTesting existing JSON file format...")

    try:
        from services.admin_config_service import admin_config_service

        # Read the current admins
        current_admins = admin_config_service.get_admins()
        print(f"Current admins in db/admins.json: {current_admins}")

        # Validate the format
        assert isinstance(current_admins, list), "Admins should be a list"
        for admin in current_admins:
            assert isinstance(admin, dict), "Each admin should be a dict"
            assert "id" in admin, "Each admin should have an 'id' field"
            assert "name" in admin, "Each admin should have a 'name' field"
            assert isinstance(admin["id"], str), "Admin ID should be a string"
            assert isinstance(admin["name"], str), "Admin name should be a string"

        print("✓ JSON format validation passed")
        print("✅ JSON format tests passed!")
        return True

    except Exception as e:
        print(f"❌ JSON format test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting admin refactoring tests...\n")

    all_passed = True

    # Run tests
    all_passed &= test_admin_config_service()
    all_passed &= test_config_integration()
    all_passed &= test_json_format()

    print(f"\n{'🎉 All tests passed!' if all_passed else '💥 Some tests failed!'}")
    sys.exit(0 if all_passed else 1)