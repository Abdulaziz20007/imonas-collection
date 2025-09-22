#!/usr/bin/env python3
"""
Test script to verify the Telegram bot setup and configuration.
"""

import sys
import os


def test_imports():
    """Test if all required modules can be imported."""
    print("🔍 Testing imports...")
    
    try:
        import fastapi
        print("✅ FastAPI imported successfully")
    except ImportError as e:
        print(f"❌ FastAPI import failed: {e}")
        return False
    
    try:
        import uvicorn
        print("✅ Uvicorn imported successfully")
    except ImportError as e:
        print(f"❌ Uvicorn import failed: {e}")
        return False
    
    try:
        import httpx
        print("✅ HTTPX imported successfully")
    except ImportError as e:
        print(f"❌ HTTPX import failed: {e}")
        return False
    
    try:
        from supabase import create_client
        print("✅ Supabase client imported successfully")
    except ImportError as e:
        print(f"❌ Supabase import failed: {e}")
        return False
    
    try:
        from telegram import Bot
        print("✅ Python Telegram Bot imported successfully")
    except ImportError as e:
        print(f"❌ Python Telegram Bot import failed: {e}")
        return False
    
    return True


def test_local_modules():
    """Test if local modules can be imported."""
    print("\n🔍 Testing local modules...")
    
    try:
        from src.config import config
        print("✅ Config module imported successfully")
    except ImportError as e:
        print(f"❌ Config module import failed: {e}")
        return False
    
    try:
        from database import db_service
        print("✅ Database service imported successfully")
    except ImportError as e:
        print(f"❌ Database service import failed: {e}")
        return False
    
    try:
        from processor import message_processor
        print("✅ Message processor imported successfully")
    except ImportError as e:
        print(f"❌ Message processor import failed: {e}")
        return False
    
    try:
        from app import app
        print("✅ FastAPI app imported successfully")
    except ImportError as e:
        print(f"❌ FastAPI app import failed: {e}")
        return False
    
    return True


def test_env_file():
    """Test if .env file exists and has required variables."""
    print("\n🔍 Testing environment configuration...")
    
    if not os.path.exists('.env'):
        print("❌ .env file not found. Run 'python setup.py' to create it.")
        return False
    
    print("✅ .env file exists")
    
    # Read .env file and check for required variables
    with open('.env', 'r') as f:
        env_content = f.read()
    
    required_vars = ['TELEGRAM_BOT_TOKEN', 'SUPABASE_URL', 'SUPABASE_KEY']
    
    for var in required_vars:
        if var in env_content:
            # Check if it's still a placeholder
            if 'your_' in env_content or '_here' in env_content:
                print(f"⚠️  {var} found but appears to be a placeholder")
            else:
                print(f"✅ {var} configured")
        else:
            print(f"❌ {var} not found in .env file")
    
    return True


def test_config_validation():
    """Test configuration validation."""
    print("\n🔍 Testing configuration validation...")
    
    try:
        from src.config import config
        config.validate()
        print("✅ Configuration validation passed")
        return True
    except ValueError as e:
        print(f"❌ Configuration validation failed: {e}")
        print("💡 Make sure to set your actual credentials in the .env file")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during config validation: {e}")
        return False


def main():
    """Main test function."""
    print("🧪 Telegram Bot Setup Test")
    print("=" * 40)
    
    all_tests_passed = True
    
    # Test imports
    if not test_imports():
        all_tests_passed = False
    
    # Test local modules
    if not test_local_modules():
        all_tests_passed = False
    
    # Test .env file
    if not test_env_file():
        all_tests_passed = False
    
    # Test config validation (only if .env exists)
    if os.path.exists('.env'):
        if not test_config_validation():
            all_tests_passed = False
    
    print("\n" + "=" * 40)
    if all_tests_passed:
        print("🎉 All tests passed! Your bot setup is ready.")
        print("💡 To start the bot: python app.py")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        print("💡 Run 'python setup.py' for setup instructions.")
    print("=" * 40)


if __name__ == "__main__":
    main()
