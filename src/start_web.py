#!/usr/bin/env python3
"""
Startup script for the Admin part of the Order Management Web Application.
This script initializes the database and starts the admin web server on ADMIN_PORT (default 4040).
"""

import os
import sys
import subprocess
import time

def check_database():
    """Check if database exists and initialize if needed."""
    try:
        from database import db_service
        db_service.initialize_database()
        print("✅ Database initialized successfully")
        return True
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

def start_web_server():
    """Start the web server."""
    try:
        print("🚀 Starting Order Management Admin Web Server...")
        port = int(os.getenv('ADMIN_PORT', '4040'))
        print(f"📋 Admin panel will be available at: http://localhost:{port}")
        print("ℹ️  Note: The user-facing web app needs to be started separately.")
        print("⏹️  Press Ctrl+C to stop the server")
        print("-" * 60)
        
        # Import and run the app
        from src.web_app import admin_app
        import uvicorn
        
        uvicorn.run(admin_app, host="0.0.0.0", port=port, log_level="info")
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting web server: {e}")
        return False

def main():
    """Main function to start the application."""
    print("🌟 Order Management System - Web Interface")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not os.path.exists("database.py"):
        print("❌ Error: Please run this script from the project root directory")
        sys.exit(1)
    
    # Check database
    if not check_database():
        sys.exit(1)
    
    # Check if uploads directory exists
    if not os.path.exists("uploads"):
        os.makedirs("uploads")
        print("📁 Created uploads directory")
    
    # Check if templates directory exists
    if not os.path.exists("templates"):
        print("❌ Error: Templates directory not found")
        sys.exit(1)
    
    # Start the web server
    start_web_server()

if __name__ == "__main__":
    main()
