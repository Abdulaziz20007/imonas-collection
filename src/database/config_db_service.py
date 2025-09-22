"""
Database service for managing persistent application configuration.
"""
import sqlite3
import logging
import threading
import os
import hashlib, json
from datetime import datetime

logger = logging.getLogger(__name__)

class ConfigDatabaseService:
    def __init__(self, db_file):
        self.db_file = db_file
        self.local = threading.local()
        if db_file and not os.path.exists(os.path.dirname(db_file)):
            os.makedirs(os.path.dirname(db_file))

    def _get_connection(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn

    def initialize_database(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'admin'
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                admin_user TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT
            )
        """)
        

        
        # Default settings from old .env for migration
        default_settings = {
            'userbot_phone_number': '+998944448088',
            'userbot_password': 'As9605600',
            'private_channel_id': '-1002961810984',
            'group_id': '-1002853927293',
            'userbot_app_id': '26156821',
            'userbot_api_hash': '96b2fb23ba2a93bb171a3b525759970e',
            'find_orders_topic_id': '4',
            'realtime_orders_topic_id': '2',
            'confirmation_topic_id': '240',
            'ai_confirmations_topic_id': '264',
            'default_ai_model': 'gemini-2.0-flash',
            'subscription_price': '500000',
            # Payment settings defaults
            'payment_checking_wait_time': '60',
            'order_delay_time': '1',
            'auto_payment_confirmation': 'true',
        }
        
        for key, value in default_settings.items():
            cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
            
        cursor.execute("SELECT COUNT(*) FROM admins")
        if cursor.fetchone()[0] == 0:
            admin_user = os.getenv('ADMIN_USER', 'admin')
            admin_pass = os.getenv('ADMIN_PASSWORD', 'password')
            password_hash = hashlib.sha256(admin_pass.encode('utf-8')).hexdigest()
            cursor.execute("INSERT INTO admins (username, password_hash, role) VALUES (?, ?, ?)",
                           (admin_user, password_hash, 'superadmin'))
            logger.info(f"Created default admin user '{admin_user}'. Please change this password.")

        conn.commit()
        logger.info("Configuration database initialized.")

    def get_all_settings(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT key, value FROM settings")
        return {row['key']: row['value'] for row in cursor.fetchall()}

    def update_settings(self, settings_dict, admin_user):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            for key, value in settings_dict.items():
                cursor.execute("UPDATE settings SET value = ? WHERE key = ?", (value, key))
                rows = cursor.rowcount
                if rows == 0:
                    cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (key, value))
                    logger.info(f"Inserted missing setting '{key}' with value '{value}'")
                else:
                    logger.info(f"Updating setting '{key}'. Rows affected: {rows}")
                self.log_action(admin_user, 'UPDATE_SETTING', f"Set {key} to '{value}'")
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating settings: {e}")
            return False

    def get_admin_user(self, username):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM admins WHERE username = ?", (username,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def check_admin_password(self, username, password):
        user = self.get_admin_user(username)
        if user:
            stored_hash = user['password_hash']
            input_hash = hashlib.sha256((password).encode('utf-8')).hexdigest()
            return stored_hash == input_hash
        return False

    def log_action(self, admin_user, action, details):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO audit_log (admin_user, action, details) VALUES (?, ?, ?)",
                       (admin_user, action, details))
        conn.commit()

    def get_audit_logs(self, limit=100):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT ?", (limit,))
        return [dict(row) for row in cursor.fetchall()]


    # CRUD for admins
    def get_all_admins(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, role FROM admins ORDER BY username")
        return [dict(row) for row in cursor.fetchall()]

    def add_admin(self, username, password, role, admin_user):
        password_hash = hashlib.sha256(password.encode('utf-8')).hexdigest()
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO admins (username, password_hash, role) VALUES (?, ?, ?)", (username, password_hash, role))
            admin_id = cursor.lastrowid
            conn.commit()
            self.log_action(admin_user, 'ADD_ADMIN', f"Added admin '{username}' with role '{role}'")
            return admin_id
        except sqlite3.IntegrityError:
            return None # Username already exists

    def delete_admin(self, admin_id_to_delete, current_admin_user):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM admins WHERE id = ?", (admin_id_to_delete,))
        conn.commit()
        self.log_action(current_admin_user, 'DELETE_ADMIN', f"Deleted admin with ID {admin_id_to_delete}")
        return cursor.rowcount > 0


config_db_service = ConfigDatabaseService(os.getenv('CONFIG_DATABASE_FILE', 'config.db'))