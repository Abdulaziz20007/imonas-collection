"""
Database service layer for the Telegram Product Submission Tracker Bot.
Handles all interactions with the SQLite database.
"""


import sqlite3
from typing import Dict, Any, Optional, List
import logging
from src.config import config
import threading
import os

logger = logging.getLogger(__name__)

class DatabaseService:
    """Service class for database operations using SQLite."""
    
    def __init__(self):
        """Initialize the database service."""
        self.db_file = config.DATABASE_FILE
        self.local = threading.local()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a thread-safe database connection."""
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn

    def initialize_database(self):
        """Create database tables if they don't exist (non-destructive)."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Ensure foreign key constraints are enforced
            cursor.execute("PRAGMA foreign_keys = ON")

            # Create users table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE,
                    username TEXT NULL,
                    name TEXT NULL,
                    surname TEXT NULL,
                    phone TEXT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT 1,
                    reg_step TEXT NOT NULL DEFAULT 'name' CHECK (reg_step IN ('name', 'surname', 'phone', 'done')),
                    code TEXT UNIQUE NULL,
                    subscription_status TEXT NOT NULL DEFAULT 'none' CHECK (subscription_status IN ('none', 'pending_payment', 'active')),
                    target_amount REAL,
                    paid_amount REAL NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create collections table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS collections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT NOT NULL CHECK (status IN ('open', 'close', 'finish')),
                    finish_at DATETIME NULL,
                    close_at DATETIME NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create orders table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    collection_id INTEGER NOT NULL,
                    amount INTEGER NULL,
                    original_message_id INTEGER NULL,
                    original_channel_id INTEGER NULL,
                    status INTEGER NOT NULL DEFAULT 1,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (collection_id) REFERENCES collections (id)
                )
            """)
            
            # Create order_files table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    file_url TEXT,
                    file_unique_id TEXT UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'downloaded', 'failed')),
                    FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE
                )
            """)
            
            # Migration: Add status column to existing orders table if it doesn't exist
            try:
                cursor.execute("PRAGMA table_info(orders)")
                columns = [column[1] for column in cursor.fetchall()]
                if 'status' not in columns:
                    cursor.execute("ALTER TABLE orders ADD COLUMN status INTEGER NOT NULL DEFAULT 1") # type: ignore
                    logger.info("Added status column to orders table")
            except Exception as e:
                logger.warning(f"Could not add status column to orders table: {e}")

            # Migration: Add status and file_unique_id to order_files
            cursor.execute("PRAGMA table_info(order_files)")
            order_files_columns = [col[1] for col in cursor.fetchall()]
            if 'status' not in order_files_columns:
                cursor.execute("ALTER TABLE order_files ADD COLUMN status TEXT NOT NULL DEFAULT 'downloaded'") # type: ignore
                cursor.execute("ALTER TABLE order_files ADD COLUMN file_unique_id TEXT") # type: ignore
                logger.info("Added status and file_unique_id to order_files table")

            # Create cards table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    number TEXT NOT NULL UNIQUE,
                    is_active BOOLEAN NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create transactions table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    card_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    card_balance REAL,
                    transaction_time DATETIME NOT NULL,
                    raw_message TEXT,
                    is_done BOOLEAN NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (card_id) REFERENCES cards (id)
                )
            """)

            # Create payment table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS payment (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    transaction_id INTEGER UNIQUE,
                    amount REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'verified', 'failed', 'manual_review')),
                    receipt_url TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (transaction_id) REFERENCES transactions (id)
                )
            """)

            # Create indexes for performance (idempotent)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaction_time ON transactions(transaction_time)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_payment_status ON payment(status)")
            # Ensure unique index on users.code for fast lookups and integrity
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_code ON users(code)")
            
            conn.commit()
            logger.info("Database initialized safely (no destructive changes).")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            conn.rollback()
        finally:
            cursor.close()
    
    def create_user(self, user_data: Dict[str, Any]) -> Optional[int]:
        """
        Creates a new user in the database.
        Args:
            user_data: Dictionary with 'telegram_id' and 'username'.
        Returns:
            int: User ID if successful, None otherwise
        """
        sql = "INSERT INTO users (telegram_id, username) VALUES (?, ?)"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (user_data['telegram_id'], user_data.get('username')))
            user_id = cursor.lastrowid
            conn.commit()
            logger.info(f"User created with ID: {user_id} for telegram_id: {user_data['telegram_id']}") # type: ignore
            return user_id
        except sqlite3.IntegrityError:
            logger.warning(f"User with telegram_id {user_data['telegram_id']} already exists.")
            # If user already exists, we can get their ID, but for a pure create function, returning None is fine.
            existing_user = self.get_user_by_telegram_id(user_data['telegram_id'])
            return existing_user['id'] if existing_user else None
        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            return None

    def update_user_info(self, telegram_id: int, field: str, value: Any) -> bool:
        """Update a specific field for a user."""
        if field not in ['name', 'surname', 'phone', 'code']:
            logger.error(f"Invalid field to update: {field}")
            return False
        sql = f"UPDATE users SET {field} = ? WHERE telegram_id = ?" # type: ignore
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (value, telegram_id)) # type: ignore
            conn.commit()
            logger.info(f"User {telegram_id} field '{field}' updated.")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating user info: {str(e)}")
            return None
    
    def create_order(self, user_id: int, collection_id: int, amount: Optional[int] = None, original_message_id: Optional[int] = None, original_channel_id: Optional[int] = None) -> Optional[int]:
        """
        Create a new order record in the database without files.
        Args:
            user_id: The internal user ID (not telegram_id)
            collection_id: The collection ID this order belongs to
            amount: Optional amount for the order
            original_message_id: The ID of the message in the source channel
            original_channel_id: The ID of the source channel
        Returns:
            int: The ID of the created order, or None if failed
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Insert into orders table
            sql_order = """
                INSERT INTO orders (user_id, collection_id, amount, original_message_id, original_channel_id) 
                VALUES (?, ?, ?, ?, ?)
            """
            cursor.execute(sql_order, (user_id, collection_id, amount, original_message_id, original_channel_id))
            order_id = cursor.lastrowid

            if not order_id:
                raise Exception("Failed to create order record.")

            conn.commit()
            logger.info(f"Order created successfully with ID: {order_id} referencing message {original_message_id}.")
            return order_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating order: {str(e)}")
            return None
        finally:
            cursor.close()
    
    def add_order_file(self, order_id: int, file_unique_id: str) -> bool:
        """Adds a placeholder for a file in an order with 'pending' status."""
        # First check if this file_unique_id already exists for this specific order
        check_sql = "SELECT COUNT(*) FROM order_files WHERE order_id = ? AND file_unique_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(check_sql, (order_id, file_unique_id))
            count = cursor.fetchone()[0]
            
            if count > 0:
                logger.info(f"File with unique_id {file_unique_id} already exists for order {order_id}.")
                return True  # File already exists for this order, which is fine
            
            # Insert the new record
            sql = "INSERT INTO order_files (order_id, file_unique_id, status) VALUES (?, ?, 'pending')"
            cursor.execute(sql, (order_id, file_unique_id))
            conn.commit()
            logger.info(f"Added file placeholder for order {order_id}, file {file_unique_id}")
            return cursor.rowcount > 0
            
        except sqlite3.IntegrityError as e:
            # The same file exists in another order, but users should be able to send the same file multiple times
            logger.info(f"File with unique_id {file_unique_id} already exists in another order, but allowing duplicate: {e}")
            # We'll create a record without the unique_id to track this file for this order
            try:
                # Use a placeholder file_url to avoid NOT NULL constraint issues
                placeholder_file_url = f"pending_download_{order_id}_{file_unique_id}"
                sql_without_unique = "INSERT INTO order_files (order_id, status, file_url) VALUES (?, 'pending', ?)"
                cursor.execute(sql_without_unique, (order_id, placeholder_file_url))
                conn.commit()
                logger.info(f"Added file placeholder for order {order_id} without unique_id (duplicate file)")
                return cursor.rowcount > 0
            except Exception as inner_e:
                logger.error(f"Error adding order file placeholder without unique_id: {str(inner_e)}")
                return False
        except Exception as e:
            logger.error(f"Error adding order file placeholder: {str(e)}")
            return False

    def update_order_file(self, file_unique_id: str, status: str, file_path: Optional[str] = None, order_id: Optional[int] = None) -> bool:
        """Updates the status and file_url for a file based on its unique ID or order_id for duplicate files."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if file_unique_id:
                # Try to update by file_unique_id first
                sql = "UPDATE order_files SET status = ?, file_url = ? WHERE file_unique_id = ?"
                cursor.execute(sql, (status, file_path, file_unique_id))
                
                if cursor.rowcount == 0 and order_id:
                    # If no rows were updated and we have an order_id, try updating by order_id for duplicate files
                    # SQLite doesn't support LIMIT in UPDATE, so we'll use a subquery approach
                    sql = """UPDATE order_files SET status = ?, file_url = ? 
                             WHERE id = (SELECT id FROM order_files 
                                       WHERE order_id = ? AND file_unique_id IS NULL AND status = 'pending' 
                                       AND (file_url LIKE 'pending_download_%' OR file_url IS NULL)
                                       LIMIT 1)"""
                    cursor.execute(sql, (status, file_path, order_id))
            elif order_id:
                # Update by order_id for duplicate files without unique_id
                # SQLite doesn't support LIMIT in UPDATE, so we'll use a subquery approach
                sql = """UPDATE order_files SET status = ?, file_url = ? 
                         WHERE id = (SELECT id FROM order_files 
                                   WHERE order_id = ? AND file_unique_id IS NULL AND status = 'pending' 
                                   AND (file_url LIKE 'pending_download_%' OR file_url IS NULL)
                                   LIMIT 1)"""
                cursor.execute(sql, (status, file_path, order_id))
            else:
                logger.error("Either file_unique_id or order_id must be provided")
                return False
                
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating order file {file_unique_id}: {str(e)}")
            return False

    def get_order_file_statuses(self, order_id: int) -> List[str]:
        """Gets the status of all files for a given order."""
        sql = "SELECT status FROM order_files WHERE order_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (order_id,))
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Error getting order file statuses for order {order_id}: {str(e)}")
            return []

    def get_user_order_count(self, telegram_id: int) -> int:
        """
        Get the total number of orders for a specific user.
        Args:
            telegram_id: The Telegram user ID
        Returns:
            int: Number of orders, or 0 if error/no orders
        """
        sql = """
            SELECT COUNT(o.id) 
            FROM orders o 
            JOIN users u ON o.user_id = u.id 
            WHERE u.telegram_id = ?
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (telegram_id,))
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            logger.error(f"Error getting user order count: {str(e)}")
            return 0
    
    def get_user_by_telegram_id(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """
        Get user information by telegram ID.
        Args:
            telegram_id: The Telegram user ID
        Returns:
            Dict containing user data or None if not found
        """
        sql = "SELECT * FROM users WHERE telegram_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (telegram_id,))
            row = cursor.fetchone()
            if row:
                user_dict = dict(row)
                # Backward-compat: derive legacy payment_step from subscription_status
                user_dict['payment_step'] = self._map_subscription_status_to_payment_step(user_dict.get('subscription_status'))
                return user_dict
            return None
        except Exception as e:
            logger.error(f"Error getting user by telegram_id: {str(e)}")
            return None
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Get user information by internal user ID.
        Args:
            user_id: The internal user ID
        Returns:
            Dict containing user data or None if not found
        """
        sql = "SELECT * FROM users WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            row = cursor.fetchone()
            if row:
                user_dict = dict(row)
                # Backward-compat: derive legacy payment_step from subscription_status
                user_dict['payment_step'] = self._map_subscription_status_to_payment_step(user_dict.get('subscription_status'))
                return user_dict
            return None
        except Exception as e:
            logger.error(f"Error getting user by id: {str(e)}")
            return None

    def get_user_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """
        Get user information by their static unique code.
        Args:
            code: The user's unique code
        Returns:
            Dict containing user data or None if not found
        """
        sql = "SELECT * FROM users WHERE UPPER(code) = UPPER(?)"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (code,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        except Exception as e:
            logger.error(f"Error getting user by code: {str(e)}")
            return None
    
    def create_collection(self, status: str = 'open') -> Optional[int]:
        """
        Create a new collection.
        Args:
            status: Collection status ('open', 'close', 'finish')
        Returns:
            int: The ID of the created collection, or None if failed
        """
        if status not in ['open', 'close', 'finish']:
            logger.error(f"Invalid collection status: {status}")
            return None
            
        sql = "INSERT INTO collections (status, created_at) VALUES (?, datetime('now'))"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (status,))
            collection_id = cursor.lastrowid
            conn.commit()
            logger.info(f"Collection created successfully with ID: {collection_id}")
            return collection_id
        except Exception as e:
            logger.error(f"Error creating collection: {str(e)}")
            return None
    
    def get_active_collection(self) -> Optional[Dict[str, Any]]:
        """
        Get the currently active (open) collection.
        Returns:
            Dict containing collection data or None if no active collection
        """
        sql = "SELECT * FROM collections WHERE status = 'open' ORDER BY created_at DESC LIMIT 1"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        except Exception as e:
            logger.error(f"Error getting active collection: {str(e)}")
            return None

    def get_collection_by_id(self, collection_id: int) -> Optional[Dict[str, Any]]:
        """Get a collection by its ID."""
        sql = "SELECT * FROM collections WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting collection by id {collection_id}: {e}")
            return None

    def get_close_collections(self) -> list:
        """
        Get all collections with 'close' status.
        Returns:
            List of collection dictionaries with 'close' status
        """
        sql = "SELECT * FROM collections WHERE status = 'close' ORDER BY created_at DESC"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting close collections: {str(e)}")
            return []

    def has_close_collections(self) -> bool:
        """
        Check if there are any collections with 'close' status.
        Returns:
            bool: True if there are collections with 'close' status, False otherwise
        """
        sql = "SELECT COUNT(*) FROM collections WHERE status = 'close'"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.error(f"Error checking for close collections: {str(e)}")
            return False

    def get_last_collections(self, limit: int = 10) -> list:
        """
        Get the last N collections ordered by creation date.
        Args:
            limit: Number of collections to retrieve (default: 10)
        Returns:
            List of collection dictionaries ordered by most recent first
        """
        sql = "SELECT * FROM collections ORDER BY created_at DESC LIMIT ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting last collections: {str(e)}")
            return []
    
    def get_collection_stats(self, collection_id: int) -> Dict[str, int]:
        """
        Get statistics for a specific collection.
        Args:
            collection_id: The ID of the collection
        Returns:
            Dict with 'item_count' and 'user_count'
        """
        stats = {'item_count': 0, 'user_count': 0}
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Get item count
            cursor.execute("SELECT COUNT(id) FROM orders WHERE collection_id = ?", (collection_id,))
            item_count_result = cursor.fetchone()
            if item_count_result:
                stats['item_count'] = item_count_result[0]

            # Get unique user count
            cursor.execute("SELECT COUNT(DISTINCT user_id) FROM orders WHERE collection_id = ?", (collection_id,))
            user_count_result = cursor.fetchone()
            if user_count_result:
                stats['user_count'] = user_count_result[0]

            return stats
        except Exception as e:
            logger.error(f"Error getting collection stats for collection_id {collection_id}: {str(e)}")
            return stats

    def update_collection_status(self, collection_id: int, status: str) -> bool:
        """
        Update the status of a collection.
        Args:
            collection_id: The collection ID
            status: New status ('open', 'close', 'finish')
        Returns:
            bool: True if successful, False otherwise
        """
        if status not in ['open', 'close', 'finish']:
            logger.error(f"Invalid collection status: {status}")
            return False
            
        # Set appropriate timestamp based on status
        if status == 'close':
            sql = "UPDATE collections SET status = ?, close_at = datetime('now') WHERE id = ?"
        elif status == 'finish':
            sql = "UPDATE collections SET status = ?, finish_at = datetime('now') WHERE id = ?"
        else:  # status == 'open'
            sql = "UPDATE collections SET status = ?, close_at = NULL, finish_at = NULL WHERE id = ?"
            
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (status, collection_id))
            conn.commit()
            logger.info(f"Collection {collection_id} status updated to {status}")
            return True
        except Exception as e:
            logger.error(f"Error updating collection status: {str(e)}")
            return False

    def update_user_registration_step(self, telegram_id: int, reg_step: str) -> bool:
        """Update the registration step for a user (compat shim)."""
        if reg_step not in ['name', 'surname', 'phone', 'done']:
            logger.error(f"Invalid registration step: {reg_step}")
            return False
        sql = "UPDATE users SET reg_step = ? WHERE telegram_id = ?" # type: ignore
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (reg_step, telegram_id)) # type: ignore
            conn.commit()
            logger.info(f"User {telegram_id} registration step updated to {reg_step}")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating user registration step: {str(e)}")
            return False

    

    def update_user_subscription_status(self, telegram_id: int, subscription_status: str) -> bool:
        """
        Update the payment step for a user.
        Args:
            telegram_id: The Telegram user ID
            payment_step: New payment step ('pending', 'awaiting_receipt', 'confirmed')
        Returns:
            bool: True if successful, False otherwise
        """
        if subscription_status not in ['none', 'pending_payment', 'active']:
            logger.error(f"Invalid subscription status: {subscription_status}")
            return False
        
        sql = "UPDATE users SET subscription_status = ? WHERE telegram_id = ?" # type: ignore
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (subscription_status, telegram_id)) # type: ignore
            conn.commit()
            logger.info(f"User {telegram_id} subscription status updated to {subscription_status}")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating user subscription status: {str(e)}")
            return False

    # --- Compatibility helpers for legacy payment_step API ---
    def _map_subscription_status_to_payment_step(self, subscription_status: Optional[str]) -> str:
        """Map new subscription_status to legacy payment_step for compatibility."""
        if subscription_status == 'active':
            return 'confirmed'
        if subscription_status == 'pending_payment':
            return 'awaiting_receipt'
        return 'pending'

    def _map_payment_step_to_subscription_status(self, payment_step: str) -> str:
        """Map legacy payment_step to new subscription_status for storage."""
        if payment_step == 'confirmed':
            return 'active'
        if payment_step in ['awaiting_receipt', 'pending']:
            return 'pending_payment'
        if payment_step == 'rejected':
            # Treat rejected as pending until user retries (no dedicated status)
            return 'pending_payment'
        return 'none'

    def update_user_payment_step(self, telegram_id: int, payment_step: str) -> bool:
        """Compatibility layer: update legacy payment_step via subscription_status."""
        subscription_status = self._map_payment_step_to_subscription_status(payment_step)
        return self.update_user_subscription_status(telegram_id, subscription_status)

    def get_users_with_payment_step(self, payment_step: str) -> List[Dict[str, Any]]:
        """Compatibility layer: fetch users by legacy payment_step value."""
        target_status = self._map_payment_step_to_subscription_status(payment_step)
        return self.get_users_by_subscription_status(target_status)


    def update_user_subscription_amounts(self, telegram_id: int, target_amount: Optional[float] = None, paid_amount: Optional[float] = None) -> bool:
        """Update user's target and/or paid amounts for subscription."""
        updates = []
        params = []
        if target_amount is not None:
            updates.append("target_amount = ?")
            params.append(target_amount)
        if paid_amount is not None:
            updates.append("paid_amount = ?")
            params.append(paid_amount)

        if not updates:
            return True

        sql = f"UPDATE users SET {', '.join(updates)} WHERE telegram_id = ?" # type: ignore
        params.append(telegram_id)

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            conn.commit()
            logger.info(f"User {telegram_id} subscription amounts updated.")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating user subscription amounts for {telegram_id}: {e}")
            return False



    def get_users_by_registration_step(self, reg_step: str) -> list:
        """
        Get all users at a specific registration step.
        Args:
            reg_step: Registration step to filter by
        Returns:
            list: List of user dictionaries
        """
        if reg_step not in ['name', 'surname', 'phone', 'done']:
            logger.error(f"Invalid registration step: {reg_step}")
            return []
            
        sql = "SELECT * FROM users WHERE reg_step = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (reg_step,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting users by registration step: {str(e)}")
            return []

    def get_all_users_with_stats(self) -> list:
        """
        Get all users with their order statistics.
        Returns:
            list: List of user dictionaries with order counts and latest order info
        """
        sql = """
            SELECT u.*, 
                   COUNT(o.id) as order_count,
                   MAX(o.id) as latest_order_id,
                   MAX(c.created_at) as latest_order_date,
                   (SELECT COUNT(DISTINCT o2.collection_id) 
                    FROM orders o2 WHERE o2.user_id = u.id) as collections_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            LEFT JOIN collections c ON o.collection_id = c.id -- This join might not be necessary if only date is needed from orders
            GROUP BY u.id
            ORDER BY u.id DESC
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting all users with stats: {str(e)}")
            return []

    def get_user_count(self) -> int:
        """Get total number of users."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting user count: {str(e)}")
            return 0
    
    def get_active_subscriber_count(self) -> int:
        """Get number of users with active subscription."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE subscription_status = 'active'")
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting active subscriber count: {str(e)}")
            return 0
    
    def get_pending_payment_count(self) -> int:
        """Get number of users with pending payment."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE subscription_status = 'pending_payment'")
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting pending payment count: {str(e)}")
            return 0
    
    def get_total_revenue(self) -> float:
        """Get total revenue from paid amounts."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COALESCE(SUM(paid_amount), 0) FROM users WHERE subscription_status = 'active'")
            result = cursor.fetchone()[0]
            return float(result) if result else 0.0
        except Exception as e:
            logger.error(f"Error getting total revenue: {str(e)}")
            return 0.0

    def get_all_orders_with_details(self, limit: int = 50) -> list:
        """
        Get all orders with user and collection details.
        Args:
            limit: Maximum number of orders to return (default: 50)
        Returns:
            list: List of order dictionaries with user and collection info
        """
        sql = """
            SELECT o.*,
                   u.name, u.surname, u.phone, u.username, u.telegram_id, u.code,
                   c.status as collection_status, c.created_at as collection_created_at,
                   (SELECT COUNT(*) FROM order_files oi WHERE oi.order_id = o.id) as file_count
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN collections c ON o.collection_id = c.id
            ORDER BY o.id DESC
            LIMIT ?
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting all orders with details: {str(e)}")
            return []
    
    def update_user_active_status(self, telegram_id: int, is_active: bool) -> bool:
        """
        Update the active status for a user.
        Args:
            telegram_id: The Telegram user ID
            is_active: Active status (True/False)
        Returns:
            bool: True if successful, False otherwise
        """
        sql = "UPDATE users SET is_active = ? WHERE telegram_id = ?" # type: ignore
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (is_active, telegram_id)) # type: ignore
            conn.commit()
            logger.info(f"User {telegram_id} active status updated to {is_active}")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating user active status: {str(e)}")
            return False

    def merge_collections(self, from_collection_id: int, to_collection_id: int) -> bool:
        """
        Merge orders from one collection to another.
        Args:
            from_collection_id: Source collection ID to merge from
            to_collection_id: Target collection ID to merge to
        Returns:
            bool: True if successful, False otherwise
        """
        sql = "UPDATE orders SET collection_id = ? WHERE collection_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (to_collection_id, from_collection_id))
            affected_rows = cursor.rowcount
            conn.commit()
            logger.info(f"Merged {affected_rows} orders from collection {from_collection_id} to {to_collection_id}")
            return True
        except Exception as e:
            logger.error(f"Error merging collections: {str(e)}")
            return False



    def delete_collection(self, collection_id: int) -> bool:
        """
        Delete a collection after its data has been merged.
        Args:
            collection_id: The collection ID to delete
        Returns:
            bool: True if successful, False otherwise
        """
        sql = "DELETE FROM collections WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            conn.commit()
            logger.info(f"Collection {collection_id} deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Error deleting collection: {str(e)}")
            return False

    def get_order_files(self, order_id: int) -> List[str]:
        """
        Get all file URLs for a specific order.
        Args:
            order_id: The order ID
        Returns:
            List[str]: List of file URLs/paths for the order
        """
        sql = "SELECT file_url FROM order_files WHERE order_id = ? ORDER BY id ASC"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (order_id,))
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Error getting order files: {str(e)}")
            return []

    def get_user_orders(self, user_id: int) -> list:
        """
        Get all orders for a specific user.
        Args:
            user_id: The internal user ID
        Returns:
            list: List of order dictionaries with collection info
        """
        sql = """
            SELECT o.*, c.status as collection_status, c.created_at as collection_created_at,
                   (SELECT file_url FROM order_files WHERE order_id = o.id LIMIT 1) as file_url
            FROM orders o 
            JOIN collections c ON o.collection_id = c.id 
            WHERE o.user_id = ? 
            ORDER BY o.id DESC
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting user orders: {str(e)}")
            return []

    def get_order_by_id(self, order_id: int) -> Optional[Dict[str, Any]]:
        """
        Get order details by order ID.
        Args:
            order_id: The order ID
        Returns:
            Dict containing order data with file URLs or None if not found
        """
        sql = """
            SELECT o.*, c.status as collection_status, c.created_at as collection_created_at
            FROM orders o 
            JOIN collections c ON o.collection_id = c.id 
            WHERE o.id = ?
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (order_id,))
            row = cursor.fetchone()
            if row:
                order_dict = dict(row)
                # Add file URLs
                file_urls = self.get_order_files(order_id)
                order_dict['file_urls'] = file_urls
                return order_dict
            return None
        except Exception as e:
            logger.error(f"Error getting order by id: {str(e)}")
            return None

    def update_user_details(self, user_id: int, name: str, surname: str, phone: str) -> bool:
        """Update user's name, surname, and phone."""
        sql = "UPDATE users SET name = ?, surname = ?, phone = ? WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (name, surname, phone, user_id))
            conn.commit()
            logger.info(f"User {user_id} details updated.")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating user details for user_id {user_id}: {e}")
            return False

    def get_order_details(self, order_id: int) -> Optional[Dict[str, Any]]:
        """Get full order details including user and files for web view."""
        sql = """
            SELECT o.*, 
                   u.name, u.surname, u.phone, u.username, u.telegram_id, u.code,
                   c.status as collection_status
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN collections c ON o.collection_id = c.id
            WHERE o.id = ?
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (order_id,))
            row = cursor.fetchone()
            if not row:
                return None
            
            order_details = dict(row)
            order_details['files'] = self.get_order_files(order_id)
            return order_details
        except Exception as e:
            logger.error(f"Error getting order details for order_id {order_id}: {e}")
            return None

    def update_order_amount(self, order_id: int, new_amount: int) -> bool:
        """Update the amount for a specific order."""
        sql = "UPDATE orders SET amount = ? WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (new_amount, order_id))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Order {order_id} amount updated to {new_amount}")
                return True
            logger.warning(f"Order {order_id} not found for amount update.")
            return False
        except Exception as e:
            logger.error(f"Error updating order amount for order {order_id}: {e}")
            return False

    def delete_order(self, order_id: int, user_id: int) -> bool:
        """Delete a specific order, ensuring it belongs to the user."""
        # First, get file paths to delete them from disk
        file_paths = self.get_order_files(order_id)
        
        # Check ownership before deleting
        order = self.get_order_by_id(order_id)
        if not order or order['user_id'] != user_id:
            logger.warning(f"Attempt to delete order {order_id} by non-owner user {user_id}.")
            return False

        sql = "DELETE FROM orders WHERE id = ? AND user_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (order_id, user_id))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Order {order_id} deleted for user {user_id}")
                # Delete associated files from disk
                for file_path in file_paths:
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            logger.info(f"Deleted file: {file_path}")
                        except Exception as e:
                            logger.error(f"Error deleting file {file_path}: {e}")
                return True
            logger.warning(f"Order {order_id} not found or does not belong to user {user_id} for deletion.")
            return False
        except Exception as e:
            logger.error(f"Error deleting order {order_id}: {e}")
            return False

    def cancel_order(self, order_id: int, user_id: int) -> bool:
        """Cancel a specific order by setting status = 0, ensuring it belongs to the user."""
        # Check ownership before cancelling
        order = self.get_order_by_id(order_id)
        if not order or order['user_id'] != user_id:
            logger.warning(f"Attempt to cancel order {order_id} by non-owner user {user_id}.")
            return False

        sql = "UPDATE orders SET status = 0 WHERE id = ? AND user_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (order_id, user_id))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Order {order_id} cancelled for user {user_id}")
                return True
            logger.warning(f"Order {order_id} not found or does not belong to user {user_id} for cancellation.")
            return False
        except Exception as e:
            logger.error(f"Error cancelling order {order_id}: {e}")
            return False

    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get key metrics for the admin dashboard."""
        conn = self._get_connection()
        cursor = conn.cursor()
        stats = {
            'total_users': 0, 'total_orders': 0, 'total_collections': 0,
            'open_collections': 0, 'new_users_today': 0, 'new_orders_today': 0
        }
        try:
            cursor.execute("SELECT COUNT(id) FROM users")
            stats['total_users'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(id) FROM orders")
            stats['total_orders'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(id) FROM collections")
            stats['total_collections'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(id) FROM collections WHERE status = 'open'")
            stats['open_collections'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(id) FROM users WHERE DATE(created_at) = DATE('now', 'localtime')")
            stats['new_users_today'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(id) FROM orders WHERE DATE(created_at) = DATE('now', 'localtime')")
            stats['new_orders_today'] = cursor.fetchone()[0]
            return stats
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {e}")
            return stats

    def get_recent_orders(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get the most recent orders for the dashboard."""
        sql = """
            SELECT o.id, o.amount, o.created_at, u.name, u.surname, c.status as collection_status
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN collections c ON o.collection_id = c.id
            ORDER BY o.created_at DESC
            LIMIT ?
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting recent orders: {e}")
            return []

    def get_recent_users(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get the most recent users for the dashboard."""
        sql = """
            SELECT name, surname, phone, created_at
            FROM users
            ORDER BY created_at DESC
            LIMIT ?
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting recent users: {e}")
            return []

    def get_user_orders_by_collection(self, user_id: int, collection_id: int, limit: int = 10) -> list:
        """
        Get user orders for a specific collection with all their files.
        Args:
            user_id: The internal user ID
            collection_id: The collection ID to filter by
            limit: Maximum number of orders to return (default: 10)
        Returns:
            list: List of order dictionaries with files included
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get orders with collection info, filtered by both user_id AND collection_id
            orders_sql = """
                SELECT o.*, c.status as collection_status, c.created_at as collection_created_at
                FROM orders o 
                JOIN collections c ON o.collection_id = c.id 
                WHERE o.user_id = ? AND o.collection_id = ?
                ORDER BY o.id DESC
                LIMIT ?
            """
            cursor.execute(orders_sql, (user_id, collection_id, limit))
            orders = [dict(row) for row in cursor.fetchall()]
            
            if not orders:
                return []
            
            # Get all files for all orders in one query
            order_ids = [order['id'] for order in orders]
            placeholders = ','.join('?' for _ in order_ids)
            images_sql = f"""
                SELECT order_id, file_url 
                FROM order_files 
                WHERE order_id IN ({placeholders})
                ORDER BY order_id, id
            """
            cursor.execute(images_sql, order_ids)
            
            # Group images by order_id
            files_by_order = {}
            for row in cursor.fetchall():
                order_id = row[0]
                file_url = row[1]
                if order_id not in files_by_order:
                    files_by_order[order_id] = []
                files_by_order[order_id].append(file_url)
            
            # Add images to orders
            for order in orders:
                order['files'] = files_by_order.get(order['id'], [])
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting user orders by collection: {str(e)}")
            return []

    def get_user_orders_with_files(self, user_id: int, limit: int = 10) -> list:
        """
        Get user orders with all their files in optimized batch queries.
        Args:
            user_id: The internal user ID
            limit: Maximum number of orders to return (default: 10)
        Returns:
            list: List of order dictionaries with files included
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get orders with collection info
            orders_sql = """
                SELECT o.*, c.status as collection_status, c.created_at as collection_created_at
                FROM orders o 
                JOIN collections c ON o.collection_id = c.id 
                WHERE o.user_id = ? 
                ORDER BY o.id DESC
                LIMIT ?
            """
            cursor.execute(orders_sql, (user_id, limit))
            orders = [dict(row) for row in cursor.fetchall()]
            
            if not orders:
                return []
            
            # Get all files for all orders in one query
            order_ids = [order['id'] for order in orders]
            placeholders = ','.join('?' for _ in order_ids)
            images_sql = f"""
                SELECT order_id, file_url 
                FROM order_files 
                WHERE order_id IN ({placeholders})
                ORDER BY order_id, id
            """
            cursor.execute(images_sql, order_ids)
            
            # Group images by order_id
            files_by_order = {}
            for row in cursor.fetchall():
                order_id = row[0]
                file_url = row[1]
                if order_id not in files_by_order:
                    files_by_order[order_id] = []
                files_by_order[order_id].append(file_url)
            
            # Add images to orders
            for order in orders:
                order['files'] = files_by_order.get(order['id'], [])
            
            return orders
            
        except Exception as e:
            logger.error(f"Error getting user orders with files: {str(e)}")
            return []
    
    def get_user_collections_summary(self, user_id: int) -> List[Dict[str, Any]]:
        """Get summary data for all collections where the user has submitted orders."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get collections where this user has orders, with summary data
            sql = """
                SELECT 
                    c.id,
                    c.status,
                    c.created_at as opened_at,
                    c.close_at as closed_at,
                    c.finish_at as finished_at,
                    COUNT(o.id) as user_total_orders
                FROM collections c
                JOIN orders o ON c.id = o.collection_id
                WHERE o.user_id = ? AND o.status = 1
                GROUP BY c.id, c.status, c.created_at, c.close_at, c.finish_at
                ORDER BY c.created_at DESC
            """
            cursor.execute(sql, (user_id,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting user collections summary: {e}")
            return []

    # --- Card Management ---
    def get_all_cards(self) -> List[Dict[str, Any]]:
        """Get all cards from the database."""
        sql = "SELECT * FROM cards ORDER BY id DESC"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting all cards: {e}")
            return []

    def add_card(self, name: str, number: str) -> Optional[int]:
        """Add a new card to the database. If this is the first card, it will be activated automatically."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Check if this will be the first card
            cursor.execute("SELECT COUNT(*) FROM cards")
            card_count = cursor.fetchone()[0]
            is_first_card = card_count == 0
            
            # Insert the new card, making it active if it's the first one
            if is_first_card:
                cursor.execute("INSERT INTO cards (name, number, is_active) VALUES (?, ?, 1)", (name, number))
            else:
                cursor.execute("INSERT INTO cards (name, number) VALUES (?, ?)", (name, number))
            
            card_id = cursor.lastrowid
            conn.commit()
            
            if is_first_card:
                logger.info(f"First card '{name}' added with ID: {card_id} and activated automatically")
            else:
                logger.info(f"Card '{name}' added with ID: {card_id}")
            
            return card_id
        except sqlite3.IntegrityError:
            logger.warning(f"Card with number {number} already exists.")
            return None
        except Exception as e:
            conn.rollback()
            logger.error(f"Error adding card: {e}")
            return None

    def delete_card(self, card_id: int) -> bool:
        """Delete a card from the database, ensuring at least one card remains active."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Check total number of cards
            cursor.execute("SELECT COUNT(*) FROM cards")
            total_cards = cursor.fetchone()[0]
            
            # Don't allow deletion if this is the only card
            if total_cards <= 1:
                logger.warning(f"Cannot delete card {card_id}: only one card remaining")
                return False
            
            # Check if the card to delete is active
            cursor.execute("SELECT is_active FROM cards WHERE id = ?", (card_id,))
            card_result = cursor.fetchone()
            
            if not card_result:
                logger.warning(f"Card {card_id} not found")
                return False
                
            is_active = card_result[0]
            # Prevent deleting currently active card
            if is_active:
                logger.warning(f"Cannot delete active card {card_id}: activate another card first")
                return False

            # Delete the non-active card
            cursor.execute("DELETE FROM cards WHERE id = ?", (card_id,))
            
            conn.commit()
            logger.info(f"Card {card_id} deleted.")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Error deleting card: {e}")
            return False

    def set_active_card(self, card_id: int) -> bool:
        """Set a card as active, deactivating all others."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Deactivate all cards
            cursor.execute("UPDATE cards SET is_active = 0")
            # Activate the selected card
            cursor.execute("UPDATE cards SET is_active = 1 WHERE id = ?", (card_id,))
            conn.commit()
            logger.info(f"Card {card_id} set to active.")
            return cursor.rowcount > 0
        except Exception as e:
            conn.rollback()
            logger.error(f"Error setting active card: {e}")
            return False

    def get_active_card(self) -> Optional[Dict[str, Any]]:
        """Get the currently active card."""
        sql = "SELECT * FROM cards WHERE is_active = 1 LIMIT 1"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting active card: {e}")
            return None

    def update_card(self, card_id: int, name: str, number: str) -> bool:
        """Update card name and number. Ensures card number uniqueness."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE cards SET name = ?, number = ? WHERE id = ?", (name, number, card_id))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Card {card_id} updated: name='{name}' number='{number}'")
                return True
            logger.warning(f"Card {card_id} update affected 0 rows")
            return False
        except sqlite3.IntegrityError:
            logger.warning(f"Cannot update card {card_id}: number {number} already exists")
            return False
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating card {card_id}: {e}")
            return False

    def update_card_name(self, card_id: int, name: str) -> bool:
        """Update only the card name."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE cards SET name = ? WHERE id = ?", (name, card_id))
            conn.commit()
            updated = cursor.rowcount > 0
            if updated:
                logger.info(f"Card {card_id} name updated to '{name}'")
            return updated
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating card name {card_id}: {e}")
            return False

    def update_card_number(self, card_id: int, number: str) -> bool:
        """Update only the card number. Ensures uniqueness."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE cards SET number = ? WHERE id = ?", (number, card_id))
            conn.commit()
            updated = cursor.rowcount > 0
            if updated:
                logger.info(f"Card {card_id} number updated to '{number}'")
            return updated
        except sqlite3.IntegrityError:
            logger.warning(f"Cannot update card {card_id}: number {number} already exists")
            return False
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating card number {card_id}: {e}")
            return False

    def get_users_by_subscription_status(self, status: str) -> List[Dict[str, Any]]:
        """Get all users with a specific subscription status."""
        if status not in ['none', 'pending_payment', 'active']:
            logger.error(f"Invalid subscription status for query: {status}")
            return []
        sql = "SELECT * FROM users WHERE subscription_status = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (status,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting users by subscription status '{status}': {e}")
            return []

    def get_users_with_incomplete_subscription(self) -> List[Dict[str, Any]]:
        """Get users who completed registration but have not fully paid/confirmed.

        Criteria:
        - reg_step = 'done'
        - subscription_status IN ('none','pending_payment')  -- not confirmed/active
        - target_amount is NULL or target_amount <= 0 or paid_amount < target_amount
        """
        sql = (
            "SELECT * FROM users "
            "WHERE reg_step = 'done' "
            "AND subscription_status IN ('none','pending_payment') "
            "AND (target_amount IS NULL OR target_amount <= 0 OR paid_amount < target_amount)"
        )
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting users with incomplete subscription: {e}")
            return []

    # --- Payment Verification ---
    def add_transaction(self, card_id: int, amount: float, transaction_time: str, card_balance: Optional[float] = None, raw_message: Optional[str] = None) -> Optional[int]:
        """Add a new transaction record."""
        sql = "INSERT INTO transactions (card_id, amount, transaction_time, card_balance, raw_message) VALUES (?, ?, ?, ?, ?)"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (card_id, amount, transaction_time, card_balance, raw_message))
            transaction_id = cursor.lastrowid
            conn.commit()
            logger.info(f"Transaction added with ID: {transaction_id}")
            return transaction_id
        except Exception as e:
            logger.error(f"Error adding transaction: {e}")
            return None

    def add_payment(self, user_id: int, amount: float, receipt_url: str) -> Optional[int]:
        """Add a new payment record from a user."""
        sql = "INSERT INTO payment (user_id, amount, receipt_url, status) VALUES (?, ?, ?, 'pending')"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (user_id, amount, receipt_url))
            payment_id = cursor.lastrowid
            conn.commit()
            logger.info(f"Payment attempt recorded for user {user_id} with ID: {payment_id}")
            return payment_id
        except Exception as e:
            logger.error(f"Error adding payment: {e}")
            return None

    def find_matching_transaction_for_payment(self, payment_id: int, time_window_minutes: int = 15) -> Optional[Dict[str, Any]]:
        """Find a single, unlinked transaction that matches a payment's amount and time."""
        sql = """
            SELECT t.*
            FROM transactions t
            LEFT JOIN payment p_linked ON t.id = p_linked.transaction_id
            WHERE
                p_linked.id IS NULL AND
                t.amount = (SELECT amount FROM payment WHERE id = ?) AND
                t.transaction_time BETWEEN
                    datetime((SELECT created_at FROM payment WHERE id = ?), ?) AND
                    datetime((SELECT created_at FROM payment WHERE id = ?), ?)
        """
        time_window_str = f'-{time_window_minutes} minutes'
        time_window_str_plus = f'+{time_window_minutes} minutes'
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (payment_id, payment_id, time_window_str, payment_id, time_window_str_plus))
            rows = cursor.fetchall()
            # Return the transaction only if there is exactly one match
            if len(rows) == 1:
                return dict(rows[0])
            logger.warning(f"Found {len(rows)} matching transactions for payment {payment_id}. Expected 1.")
            return None
        except Exception as e:
            logger.error(f"Error finding matching transaction for payment {payment_id}: {e}")
            return None

    def link_payment_and_transaction(self, payment_id: int, transaction_id: int) -> bool:
        """Link a payment to a transaction."""
        sql = "UPDATE payment SET transaction_id = ?, status = 'verified' WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (transaction_id, payment_id))
            conn.commit()
            logger.info(f"Payment {payment_id} linked to transaction {transaction_id}")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error linking payment {payment_id} to transaction {transaction_id}: {e}")
            return False

    def update_payment_status(self, payment_id: int, status: str) -> bool:
        """Update the status of a payment record."""
        if status not in ['pending', 'verified', 'failed', 'manual_review']:
            logger.error(f"Invalid payment status: {status}")
            return False
        sql = "UPDATE payment SET status = ? WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (status, payment_id))
            conn.commit()
            logger.info(f"Payment {payment_id} status updated to {status}")
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating payment status for {payment_id}: {e}")
            return False

    def get_payment_by_id(self, payment_id: int) -> Optional[Dict[str, Any]]:
        """Get payment details by ID."""
        sql = "SELECT * FROM payment WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (payment_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting payment by ID {payment_id}: {e}")
            return None

    def get_payments_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get all payments with a specific status."""
        if status not in ['pending', 'verified', 'failed', 'manual_review']:
            logger.error(f"Invalid payment status: {status}")
            return []
        
        sql = "SELECT * FROM payment WHERE status = ? ORDER BY created_at ASC"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (status,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting payments by status {status}: {e}")
            return []

    def get_latest_payment_for_user(self, user_id: int, statuses: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """Get the latest payment for a given user filtered by status list.

        Args:
            user_id: Internal user id
            statuses: Optional list of acceptable statuses; defaults to ['pending', 'manual_review']
        Returns:
            The most recent payment row as dict, or None if not found
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            if not statuses:
                statuses = ['pending', 'manual_review']

            # Build a dynamic IN clause with proper placeholders
            placeholders = ','.join('?' for _ in statuses)
            sql = f"SELECT * FROM payment WHERE user_id = ? AND status IN ({placeholders}) ORDER BY created_at DESC LIMIT 1"
            params = [user_id] + statuses
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting latest payment for user {user_id}: {e}")
            return None

    def get_transaction_by_id(self, transaction_id: int) -> Optional[Dict[str, Any]]:
        """Get transaction details by ID."""
        sql = "SELECT * FROM transactions WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (transaction_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting transaction by ID {transaction_id}: {e}")
            return None

    def get_pending_transactions(self) -> List[Dict[str, Any]]:
        """Get all transactions where is_done = false."""
        sql = "SELECT * FROM transactions WHERE is_done = 0 ORDER BY transaction_time DESC"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting pending transactions: {e}")
            return []

    def mark_transaction_done(self, transaction_id: int) -> bool:
        """Mark a transaction as done (is_done = true)."""
        sql = "UPDATE transactions SET is_done = 1 WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (transaction_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Transaction {transaction_id} marked as done")
                return True
            else:
                logger.warning(f"Transaction {transaction_id} not found for marking done")
                return False
        except Exception as e:
            logger.error(f"Error marking transaction {transaction_id} as done: {e}")
            return False

    def get_expired_finished_collections(self, days_ago: int = 30) -> List[Dict[str, Any]]:
        """Get finished collections that were finished more than `days_ago` days ago."""
        sql = "SELECT * FROM collections WHERE status = 'finish' AND finish_at IS NOT NULL AND finish_at < datetime('now', ?)"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (f'-{days_ago} days',))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting expired finished collections: {e}")
            return []

    def get_order_ids_by_collection(self, collection_id: int) -> List[int]:
        """Get all order IDs for a specific collection."""
        sql = "SELECT id FROM orders WHERE collection_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Error getting order IDs by collection: {e}")
            return []

# Create a global database service instance
db_service = DatabaseService()
