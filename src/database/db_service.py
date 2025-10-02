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
                    region TEXT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT 1,
                    reg_step TEXT NOT NULL DEFAULT 'name' CHECK (reg_step IN ('name', 'surname', 'phone', 'region', 'done')),
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
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    all_reports_sent BOOLEAN NOT NULL DEFAULT 0
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
                    final_notification_sent BOOLEAN NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (collection_id) REFERENCES collections (id)
                )
            """)
            
            # Create order_files table - idempotent (new schema allows many-to-many via composite unique)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    file_url TEXT,
                    file_unique_id TEXT,
                    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'downloaded', 'failed')),
                    FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
                    UNIQUE (order_id, file_unique_id)
                )
            """)
            
            # Create product_media table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_channel_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    file_unique_id TEXT NOT NULL UNIQUE,
                    media_type TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    thumbnail_path TEXT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (source_channel_id, source_message_id)
                )
            """)
            
            # Migration: Add status column to existing orders table if it doesn't exist
            try:
                cursor.execute("PRAGMA table_info(orders)")
                columns = [column[1] for column in cursor.fetchall()]
                if 'status' not in columns:
                    cursor.execute("ALTER TABLE orders ADD COLUMN status INTEGER NOT NULL DEFAULT 1") # type: ignore
                    logger.info("Added status column to orders table")
                if 'final_notification_sent' not in columns:
                    cursor.execute("ALTER TABLE orders ADD COLUMN final_notification_sent BOOLEAN NOT NULL DEFAULT 0") # type: ignore
                    logger.info("Added final_notification_sent column to orders table")
            except Exception as e:
                logger.warning(f"Could not add status column to orders table: {e}")

            # Migration: Ensure order_files supports many-to-many (remove UNIQUE on file_unique_id, add UNIQUE(order_id,file_unique_id))
            # Migration: Add all_reports_sent to collections if it doesn't exist
            try:
                cursor.execute("PRAGMA table_info(collections)")
                columns = [column[1] for column in cursor.fetchall()]
                if 'all_reports_sent' not in columns:
                    cursor.execute("ALTER TABLE collections ADD COLUMN all_reports_sent BOOLEAN NOT NULL DEFAULT 0") # type: ignore
                    logger.info("Added all_reports_sent column to collections table")
            except Exception as e:
                logger.warning(f"Could not add all_reports_sent column to collections table: {e}")
            try:
                needs_migration = False
                has_composite_unique = False
                # Inspect indexes
                cursor.execute("PRAGMA index_list(order_files)")
                index_list = cursor.fetchall() or []
                for idx in index_list:
                    idx_name = idx[1]
                    is_unique = bool(idx[2])
                    if not is_unique:
                        continue
                    cursor.execute(f"PRAGMA index_info('{idx_name}')")
                    cols = [row[2] for row in cursor.fetchall()]
                    if cols == ['file_unique_id']:
                        needs_migration = True
                    if cols == ['order_id', 'file_unique_id']:
                        has_composite_unique = True
                if needs_migration or not has_composite_unique:
                    logger.info("Migrating order_files table to many-to-many (composite UNIQUE)")
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS order_files_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            order_id INTEGER NOT NULL,
                            file_url TEXT,
                            file_unique_id TEXT,
                            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','downloaded','failed')),
                            FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
                            UNIQUE (order_id, file_unique_id)
                        )
                    """)
                    cursor.execute("INSERT INTO order_files_new (order_id, file_url, file_unique_id, status) SELECT order_id, file_url, file_unique_id, status FROM order_files")
                    cursor.execute("ALTER TABLE order_files RENAME TO order_files_backup")
                    cursor.execute("ALTER TABLE order_files_new RENAME TO order_files")
                    cursor.execute("DROP TABLE IF EXISTS order_files_backup")
                    logger.info("order_files migration completed")
            except Exception as e:
                logger.warning(f"Could not ensure many-to-many for order_files: {e}")

            # Migration: Add status column to product_media table if it doesn't exist
            try:
                cursor.execute("PRAGMA table_info(product_media)")
                product_media_columns = [column[1] for column in cursor.fetchall()]
                if 'status' not in product_media_columns:
                    cursor.execute("ALTER TABLE product_media ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'") # type: ignore
                    logger.info("Added status column to product_media table")
            except Exception as e:
                logger.warning(f"Could not add status column to product_media table: {e}")

            # Create cards table - idempotent (no type column)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    number TEXT NOT NULL UNIQUE,
                    is_active BOOLEAN NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Migration: Remove legacy 'type' column if present via table rebuild
            try:
                cursor.execute("PRAGMA table_info(cards)")
                card_columns = [column[1] for column in cursor.fetchall()]
                if 'type' in card_columns:
                    logger.info("Rebuilding cards table to drop legacy 'type' column")
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS cards_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            number TEXT NOT NULL UNIQUE,
                            is_active BOOLEAN NOT NULL DEFAULT 0,
                            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("INSERT INTO cards_new (id, name, number, is_active, created_at) SELECT id, name, number, is_active, created_at FROM cards")
                    cursor.execute("ALTER TABLE cards RENAME TO cards_backup")
                    cursor.execute("ALTER TABLE cards_new RENAME TO cards")
                    cursor.execute("DROP TABLE IF EXISTS cards_backup")
                    logger.info("Dropped legacy 'type' column from cards table")
            except Exception as e:
                logger.warning(f"Could not rebuild cards table to drop 'type' column: {e}")

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
            # Optimize frequent queries on orders and order_files
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_collection ON orders(user_id, collection_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_collection ON orders(collection_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_files_order_id ON order_files(order_id)")

            # Create user_reports table - idempotent
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    collection_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    sent_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NULL,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    FOREIGN KEY (collection_id) REFERENCES collections (id)
                )
            """)
            # Helpful indexes for reports
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_reports_user ON user_reports(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_reports_collection ON user_reports(collection_id)")
            
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
        if field not in ['name', 'surname', 'phone', 'code', 'region']:
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
    
    def find_existing_order(self, user_id: int, collection_id: int, original_message_id: int, original_channel_id: int) -> Optional[Dict[str, Any]]:
        """
        Finds an existing, active order for a user, collection, and specific product.
        Args:
            user_id: The internal user ID.
            collection_id: The ID of the active collection.
            original_message_id: The ID of the forwarded message.
            original_channel_id: The ID of the source channel.
        Returns:
            Dict containing order data if found, otherwise None.
        """
        sql = """
            SELECT * FROM orders
            WHERE user_id = ?
              AND collection_id = ?
              AND original_message_id = ?
              AND original_channel_id = ?
              AND status = 1
            LIMIT 1
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (user_id, collection_id, original_message_id, original_channel_id))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error finding existing order: {e}")
            return None

    def add_order_file(self, order_id: int, file_unique_id: str) -> bool:
        """Adds a placeholder for a file in an order with 'pending' status.

        Allows the same file to be used across different orders.
        Prevents duplicates within the same order via composite UNIQUE.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = "INSERT OR IGNORE INTO order_files (order_id, file_unique_id, status) VALUES (?, ?, 'pending')"
            cursor.execute(sql, (order_id, file_unique_id))
            conn.commit()
            if cursor.rowcount == 0:
                logger.info(f"File {file_unique_id} already present for order {order_id}, skipping placeholder insert")
                return True
            logger.info(f"Added file placeholder for order {order_id}, file {file_unique_id}")
            return True
        except Exception as e:
            logger.error(f"Error adding order file placeholder: {str(e)}")
            return False

    def update_order_file(self, file_unique_id: str, status: str, file_path: Optional[str] = None, order_id: Optional[int] = None) -> bool:
        """Updates the status and file_url for a file.

        Preferred path: update by (order_id, file_unique_id).
        Fallback: update the first pending row for order when file_unique_id is missing (legacy placeholders).
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if file_unique_id and order_id is not None:
                # Update by composite key for correctness
                sql = "UPDATE order_files SET status = ?, file_url = ? WHERE order_id = ? AND file_unique_id = ?"
                cursor.execute(sql, (status, file_path, order_id, file_unique_id))
                if cursor.rowcount == 0:
                    # Fallback: update by file_unique_id only (older schema rows)
                    sql2 = "UPDATE order_files SET status = ?, file_url = ? WHERE file_unique_id = ?"
                    cursor.execute(sql2, (status, file_path, file_unique_id))
                if cursor.rowcount == 0 and order_id is not None:
                    # Final fallback for legacy placeholder without unique_id
                    sql3 = """UPDATE order_files SET status = ?, file_url = ? 
                              WHERE id = (SELECT id FROM order_files 
                                        WHERE order_id = ? AND file_unique_id IS NULL AND status = 'pending' 
                                        AND (file_url LIKE 'pending_download_%' OR file_url IS NULL)
                                        LIMIT 1)"""
                    cursor.execute(sql3, (status, file_path, order_id))
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

    def atomically_open_new_collection(self) -> Optional[int]:
        """Atomically close the current open collection (if any) and open a new one.

        Ensures there is at most one open collection by performing both operations
        within a single SQLite transaction.

        Returns:
            The new collection id if successful, otherwise None.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Begin an immediate transaction to acquire a RESERVED lock and prevent races
            cursor.execute("BEGIN IMMEDIATE")

            # Close currently active collection if it exists
            cursor.execute("SELECT id FROM collections WHERE status = 'open' ORDER BY created_at DESC LIMIT 1")
            row = cursor.fetchone()
            if row:
                active_id = row[0]
                cursor.execute(
                    "UPDATE collections SET status = 'close', close_at = datetime('now') WHERE id = ?",
                    (active_id,)
                )

            # Create the new open collection
            cursor.execute(
                "INSERT INTO collections (status, created_at) VALUES ('open', datetime('now'))"
            )
            new_collection_id = cursor.lastrowid

            conn.commit()
            logger.info(f"Atomically opened new collection #{new_collection_id} (previous closed: {row[0] if row else 'none'})")
            return int(new_collection_id) if new_collection_id else None
        except Exception as e:
            try:
                # Rollback if something went wrong
                self._get_connection().rollback()
            except Exception:
                pass
            logger.error(f"Error performing atomic collection rotation: {e}")
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
        if reg_step not in ['name', 'surname', 'phone', 'region', 'done']:
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
        Get all orders with user and collection details including first file URL.
        Args:
            limit: Maximum number of orders to return (default: 50)
        Returns:
            list: List of order dictionaries with user and collection info
        """
        sql = """
            SELECT o.*,
                   u.name, u.surname, u.phone, u.username, u.telegram_id, u.code,
                   c.status as collection_status, c.created_at as collection_created_at,
                   (SELECT COUNT(*) FROM order_files oi WHERE oi.order_id = o.id) as file_count,
                   (SELECT file_url FROM order_files oi WHERE oi.order_id = o.id ORDER BY oi.id LIMIT 1) as first_file_url
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
        Merge orders from one collection to another, merging duplicate products for the same user.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Get all active orders from the 'from' collection
            cursor.execute("SELECT * FROM orders WHERE collection_id = ? AND status = 1", (from_collection_id,))
            from_orders = [dict(row) for row in cursor.fetchall()]

            for from_order in from_orders:
                # Check for a matching order in the 'to' collection
                cursor.execute("""
                    SELECT id, amount FROM orders
                    WHERE user_id = ?
                      AND collection_id = ?
                      AND original_message_id = ?
                      AND original_channel_id = ?
                      AND status = 1
                    LIMIT 1
                """, (from_order['user_id'], to_collection_id, from_order['original_message_id'], from_order['original_channel_id']))
                
                to_order_match = cursor.fetchone()

                if to_order_match:
                    to_order_id = to_order_match['id']
                    to_order_amount = to_order_match['amount'] or 0
                    from_order_amount = from_order['amount'] or 0
                    
                    new_amount = to_order_amount + from_order_amount
                    
                    # Update the 'to' order with the merged amount
                    cursor.execute("UPDATE orders SET amount = ? WHERE id = ?", (new_amount, to_order_id))
                    
                    # Move files from 'from' order to 'to' order, ignoring duplicates
                    cursor.execute("SELECT file_url, file_unique_id, status FROM order_files WHERE order_id = ?", (from_order['id'],))
                    from_files = cursor.fetchall()
                    for file_row in from_files:
                        cursor.execute("""
                            INSERT OR IGNORE INTO order_files (order_id, file_url, file_unique_id, status) 
                            VALUES (?, ?, ?, ?)
                        """, (to_order_id, file_row['file_url'], file_row['file_unique_id'], file_row['status']))

                    # Delete the now-merged 'from' order
                    cursor.execute("DELETE FROM orders WHERE id = ?", (from_order['id'],))
                    logger.info(f"Merged order #{from_order['id']} into #{to_order_id}. New amount: {new_amount}")
                else:
                    # No match, just move the order
                    cursor.execute("UPDATE orders SET collection_id = ? WHERE id = ?", (to_collection_id, from_order['id']))
            
            # Move any non-active (cancelled) orders as well, without merging
            cursor.execute("UPDATE orders SET collection_id = ? WHERE collection_id = ? AND status != 1", (to_collection_id, from_collection_id))

            conn.commit()
            logger.info(f"Finished merging orders from collection {from_collection_id} to {to_collection_id}")
            return True
        except Exception as e:
            conn.rollback()
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
        sql = "SELECT file_url FROM order_files WHERE order_id = ? AND status = 'downloaded' AND file_url IS NOT NULL ORDER BY id ASC"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (order_id,))
            rows = cursor.fetchall()
            file_urls = [row[0] for row in rows if row[0]]

            # Fallback: if no order_files, try resolving via product_media using order's original message
            if not file_urls:
                try:
                    # Get order details directly to avoid recursion
                    order_sql = "SELECT original_channel_id, original_message_id FROM orders WHERE id = ?"
                    cursor.execute(order_sql, (order_id,))
                    order_row = cursor.fetchone()
                    
                    if order_row and order_row['original_channel_id'] and order_row['original_message_id']:
                        pm = self.get_product_media_by_message_id(order_row['original_channel_id'], order_row['original_message_id'])
                        if pm and pm.get('file_path'):
                            file_urls = [pm['file_path']]
                except Exception as _e:
                    logger.warning(f"Fallback to product_media failed for order {order_id}: {_e}", exc_info=True)

            return file_urls
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

    def update_order_amount(self, order_id: int, amount: int, merge: bool = False) -> bool:
        """Update the amount for a specific order. Can set or add to the existing amount."""
        if merge:
            sql = "UPDATE orders SET amount = COALESCE(amount, 0) + ? WHERE id = ?"
        else:
            sql = "UPDATE orders SET amount = ? WHERE id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (amount, order_id))
            conn.commit()
            if cursor.rowcount > 0:
                if merge:
                    logger.info(f"Order {order_id} amount merged with {amount}")
                else:
                    logger.info(f"Order {order_id} amount updated to {amount}")
                return True
            logger.warning(f"Order {order_id} not found for amount update.")
            return False
        except Exception as e:
            logger.error(f"Error updating order amount for order {order_id}: {e}")
            return False

    def mark_final_notification_sent(self, order_id: int) -> bool:
        """Mark that the final notification has been sent for this order."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE orders SET final_notification_sent = 1 WHERE id = ?",
                (order_id,)
            )
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Marked final notification as sent for order {order_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error marking final notification sent for order {order_id}: {e}")
            return False

    def has_final_notification_been_sent(self, order_id: int) -> bool:
        """Check if the final notification has already been sent for this order."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT final_notification_sent FROM orders WHERE id = ?",
                (order_id,)
            )
            result = cursor.fetchone()
            return bool(result and result[0]) if result else False
        except Exception as e:
            logger.error(f"Error checking final notification status for order {order_id}: {e}")
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

    # Legacy helper removed: type is no longer tracked; suffix-only lookup is not required for storage

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
                (
                    t.transaction_time BETWEEN
                        datetime((SELECT created_at FROM payment WHERE id = ?), ?) AND
                        datetime((SELECT created_at FROM payment WHERE id = ?), ?)
                    OR
                    t.transaction_time BETWEEN
                        datetime((SELECT created_at FROM payment WHERE id = ?), 'localtime', ?) AND
                        datetime((SELECT created_at FROM payment WHERE id = ?), 'localtime', ?)
                )
        """
        time_window_str = f'-{time_window_minutes} minutes'
        time_window_str_plus = f'+{time_window_minutes} minutes'
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                sql,
                (
                    payment_id,
                    payment_id, time_window_str, payment_id, time_window_str_plus,
                    payment_id, time_window_str, payment_id, time_window_str_plus,
                ),
            )
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

    def get_pending_transactions_for_payment(self, payment_id: int, time_window_minutes: int) -> List[Dict[str, Any]]:
        """
        Get pending (unlinked, not done) transactions within a time window around the payment's created_at.

        Args:
            payment_id: ID of the payment to anchor the time window
            time_window_minutes: Minutes before and after payment.created_at to include

        Returns:
            List of transaction dicts matching the window and not marked done
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Fetch the payment created_at to anchor the window
            cursor.execute("SELECT created_at FROM payment WHERE id = ?", (payment_id,))
            row = cursor.fetchone()
            if not row or not row[0]:
                logger.warning(f"Payment {payment_id} not found or missing created_at for windowed transaction fetch")
                return []

            # SQLite datetime modifiers require strings like '-10 minutes' and '+10 minutes'
            minus_mod = f"-{int(time_window_minutes)} minutes"
            plus_mod = f"+{int(time_window_minutes)} minutes"

            sql = (
                """
                SELECT t.*
                FROM transactions t
                LEFT JOIN payment p_linked ON t.id = p_linked.transaction_id
                WHERE t.is_done = 0
                  AND p_linked.id IS NULL
                  AND (
                        t.transaction_time BETWEEN datetime(?, ?) AND datetime(?, ?)
                     OR t.transaction_time BETWEEN datetime(?, 'localtime', ?) AND datetime(?, 'localtime', ?)
                  )
                ORDER BY t.transaction_time DESC
                """
            )
            cursor.execute(sql, (row[0], minus_mod, row[0], plus_mod, row[0], minus_mod, row[0], plus_mod))
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Error getting windowed pending transactions for payment {payment_id}: {e}")
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

    # --- Stale data batch queries for cleanup ---
    def get_stale_orders_batch(self, cutoff_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Return a batch of stale orders older than cutoff with associated file paths.

        Each item: { 'id': int, 'file_paths': List[str] }
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            # Fetch order ids older than cutoff in small batches
            cursor.execute(
                """
                SELECT o.id
                FROM orders o
                WHERE o.created_at < ?
                ORDER BY o.id ASC
                LIMIT ?
                """,
                (cutoff_iso, int(limit)),
            )
            order_rows = cursor.fetchall()
            if not order_rows:
                return []

            order_ids = [int(row[0]) for row in order_rows]

            # Fetch files for these orders in one query
            placeholders = ','.join('?' for _ in order_ids)
            cursor.execute(
                f"""
                SELECT order_id, file_url
                FROM order_files
                WHERE order_id IN ({placeholders}) AND file_url IS NOT NULL AND status = 'downloaded'
                ORDER BY order_id, id
                """,
                order_ids,
            )
            files_by_order: Dict[int, List[str]] = {}
            for row in cursor.fetchall():
                oid = int(row[0])
                fpath = row[1]
                if oid not in files_by_order:
                    files_by_order[oid] = []
                if fpath:
                    files_by_order[oid].append(fpath)

            return [{"id": oid, "file_paths": files_by_order.get(oid, [])} for oid in order_ids]
        except Exception as e:
            logger.error(f"Error fetching stale orders batch: {e}")
            return []

    def get_stale_payments_batch(self, cutoff_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Return a batch of stale payments older than cutoff with receipt paths if any."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, receipt_url
                FROM payment
                WHERE created_at < ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (cutoff_iso, int(limit)),
            )
            rows = cursor.fetchall()
            items: List[Dict[str, Any]] = []
            for row in rows:
                pid = int(row[0])
                rurl = row[1]
                file_paths: List[str] = [rurl] if rurl else []
                items.append({"id": pid, "file_paths": file_paths})
            return items
        except Exception as e:
            logger.error(f"Error fetching stale payments batch: {e}")
            return []

    def get_stale_product_media_batch(self, cutoff_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Return a batch of stale product_media older than cutoff with file and thumbnail paths."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, file_path, thumbnail_path
                FROM product_media
                WHERE created_at < ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (cutoff_iso, int(limit)),
            )
            rows = cursor.fetchall()
            items: List[Dict[str, Any]] = []
            for row in rows:
                mid = int(row[0])
                fpath = row[1]
                tpath = row[2]
                file_paths: List[str] = []
                if fpath:
                    file_paths.append(fpath)
                if tpath:
                    file_paths.append(tpath)
                items.append({"id": mid, "file_paths": file_paths})
            return items
        except Exception as e:
            logger.error(f"Error fetching stale product media batch: {e}")
            return []

    def get_stale_transactions_batch(self, cutoff_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Return a batch of stale transactions older than cutoff that are not linked to any payment. No files to delete."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT t.id
                FROM transactions t
                WHERE t.created_at < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM payment p WHERE p.transaction_id = t.id
                  )
                ORDER BY id ASC
                LIMIT ?
                """,
                (cutoff_iso, int(limit)),
            )
            rows = cursor.fetchall()
            return [{"id": int(row[0]), "file_paths": []} for row in rows]
        except Exception as e:
            logger.error(f"Error fetching stale transactions batch: {e}")
            return []

    def get_stale_collections_batch(self, cutoff_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Return a batch of stale collections older than cutoff that have no dependent orders or reports."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT c.id
                FROM collections c
                WHERE c.created_at < ?
                  AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.collection_id = c.id)
                  AND NOT EXISTS (SELECT 1 FROM user_reports ur WHERE ur.collection_id = c.id)
                ORDER BY id ASC
                LIMIT ?
                """,
                (cutoff_iso, int(limit)),
            )
            rows = cursor.fetchall()
            return [{"id": int(row[0]), "file_paths": []} for row in rows]
        except Exception as e:
            logger.error(f"Error fetching stale collections batch: {e}")
            return []

    def get_stale_reports_batch(self, cutoff_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Return a batch of stale reports older than cutoff based on updated_at/sent_at."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, file_path
                FROM user_reports
                WHERE COALESCE(updated_at, sent_at) < ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (cutoff_iso, int(limit)),
            )
            rows = cursor.fetchall()
            items: List[Dict[str, Any]] = []
            for row in rows:
                rid = int(row[0])
                fpath = row[1]
                file_paths: List[str] = [fpath] if fpath else []
                items.append({"id": rid, "file_paths": file_paths})
            return items
        except Exception as e:
            logger.error(f"Error fetching stale reports batch: {e}")
            return []

    def delete_records_by_ids(self, table_name: str, ids: List[int]) -> bool:
        """Bulk delete records by ids with a whitelist of tables."""
        try:
            if not ids:
                return True
            allowed_tables = {"orders", "payment", "user_reports", "product_media", "transactions", "collections"}
            if table_name not in allowed_tables:
                logger.error(f"Attempted delete on non-allowed table: {table_name}")
                return False

            conn = self._get_connection()
            cursor = conn.cursor()
            placeholders = ','.join('?' for _ in ids)
            sql = f"DELETE FROM {table_name} WHERE id IN ({placeholders})"
            cursor.execute(sql, [int(i) for i in ids])
            conn.commit()
            logger.info(f"Deleted {cursor.rowcount} rows from {table_name} (ids count: {len(ids)})")
            return True
        except Exception as e:
            logger.error(f"Error bulk deleting from {table_name}: {e}")
            return False

    def get_product_media_by_message_id(self, channel_id: int, message_id: int) -> Optional[Dict[str, Any]]:
        """Fetch product_media by (source_channel_id, source_message_id)."""
        sql = (
            "SELECT * FROM product_media WHERE source_channel_id = ? AND source_message_id = ? LIMIT 1"
        )
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (channel_id, message_id))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting product_media for channel {channel_id} message {message_id}: {e}")
            return None

    def create_product_media(self, media_data: Dict[str, Any]) -> Optional[int]:
        """Insert a new product_media row.
        Required keys: source_channel_id, source_message_id, file_unique_id, media_type, file_path, thumbnail_path (optional)
        """
        sql = (
            "INSERT INTO product_media (source_channel_id, source_message_id, file_unique_id, media_type, file_path, thumbnail_path) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                sql,
                (
                    media_data["source_channel_id"],
                    media_data["source_message_id"],
                    media_data["file_unique_id"],
                    media_data["media_type"],
                    media_data["file_path"],
                    media_data.get("thumbnail_path"),
                ),
            )
            new_id = cursor.lastrowid
            conn.commit()
            return new_id
        except sqlite3.IntegrityError as e:
            logger.warning(f"Product media already exists or constraint error: {e}")
            return None
        except Exception as e:
            logger.error(f"Error creating product_media: {e}")
            return None

    def update_product_media_status(self, file_unique_id: str, status: str) -> bool:
        """
        Update the status of a product_media entry.

        Args:
            file_unique_id: The unique file ID to update
            status: New status ('pending', 'downloading', 'available', 'failed')

        Returns:
            True if updated successfully, False otherwise
        """
        sql = "UPDATE product_media SET status = ? WHERE file_unique_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (status, file_unique_id))
            conn.commit()

            if cursor.rowcount > 0:
                logger.debug(f"Updated product_media status to '{status}' for file {file_unique_id}")
                return True
            else:
                logger.warning(f"No product_media found with file_unique_id: {file_unique_id}")
                return False

        except Exception as e:
            logger.error(f"Error updating product_media status: {e}")
            return False

    def get_product_media_by_file_id(self, file_unique_id: str) -> Optional[Dict[str, Any]]:
        """Get product_media by file_unique_id."""
        sql = "SELECT * FROM product_media WHERE file_unique_id = ? LIMIT 1"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (file_unique_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting product_media by file_unique_id {file_unique_id}: {e}")
            return None

    def get_pending_product_media(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get product_media entries with status 'pending' or 'failed' for retry."""
        sql = "SELECT * FROM product_media WHERE status IN ('pending', 'failed') ORDER BY created_at ASC LIMIT ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting pending product_media: {e}")
            return []

    def get_collection_details(self, collection_id: int) -> Optional[Dict[str, Any]]:
        """Get collection metadata and key statistics."""
        sql = """
            SELECT
                c.*,
                COUNT(o.id) as total_orders,
                COUNT(DISTINCT o.user_id) as unique_users,
                COALESCE(SUM(o.amount), 0) as total_revenue
            FROM collections c
            LEFT JOIN orders o ON c.id = o.collection_id AND o.status = 1
            WHERE c.id = ?
            GROUP BY c.id
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting collection details for id {collection_id}: {e}")
            return None

    def get_users_with_orders_in_collection(self, collection_id: int) -> List[Dict[str, Any]]:
        """
        Fetches all users and their orders for a specific collection in an optimized way.
        Returns a list of users, each containing their list of orders.
        """
        sql = """
            SELECT
                u.id as user_id,
                u.name,
                u.surname,
                u.username,
                u.telegram_id,
                u.region,
                u.code,
                u.phone,
                o.id as order_id,
                o.amount,
                o.created_at,
                o.status as order_status,
                (SELECT COUNT(*) FROM order_files ofc WHERE ofc.order_id = o.id) as file_count,
                (SELECT of.file_url FROM order_files of WHERE of.order_id = o.id ORDER BY of.id LIMIT 1) as first_file_url
            FROM orders o
            JOIN users u ON o.user_id = u.id
            WHERE o.collection_id = ? AND o.status = 1
            ORDER BY u.name, u.surname, o.id DESC
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            rows = cursor.fetchall()

            users_dict = {}
            for row in rows:
                user_id = row['user_id']
                if user_id not in users_dict:
                    users_dict[user_id] = {
                        'id': user_id,
                        'name': row['name'],
                        'surname': row['surname'],
                        'username': row['username'],
                        'telegram_id': row['telegram_id'],
                        'region': row['region'],
                        'code': row['code'],
                        'phone': row['phone'],
                        'orders': []
                    }
            
                users_dict[user_id]['orders'].append({
                    'id': row['order_id'],
                    'amount': row['amount'],
                    'created_at': row['created_at'],
                    'status': row['order_status'],
                    'file_count': row['file_count'],
                    'first_file_url': row['first_file_url']
                })
            
            return list(users_dict.values())
        except Exception as e:
            logger.error(f"Error getting users with orders for collection {collection_id}: {e}")
            return []

    def get_reports_for_user(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all reports for a specific user, joined with collection date."""
        sql = (
            """
            SELECT ur.*, c.created_at as collection_date
            FROM user_reports ur
            JOIN collections c ON ur.collection_id = c.id
            WHERE ur.user_id = ?
            ORDER BY ur.sent_at DESC
            """
        )
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting reports for user {user_id}: {e}")
            return []

    def create_or_update_user_report(self, user_id: int, collection_id: int, file_path: str) -> Optional[int]:
        """Create or update a report for a user in a collection."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Check if a report already exists
            cursor.execute(
                "SELECT id FROM user_reports WHERE user_id = ? AND collection_id = ?",
                (user_id, collection_id),
            )
            existing_report = cursor.fetchone()

            if existing_report:
                report_id = existing_report[0]
                sql = "UPDATE user_reports SET file_path = ?, updated_at = datetime('now') WHERE id = ?"
                cursor.execute(sql, (file_path, report_id))
                logger.info(
                    f"Updated report {report_id} for user {user_id}, collection {collection_id}"
                )
            else:
                sql = (
                    "INSERT INTO user_reports (user_id, collection_id, file_path) VALUES (?, ?, ?)"
                )
                cursor.execute(sql, (user_id, collection_id, file_path))
                report_id = cursor.lastrowid
                logger.info(
                    f"Created new report {report_id} for user {user_id}, collection {collection_id}"
                )

            conn.commit()
            return int(report_id)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(
                f"Error creating/updating report for user {user_id}, collection {collection_id}: {e}"
            )
            return None

    def get_user_report_status_for_collection(self, collection_id: int) -> Dict[int, bool]:
        """
        Get a dictionary of user_id -> has_report for a given collection.
        """
        sql = "SELECT user_id FROM user_reports WHERE collection_id = ?"
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            rows = cursor.fetchall()
            return {int(row[0]): True for row in rows}
        except Exception as e:
            logger.error(f"Error getting report status for collection {collection_id}: {e}")
            return {}

    def check_and_set_all_reports_sent(self, collection_id: int) -> bool:
        """
        Check if all users with orders in a collection have a report.
        If so, set the all_reports_sent flag on the collection.
        """
        try:
            users_with_orders = self.get_users_with_orders_in_collection(collection_id)
            if not users_with_orders:
                return False  # No users, so nothing to do

            user_ids_with_orders = {int(user['id']) for user in users_with_orders}

            users_with_reports = self.get_user_report_status_for_collection(collection_id)
            user_ids_with_reports = set(users_with_reports.keys())

            if user_ids_with_orders.issubset(user_ids_with_reports):
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE collections SET all_reports_sent = 1 WHERE id = ?",
                    (collection_id,),
                )
                conn.commit()
                logger.info(
                    f"All reports sent for collection {collection_id}. Flag set."
                )
                return True
            else:
                missing_ids = user_ids_with_orders - user_ids_with_reports
                logger.info(
                    f"Not all reports sent for collection {collection_id}. Missing for users: {missing_ids}"
                )
                return False
        except Exception as e:
            logger.error(
                f"Error in check_and_set_all_reports_sent for collection {collection_id}: {e}"
            )
            return False

# Create a global database service instance
db_service = DatabaseService()
