"""
Configuration management for the Telegram Product Submission Tracker Bot.
Loads bootstrap environment variables and then loads dynamic settings from a config database.
"""

import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration class that loads and validates environment variables."""
    
    # Bootstrap settings from .env
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    DATABASE_FILE = os.getenv('DATABASE_FILE', 'database.db')
    CONFIG_DATABASE_FILE = os.getenv('CONFIG_DATABASE_FILE', 'config.db')
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
    SECRET_KEY = os.getenv('SECRET_KEY')
    TEST_ACCOUNT_ID = int(os.getenv('TEST_ACCOUNT_ID')) if os.getenv('TEST_ACCOUNT_ID') else None
    PAYMENT_CHECKING_WAIT_TIME = int(os.getenv('PAYMENT_TIME', '30'))
    AI_CONFIRMATION_TIME_WINDOW_MINUTES = int(os.getenv('AI_CONFIRMATION_TIME_WINDOW_MINUTES', '10'))
    TIMEZONE = os.getenv('TIMEZONE', 'Asia/Tashkent')
    # Comma-separated list of allowed bank bot IDs (e.g., "856254490,915326936")
    PRIVATE_CHANNEL_ID = os.getenv('PRIVATE_CHANNEL_ID')
    GROUP_ID = os.getenv('GROUP_ID')
    FIND_ORDERS_TOPIC_ID = os.getenv('FIND_ORDERS_TOPIC_ID')
    CONFIRMATION_TOPIC_ID = os.getenv('CONFIRMATION_TOPIC_ID')
    AI_CONFIRMATIONS_TOPIC_ID = os.getenv('AI_CONFIRMATIONS_TOPIC_ID')

    # Cleanup/Retention settings (configurable via .env)
    DATA_RETENTION_DAYS = int(os.getenv('DATA_RETENTION_DAYS', '60'))
    CLEANUP_BATCH_SIZE = int(os.getenv('CLEANUP_BATCH_SIZE', '50'))
    CLEANUP_BATCH_DELAY_SECONDS = int(os.getenv('CLEANUP_BATCH_DELAY_SECONDS', '2'))
    CLEANUP_DRY_RUN = os.getenv('CLEANUP_DRY_RUN', 'false').lower() in ('1', 'true', 'yes', 'y')
    # Daily cleanup schedule (24h time)
    CLEANUP_DAILY_HOUR = int(os.getenv('CLEANUP_DAILY_HOUR', '3'))
    CLEANUP_DAILY_MINUTE = int(os.getenv('CLEANUP_DAILY_MINUTE', '0'))

    try:
        _allowed_ids_env = os.getenv('ALLOWED_BANK_BOT_IDS', '856254490,915326936')
        ALLOWED_BANK_BOT_IDS = {
            int(x.strip()) for x in _allowed_ids_env.split(',') if x.strip()
        }
    except Exception:
        # Fallback to defaults on parse error
        ALLOWED_BANK_BOT_IDS = {856254490, 915326936}

    # Concurrency settings
    THREAD_POOL_SIZE = int(os.getenv('THREAD_POOL_SIZE', '8'))  # Number of background worker threads
    MAX_CONCURRENT_DOWNLOADS = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '5'))  # Max concurrent file downloads
    MAX_CONCURRENT_IO_TASKS = int(os.getenv('MAX_CONCURRENT_IO_TASKS', '10')) # Max concurrent async I/O tasks
    DOWNLOAD_TIMEOUT = int(os.getenv('DOWNLOAD_TIMEOUT', '600'))  # Download timeout in seconds (10 minutes)
    RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', '3'))  # Number of retry attempts for failed operations
    
    # Telegram API credentials from environment (.env)
    API_ID = int(os.getenv('APP_ID')) if os.getenv('APP_ID') else None
    API_HASH = os.getenv('API_HASH')

    # Settings from config database (will be populated by load_from_db)
    ADMIN_IDS = []
    PHONE_NUMBER = None
    PASSWORD = None
    TELEGRAM_BOT_USERNAME = os.getenv('TELEGRAM_BOT_USERNAME', 'YOUR_BOT_USERNAME')

    @classmethod
    def load_from_db(cls):
        """Load configuration from the config database."""
        from src.database.config_db_service import config_db_service
        from src.services.admin_config_service import admin_config_service
        import logging

        logger = logging.getLogger(__name__)
        settings = config_db_service.get_all_settings()

        def get_setting(key, default=None):
            value = settings.get(key)
            return value if value is not None and value != '' else default

        # Load admins from JSON file
        try:
            admins = admin_config_service.get_admins()
            cls.ADMIN_IDS = [admin['id'] for admin in admins]
            logger.debug(f"Loaded {len(admins)} admins from JSON file")
        except Exception as e:
            logger.error(f"Error loading admins from JSON file: {e}")
            cls.ADMIN_IDS = []
        cls.PHONE_NUMBER = get_setting('userbot_phone_number')
        cls.PASSWORD = get_setting('userbot_password')
        # Bot username (configurable via DB with env default)
        bot_username = get_setting('telegram_bot_username')
        if bot_username:
            cls.TELEGRAM_BOT_USERNAME = bot_username
        # AI confirmation window (configurable via DB with env default)
        try:
            cls.AI_CONFIRMATION_TIME_WINDOW_MINUTES = int(
                get_setting('ai_confirmation_time_window_minutes', cls.AI_CONFIRMATION_TIME_WINDOW_MINUTES)
            )
        except Exception:
            # Fallback to existing value if parsing fails
            pass
        # Optionally load timezone from config DB
        try:
            tz = get_setting('timezone')
            if tz:
                cls.TIMEZONE = tz
        except Exception:
            pass
        # Optionally load allowed bank bot IDs from config DB setting key 'allowed_bank_bot_ids'
        try:
            ids_str = get_setting('allowed_bank_bot_ids')
            if ids_str:
                parsed_ids = {int(x.strip()) for x in ids_str.split(',') if x.strip()}
                if parsed_ids:
                    cls.ALLOWED_BANK_BOT_IDS = parsed_ids
        except Exception:
            # Keep previously set env/default values on any error
            pass
    
    @classmethod
    def get_admins_data(cls):
        """Get admin data as a list of dictionaries with id and name."""
        from src.services.admin_config_service import admin_config_service
        return admin_config_service.get_admins()
    
    @classmethod
    def validate(cls):
        """Validate that all required environment variables are present."""
        required_vars = {
            'TELEGRAM_BOT_TOKEN': cls.TELEGRAM_BOT_TOKEN,
            'GEMINI_API_KEY': cls.GEMINI_API_KEY,
            'SECRET_KEY': cls.SECRET_KEY,
            'APP_ID': cls.API_ID,
            'API_HASH': cls.API_HASH,
            'PRIVATE_CHANNEL_ID': cls.PRIVATE_CHANNEL_ID,
            'GROUP_ID': cls.GROUP_ID,
            'FIND_ORDERS_TOPIC_ID': cls.FIND_ORDERS_TOPIC_ID,
            'CONFIRMATION_TOPIC_ID': cls.CONFIRMATION_TOPIC_ID,
            'AI_CONFIRMATIONS_TOPIC_ID': cls.AI_CONFIRMATIONS_TOPIC_ID,
        }
        
        missing_vars = [name for name, value in required_vars.items() if not value]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

        # Validate settings loaded from DB
        required_db_vars = {
            'PHONE_NUMBER': cls.PHONE_NUMBER,
            'PASSWORD': cls.PASSWORD,
        }
        
        missing_db_vars = [name for name, value in required_db_vars.items() if not value]
        
        if missing_db_vars:
            raise ValueError(f"Missing required settings in config DB: {', '.join(missing_db_vars)}. "
                             "Please configure them via the web admin panel.")

        return True

# Create a global instance
config = Config()
