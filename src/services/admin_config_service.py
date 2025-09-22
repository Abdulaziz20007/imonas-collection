"""
Admin configuration service for managing Telegram admin IDs in db/admins.json.
Provides thread-safe operations with file locking and atomic writes.
"""
import json
import os
import threading
import logging
from typing import List, Dict, Any
import tempfile

logger = logging.getLogger(__name__)

class AdminConfigService:
    """Service for managing admin configuration in JSON file."""

    def __init__(self, config_file: str = "db/admins.json"):
        self.config_file = config_file
        # Use reentrant lock to avoid deadlocks when methods nest (e.g., add_admin -> get_admins)
        self._lock = threading.RLock()
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        """Ensure the directory for the config file exists."""
        directory = os.path.dirname(self.config_file)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    def get_admins(self) -> List[Dict[str, Any]]:
        """
        Get all admins from the JSON file.
        Returns an empty list if the file doesn't exist, is empty, or has invalid JSON.
        """
        with self._lock:
            try:
                if not os.path.exists(self.config_file):
                    logger.info(f"Admin config file {self.config_file} does not exist, returning empty list")
                    return []

                with open(self.config_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if not content:
                        logger.info(f"Admin config file {self.config_file} is empty, returning empty list")
                        return []

                    # Remove JSON comments (lines starting with //)
                    lines = content.split('\n')
                    clean_lines = []
                    for line in lines:
                        stripped = line.strip()
                        if not stripped.startswith('//'):
                            clean_lines.append(line)
                    clean_content = '\n'.join(clean_lines)

                    data = json.loads(clean_content)
                    if isinstance(data, list):
                        # Ensure all admins have string IDs for consistency
                        for admin in data:
                            if 'id' in admin:
                                admin['id'] = str(admin['id'])
                        return data
                    else:
                        logger.warning(f"Admin config file {self.config_file} does not contain a list, returning empty list")
                        return []

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in admin config file {self.config_file}: {e}")
                return []
            except Exception as e:
                logger.error(f"Error reading admin config file {self.config_file}: {e}")
                return []

    def _save_admins(self, admins_data: List[Dict[str, Any]]) -> bool:
        """
        Persist admins data using an atomic, cross-platform safe replace.
        Uses a temporary file + os.replace to avoid partial writes and
        Windows rename limitations.
        """
        # Ensure directory exists before any write
        self._ensure_directory_exists()

        temp_file_path = None
        try:
            # Create a temporary file in the same directory for atomicity
            dir_name = os.path.dirname(self.config_file) or '.'
            with tempfile.NamedTemporaryFile('w', delete=False, dir=dir_name, encoding='utf-8') as tmp:
                temp_file_path = tmp.name
                json.dump(admins_data, tmp, indent=2, ensure_ascii=False)
                tmp.flush()
                os.fsync(tmp.fileno())

            # Atomically replace destination (works on Windows and POSIX)
            os.replace(temp_file_path, self.config_file)

            logger.info(f"Successfully saved {len(admins_data)} admins to {self.config_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving admin config file {self.config_file}: {e}")
            return False
        finally:
            # Best-effort cleanup if temp file still exists
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception:
                    pass

    def add_admin(self, admin_id: str, admin_name: str) -> bool:
        """
        Add a new admin if the ID doesn't already exist.
        Returns True if successful, False if admin already exists or on error.
        """
        with self._lock:
            admins = self.get_admins()

            # Check if admin already exists
            admin_id_str = str(admin_id)
            for admin in admins:
                if admin.get('id') == admin_id_str:
                    logger.warning(f"Admin with ID {admin_id_str} already exists")
                    return False

            # Add new admin
            admins.append({
                "id": admin_id_str,
                "name": admin_name
            })

            return self._save_admins(admins)

    def update_admin(self, admin_id: str, new_name: str) -> bool:
        """
        Update an admin's name.
        Returns True if successful, False if admin not found or on error.
        """
        with self._lock:
            admins = self.get_admins()

            admin_id_str = str(admin_id)
            for admin in admins:
                if admin.get('id') == admin_id_str:
                    admin['name'] = new_name
                    return self._save_admins(admins)

            logger.warning(f"Admin with ID {admin_id_str} not found for update")
            return False

    def delete_admin(self, admin_id: str) -> bool:
        """
        Delete an admin by ID.
        Returns True if successful, False if admin not found or on error.
        """
        with self._lock:
            admins = self.get_admins()

            admin_id_str = str(admin_id)
            original_count = len(admins)
            admins = [admin for admin in admins if admin.get('id') != admin_id_str]

            if len(admins) == original_count:
                logger.warning(f"Admin with ID {admin_id_str} not found for deletion")
                return False

            return self._save_admins(admins)

# Create singleton instance
admin_config_service = AdminConfigService()