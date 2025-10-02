import asyncio
import logging
from contextlib import asynccontextmanager
from src.config import config

logger = logging.getLogger(__name__)


class ConcurrencyManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConcurrencyManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._limit = config.MAX_CONCURRENT_IO_TASKS
        self._semaphore = asyncio.Semaphore(self._limit)
        self._initialized = True
        logger.info(f"ConcurrencyManager initialized with a limit of {self._limit} tasks.")

    @asynccontextmanager
    async def limit(self):
        """An async context manager to limit concurrent executions."""
        task = asyncio.current_task()
        task_name = task.get_name() if task else 'unnamed_task'
        logger.info(
            f"Task '{task_name}' waiting for I/O permit. In queue: "
            f"{len(self._semaphore._waiters) if hasattr(self._semaphore, '_waiters') and self._semaphore._waiters else 0}"
        )
        async with self._semaphore:
            running_tasks = self._limit - self._semaphore._value
            logger.info(
                f"Task '{task_name}' acquired I/O permit. Running tasks: {running_tasks}/{self._limit}"
            )
            try:
                yield
            finally:
                logger.info(
                    f"Task '{task_name}' releasing I/O permit. Running tasks will be: {running_tasks - 1}/{self._limit}"
                )


# Singleton instance
concurrency_manager = ConcurrencyManager()

