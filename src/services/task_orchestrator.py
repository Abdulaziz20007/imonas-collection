"""
Task Orchestrator Service for managing background tasks.
Provides centralized control over ThreadPoolExecutor for non-blocking operations.
"""
import asyncio
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, Any, Optional
from src.config import config

logger = logging.getLogger(__name__)


class TaskOrchestrator:
    """Centralized task orchestrator for background operations."""

    def __init__(self):
        """Initialize the task orchestrator with configurable thread pool."""
        # Get thread pool size from environment or use default
        max_workers = int(os.getenv('THREAD_POOL_SIZE', getattr(config, 'THREAD_POOL_SIZE', 8)))

        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="TaskOrchestrator"
        )
        self._shutdown = False
        self._lock = threading.Lock()

        logger.info(f"TaskOrchestrator initialized with {max_workers} workers")

    async def submit(self, func: Callable, *args, **kwargs) -> Any:
        """
        Submit a function to run in the background thread pool.

        Args:
            func: Function to execute in background
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            The result of the function execution

        Raises:
            RuntimeError: If orchestrator is shut down
        """
        if self._shutdown:
            raise RuntimeError("TaskOrchestrator has been shut down")

        loop = asyncio.get_event_loop()

        try:
            # Submit to thread pool and await result
            future = loop.run_in_executor(self._executor, func, *args)
            result = await future
            return result

        except Exception as e:
            logger.error(f"Error executing background task {func.__name__}: {e}", exc_info=True)
            raise

    def submit_nowait(self, func: Callable, *args, **kwargs) -> Future:
        """
        Submit a function to run in background without waiting for result.

        Args:
            func: Function to execute in background
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Future object representing the execution

        Raises:
            RuntimeError: If orchestrator is shut down
        """
        if self._shutdown:
            raise RuntimeError("TaskOrchestrator has been shut down")

        try:
            # Submit to thread pool and return future immediately
            future = self._executor.submit(func, *args, **kwargs)
            return future

        except Exception as e:
            logger.error(f"Error submitting background task {func.__name__}: {e}", exc_info=True)
            raise

    async def submit_with_callback(self, func: Callable, callback: Optional[Callable] = None, *args, **kwargs) -> Any:
        """
        Submit a function with optional callback on completion.

        Args:
            func: Function to execute in background
            callback: Optional callback function to execute on completion
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            The result of the function execution
        """
        try:
            result = await self.submit(func, *args, **kwargs)

            if callback:
                try:
                    # Execute callback in current thread (main event loop)
                    if asyncio.iscoroutinefunction(callback):
                        await callback(result)
                    else:
                        callback(result)
                except Exception as cb_error:
                    logger.error(f"Error in callback execution: {cb_error}", exc_info=True)

            return result

        except Exception as e:
            logger.error(f"Error in submit_with_callback for {func.__name__}: {e}", exc_info=True)
            raise

    def get_stats(self) -> dict:
        """Get current statistics about the thread pool."""
        with self._lock:
            if self._shutdown:
                return {"status": "shutdown"}

            return {
                "status": "active",
                "max_workers": self._executor._max_workers,
                "active_threads": len(self._executor._threads) if hasattr(self._executor, '_threads') else 0,
                "shutdown": self._shutdown
            }

    def shutdown(self, wait: bool = True, timeout: Optional[float] = 30.0):
        """
        Shutdown the task orchestrator.

        Args:
            wait: Whether to wait for running tasks to complete
            timeout: Maximum time to wait for shutdown
        """
        with self._lock:
            if self._shutdown:
                return

            self._shutdown = True
            logger.info("Shutting down TaskOrchestrator...")

            try:
                self._executor.shutdown(wait=wait, timeout=timeout)
                logger.info("TaskOrchestrator shutdown completed")
            except Exception as e:
                logger.error(f"Error during TaskOrchestrator shutdown: {e}")


# Global instance
_task_orchestrator: Optional[TaskOrchestrator] = None
_orchestrator_lock = threading.Lock()


def get_task_orchestrator() -> TaskOrchestrator:
    """Get the global task orchestrator instance (singleton pattern)."""
    global _task_orchestrator

    with _orchestrator_lock:
        if _task_orchestrator is None:
            _task_orchestrator = TaskOrchestrator()
        return _task_orchestrator


def shutdown_task_orchestrator(wait: bool = True, timeout: Optional[float] = 30.0):
    """Shutdown the global task orchestrator instance."""
    global _task_orchestrator

    with _orchestrator_lock:
        if _task_orchestrator is not None:
            _task_orchestrator.shutdown(wait=wait, timeout=timeout)
            _task_orchestrator = None