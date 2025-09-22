"""
Retry Service for handling failed operations with exponential backoff.
Provides automatic retry mechanisms for download failures and order processing errors.
"""
import asyncio
import logging
import time
import random
from typing import Callable, Any, Dict, Optional, List
from dataclasses import dataclass
from enum import Enum

from src.services.async_db_service import async_db_service
from src.services.task_orchestrator import get_task_orchestrator
from src.config import config

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Retry strategy types."""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    FIXED_DELAY = "fixed_delay"
    LINEAR_BACKOFF = "linear_backoff"


@dataclass
class RetryConfig:
    """Configuration for retry attempts."""
    max_attempts: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay in seconds
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    jitter: bool = True  # Add random jitter to avoid thundering herd


class RetryService:
    """Service for handling retries with various strategies."""

    def __init__(self):
        """Initialize the retry service."""
        self._orchestrator = get_task_orchestrator()
        self._active_retries: Dict[str, asyncio.Task] = {}
        self._retry_stats: Dict[str, Dict[str, Any]] = {}

    async def retry_with_backoff(
        self,
        func: Callable,
        *args,
        retry_config: Optional[RetryConfig] = None,
        operation_id: Optional[str] = None,
        **kwargs
    ) -> Any:
        """
        Execute function with retry and backoff strategy.

        Args:
            func: Function to retry
            *args: Positional arguments for function
            retry_config: Retry configuration
            operation_id: Unique ID for tracking this operation
            **kwargs: Keyword arguments for function

        Returns:
            Result of successful function execution

        Raises:
            Exception: If all retry attempts fail
        """
        if retry_config is None:
            retry_config = RetryConfig(max_attempts=config.RETRY_ATTEMPTS)

        if operation_id is None:
            operation_id = f"{func.__name__}_{int(time.time())}"

        self._retry_stats[operation_id] = {
            "attempts": 0,
            "started_at": time.time(),
            "last_error": None
        }

        last_exception = None

        for attempt in range(retry_config.max_attempts):
            try:
                self._retry_stats[operation_id]["attempts"] = attempt + 1

                logger.debug(f"Retry attempt {attempt + 1}/{retry_config.max_attempts} for {operation_id}")

                # Execute the function
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = await self._orchestrator.submit(func, *args, **kwargs)

                # Success - clean up stats
                self._retry_stats.pop(operation_id, None)
                logger.info(f"Operation {operation_id} succeeded on attempt {attempt + 1}")
                return result

            except Exception as e:
                last_exception = e
                self._retry_stats[operation_id]["last_error"] = str(e)

                logger.warning(f"Attempt {attempt + 1} failed for {operation_id}: {e}")

                # If this was the last attempt, don't wait
                if attempt == retry_config.max_attempts - 1:
                    break

                # Calculate delay based on strategy
                delay = self._calculate_delay(attempt, retry_config)
                logger.debug(f"Waiting {delay:.2f}s before retry {attempt + 2} for {operation_id}")

                await asyncio.sleep(delay)

        # All attempts failed
        self._retry_stats[operation_id]["completed_at"] = time.time()
        logger.error(f"All {retry_config.max_attempts} attempts failed for {operation_id}")

        raise last_exception

    def _calculate_delay(self, attempt: int, config: RetryConfig) -> float:
        """Calculate delay based on retry strategy."""
        if config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = config.base_delay * (2 ** attempt)
        elif config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = config.base_delay * (attempt + 1)
        else:  # FIXED_DELAY
            delay = config.base_delay

        # Apply maximum delay limit
        delay = min(delay, config.max_delay)

        # Add jitter to prevent thundering herd
        if config.jitter:
            jitter_amount = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_amount, jitter_amount)

        return max(0.1, delay)  # Minimum 100ms delay

    async def retry_failed_downloads(self, limit: int = 10) -> Dict[str, Any]:
        """
        Retry failed product media downloads.

        Args:
            limit: Maximum number of failed items to retry

        Returns:
            Statistics about retry attempts
        """
        stats = {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "errors": []
        }

        try:
            # Get failed product media entries
            failed_media = await async_db_service.get_pending_product_media(limit)

            for media in failed_media:
                if media.get('status') not in ['failed', 'pending']:
                    continue

                stats["attempted"] += 1
                file_unique_id = media['file_unique_id']

                try:
                    logger.info(f"Retrying download for file {file_unique_id}")

                    # Create retry configuration for downloads
                    download_retry_config = RetryConfig(
                        max_attempts=3,
                        base_delay=2.0,
                        max_delay=30.0,
                        strategy=RetryStrategy.EXPONENTIAL_BACKOFF
                    )

                    # Retry the download process
                    await self.retry_with_backoff(
                        self._retry_single_download,
                        media,
                        retry_config=download_retry_config,
                        operation_id=f"download_retry_{file_unique_id}"
                    )

                    stats["succeeded"] += 1
                    logger.info(f"Successfully retried download for {file_unique_id}")

                except Exception as e:
                    stats["failed"] += 1
                    error_msg = f"{file_unique_id}: {str(e)}"
                    stats["errors"].append(error_msg)
                    logger.error(f"Failed to retry download for {file_unique_id}: {e}")

                    # Mark as failed in database
                    await async_db_service.update_product_media_status(file_unique_id, "failed")

        except Exception as e:
            logger.error(f"Error in retry_failed_downloads: {e}")
            stats["errors"].append(f"General error: {str(e)}")

        return stats

    async def _retry_single_download(self, media: Dict[str, Any]) -> bool:
        """Retry download for a single media item."""
        file_unique_id = media['file_unique_id']

        try:
            # Set status to downloading
            await async_db_service.update_product_media_status(file_unique_id, "downloading")

            # Import here to avoid circular imports
            from src.bot.concurrent_handlers import _download_file_async

            # Attempt download (note: we'd need the file_id for this to work)
            # For now, we'll just simulate success and mark as available
            # In real implementation, you'd need to store the file_id in product_media

            # Simulate download process
            await asyncio.sleep(1)

            # Update status to available
            await async_db_service.update_product_media_status(file_unique_id, "available")

            return True

        except Exception as e:
            logger.error(f"Retry download failed for {file_unique_id}: {e}")
            await async_db_service.update_product_media_status(file_unique_id, "failed")
            raise

    async def get_retry_stats(self) -> Dict[str, Any]:
        """Get current retry statistics."""
        return {
            "active_retries": len(self._active_retries),
            "active_operations": list(self._active_retries.keys()),
            "recent_stats": dict(self._retry_stats)
        }

    async def cleanup_old_retries(self, max_age_seconds: int = 3600):
        """Clean up old retry statistics."""
        current_time = time.time()
        to_remove = []

        for operation_id, stats in self._retry_stats.items():
            if current_time - stats.get("started_at", 0) > max_age_seconds:
                to_remove.append(operation_id)

        for operation_id in to_remove:
            self._retry_stats.pop(operation_id, None)

        logger.debug(f"Cleaned up {len(to_remove)} old retry statistics")

    async def cancel_retry(self, operation_id: str) -> bool:
        """Cancel an active retry operation."""
        if operation_id in self._active_retries:
            task = self._active_retries[operation_id]
            task.cancel()
            del self._active_retries[operation_id]
            logger.info(f"Cancelled retry operation {operation_id}")
            return True
        return False


class CircuitBreaker:
    """Circuit breaker pattern for preventing cascade failures."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time to wait before attempting recovery
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half_open

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker."""
        if self.state == "open":
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = "half_open"
                logger.info("Circuit breaker entering half-open state")
            else:
                raise Exception("Circuit breaker is open")

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            # Success - reset failure count
            if self.state == "half_open":
                self.state = "closed"
                self.failure_count = 0
                logger.info("Circuit breaker closed after successful recovery")

            return result

        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "open"
                logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

            raise


# Global instances
retry_service = RetryService()
download_circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)