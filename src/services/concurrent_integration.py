"""
Concurrent Integration Service
Integrates all concurrent services and provides a unified interface for the bot application.
"""
import asyncio
import logging
import atexit
from typing import Optional
from telegram.ext import Application

from src.services.task_orchestrator import get_task_orchestrator, shutdown_task_orchestrator
from src.services.order_processing_service import order_processing_service
from src.services.retry_service import retry_service
from src.services.async_db_service import async_db_service

logger = logging.getLogger(__name__)


class ConcurrentIntegrationService:
    """Service that integrates all concurrent components."""

    def __init__(self):
        """Initialize the integration service."""
        self._initialized = False
        self._bot_app: Optional[Application] = None
        self._background_tasks = []
        self._shutdown_requested = False

    async def initialize(self, bot_app: Application):
        """
        Initialize all concurrent services.

        Args:
            bot_app: The Telegram bot application
        """
        if self._initialized:
            logger.warning("Concurrent services already initialized")
            return

        try:
            logger.info("Initializing concurrent services...")

            # Store bot application reference
            self._bot_app = bot_app

            # Set bot app in order processing service
            order_processing_service.set_bot_app(bot_app)

            # Test database connectivity
            db_healthy = await async_db_service.health_check()
            if not db_healthy:
                raise Exception("Database health check failed")

            # Start background maintenance tasks
            await self._start_background_tasks()

            # Register shutdown handler
            atexit.register(self._emergency_shutdown)

            self._initialized = True
            logger.info("✅ Concurrent services initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize concurrent services: {e}")
            raise

    async def _start_background_tasks(self):
        """Start background maintenance tasks."""
        try:
            # Task 1: Periodic retry of failed downloads
            retry_task = asyncio.create_task(self._periodic_retry_task())
            self._background_tasks.append(retry_task)

            # Task 2: Cleanup old retry statistics
            cleanup_task = asyncio.create_task(self._periodic_cleanup_task())
            self._background_tasks.append(cleanup_task)

            # Task 3: Health monitoring
            health_task = asyncio.create_task(self._periodic_health_check())
            self._background_tasks.append(health_task)

            logger.info("Started background maintenance tasks")

        except Exception as e:
            logger.error(f"Failed to start background tasks: {e}")

    async def _periodic_retry_task(self):
        """Periodically retry failed downloads."""
        while not self._shutdown_requested:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes

                if self._shutdown_requested:
                    break

                logger.debug("Running periodic retry task")
                stats = await retry_service.retry_failed_downloads(limit=5)

                if stats["attempted"] > 0:
                    logger.info(
                        f"Retry task completed: {stats['succeeded']} succeeded, "
                        f"{stats['failed']} failed out of {stats['attempted']} attempted"
                    )

            except asyncio.CancelledError:
                logger.info("Periodic retry task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic retry task: {e}")
                await asyncio.sleep(60)  # Wait before retrying

    async def _periodic_cleanup_task(self):
        """Periodically clean up old data."""
        while not self._shutdown_requested:
            try:
                await asyncio.sleep(3600)  # Run every hour

                if self._shutdown_requested:
                    break

                logger.debug("Running periodic cleanup task")

                # Clean up old retry statistics
                await retry_service.cleanup_old_retries(max_age_seconds=3600)

                # Clean up task orchestrator stats if needed
                orchestrator = get_task_orchestrator()
                stats = orchestrator.get_stats()
                logger.debug(f"Task orchestrator stats: {stats}")

            except asyncio.CancelledError:
                logger.info("Periodic cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic cleanup task: {e}")
                await asyncio.sleep(300)

    async def _periodic_health_check(self):
        """Periodically check system health."""
        while not self._shutdown_requested:
            try:
                await asyncio.sleep(600)  # Run every 10 minutes

                if self._shutdown_requested:
                    break

                logger.debug("Running periodic health check")

                # Check database health
                db_healthy = await async_db_service.health_check()
                if not db_healthy:
                    logger.error("Database health check failed!")

                # Check task orchestrator health
                orchestrator = get_task_orchestrator()
                stats = orchestrator.get_stats()
                if stats.get("status") != "active":
                    logger.error(f"Task orchestrator not healthy: {stats}")

                # Check retry service health
                retry_stats = await retry_service.get_retry_stats()
                active_retries = retry_stats.get("active_retries", 0)
                if active_retries > 10:  # Too many active retries might indicate issues
                    logger.warning(f"High number of active retries: {active_retries}")

            except asyncio.CancelledError:
                logger.info("Periodic health check task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic health check: {e}")
                await asyncio.sleep(300)

    async def get_system_status(self) -> dict:
        """Get comprehensive system status."""
        try:
            # Database status
            db_status = await async_db_service.health_check()

            # Task orchestrator status
            orchestrator = get_task_orchestrator()
            orchestrator_stats = orchestrator.get_stats()

            # Retry service status
            retry_stats = await retry_service.get_retry_stats()

            return {
                "initialized": self._initialized,
                "database": {
                    "healthy": db_status,
                    "status": "connected" if db_status else "disconnected"
                },
                "task_orchestrator": orchestrator_stats,
                "retry_service": retry_stats,
                "background_tasks": {
                    "active": len([t for t in self._background_tasks if not t.done()]),
                    "total": len(self._background_tasks)
                }
            }

        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                "initialized": self._initialized,
                "error": str(e)
            }

    async def shutdown(self):
        """Gracefully shutdown all concurrent services."""
        if not self._initialized:
            return

        logger.info("Shutting down concurrent services...")
        self._shutdown_requested = True

        try:
            # Cancel background tasks
            for task in self._background_tasks:
                if not task.done():
                    task.cancel()

            # Wait for background tasks to complete
            if self._background_tasks:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)

            # Shutdown task orchestrator
            shutdown_task_orchestrator(wait=True, timeout=30)

            logger.info("✅ Concurrent services shutdown completed")

        except Exception as e:
            logger.error(f"Error during concurrent services shutdown: {e}")

        finally:
            self._initialized = False

    def _emergency_shutdown(self):
        """Emergency shutdown handler for atexit."""
        if self._initialized:
            logger.warning("Emergency shutdown of concurrent services")
            shutdown_task_orchestrator(wait=False, timeout=10)

    def is_initialized(self) -> bool:
        """Check if services are initialized."""
        return self._initialized

    async def process_order_concurrent(
        self,
        channel_id: int,
        message_id: int,
        file_unique_id: str,
        media_type: str,
        order_id: int,
        user_telegram_id: int
    ) -> dict:
        """
        Process an order using the concurrent architecture.

        This is the main entry point for processing orders in the background.
        """
        if not self._initialized:
            raise Exception("Concurrent services not initialized")

        try:
            result = await order_processing_service.process_product_in_background(
                channel_id=channel_id,
                message_id=message_id,
                file_unique_id=file_unique_id,
                media_type=media_type,
                order_id=order_id,
                user_telegram_id=user_telegram_id
            )

            return result

        except Exception as e:
            logger.error(f"Error in concurrent order processing: {e}")
            raise


# Global instance
concurrent_integration = ConcurrentIntegrationService()


# Convenience functions for easy access
async def initialize_concurrent_services(bot_app: Application):
    """Initialize all concurrent services."""
    await concurrent_integration.initialize(bot_app)


async def shutdown_concurrent_services():
    """Shutdown all concurrent services."""
    await concurrent_integration.shutdown()


async def get_concurrent_system_status() -> dict:
    """Get system status."""
    return await concurrent_integration.get_system_status()


def is_concurrent_services_ready() -> bool:
    """Check if concurrent services are ready."""
    return concurrent_integration.is_initialized()