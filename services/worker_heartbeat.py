"""
Worker Heartbeat Service
Tracks active workers in Redis to prevent over-provisioning.
"""

import os
import time
import threading
import logging
from typing import Optional
from dotenv import load_dotenv

from services.redis_connection import (
    get_redis_client,
    is_redis_available
)

load_dotenv('config.env')

logger = logging.getLogger(__name__)

# Configuration
WORKER_HEARTBEAT_KEY = "workers:active"
# 30 seconds TTL (heartbeat every 10s = 3x safety margin)
WORKER_HEARTBEAT_TTL = 30
WORKER_HEARTBEAT_INTERVAL = 10  # Refresh TTL every 10 seconds
MAX_ACTIVE_WORKERS = int(os.getenv("MAX_ACTIVE_WORKERS", "5"))
ALLOW_OVERPROVISION = (
    os.getenv("ALLOW_OVERPROVISION_WORKERS", "false").lower() == "true"
)


class WorkerHeartbeat:
    """
    Manages worker heartbeat registration and tracking in Redis.
    """

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.running = False
        self.registered = False

    def register(self) -> bool:
        """
        Register worker in Redis set with TTL.

        Returns:
            bool: True if registered successfully, False otherwise
        """
        if not is_redis_available():
            logger.error("Redis not available - cannot register worker")
            return False

        try:
            redis_client = get_redis_client()
            # Add worker to set with TTL
            redis_client.sadd(WORKER_HEARTBEAT_KEY, self.worker_id)
            redis_client.expire(WORKER_HEARTBEAT_KEY, WORKER_HEARTBEAT_TTL)

            self.registered = True
            logger.info(
                f"✅ Worker {self.worker_id} registered in Redis "
                f"(active workers: {get_active_worker_count()})"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to register worker {self.worker_id}: {e}",
                exc_info=True
            )
            return False

    def unregister(self):
        """Remove worker from Redis set."""
        if not self.registered:
            return

        if not is_redis_available():
            logger.warning("Redis not available - cannot unregister worker")
            return

        try:
            redis_client = get_redis_client()
            redis_client.srem(WORKER_HEARTBEAT_KEY, self.worker_id)
            logger.info(
                f"✅ Worker {self.worker_id} unregistered from Redis "
                f"(active workers: {get_active_worker_count()})"
            )
        except Exception as e:
            logger.error(
                f"Failed to unregister worker {self.worker_id}: {e}",
                exc_info=True
            )
        finally:
            self.registered = False

    def start_heartbeat(self):
        """Start background thread to refresh worker TTL."""
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            return

        self.running = True
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"worker-heartbeat-{self.worker_id}"
        )
        self.heartbeat_thread.start()
        logger.debug(
            f"Started heartbeat thread for worker {self.worker_id}"
        )

    def stop_heartbeat(self):
        """Stop heartbeat thread."""
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=2)
            logger.debug(
                f"Stopped heartbeat thread for worker {self.worker_id}"
            )

    def _heartbeat_loop(self):
        """Background loop to refresh worker TTL."""
        while self.running:
            try:
                if is_redis_available() and self.registered:
                    redis_client = get_redis_client()
                    # Refresh TTL for worker set
                    redis_client.expire(
                        WORKER_HEARTBEAT_KEY, WORKER_HEARTBEAT_TTL
                    )
                    logger.debug(
                        f"💓 Heartbeat refreshed for worker {self.worker_id}"
                    )
                else:
                    logger.warning(
                        f"Redis unavailable or worker not registered - "
                        f"skipping heartbeat for {self.worker_id}"
                    )

                # Sleep for heartbeat interval
                time.sleep(WORKER_HEARTBEAT_INTERVAL)
            except Exception as e:
                logger.error(
                    f"Error in heartbeat loop for {self.worker_id}: {e}",
                    exc_info=True
                )
                # Continue heartbeat loop even on error
                time.sleep(WORKER_HEARTBEAT_INTERVAL)


def get_active_worker_count() -> int:
    """
    Get count of active workers from Redis.

    Returns:
        int: Number of active workers (0 if Redis unavailable)
    """
    if not is_redis_available():
        return 0

    try:
        redis_client = get_redis_client()
        # Clean up expired workers (those with expired TTL)
        # Note: Redis automatically removes expired keys, but we can check
        count = redis_client.scard(WORKER_HEARTBEAT_KEY)
        return count
    except Exception as e:
        logger.error(f"Error getting active worker count: {e}", exc_info=True)
        return 0


def can_start_worker(worker_id: str) -> tuple[bool, str]:
    """
    Check if a new worker can start based on active worker count.

    Args:
        worker_id: Worker identifier

    Returns:
        tuple[bool, str]: (can_start, reason_message)
    """
    if ALLOW_OVERPROVISION:
        logger.warning(
            f"⚠️ ALLOW_OVERPROVISION_WORKERS=true - "
            f"Worker {worker_id} allowed to start despite limit"
        )
        return True, "Over-provisioning allowed"

    active_count = get_active_worker_count()

    if active_count >= MAX_ACTIVE_WORKERS:
        error_msg = (
            f"\n{'='*70}\n"
            f"🚨 WORKER LIMIT EXCEEDED\n"
            f"{'='*70}\n"
            f"\n"
            f"Active workers: {active_count}/{MAX_ACTIVE_WORKERS}\n"
            f"Worker {worker_id} cannot start.\n"
            f"\n"
            f"This prevents:\n"
            f"  - Queue thrash from too many workers competing\n"
            f"  - Resource exhaustion (CPU, memory, DB connections)\n"
            f"  - Unnecessary costs from over-provisioning\n"
            f"\n"
            f"TO FIX:\n"
            f"  1. Wait for existing workers to complete jobs\n"
            f"  2. Manually stop excess workers\n"
            f"  3. OR set ALLOW_OVERPROVISION_WORKERS=true (not recommended)\n"
            f"\n"
            f"Current limit: MAX_ACTIVE_WORKERS={MAX_ACTIVE_WORKERS}\n"
            f"{'='*70}\n"
        )
        return False, error_msg

    return True, f"Active workers: {active_count}/{MAX_ACTIVE_WORKERS}"
