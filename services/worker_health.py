"""
Worker Health Reporting Service
Lightweight, passive health reporting for background workers.
Workers periodically update their health status in Redis.
API can query this information without coupling to worker availability.
"""

import os
import json
import time
from typing import Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

from services.redis_connection import (
    get_redis_client,
    is_redis_available
)

load_dotenv('config.env')

# Health report TTL (workers must update within this time or be dead)
WORKER_HEALTH_TTL = int(os.getenv("WORKER_HEALTH_TTL", 60))  # 60s default
# Update interval (seconds)
WORKER_HEALTH_UPDATE_INTERVAL = int(
    os.getenv("WORKER_HEALTH_UPDATE_INTERVAL", 30)
)


class WorkerHealthReporter:
    """
    Lightweight worker health reporting service.
    Workers use this to periodically report their health status.
    """

    def __init__(self):
        self.redis = get_redis_client() if is_redis_available() else None
        self.health_prefix = "worker:health:"

    def report_health(
        self,
        worker_id: str,
        redis_available: bool,
        active_jobs: int,
        error_state: Optional[str] = None,
        processed_count: int = 0,
        error_count: int = 0,
        additional_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Report worker health status (passive, read-only from API perspective)

        Args:
            worker_id: Unique worker identifier
            redis_available: Whether Redis is currently available
            active_jobs: Number of currently processing jobs
            error_state: Optional error state description (None if healthy)
            processed_count: Total jobs processed successfully
            error_count: Total jobs that failed
            additional_info: Optional additional health information

        Returns:
            True if health was reported successfully, False otherwise
        """
        if not self.redis:
            return False

        try:
            health_data = {
                "worker_id": worker_id,
                "liveness": "alive",  # Worker is alive if reporting
                "redis_available": redis_available,
                "active_jobs": active_jobs,
                "error_state": error_state,  # None if healthy
                "processed_count": processed_count,
                "error_count": error_count,
                "last_update": datetime.utcnow().isoformat(),
                "timestamp": time.time()
            }

            # Add additional info if provided
            if additional_info:
                health_data.update(additional_info)

            # Store health report with TTL (auto-cleanup for dead workers)
            health_key = f"{self.health_prefix}{worker_id}"
            self.redis.setex(
                health_key,
                WORKER_HEALTH_TTL,
                json.dumps(health_data, default=str)
            )

            return True
        except Exception as e:
            # Fail silently - health reporting should never crash worker
            print(f"⚠️ Failed to report worker health: {e}")
            return False

    def get_worker_health(
        self, worker_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get health status for a specific worker (read-only)

        Args:
            worker_id: Worker identifier

        Returns:
            Health data dict or None if worker not found or Redis unavailable
        """
        if not self.redis:
            return None

        try:
            health_key = f"{self.health_prefix}{worker_id}"
            health_data = self.redis.get(health_key)

            if health_data:
                return json.loads(health_data)
            return None
        except Exception as e:
            print(f"⚠️ Failed to get worker health: {e}")
            return None

    def get_all_workers_health(self) -> Dict[str, Dict[str, Any]]:
        """
        Get health status for all active workers (read-only)

        Returns:
            Dict mapping worker_id to health data
        """
        if not self.redis:
            return {}

        try:
            workers_health = {}
            pattern = f"{self.health_prefix}*"
            cursor = 0

            while True:
                cursor, keys = self.redis.scan(
                    cursor, match=pattern, count=100
                )

                for key in keys:
                    try:
                        health_data = self.redis.get(key)
                        if health_data:
                            data = json.loads(health_data)
                            worker_id = data.get(
                                "worker_id",
                                key.replace(self.health_prefix, "")
                            )
                            workers_health[worker_id] = data
                    except (json.JSONDecodeError, KeyError) as e:
                        print(
                            f"⚠️ Failed to parse worker health "
                            f"for {key}: {e}"
                        )
                        continue

                if cursor == 0:
                    break

            return workers_health
        except Exception as e:
            print(f"⚠️ Failed to get all workers health: {e}")
            return {}

    def get_workers_summary(self) -> Dict[str, Any]:
        """
        Get summary of all workers health (read-only, for observability)

        Returns:
            Summary dict with counts and status
        """
        all_workers = self.get_all_workers_health()

        if not all_workers:
            return {
                "total_workers": 0,
                "alive_workers": 0,
                "workers_with_redis": 0,
                "total_active_jobs": 0,
                "workers_with_errors": 0,
                "timestamp": datetime.utcnow().isoformat()
            }

        alive_count = 0
        redis_available_count = 0
        total_active_jobs = 0
        error_count = 0

        for worker_id, health in all_workers.items():
            if health.get("liveness") == "alive":
                alive_count += 1

            if health.get("redis_available", False):
                redis_available_count += 1

            total_active_jobs += health.get("active_jobs", 0)

            if health.get("error_state"):
                error_count += 1

        return {
            "total_workers": len(all_workers),
            "alive_workers": alive_count,
            "workers_with_redis": redis_available_count,
            "total_active_jobs": total_active_jobs,
            "workers_with_errors": error_count,
            "workers": all_workers,
            "timestamp": datetime.utcnow().isoformat()
        }


# Global instance
worker_health_reporter = WorkerHealthReporter()
