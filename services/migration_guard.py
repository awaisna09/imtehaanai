"""
Migration Guard Service
Prevents schema cache reload storms by blocking migrations during peak hours
and enforcing cooldown periods between DDL operations.
"""

import os
import logging
from datetime import datetime
from typing import Optional, Tuple
from dotenv import load_dotenv

load_dotenv('config.env')

logger = logging.getLogger(__name__)

# Configuration
MIGRATION_GUARD_ENABLED = os.getenv(
    "MIGRATION_GUARD_ENABLED", "true"
).lower() == "true"

# Peak hours: 9 AM - 9 PM UTC (adjust for your timezone)
PEAK_HOUR_START = int(os.getenv("MIGRATION_PEAK_HOUR_START", "9"))
PEAK_HOUR_END = int(os.getenv("MIGRATION_PEAK_HOUR_END", "21"))

# Minimum cooldown between migrations (hours)
MIGRATION_COOLDOWN_HOURS = int(
    os.getenv("MIGRATION_COOLDOWN_HOURS", "6")
)

# Redis key for tracking last migration
LAST_MIGRATION_KEY = "migration:last_execution"


def is_peak_hours(utc_now: Optional[datetime] = None) -> bool:
    """
    Check if current time is within peak hours.

    Args:
        utc_now: Optional datetime (defaults to now)

    Returns:
        True if within peak hours, False otherwise
    """
    if utc_now is None:
        utc_now = datetime.utcnow()

    current_hour = utc_now.hour
    return PEAK_HOUR_START <= current_hour < PEAK_HOUR_END


def can_run_migration(
    force: bool = False,
    check_redis: bool = True
) -> Tuple[bool, str]:
    """
    Check if migration can be run safely.

    Args:
        force: If True, bypass all checks (use with caution)
        check_redis: If True, check Redis for last migration time

    Returns:
        Tuple of (can_run: bool, reason: str)
    """
    if not MIGRATION_GUARD_ENABLED:
        return True, "Migration guard disabled"

    if force:
        logger.warning("⚠️ Migration guard bypassed (force=True)")
        return True, "Forced execution"

    # Check peak hours
    if is_peak_hours():
        peak_range = f"{PEAK_HOUR_START}:00-{PEAK_HOUR_END}:00 UTC"
        return False, (
            f"Peak hours active ({peak_range}). "
            f"Run migrations during off-peak hours to avoid "
            f"schema cache reload storms."
        )

    # Check cooldown period (if Redis available)
    if check_redis:
        try:
            from services.redis_connection import (
                get_redis_client,
                is_redis_available
            )

            if is_redis_available():
                redis_client = get_redis_client()
                last_migration_str = redis_client.get(LAST_MIGRATION_KEY)

                if last_migration_str:
                    try:
                        from datetime import datetime
                        last_migration = datetime.fromisoformat(
                            last_migration_str.decode('utf-8')
                        )
                        hours_since = (
                            datetime.utcnow() - last_migration
                        ).total_seconds() / 3600

                        if hours_since < MIGRATION_COOLDOWN_HOURS:
                            remaining = MIGRATION_COOLDOWN_HOURS - hours_since
                            return False, (
                                f"Migration cooldown active. "
                                f"Last migration was {hours_since:.1f}h ago. "
                                f"Wait {remaining:.1f}h more to avoid "
                                f"schema cache reload storms."
                            )
                    except (ValueError, AttributeError) as e:
                        logger.warning(
                            f"Failed to parse last migration time: {e}"
                        )
        except Exception as e:
            logger.warning(
                f"Failed to check migration cooldown: {e}"
            )
            # Fail open: allow migration if Redis check fails

    return True, "Migration allowed"


def record_migration_execution():
    """
    Record migration execution time in Redis (for cooldown tracking).
    """
    try:
        from services.redis_connection import (
            get_redis_client,
            is_redis_available
        )

        if is_redis_available():
            redis_client = get_redis_client()
            now = datetime.utcnow().isoformat()
            # Store for 24 hours (longer than cooldown)
            redis_client.setex(
                LAST_MIGRATION_KEY,
                86400,  # 24 hours
                now
            )
            logger.info(f"✅ Recorded migration execution: {now}")
    except Exception as e:
        logger.warning(
            f"Failed to record migration execution: {e}"
        )
        # Non-blocking: continue even if recording fails


def get_migration_safety_status() -> dict:
    """
    Get current migration safety status.

    Returns:
        Dictionary with safety status information
    """
    can_run, reason = can_run_migration()
    is_peak = is_peak_hours()

    status = {
        "guard_enabled": MIGRATION_GUARD_ENABLED,
        "can_run": can_run,
        "reason": reason,
        "is_peak_hours": is_peak,
        "peak_hours": f"{PEAK_HOUR_START}:00-{PEAK_HOUR_END}:00 UTC",
        "cooldown_hours": MIGRATION_COOLDOWN_HOURS
    }

    # Add last migration time if available
    try:
        from services.redis_connection import (
            get_redis_client,
            is_redis_available
        )

        if is_redis_available():
            redis_client = get_redis_client()
            last_migration_str = redis_client.get(LAST_MIGRATION_KEY)

            if last_migration_str:
                from datetime import datetime
                last_migration = datetime.fromisoformat(
                    last_migration_str.decode('utf-8')
                )
                hours_since = (
                    datetime.utcnow() - last_migration
                ).total_seconds() / 3600
                status["last_migration"] = (
                    last_migration.isoformat()
                )
                status["hours_since_last"] = round(hours_since, 1)
    except Exception:
        pass

    return status
