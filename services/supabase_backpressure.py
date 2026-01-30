"""
Supabase Request Budget + Backpressure
Implements global request budget with backpressure handling.
"""

import os
import time
import logging
from typing import Optional, Tuple
from dotenv import load_dotenv

from services.redis_semaphore import (
    get_redis_client,
    is_redis_available,
    DEFAULT_SEMAPHORE_KEY
)

load_dotenv('config.env')

logger = logging.getLogger(__name__)

# Configuration
SUPABASE_BUDGET_ENABLED = os.getenv("SUPABASE_BUDGET_ENABLED", "true").lower() == "true"
# 90% of limit triggers backpressure
SUPABASE_BUDGET_SATURATION_THRESHOLD = float(
    os.getenv("SUPABASE_BUDGET_SATURATION_THRESHOLD", "0.9")
)
# Default 5s retry delay
SUPABASE_BUDGET_RETRY_AFTER_SECONDS = int(
    os.getenv("SUPABASE_BUDGET_RETRY_AFTER_SECONDS", "5")
)


class SupabaseBudgetSaturated(Exception):
    """Raised when Supabase request budget is saturated"""
    def __init__(self, message: str, retry_after: int = None):
        super().__init__(message)
        self.retry_after = (
            retry_after or SUPABASE_BUDGET_RETRY_AFTER_SECONDS
        )


def check_budget_saturation(
    limit: int = 18,
    key: str = DEFAULT_SEMAPHORE_KEY
) -> Tuple[bool, Optional[int]]:
    """
    Check if Supabase request budget is saturated.

    Args:
        limit: Maximum concurrent permits
        key: Redis semaphore key

    Returns:
        Tuple of (is_saturated, active_count)
        - is_saturated: True if active permits >= threshold
        - active_count: Current number of active permits
    """
    if not SUPABASE_BUDGET_ENABLED:
        return False, None
    
    if not is_redis_available():
        # If Redis unavailable, assume not saturated (fallback to local limiter)
        return False, None
    
    try:
        redis_client = get_redis_client()
        current_time = time.time()
        
        # Clean up expired permits
        redis_client.zremrangebyscore(key, 0, current_time)
        
        # Count active permits
        active_count = redis_client.zcard(key)
        
        # Check if saturated (active >= threshold * limit)
        threshold = int(limit * SUPABASE_BUDGET_SATURATION_THRESHOLD)
        is_saturated = active_count >= threshold
        
        if is_saturated:
            saturation_pct = (
                (active_count / limit * 100) if limit > 0 else 0
            )
            logger.warning(
                f"⚠️ Supabase budget saturated: {active_count}/{limit} "
                f"(threshold: {threshold})"
            )
            # Log structured event for observability
            try:
                from services.structured_logging import structured_logger
                structured_logger.log_supabase_budget_saturated(
                    active_count=active_count,
                    limit=limit,
                    threshold=threshold,
                    saturation_percent=saturation_pct
                )
            except Exception:
                pass  # Non-blocking
        
        return is_saturated, active_count
    
    except Exception as e:
        logger.error(
            f"Error checking Supabase budget saturation: {e}",
            exc_info=True
        )
        # On error, assume not saturated (fail open)
        return False, None


def raise_budget_saturated_error(
    retry_after: Optional[int] = None
) -> None:
    """
    Raise SupabaseBudgetSaturated exception with retry-after.

    Args:
        retry_after: Retry-after seconds (default: from config)

    Raises:
        SupabaseBudgetSaturated: Always raises this exception
    """
    retry_after = retry_after or SUPABASE_BUDGET_RETRY_AFTER_SECONDS
    msg = (
        "Supabase request budget saturated. "
        "Please retry after cooldown period."
    )
    raise SupabaseBudgetSaturated(msg, retry_after=retry_after)
