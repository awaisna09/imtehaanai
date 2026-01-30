"""
Redis Distributed Semaphore
Provides cross-process concurrency limiting using Redis with atomic Lua scripts.
"""

import os
import time
import uuid
import logging
from typing import Optional
from dotenv import load_dotenv

from services.redis_connection import (
    get_redis_client,
    is_redis_available
)

load_dotenv('config.env')

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_SEMAPHORE_KEY = os.getenv(
    "SUPABASE_GLOBAL_SEMAPHORE_KEY",
    "supabase:global:permits"
)
DEFAULT_TTL_SECONDS = int(os.getenv("SUPABASE_GLOBAL_SEMAPHORE_TTL", "20"))
DEFAULT_ACQUIRE_TIMEOUT = float(
    os.getenv("SUPABASE_GLOBAL_ACQUIRE_TIMEOUT", "2")
)


class SupabaseGlobalLimitExceeded(Exception):
    """Raised when global Supabase concurrency limit exceeded"""
    pass


# Lua script for atomic permit acquisition
# Returns: token if acquired, nil if limit reached
# Args: KEYS[1] = semaphore key, ARGV[1] = current_time, ARGV[2] = limit,
#       ARGV[3] = token, ARGV[4] = expiration_time
ACQUIRE_PERMIT_SCRIPT = """
    local key = KEYS[1]
    local current_time = tonumber(ARGV[1])
    local limit = tonumber(ARGV[2])
    local token = ARGV[3]
    local expiration_time = tonumber(ARGV[4])
    
    -- Clean up expired permits
    redis.call('ZREMRANGEBYSCORE', key, 0, current_time)
    
    -- Count active permits
    local active_count = redis.call('ZCARD', key)
    
    -- Check if under limit
    if active_count < limit then
        -- Add permit atomically
        redis.call('ZADD', key, expiration_time, token)
        return token
    else
        return nil
    end
"""


def acquire_permit(
    key: str = DEFAULT_SEMAPHORE_KEY,
    limit: int = 18,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    acquire_timeout: float = DEFAULT_ACQUIRE_TIMEOUT
) -> str:
    """
    Acquire a permit from the distributed semaphore using atomic Lua script.

    Uses Redis sorted set pattern with Lua script for atomicity:
    - Key: sorted set containing permit tokens (UUIDs) as members
    - Score: expiration timestamp
    - Automatically cleans up expired permits

    Args:
        key: Redis key for the semaphore
        limit: Maximum number of concurrent permits
        ttl_seconds: Time-to-live for each permit (auto-expires if crash)
        acquire_timeout: Maximum time to wait for a permit (seconds)

    Returns:
        Permit token (UUID string) if acquired

    Raises:
        SupabaseGlobalLimitExceeded: If cannot acquire within timeout
    """
    if not is_redis_available():
        logger.warning(
            "Redis not available - global semaphore disabled, "
            "falling back to local limiter only"
        )
        raise SupabaseGlobalLimitExceeded(
            "Global Supabase concurrency limit reached. "
            "Redis unavailable for distributed semaphore."
        )

    try:
        redis_client = get_redis_client()
        permit_token = str(uuid.uuid4())
        expiration_time = time.time() + ttl_seconds
        start_time = time.time()

        # Register Lua script (cached by Redis)
        acquire_script = redis_client.register_script(ACQUIRE_PERMIT_SCRIPT)

        # Retry loop with timeout
        while time.time() - start_time < acquire_timeout:
            current_time = time.time()
            expiration_time = current_time + ttl_seconds

            # Try to acquire permit atomically via Lua script
            result = acquire_script(
                keys=[key],
                args=[
                    str(current_time),
                    str(limit),
                    permit_token,
                    str(expiration_time)
                ]
            )

            if result:
                # Successfully acquired permit
                active_count = redis_client.zcard(key)
                logger.debug(
                    f"✅ Acquired global Supabase permit: {permit_token[:8]}... "
                    f"(active: {active_count}/{limit})"
                )
                return permit_token
            else:
                # At limit, wait a bit and retry
                time.sleep(0.1)

        # Timeout - could not acquire permit
        current_time = time.time()
        redis_client.zremrangebyscore(key, 0, current_time)
        active_count = redis_client.zcard(key)

        logger.warning(
            f"⚠️ Global Supabase semaphore timeout: "
            f"could not acquire permit within {acquire_timeout}s "
            f"(active: {active_count}/{limit})"
        )
        raise SupabaseGlobalLimitExceeded(
            "Global Supabase concurrency limit reached"
        )

    except SupabaseGlobalLimitExceeded:
        raise
    except Exception as e:
        logger.error(
            f"Error acquiring global Supabase semaphore permit: {e}",
            exc_info=True
        )
        # On Redis errors, raise limit exceeded (safer than allowing)
        raise SupabaseGlobalLimitExceeded(
            "Global Supabase concurrency limit reached. "
            f"Redis error: {str(e)}"
        )


def release_permit(key: str = DEFAULT_SEMAPHORE_KEY, token: str = None):
    """
    Release a permit back to the distributed semaphore.

    Args:
        key: Redis key for the semaphore
        token: Permit token to release (UUID string)
    """
    if not token:
        return

    if not is_redis_available():
        return

    try:
        redis_client = get_redis_client()

        # Remove the permit token
        removed = redis_client.zrem(key, token)

        if removed:
            active_count = redis_client.zcard(key)
            logger.debug(
                f"✅ Released global Supabase permit: {token[:8]}... "
                f"(active: {active_count})"
            )
        else:
            # Permit may have already expired or been cleaned up
            logger.debug(
                f"Permit {token[:8]}... not found (may have expired)"
            )

    except Exception as e:
        logger.error(
            f"Error releasing global Supabase semaphore permit: {e}",
            exc_info=True
        )
        # Don't raise - permit will auto-expire via TTL
