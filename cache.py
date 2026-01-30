"""
Simple cache module for round-robin position tracking and other caching needs.
Uses Redis when available, falls back to in-memory cache.
"""

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Try to use Redis for persistent caching
try:
    from services.redis_connection import get_redis_client, is_redis_available
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis connection not available, using in-memory cache fallback")

# Fallback in-memory cache (only used if Redis unavailable)
_in_memory_cache = {}
_in_memory_cache_ttl = {}


def cache_get(key: str) -> Optional[Any]:
    """
    Get value from cache.
    
    Args:
        key: Cache key
        
    Returns:
        Cached value or None if not found
    """
    if REDIS_AVAILABLE and is_redis_available():
        try:
            redis_client = get_redis_client()
            value = redis_client.get(key)
            if value is not None:
                # Try to deserialize JSON, fallback to string
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    return value
            return None
        except Exception as e:
            logger.warning(f"Redis cache_get failed for key {key}: {e}, falling back to in-memory")
            # Fall through to in-memory cache
    
    # Fallback to in-memory cache
    import time
    if key in _in_memory_cache:
        # Check TTL
        if key in _in_memory_cache_ttl:
            expiry = _in_memory_cache_ttl[key]
            if time.time() > expiry:
                # Expired, remove it
                del _in_memory_cache[key]
                del _in_memory_cache_ttl[key]
                return None
        return _in_memory_cache[key]
    return None


def cache_set(key: str, value: Any, ttl: int = 3600) -> bool:
    """
    Set value in cache with TTL.
    
    Args:
        key: Cache key
        value: Value to cache
        ttl: Time-to-live in seconds (default: 1 hour)
        
    Returns:
        True if successful, False otherwise
    """
    if REDIS_AVAILABLE and is_redis_available():
        try:
            redis_client = get_redis_client()
            # Serialize value to JSON if it's not a string
            if isinstance(value, (dict, list, int, float, bool)) or value is None:
                serialized_value = json.dumps(value)
            else:
                serialized_value = str(value)
            
            redis_client.setex(key, ttl, serialized_value)
            return True
        except Exception as e:
            logger.warning(f"Redis cache_set failed for key {key}: {e}, falling back to in-memory")
            # Fall through to in-memory cache
    
    # Fallback to in-memory cache
    import time
    _in_memory_cache[key] = value
    _in_memory_cache_ttl[key] = time.time() + ttl
    return True


def cache_delete(key: str) -> bool:
    """
    Delete value from cache.
    
    Args:
        key: Cache key
        
    Returns:
        True if successful, False otherwise
    """
    if REDIS_AVAILABLE and is_redis_available():
        try:
            redis_client = get_redis_client()
            redis_client.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Redis cache_delete failed for key {key}: {e}, falling back to in-memory")
            # Fall through to in-memory cache
    
    # Fallback to in-memory cache
    if key in _in_memory_cache:
        del _in_memory_cache[key]
    if key in _in_memory_cache_ttl:
        del _in_memory_cache_ttl[key]
    return True


def _hash_string(s: str) -> str:
    """
    Hash a string (for compatibility with existing code).
    
    Args:
        s: String to hash
        
    Returns:
        Hashed string
    """
    import hashlib
    return hashlib.md5(s.encode()).hexdigest()
