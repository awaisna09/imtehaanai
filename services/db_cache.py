"""
Database Read Cache Service
Minimizes database reads and reuses cached data where possible
"""

import os
import json
import time
from typing import Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
from functools import wraps
from dotenv import load_dotenv

from services.redis_connection import get_redis_client

load_dotenv('config.env')

# Cache configuration
CACHE_TTL = int(os.getenv("DB_CACHE_TTL", 300))  # 5 minutes default
CACHE_PREFIX = "db_cache:"
MAX_CACHE_SIZE = int(os.getenv("MAX_CACHE_SIZE", 1000))  # Maximum cache entries


class DBCache:
    """Database read cache with TTL and size limits"""
    
    def __init__(self):
        self.redis = get_redis_client()
        self.local_cache: Dict[str, Tuple[Any, float]] = {}  # In-memory cache (key -> (value, expiry))
        self.max_local_size = 100  # Local cache size limit
    
    def _generate_key(self, table: str, filters: Dict[str, Any]) -> str:
        """Generate cache key from table and filters"""
        sorted_filters = json.dumps(filters, sort_keys=True, default=str)
        key_input = f"{table}:{sorted_filters}".encode('utf-8')
        import hashlib
        key_hash = hashlib.md5(key_input).hexdigest()
        return f"{CACHE_PREFIX}{table}:{key_hash}"
    
    def get(self, table: str, filters: Dict[str, Any]) -> Optional[Any]:
        """
        Get cached data for table query
        
        Args:
            table: Table name
            filters: Query filters (dict)
        
        Returns:
            Cached data or None if not found
        """
        key = self._generate_key(table, filters)
        
        # Check local cache first
        if key in self.local_cache:
            value, expiry = self.local_cache[key]
            if time.time() < expiry:
                return value
            else:
                # Expired, remove from local cache
                del self.local_cache[key]
        
        # Check Redis cache
        try:
            cached_data = self.redis.get(key)
            if cached_data:
                data = json.loads(cached_data)
                # Store in local cache
                self._set_local_cache(key, data, CACHE_TTL)
                return data
        except Exception as e:
            print(f"⚠️ Cache read error: {e}")
        
        return None
    
    def set(self, table: str, filters: Dict[str, Any], data: Any, ttl: Optional[int] = None):
        """
        Cache data for table query
        
        Args:
            table: Table name
            filters: Query filters (dict)
            data: Data to cache
            ttl: Time-to-live in seconds (default: CACHE_TTL)
        """
        key = self._generate_key(table, filters)
        ttl = ttl if ttl is not None else CACHE_TTL
        
        try:
            # Store in Redis
            self.redis.setex(key, ttl, json.dumps(data, default=str))
            
            # Store in local cache
            self._set_local_cache(key, data, ttl)
        except Exception as e:
            print(f"⚠️ Cache write error: {e}")
    
    def _set_local_cache(self, key: str, value: Any, ttl: int):
        """Store in local in-memory cache"""
        # Enforce size limit
        if len(self.local_cache) >= self.max_local_size:
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(self.local_cache))
            del self.local_cache[oldest_key]
        
        expiry = time.time() + ttl
        self.local_cache[key] = (value, expiry)
    
    def invalidate(self, table: str, filters: Optional[Dict[str, Any]] = None):
        """
        Invalidate cache for table (optionally for specific filters)
        
        Args:
            table: Table name
            filters: Optional specific filters to invalidate
        """
        try:
            if filters:
                # Invalidate specific key
                key = self._generate_key(table, filters)
                self.redis.delete(key)
                if key in self.local_cache:
                    del self.local_cache[key]
            else:
                # Invalidate all keys for table
                pattern = f"{CACHE_PREFIX}{table}:*"
                cursor = 0
                while True:
                    cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                    if keys:
                        self.redis.delete(*keys)
                        # Remove from local cache
                        for key in keys:
                            if key in self.local_cache:
                                del self.local_cache[key]
                    if cursor == 0:
                        break
        except Exception as e:
            print(f"⚠️ Cache invalidation error: {e}")
    
    def clear(self):
        """Clear all cache"""
        try:
            # Clear Redis cache
            pattern = f"{CACHE_PREFIX}*"
            cursor = 0
            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                if keys:
                    self.redis.delete(*keys)
                if cursor == 0:
                    break
            
            # Clear local cache
            self.local_cache.clear()
        except Exception as e:
            print(f"⚠️ Cache clear error: {e}")


# Global cache instance
db_cache = DBCache()


def cached_query(table: str, ttl: Optional[int] = None):
    """
    Decorator to cache database query results
    
    Usage:
        @cached_query('users', ttl=600)
        def get_user(user_id: str):
            return supabase.table('users').select('*').eq('id', user_id).execute()
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            cache_key = {
                'func': func.__name__,
                'args': str(args),
                'kwargs': str(kwargs)
            }
            
            # Try to get from cache
            cached = db_cache.get(table, cache_key)
            if cached is not None:
                return cached
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            db_cache.set(table, cache_key, result, ttl)
            
            return result
        return wrapper
    return decorator
