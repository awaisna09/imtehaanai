"""
Read-Through Cache Service
Caching for static/semi-static reference data and read-heavy database queries
Uses Redis with TTL-based expiration (no explicit invalidation needed)
"""

import os
import json
import hashlib
import time
from typing import Any, Optional, Dict, Callable
from functools import wraps
from dotenv import load_dotenv

from services.redis_connection import get_redis_client

load_dotenv('config.env')

# Cache configuration
CACHE_KEY_PREFIX = "cache:"
DEFAULT_CACHE_TTL = int(os.getenv("DEFAULT_CACHE_TTL", 3600))  # 1 hour default


class CacheTTL:
    """Cache TTL constants for different data types"""
    # Static reference data (changes rarely)
    STATIC_REFERENCE = int(os.getenv("CACHE_TTL_STATIC", 86400))  # 24 hours
    
    # Semi-static data (changes occasionally)
    SEMI_STATIC = int(os.getenv("CACHE_TTL_SEMI_STATIC", 3600))  # 1 hour
    
    # Frequently changing data (but still cacheable)
    FREQUENT = int(os.getenv("CACHE_TTL_FREQUENT", 300))  # 5 minutes
    
    # User-specific data
    USER_DATA = int(os.getenv("CACHE_TTL_USER", 1800))  # 30 minutes
    
    # Query results
    QUERY_RESULT = int(os.getenv("CACHE_TTL_QUERY", 600))  # 10 minutes


class ReadThroughCache:
    """Read-through cache for database queries"""
    
    def __init__(self):
        self.redis = get_redis_client()
        self.key_prefix = CACHE_KEY_PREFIX
    
    def _generate_key(self, namespace: str, identifier: str, filters: Optional[Dict] = None) -> str:
        """
        Generate cache key from namespace, identifier, and optional filters
        
        Args:
            namespace: Cache namespace (e.g., 'topics', 'subjects', 'users')
            identifier: Unique identifier (e.g., '123', 'all', 'user_id:456')
            filters: Optional filters dict for complex queries
        
        Returns:
            Cache key string
        """
        key_parts = [self.key_prefix, namespace, identifier]
        
        if filters:
            # Sort filters for consistent key generation
            sorted_filters = json.dumps(filters, sort_keys=True, default=str)
            # Create hash of filters for shorter keys
            filter_hash = hashlib.md5(sorted_filters.encode()).hexdigest()[:12]
            key_parts.append(filter_hash)
        
        return ":".join(key_parts)
    
    def get(
        self,
        namespace: str,
        identifier: str,
        filters: Optional[Dict] = None
    ) -> Optional[Any]:
        """
        Get cached value (read-through pattern)
        
        Args:
            namespace: Cache namespace
            identifier: Unique identifier
            filters: Optional filters dict
        
        Returns:
            Cached value or None if not found
        """
        key = self._generate_key(namespace, identifier, filters)
        
        try:
            cached_value = self.redis.get(key)
            if cached_value:
                return json.loads(cached_value)
            return None
        except Exception as e:
            print(f"⚠️ Cache read error for {key}: {e}")
            return None
    
    def set(
        self,
        namespace: str,
        identifier: str,
        value: Any,
        ttl: int = DEFAULT_CACHE_TTL,
        filters: Optional[Dict] = None
    ):
        """
        Set cached value with TTL
        
        Args:
            namespace: Cache namespace
            identifier: Unique identifier
            value: Value to cache (must be JSON serializable)
            ttl: Time-to-live in seconds
            filters: Optional filters dict
        """
        key = self._generate_key(namespace, identifier, filters)
        
        try:
            serialized_value = json.dumps(value, default=str)
            self.redis.setex(key, ttl, serialized_value)
        except Exception as e:
            print(f"⚠️ Cache write error for {key}: {e}")
    
    def get_or_fetch(
        self,
        namespace: str,
        identifier: str,
        fetch_func: Callable[[], Any],
        ttl: int = DEFAULT_CACHE_TTL,
        filters: Optional[Dict] = None
    ) -> Any:
        """
        Read-through cache: get from cache or fetch from database
        
        Args:
            namespace: Cache namespace
            identifier: Unique identifier
            fetch_func: Function that fetches data from database (called on cache miss)
            ttl: Time-to-live in seconds
            filters: Optional filters dict
        
        Returns:
            Cached or fetched value
        """
        # Try cache first
        cached = self.get(namespace, identifier, filters)
        if cached is not None:
            return cached
        
        # Cache miss: fetch from database
        try:
            value = fetch_func()
            
            # Cache the result
            if value is not None:
                self.set(namespace, identifier, value, ttl, filters)
            
            return value
        except Exception as e:
            print(f"⚠️ Cache fetch error for {namespace}:{identifier}: {e}")
            raise
    
    def delete(
        self,
        namespace: str,
        identifier: Optional[str] = None,
        filters: Optional[Dict] = None
    ):
        """
        Delete cached value(s)
        
        Args:
            namespace: Cache namespace
            identifier: Optional specific identifier (None deletes all in namespace)
            filters: Optional filters dict
        """
        try:
            if identifier:
                key = self._generate_key(namespace, identifier, filters)
                self.redis.delete(key)
            else:
                # Delete all keys in namespace
                pattern = f"{self.key_prefix}{namespace}:*"
                cursor = 0
                while True:
                    cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                    if keys:
                        self.redis.delete(*keys)
                    if cursor == 0:
                        break
        except Exception as e:
            print(f"⚠️ Cache delete error: {e}")
    
    def clear_namespace(self, namespace: str):
        """Clear all cached values in a namespace"""
        self.delete(namespace)
    
    def get_stats(self, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Get cache statistics
        
        Args:
            namespace: Optional namespace to get stats for
        
        Returns:
            Dict with cache statistics
        """
        try:
            pattern = f"{self.key_prefix}{namespace}:*" if namespace else f"{self.key_prefix}*"
            cursor = 0
            total_keys = 0
            total_memory = 0
            
            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                total_keys += len(keys)
                
                if keys:
                    # Estimate memory usage
                    for key in keys:
                        try:
                            ttl = self.redis.ttl(key)
                            size = self.redis.memory_usage(key) or 0
                            total_memory += size
                        except Exception:
                            pass
                
                if cursor == 0:
                    break
            
            return {
                "total_keys": total_keys,
                "estimated_memory_bytes": total_memory,
                "namespace": namespace or "all"
            }
        except Exception as e:
            print(f"⚠️ Cache stats error: {e}")
            return {"error": str(e)}


# Global cache instance
read_cache = ReadThroughCache()


def cached_query(
    namespace: str,
    ttl: int = DEFAULT_CACHE_TTL,
    key_func: Optional[Callable] = None
):
    """
    Decorator for caching database query results
    
    Usage:
        @cached_query('topics', ttl=CacheTTL.STATIC_REFERENCE)
        def get_topic(topic_id: int):
            return supabase.table('topics').select('*').eq('id', topic_id).single().execute()
        
        @cached_query('subjects', ttl=CacheTTL.STATIC_REFERENCE, key_func=lambda args, kwargs: f"all")
        def get_all_subjects():
            return supabase.table('subjects').select('*').execute()
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key from function arguments
            if key_func:
                identifier = key_func(args, kwargs)
            else:
                # Default: use first argument as identifier
                identifier = str(args[0]) if args else "default"
                if kwargs:
                    # Include kwargs in identifier
                    kwargs_str = json.dumps(kwargs, sort_keys=True, default=str)
                    kwargs_hash = hashlib.md5(kwargs_str.encode()).hexdigest()[:12]
                    identifier = f"{identifier}:{kwargs_hash}"
            
            # Use read-through cache
            return read_cache.get_or_fetch(
                namespace=namespace,
                identifier=identifier,
                fetch_func=lambda: func(*args, **kwargs),
                ttl=ttl
            )
        return wrapper
    return decorator
