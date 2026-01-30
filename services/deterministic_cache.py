#!/usr/bin/env python3
"""
Deterministic Operation Cache Service
Centralized caching for deterministic, side-effect-free operations
with metrics.

This service provides:
- Read-through caching with configurable TTLs
- Cache hit/miss metrics
- Graceful fallback on cache failures
- Standardized cache key generation
- Explicit invalidation strategies
"""

import os
import json
import hashlib
import logging
from typing import Any, Optional, Dict, Callable
from functools import wraps
from enum import Enum

# Import performance instrumentation
try:
    from services.performance_instrumentation import (
        timed_operation, StageType
    )
    PERFORMANCE_INSTRUMENTATION_AVAILABLE = True
except ImportError:
    PERFORMANCE_INSTRUMENTATION_AVAILABLE = False
    def timed_operation(*args, **kwargs):
        from contextlib import nullcontext
        return nullcontext()

# Import base cache functions
try:
    from cache import cache_get, cache_set, cache_delete
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    def cache_get(key): return None
    def cache_set(key, value, ttl=3600): return False
    def cache_delete(key): return False

logger = logging.getLogger(__name__)


class CacheOperation(str, Enum):
    """Types of cacheable operations"""
    CONCEPT_SEARCH = "concept_search"
    CONCEPT_KEYWORD_MATCH = "concept_keyword_match"
    CONCEPT_BY_TOPIC = "concept_by_topic"
    REASONING_CLASSIFICATION = "reasoning_classification"
    MISCONCEPTION_DETECTION = "misconception_detection"
    READINESS_ASSESSMENT = "readiness_assessment"
    EMBEDDING_PREGENERATED = "embedding_pregenerated"


class CacheTTL:
    """Configurable cache TTLs for different operation types"""
    # Concept searches (embedding-based, stable results)
    CONCEPT_SEARCH = int(
        os.getenv("CACHE_TTL_CONCEPT_SEARCH", 3600)
    )  # 1 hour

    # Concept keyword matching (stable results)
    CONCEPT_KEYWORD_MATCH = int(
        os.getenv("CACHE_TTL_CONCEPT_KEYWORD_MATCH", 3600)
    )  # 1 hour

    # Concepts by topic (stable, changes only when topic updated)
    CONCEPT_BY_TOPIC = int(
        os.getenv("CACHE_TTL_CONCEPT_BY_TOPIC", 86400)
    )  # 24 hours

    # Reasoning classification (deterministic for same message)
    REASONING_CLASSIFICATION = int(
        os.getenv("CACHE_TTL_REASONING_CLASSIFICATION", 300)
    )  # 5 minutes

    # Misconception detection (deterministic for same Q&A pair)
    MISCONCEPTION_DETECTION = int(
        os.getenv("CACHE_TTL_MISCONCEPTION_DETECTION", 1800)
    )  # 30 minutes

    # Readiness assessment (changes with mastery updates)
    # Increased to 15 minutes (900s) - deterministic over short time windows,
    # invalidated on mastery updates. Cache keys include user_id and concept_ids.
    READINESS_ASSESSMENT = int(
        os.getenv("CACHE_TTL_READINESS_ASSESSMENT", 900)
    )  # 15 minutes (was 10 minutes)


class CacheMetrics:
    """Track cache hit/miss metrics"""
    _metrics: Dict[str, Dict[str, int]] = {}
    _lock = None

    @classmethod
    def _get_lock(cls):
        """Get thread lock for metrics (lazy initialization)"""
        if cls._lock is None:
            import threading
            cls._lock = threading.Lock()
        return cls._lock

    @classmethod
    def record_hit(cls, operation: str):
        """Record a cache hit"""
        with cls._get_lock():
            if operation not in cls._metrics:
                cls._metrics[operation] = {"hits": 0, "misses": 0}
            cls._metrics[operation]["hits"] += 1

    @classmethod
    def record_miss(cls, operation: str):
        """Record a cache miss"""
        with cls._get_lock():
            if operation not in cls._metrics:
                cls._metrics[operation] = {"hits": 0, "misses": 0}
            cls._metrics[operation]["misses"] += 1

    @classmethod
    def get_metrics(cls) -> Dict[str, Dict[str, int]]:
        """Get current cache metrics"""
        with cls._get_lock():
            return cls._metrics.copy()

    @classmethod
    def get_hit_rate(cls, operation: str) -> float:
        """Get cache hit rate for an operation"""
        with cls._get_lock():
            if operation not in cls._metrics:
                return 0.0
            metrics = cls._metrics[operation]
            total = metrics["hits"] + metrics["misses"]
            if total == 0:
                return 0.0
            return metrics["hits"] / total

    @classmethod
    def reset(cls):
        """Reset all metrics"""
        with cls._get_lock():
            cls._metrics.clear()


def generate_cache_key(
    operation: CacheOperation,
    *args,
    **kwargs
) -> str:
    """
    Generate a stable cache key from operation and inputs.

    Args:
        operation: Type of cacheable operation
        *args: Positional arguments (used for key generation)
        **kwargs: Keyword arguments (used for key generation)

    Returns:
        Cache key string
    """
    # Build key components
    key_parts = [f"det_cache:{operation.value}"]

    # Add args (convert to strings, handle None)
    for arg in args:
        if arg is None:
            key_parts.append("None")
        elif isinstance(arg, (str, int, float, bool)):
            key_parts.append(str(arg))
        elif isinstance(arg, list):
            # Sort list for consistent keys
            sorted_list = sorted([str(x) for x in arg])
            list_hash = hashlib.md5(
                ":".join(sorted_list).encode()
            ).hexdigest()[:12]
            key_parts.append(f"list:{list_hash}")
        else:
            # Hash complex objects
            obj_str = json.dumps(arg, sort_keys=True, default=str)
            obj_hash = hashlib.md5(obj_str.encode()).hexdigest()[:12]
            key_parts.append(f"obj:{obj_hash}")

    # Add kwargs (sorted for consistency)
    if kwargs:
        sorted_kwargs = json.dumps(kwargs, sort_keys=True, default=str)
        kwargs_hash = hashlib.md5(
            sorted_kwargs.encode()
        ).hexdigest()[:12]
        key_parts.append(f"kwargs:{kwargs_hash}")

    # Join and hash if too long
    key = ":".join(key_parts)
    if len(key) > 200:  # Redis key length limit
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return f"det_cache:{operation.value}:{key_hash}"

    return key


def cached_operation(
    operation: CacheOperation,
    ttl: Optional[int] = None,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None
):
    """
    Decorator for caching deterministic operations.

    Args:
        operation: Type of cacheable operation
        ttl: Time-to-live in seconds (uses default if None)
        job_id: Optional job ID for instrumentation
        trace_id: Optional trace ID for instrumentation

    Usage:
        @cached_operation(CacheOperation.REASONING_CLASSIFICATION)
        def classify_reasoning(message: str) -> str:
            # ... computation ...
            return result
    """
    # Get TTL from config if not provided
    if ttl is None:
        ttl_map = {
            CacheOperation.CONCEPT_SEARCH: CacheTTL.CONCEPT_SEARCH,
            CacheOperation.CONCEPT_KEYWORD_MATCH: (
                CacheTTL.CONCEPT_KEYWORD_MATCH
            ),
            CacheOperation.CONCEPT_BY_TOPIC: CacheTTL.CONCEPT_BY_TOPIC,
            CacheOperation.REASONING_CLASSIFICATION: (
                CacheTTL.REASONING_CLASSIFICATION
            ),
            CacheOperation.MISCONCEPTION_DETECTION: (
                CacheTTL.MISCONCEPTION_DETECTION
            ),
            CacheOperation.READINESS_ASSESSMENT: (
                CacheTTL.READINESS_ASSESSMENT
            ),
        }
        ttl = ttl_map.get(operation, 3600)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = generate_cache_key(operation, *args, **kwargs)

            # Try cache first
            if CACHE_AVAILABLE:
                try:
                    cached_result = cache_get(cache_key)
                    if cached_result is not None:
                        # Record cache hit
                        CacheMetrics.record_hit(operation.value)

                        # Log cache hit
                        if PERFORMANCE_INSTRUMENTATION_AVAILABLE:
                            with timed_operation(
                                stage_name=f"cache_hit_{operation.value}",
                                stage_type=StageType.CACHE_READ,
                                job_id=job_id,
                                trace_id=trace_id,
                                additional_context={
                                    "cache_key": cache_key,
                                    "operation": operation.value,
                                    "cache_hit": True
                                }
                            ):
                                pass

                        # Log cache hit at INFO level for production monitoring
                        logger.info(
                            f"[CACHE HIT] {operation.value} - "
                            f"key: {cache_key[:50]}... "
                            f"(user_id: {args[0] if args else 'N/A'}, "
                            f"concept_count: {len(args[1]) if len(args) > 1 and isinstance(args[1], list) else 'N/A'})"
                        )
                        return cached_result
                except Exception as e:
                    logger.warning(
                        f"[CACHE ERROR] Failed to read cache for "
                        f"{operation.value}: {e}"
                    )
                    # Continue to computation (graceful fallback)

            # Cache miss - record and compute
            CacheMetrics.record_miss(operation.value)

            # Log cache miss
            if PERFORMANCE_INSTRUMENTATION_AVAILABLE:
                with timed_operation(
                    stage_name=f"cache_miss_{operation.value}",
                    stage_type=StageType.CACHE_READ,
                    job_id=job_id,
                    trace_id=trace_id,
                    additional_context={
                        "cache_key": cache_key,
                        "operation": operation.value,
                        "cache_hit": False
                    }
                ):
                    pass

            # Log cache miss at INFO level for production monitoring
            logger.info(
                f"[CACHE MISS] {operation.value} - "
                f"key: {cache_key[:50]}... "
                f"(user_id: {args[0] if args else 'N/A'}, "
                f"concept_count: {len(args[1]) if len(args) > 1 and isinstance(args[1], list) else 'N/A'})"
            )

            # Compute result
            try:
                result = func(*args, **kwargs)

                # Cache the result (graceful failure)
                if CACHE_AVAILABLE and result is not None:
                    try:
                        cache_set(cache_key, result, ttl=ttl)

                        if PERFORMANCE_INSTRUMENTATION_AVAILABLE:
                            with timed_operation(
                                stage_name=f"cache_set_{operation.value}",
                                stage_type=StageType.CACHE_WRITE,
                                job_id=job_id,
                                trace_id=trace_id,
                                additional_context={
                                    "cache_key": cache_key,
                                    "operation": operation.value,
                                    "ttl": ttl
                                }
                            ):
                                pass

                        logger.debug(
                            f"[CACHE SET] {operation.value} - "
                            f"key: {cache_key[:50]}... (TTL: {ttl}s)"
                        )
                    except Exception as e:
                        logger.warning(
                            f"[CACHE ERROR] Failed to write cache for "
                            f"{operation.value}: {e}"
                        )
                        # Continue - cache failure doesn't affect result

                return result
            except Exception as e:
                logger.error(
                    f"[CACHE ERROR] Computation failed for "
                    f"{operation.value}: {e}",
                    exc_info=True
                )
                raise

        return wrapper
    return decorator


def invalidate_cache(
    operation: CacheOperation,
    *args,
    **kwargs
) -> bool:
    """
    Invalidate cache entry for a specific operation and inputs.

    Args:
        operation: Type of cacheable operation
        *args: Positional arguments matching the cached operation
        **kwargs: Keyword arguments matching the cached operation

    Returns:
        bool: True if invalidation succeeded
    """
    cache_key = generate_cache_key(operation, *args, **kwargs)

    try:
        if CACHE_AVAILABLE:
            cache_delete(cache_key)
            logger.debug(
                f"[CACHE INVALIDATE] {operation.value} - "
                f"key: {cache_key[:50]}..."
            )
            return True
    except Exception as e:
        logger.warning(
            f"[CACHE ERROR] Failed to invalidate cache for "
            f"{operation.value}: {e}"
        )

    return False


def invalidate_operation_cache(operation: CacheOperation) -> int:
    """
    Invalidate all cache entries for an operation type.
    Note: This requires pattern matching and may be slow for large caches.

    Args:
        operation: Type of cacheable operation

    Returns:
        int: Number of keys invalidated
    """
    # This is a simplified implementation
    # In production, you might want to use Redis SCAN with pattern matching
    logger.warning(
        f"[CACHE INVALIDATE] Pattern invalidation for {operation.value} "
        f"not fully implemented - use specific invalidation instead"
    )
    return 0


def get_cache_metrics() -> Dict[str, Any]:
    """
    Get cache metrics for all operations.

    Returns:
        Dict with metrics including hit rates, total hits/misses
    """
    metrics = CacheMetrics.get_metrics()

    result = {
        "operations": {},
        "summary": {
            "total_hits": 0,
            "total_misses": 0,
            "overall_hit_rate": 0.0
        }
    }

    total_hits = 0
    total_misses = 0

    for operation, op_metrics in metrics.items():
        hits = op_metrics["hits"]
        misses = op_metrics["misses"]
        total = hits + misses
        hit_rate = (hits / total * 100) if total > 0 else 0.0

        result["operations"][operation] = {
            "hits": hits,
            "misses": misses,
            "total": total,
            "hit_rate_percent": round(hit_rate, 2)
        }

        total_hits += hits
        total_misses += misses

    overall_total = total_hits + total_misses
    result["summary"]["total_hits"] = total_hits
    result["summary"]["total_misses"] = total_misses
    result["summary"]["overall_hit_rate"] = (
        round((total_hits / overall_total * 100), 2)
        if overall_total > 0 else 0.0
    )

    return result
