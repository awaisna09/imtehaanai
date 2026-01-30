"""
Rate Limiting Helper Functions
Helper functions for rate limiting checks in API endpoints
"""

from typing import Tuple, Optional
from fastapi import HTTPException
import time

try:
    from services.rate_limiter import rate_limiter, RateLimitCategory
    from services.auth_middleware import get_user_tier
    RATE_LIMITER_AVAILABLE = True
except ImportError:
    RATE_LIMITER_AVAILABLE = False
    print("[WARNING] Rate limiter not available")


def check_rate_limit_for_endpoint(
    user_id: str,
    category: RateLimitCategory,
    endpoint_name: str = "",
    check_queue_back_pressure: bool = True
) -> None:
    """
    Check rate limit for user and category, raise HTTPException if exceeded
    PERMANENT ENFORCEMENT: Always enforced, no toggles, fail-closed on errors
    
    Args:
        user_id: Authenticated user ID (REQUIRED)
        category: Rate limit category
        endpoint_name: Optional endpoint name for error messages
        check_queue_back_pressure: If True, also check queue depth (default: True)
    
    Raises:
        HTTPException: 
            - 401 if user not authenticated
            - 429 if rate limit exceeded
            - 503 if rate limiting service unavailable (fail-closed)
    """
    # PERMANENT ENFORCEMENT: Rate limiting is always required
    if not RATE_LIMITER_AVAILABLE:
        # Fail closed: reject if rate limiter not available
        raise HTTPException(
            status_code=503,
            detail={
                "error": "Rate limiting service unavailable",
                "message": "Request rejected for safety. Rate limiting is required for all AI operations."
            }
        )
    
    # PERMANENT ENFORCEMENT: Require authenticated user (no anonymous users)
    if not user_id or user_id == "anonymous":
        raise HTTPException(
            status_code=401,
            detail={
                "error": "Authentication required",
                "message": "Rate limiting is based on authenticated user identity. Anonymous users are not allowed for AI operations."
            }
        )
    
    try:
        user_tier = get_user_tier(user_id)
        allowed, rate_info = rate_limiter.check_rate_limit(
            user_id,
            category,
            user_tier,
            check_queue_back_pressure
        )
        
        if not allowed:
            reset_at = rate_info.get('reset_at', 0)
            remaining = rate_info.get('remaining', 0)
            limit = rate_info.get('limit', 0)
            retry_after = max(0, int(reset_at - time.time()))
            queue_back_pressure = rate_info.get('queue_back_pressure', False)
            queue_depth = rate_info.get('queue_depth', 0)
            
            # Clear, deterministic error message
            if queue_back_pressure:
                error_message = (
                    f"Rate limit exceeded for {endpoint_name or category.value}. "
                    f"Queue is under back-pressure (depth: {queue_depth}). "
                    f"Please try again later."
                )
            else:
                error_message = (
                    f"Rate limit exceeded for {endpoint_name or category.value}. "
                    f"Limit: {limit} requests per {rate_info.get('window_seconds', 3600)} seconds. "
                    f"Remaining: {remaining}. Retry after {retry_after} seconds."
                )
            
            raise HTTPException(
                status_code=429,
                detail={
                    "error": error_message,
                    "category": category.value,
                    "limit": limit,
                    "remaining": remaining,
                    "retry_after": retry_after,
                    "reset_at": reset_at,
                    "queue_back_pressure": queue_back_pressure,
                    "queue_depth": queue_depth
                },
                headers={
                    "X-RateLimit-Limit": str(limit),
                    "X-RateLimit-Remaining": str(remaining),
                    "X-RateLimit-Reset": str(int(reset_at)),
                    "Retry-After": str(retry_after),
                    "X-Queue-Back-Pressure": "true" if queue_back_pressure else "false",
                    "X-Queue-Depth": str(queue_depth)
                }
            )
    except HTTPException:
        raise
    except ValueError as e:
        # Authentication required error
        raise HTTPException(
            status_code=401,
            detail={
                "error": "Authentication required",
                "message": str(e)
            }
        )
    except RuntimeError as e:
        # Rate limiting service unavailable - fail closed
        raise HTTPException(
            status_code=503,
            detail={
                "error": "Rate limiting service unavailable",
                "message": str(e)
            }
        )
    except Exception as e:
        # PERMANENT ENFORCEMENT: Fail closed on any other error
        import logging
        logging.error(f"Rate limit check failed for {category.value}: {e}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "Rate limiting check failed",
                "message": "Request rejected for safety. Please try again later."
            }
        )


def get_rate_limit_status(user_id: str, category: RateLimitCategory) -> Optional[dict]:
    """
    Get current rate limit status for user and category
    
    Args:
        user_id: Authenticated user ID
        category: Rate limit category
    
    Returns:
        Dict with rate limit status or None if rate limiter not available
    """
    if not RATE_LIMITER_AVAILABLE or not user_id:
        return None
    
    try:
        user_tier = get_user_tier(user_id)
        return rate_limiter.get_rate_limit_status(user_id, category, user_tier)
    except Exception:
        return None
