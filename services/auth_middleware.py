"""
Authentication and Authorization Middleware
Validates user authentication and authorization for API endpoints
Includes rate limiting based on authenticated user identity
"""

from typing import Optional
from fastapi import HTTPException, Request, Depends, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os
from dotenv import load_dotenv

load_dotenv('config.env')

# Import rate limiter
try:
    from services.rate_limiter import rate_limiter, RateLimitCategory
    RATE_LIMITER_AVAILABLE = True
except ImportError:
    RATE_LIMITER_AVAILABLE = False
    print("[WARNING] Rate limiter not available")

# Security scheme
security = HTTPBearer(auto_error=False)

# Supabase client for auth verification (singleton)
try:
    from services.supabase_client import get_supabase_client
    supabase_auth_client = get_supabase_client()
except Exception as e:
    print(f"[WARNING] Could not initialize Supabase auth client: {e}")
    supabase_auth_client = None


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Optional[str]:
    """
    Authenticate user from Authorization header
    Returns user_id if authenticated, None if not authenticated (for public endpoints)
    Raises HTTPException if invalid token
    """
    if not credentials:
        # Allow unauthenticated requests for some endpoints
        return None
    
    try:
        token = credentials.credentials
        
        # Verify token with Supabase
        if supabase_auth_client:
            try:
                # Get user from token
                response = supabase_auth_client.auth.get_user(token)
                if response.user:
                    return response.user.id
            except Exception as e:
                # If token verification fails, try to parse as user_id directly
                # (for development/testing)
                if len(token) > 20:  # Likely a JWT token
                    raise HTTPException(
                        status_code=401,
                        detail=f"Invalid or expired token: {str(e)}"
                    )
                # Otherwise, treat as user_id for backward compatibility
                return token
        
        # Fallback: treat token as user_id (for development)
        # In production, this should be removed
        if len(token) < 50:  # Likely a user_id, not a JWT
            return token
        
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication token"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail=f"Authentication error: {str(e)}"
        )


async def require_auth(
    user_id: Optional[str] = Depends(get_current_user)
) -> str:
    """
    Require authentication for protected endpoints
    Returns user_id if authenticated, raises 401 if not
    """
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required"
        )
    return user_id


def authorize_user_action(user_id: str, resource_user_id: Optional[str]) -> bool:
    """
    Authorize user to access a resource
    Users can only access their own resources (unless admin)
    
    Args:
        user_id: Authenticated user ID
        resource_user_id: User ID of resource owner
    
    Returns:
        True if authorized, False otherwise
    """
    if not resource_user_id:
        return True  # Public resource
    
    if user_id == resource_user_id:
        return True
    
    # Check if user is admin (future enhancement)
    # For now, users can only access their own resources
    return False


def validate_user_id(user_id: Optional[str], required: bool = True) -> str:
    """
    Validate user_id format and presence
    
    Args:
        user_id: User ID to validate
        required: Whether user_id is required
    
    Returns:
        Validated user_id
    
    Raises:
        HTTPException if validation fails
    """
    if not user_id:
        if required:
            raise HTTPException(
                status_code=400,
                detail="user_id is required"
            )
        return "anonymous"
    
    # Basic validation (UUID format or alphanumeric)
    if len(user_id) < 1 or len(user_id) > 255:
        raise HTTPException(
            status_code=400,
            detail="Invalid user_id format"
        )
    
    if user_id == "anonymous" and required:
        raise HTTPException(
            status_code=400,
            detail="user_id cannot be 'anonymous' for this endpoint"
        )
    
    return user_id


async def check_rate_limit_middleware(
    category: RateLimitCategory,
    current_user: Optional[str] = Depends(get_current_user)
) -> str:
    """
    Rate limiting middleware dependency
    Checks rate limit for user and category, raises HTTPException if exceeded
    
    Args:
        category: Rate limit category for this endpoint
        current_user: Authenticated user ID (from get_current_user)
    
    Returns:
        user_id: User ID if within rate limit
    
    Raises:
        HTTPException: 429 if rate limit exceeded, 401 if not authenticated
    """
    if not RATE_LIMITER_AVAILABLE:
        # Rate limiter not available, allow all requests
        if not current_user:
            raise HTTPException(status_code=401, detail="Authentication required for rate limiting")
        return current_user
    
    # Require authentication for rate limiting
    if not current_user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required. Rate limiting is based on authenticated user identity."
        )
    
    # Get user tier (default to 'standard', can be retrieved from user profile)
    user_tier = "standard"  # TODO: Get from user profile/database
    
    # Check rate limit
    allowed, info = rate_limiter.check_rate_limit(current_user, category, user_tier)
    
    if not allowed:
        reset_at = info.get('reset_at', 0)
        remaining = info.get('remaining', 0)
        limit = info.get('limit', 0)
        
        # Calculate retry_after in seconds
        retry_after = max(0, int(reset_at - time.time()))
        
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "category": category.value,
                "limit": limit,
                "remaining": remaining,
                "reset_at": reset_at,
                "retry_after": retry_after
            },
            headers={
                "X-RateLimit-Limit": str(limit),
                "X-RateLimit-Remaining": str(remaining),
                "X-RateLimit-Reset": str(int(reset_at)),
                "Retry-After": str(retry_after)
            }
        )
    
    # Rate limit passed, return user_id
    return current_user


def get_user_tier(user_id: str) -> str:
    """
    Get user tier from database or cache
    Defaults to 'standard' if not found
    
    Args:
        user_id: User ID
    
    Returns:
        User tier: 'standard', 'premium', or 'admin'
    """
    try:
        # Use cached query helper
        from utils.cached_queries import get_user_tier as cached_get_user_tier
        return cached_get_user_tier(user_id)
    except Exception:
        # Fallback to default
        return "standard"


import time  # Add time import for rate limiting
