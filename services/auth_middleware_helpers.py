"""
Helper functions for authentication middleware
"""

from typing import Optional
from fastapi import Request


async def get_current_user_from_request(request: Request) -> Optional[str]:
    """
    Extract user ID from request (for middleware use)
    
    Args:
        request: FastAPI request object
    
    Returns:
        user_id or None if not authenticated
    """
    try:
        from services.auth_middleware import get_current_user
        from fastapi.security import HTTPBearer
        from fastapi import HTTPException
        
        security = HTTPBearer(auto_error=False)
        credentials = await security(request)
        
        if credentials:
            # Use existing get_current_user logic
            user = await get_current_user(credentials)
            return user
        return None
    except Exception:
        return None
