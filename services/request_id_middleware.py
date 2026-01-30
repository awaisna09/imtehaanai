"""
Request ID Middleware
Adds request_id to all requests for correlation across logs.
"""

import uuid
import contextvars
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Context variable for request ID (thread-safe)
request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    'request_id',
    default=''
)


def get_request_id() -> str:
    """Get current request ID from context"""
    return request_id_var.get('')


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Middleware to add request_id to all requests"""

    async def dispatch(self, request: Request, call_next):
        # Generate or extract request ID
        request_id = request.headers.get('X-Request-ID')
        if not request_id:
            request_id = str(uuid.uuid4())

        # Set in context for this request
        request_id_var.set(request_id)

        # Set endpoint in context for observability
        try:
            from services.endpoint_context import set_endpoint
            endpoint = f"{request.method} {request.url.path}"
            set_endpoint(endpoint)
        except Exception:
            pass  # Non-blocking: continue without endpoint context

        # Process request
        response = await call_next(request)

        # Add request ID to response headers
        response.headers['X-Request-ID'] = request_id

        return response
