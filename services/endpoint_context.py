"""
Endpoint Context Manager
Provides context variables for tracking current endpoint/job for observability.
"""

import contextvars
from typing import Optional

# Context variables for endpoint/job tracking (thread-safe)
endpoint_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'endpoint',
    default=None
)

job_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'job_id',
    default=None
)


def set_endpoint(endpoint: str) -> None:
    """Set current endpoint in context"""
    endpoint_var.set(endpoint)


def get_endpoint() -> Optional[str]:
    """Get current endpoint from context"""
    return endpoint_var.get(None)


def set_job_id(job_id: str) -> None:
    """Set current job ID in context"""
    job_id_var.set(job_id)


def get_job_id() -> Optional[str]:
    """Get current job ID from context"""
    return job_id_var.get(None)
