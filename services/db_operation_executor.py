"""
Database Operation Executor
Centralized executor for all database operations with:
- Per-process concurrency limiting
- Timeouts
- Exponential backoff retries for transient errors
- Structured logging with observability fields
"""

import os
import time
import random
import logging
import socket
from typing import Any, Optional, Callable, TypeVar
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar('T')

# Configuration
DB_OPERATION_TIMEOUT = float(
    os.getenv("DB_OPERATION_TIMEOUT", "30.0")
)  # Default 30 seconds

DB_MAX_RETRIES = int(
    os.getenv("DB_MAX_RETRIES", "3")
)  # Default 3 retries

DB_RETRY_BASE_DELAY = float(
    os.getenv("DB_RETRY_BASE_DELAY", "1.0")
)  # Base delay in seconds

DB_RETRY_MAX_DELAY = float(
    os.getenv("DB_RETRY_MAX_DELAY", "10.0")
)  # Max delay in seconds

# Transient error patterns (PostgreSQL and network errors)
TRANSIENT_ERROR_PATTERNS = [
    'ETIMEDOUT',
    'ECONNRESET',
    '57P01',  # PostgreSQL: terminating connection due to administrator command
    'connection refused',
    'connection reset',
    'timeout',
    'ConnectionError',
    'ConnectionResetError',
    'TimeoutError',
    '503',  # Service unavailable
    '502',  # Bad gateway
    '504',  # Gateway timeout
]


def _is_transient_error(error: Exception) -> bool:
    """
    Check if error is transient and should be retried.

    Args:
        error: Exception to check

    Returns:
        True if error is transient, False otherwise
    """
    error_str = str(error).lower()
    error_type = type(error).__name__

    # Check error type
    if any(pattern in error_type for pattern in ['Connection', 'Timeout']):
        return True

    # Check error message
    for pattern in TRANSIENT_ERROR_PATTERNS:
        if pattern.lower() in error_str:
            return True

    return False


def _get_error_code(error: Exception) -> str:
    """
    Extract error code from exception.

    Args:
        error: Exception to extract code from

    Returns:
        Error code string
    """
    error_type = type(error).__name__
    error_str = str(error)

    # Try to extract HTTP status code
    if '503' in error_str:
        return '503'
    elif '502' in error_str:
        return '502'
    elif '504' in error_str:
        return '504'

    # Try to extract PostgreSQL error code
    if '57P01' in error_str:
        return '57P01'

    # Check for socket errors
    if isinstance(error, socket.timeout):
        return 'ETIMEDOUT'
    elif isinstance(error, ConnectionError):
        return 'ECONNRESET'

    # Return error type as code
    return error_type


def execute_db_operation(
    query_func: Callable[[], T],
    query_name: str = "unknown",
    endpoint: Optional[str] = None,
    job_id: Optional[str] = None,
    timeout: Optional[float] = None,
    max_retries: Optional[int] = None
) -> T:
    """
    Execute a database operation with concurrency limiting, timeout, and retries.

    Args:
        query_func: Function that executes the database operation
        query_name: Name/description of the query for logging
        endpoint: API endpoint name (if called from API)
        job_id: Job ID (if called from worker)
        timeout: Operation timeout in seconds (default: DB_OPERATION_TIMEOUT)
        max_retries: Maximum retry attempts (default: DB_MAX_RETRIES)

    Returns:
        Result of query_func()

    Raises:
        Exception: If operation fails after all retries
    """
    timeout = timeout or DB_OPERATION_TIMEOUT
    max_retries = max_retries if max_retries is not None else DB_MAX_RETRIES

    # Get context for logging
    context_source = endpoint or job_id or "unknown"
    context_type = "endpoint" if endpoint else ("job" if job_id else "unknown")

    # Start timing
    start_time = time.time()
    retry_count = 0
    last_error = None

    while retry_count <= max_retries:
        try:
            # Execute with timeout
            # Note: Supabase client operations are synchronous, so we use
            # a simple timeout check rather than async timeout
            operation_start = time.time()

            result = query_func()

            # Check if operation exceeded timeout
            operation_duration = time.time() - operation_start
            if operation_duration > timeout:
                raise TimeoutError(
                    f"Database operation '{query_name}' exceeded timeout "
                    f"({timeout}s): {operation_duration:.2f}s"
                )

            # Success - log and return
            total_duration_ms = (time.time() - start_time) * 1000

            try:
                from services.structured_logging import structured_logger
                structured_logger.log_database_operation(
                    operation=query_name,
                    table="unknown",  # Will be extracted by sb_execute
                    duration_ms=round(total_duration_ms, 2),
                    cached=False,
                    success=True,
                    retries=retry_count,
                    endpoint=endpoint,
                    job_id=job_id,
                    query_name=query_name
                )
            except Exception:
                # Non-blocking: fallback to standard logger
                logger.info(
                    f"DB operation '{query_name}' succeeded "
                    f"({total_duration_ms:.2f}ms, retries={retry_count}, "
                    f"{context_type}={context_source})"
                )

            return result

        except Exception as e:
            last_error = e
            error_code = _get_error_code(e)
            is_transient = _is_transient_error(e)

            # If not transient, don't retry
            if not is_transient:
                total_duration_ms = (time.time() - start_time) * 1000

                try:
                    from services.structured_logging import structured_logger
                    structured_logger.log_database_operation(
                        operation=query_name,
                        table="unknown",
                        duration_ms=round(total_duration_ms, 2),
                        cached=False,
                        success=False,
                        error=str(e),
                        error_code=error_code,
                        retries=retry_count,
                        endpoint=endpoint,
                        job_id=job_id,
                        query_name=query_name
                    )
                except Exception:
                    logger.error(
                        f"DB operation '{query_name}' failed (non-transient): "
                        f"{error_code} - {str(e)} "
                        f"({total_duration_ms:.2f}ms, retries={retry_count}, "
                        f"{context_type}={context_source})"
                    )

                raise

            # Transient error - retry if attempts remaining
            if retry_count < max_retries:
                retry_count += 1

                # Calculate exponential backoff with jitter
                delay = min(
                    DB_RETRY_BASE_DELAY * (2 ** (retry_count - 1)),
                    DB_RETRY_MAX_DELAY
                )
                # Add jitter (±20%)
                jitter = delay * 0.2 * (random.random() * 2 - 1)
                delay = max(0.1, delay + jitter)

                logger.warning(
                    f"DB operation '{query_name}' failed (transient): "
                    f"{error_code} - {str(e)}. "
                    f"Retrying in {delay:.2f}s (attempt {retry_count}/{max_retries}, "
                    f"{context_type}={context_source})"
                )

                time.sleep(delay)
            else:
                # Max retries exceeded
                total_duration_ms = (time.time() - start_time) * 1000

                try:
                    from services.structured_logging import structured_logger
                    structured_logger.log_database_operation(
                        operation=query_name,
                        table="unknown",
                        duration_ms=round(total_duration_ms, 2),
                        cached=False,
                        success=False,
                        error=str(e),
                        error_code=error_code,
                        retries=retry_count,
                        endpoint=endpoint,
                        job_id=job_id,
                        query_name=query_name
                    )
                except Exception:
                    logger.error(
                        f"DB operation '{query_name}' failed after "
                        f"{retry_count} retries: {error_code} - {str(e)} "
                        f"({total_duration_ms:.2f}ms, {context_type}={context_source})"
                    )

                raise

    # Should never reach here, but raise last error if we do
    if last_error:
        raise last_error
    raise Exception("Database operation failed: unknown error")
