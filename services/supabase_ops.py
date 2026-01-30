"""
Supabase Operations Helper
Provides safe wrappers for Supabase query execution with concurrency limiting
and circuit breaker protection.
"""

import os
import logging
from typing import Any

from services.supabase_limiter import run_with_supabase_limit
from services.supabase_circuit_breaker import (
    get_supabase_circuit_breaker,
    CircuitBreakerOpenError
)
from services.redis_semaphore import (
    acquire_permit,
    release_permit,
    SupabaseGlobalLimitExceeded
)
from services.supabase_backpressure import (
    check_budget_saturation,
    raise_budget_saturated_error,
    SupabaseBudgetSaturated
)

logger = logging.getLogger(__name__)

# Global semaphore configuration
GLOBAL_SEMAPHORE_KEY = os.getenv(
    "SUPABASE_GLOBAL_SEMAPHORE_KEY",
    "supabase:global:permits"
)
GLOBAL_MAX_CONCURRENCY = int(
    os.getenv("SUPABASE_GLOBAL_MAX_CONCURRENCY", "18")
)
GLOBAL_SEMAPHORE_TTL = int(
    os.getenv("SUPABASE_GLOBAL_SEMAPHORE_TTL", "20")
)
GLOBAL_ACQUIRE_TIMEOUT = float(
    os.getenv("SUPABASE_GLOBAL_ACQUIRE_TIMEOUT", "2")
)


def sb_execute(query_builder: Any) -> Any:
    """
    Execute a Supabase query builder with global + local concurrency limiting
    and circuit breaker protection.

    This function wraps the `.execute()` call on Supabase query builders
    (e.g., table().select(), table().insert(), table().upsert(), etc.)
    to ensure all Supabase HTTP requests are throttled across all processes
    and protected by circuit breaker.

    Execution flow:
    1. Check circuit breaker
    2. Check budget saturation (backpressure)
    3. Acquire global Redis semaphore permit (cross-process limit)
    4. Acquire local process semaphore permit (per-process limit)
    5. Update DB activity timestamp (for idle timeout guard)
    6. Execute query with timing logs
    7. Release permits (always, even on error)
    """
    import time
    
    # Extract table name and operation type for observability
    table_name = "unknown"
    operation_type = "unknown"
    try:
        # Try to extract table name from query builder
        if hasattr(query_builder, 'table_name'):
            table_name = query_builder.table_name
        elif hasattr(query_builder, '_table'):
            table_name = str(query_builder._table)
        # Try to detect operation type
        if hasattr(query_builder, '_method'):
            operation_type = query_builder._method
        elif 'insert' in str(type(query_builder)).lower():
            operation_type = "insert"
        elif 'update' in str(type(query_builder)).lower():
            operation_type = "update"
        elif 'select' in str(type(query_builder)).lower():
            operation_type = "select"
        elif 'delete' in str(type(query_builder)).lower():
            operation_type = "delete"
    except Exception:
        pass  # Non-blocking: continue with defaults
    
    # Update DB activity timestamp (for idle timeout guard)
    try:
        from services.supabase_client import _update_activity_time
        _update_activity_time()
    except Exception:
        pass  # Non-blocking: continue even if activity tracking fails
    
    # Check budget saturation (backpressure) BEFORE acquiring permit
    try:
        from services.redis_semaphore import DEFAULT_SEMAPHORE_KEY
        is_saturated, active_count = check_budget_saturation(
            limit=GLOBAL_MAX_CONCURRENCY,  # Use local variable defined at top of file
            key=DEFAULT_SEMAPHORE_KEY
        )
        if is_saturated:
            # Budget saturated - raise error with retry-after
            logger.warning(
                f"⚠️ Supabase budget saturated: {active_count}/{GLOBAL_MAX_CONCURRENCY} "
                f"(table: {table_name}, operation: {operation_type})"
            )
            raise_budget_saturated_error()
    except SupabaseBudgetSaturated:
        # Re-raise budget saturated errors
        raise
    except Exception as backpressure_error:
        # If backpressure check fails, continue (fail open)
        pass  # Non-blocking: continue even if backpressure check fails
    
    circuit_breaker = get_supabase_circuit_breaker()
    
    # Check if circuit breaker allows execution
    can_execute, reason = circuit_breaker.can_execute()
    if not can_execute:
        logger.warning(
            f"Supabase circuit breaker is OPEN - failing fast "
            f"(reason: {reason})"
        )
        raise CircuitBreakerOpenError(
            f"Supabase service is temporarily unavailable. "
            f"Please retry after cooldown period."
        )
    
    # Acquire global permit (cross-process limit)
    global_permit_token = None
    try:
        global_permit_token = acquire_permit(
            key=GLOBAL_SEMAPHORE_KEY,
            limit=GLOBAL_MAX_CONCURRENCY,
            ttl_seconds=GLOBAL_SEMAPHORE_TTL,
            acquire_timeout=GLOBAL_ACQUIRE_TIMEOUT
        )
    except (SupabaseGlobalLimitExceeded, SupabaseBudgetSaturated):
        # Re-raise global limit or budget saturated errors
        raise
    
    def _execute():
        return query_builder.execute()
    
    # Start timing for observability
    start_time = time.time()
    request_id = None
    query_name = f"{table_name}.{operation_type}"
    endpoint = None
    job_id = None
    
    try:
        # Try to get request_id from context (if available)
        import contextvars
        try:
            request_id_var = contextvars.ContextVar('request_id', default=None)
            request_id = request_id_var.get()
        except Exception:
            pass
        
        # Try to get endpoint/job_id from context
        try:
            endpoint_var = contextvars.ContextVar('endpoint', default=None)
            endpoint = endpoint_var.get()
        except Exception:
            pass
        
        try:
            job_id_var = contextvars.ContextVar('job_id', default=None)
            job_id = job_id_var.get()
        except Exception:
            pass
    except Exception:
        pass
    
    try:
        # Wrap execution with timeout and retries using db_operation_executor
        from services.db_operation_executor import execute_db_operation
        
        def query_func():
            # Execute with local concurrency limiting (per-process limit)
            return run_with_supabase_limit(_execute)
        
        result = execute_db_operation(
            query_func=query_func,
            query_name=query_name,
            endpoint=endpoint,
            job_id=job_id,
            timeout=None,  # Use default from DB_OPERATION_TIMEOUT
            max_retries=None  # Use default from DB_MAX_RETRIES
        )
        
        # Record success
        circuit_breaker.record_success()
        
        # Logging is handled by execute_db_operation
        return result
    except Exception as e:
        # Record failure (only if it's a breaker-triggering error)
        circuit_breaker.record_failure(e)
        
        # Logging is handled by execute_db_operation (includes retries, error codes, etc.)
        # Only log here if execute_db_operation didn't handle it (shouldn't happen)
        logger.error(
            f"Error executing Supabase query: {e}",
            exc_info=True
        )
        raise
    finally:
        # Always release global permit (even on error)
        if global_permit_token:
            try:
                release_permit(
                    key=GLOBAL_SEMAPHORE_KEY,
                    token=global_permit_token
                )
            except Exception as e:
                logger.warning(
                    f"Error releasing global permit (will auto-expire): {e}"
                )
