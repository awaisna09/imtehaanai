"""
Supabase PostgREST Concurrency Limiter
Prevents connection storms by limiting concurrent Supabase HTTP requests.
"""

import os
import sys
import threading
import logging
from typing import Callable, Any, TypeVar, Optional
from functools import wraps

logger = logging.getLogger(__name__)

# Type variable for generic function return type
T = TypeVar('T')

# Global semaphore instance
_semaphore: Optional[threading.Semaphore] = None
_semaphore_lock = threading.Lock()


def _get_semaphore() -> threading.Semaphore:
    """Get or create the global semaphore instance."""
    global _semaphore
    
    if _semaphore is not None:
        return _semaphore
    
    with _semaphore_lock:
        # Double-check pattern
        if _semaphore is not None:
            return _semaphore
        
        # Determine max concurrency based on environment
        # Check if we're in a worker process
        is_worker = (
            'worker' in os.getenv("PROCESS_TYPE", "").lower() or
            'enhanced_worker' in str(os.getenv("WORKER_ID", "")) or
            os.path.basename(sys.argv[0] if len(sys.argv) > 0 else '') in [
                'enhanced_worker.py', 'embedding_pregen_worker.py'
            ]
        )
        
        if is_worker:
            max_concurrency = int(
                os.getenv("SUPABASE_MAX_CONCURRENCY_WORKER", "2")
            )
        else:
            max_concurrency = int(
                os.getenv("SUPABASE_MAX_CONCURRENCY", "5")
            )
        
        _semaphore = threading.Semaphore(max_concurrency)
        logger.info(
            f"Supabase concurrency limiter initialized: "
            f"max_concurrency={max_concurrency} "
            f"(worker={is_worker})"
        )
        
        return _semaphore


def run_with_supabase_limit(
    fn: Callable[..., T],
    *args: Any,
    **kwargs: Any
) -> T:
    """
    Execute a callable under the Supabase concurrency limiter.
    
    Args:
        fn: Callable to execute
        *args: Positional arguments for fn
        **kwargs: Keyword arguments for fn
    
    Returns:
        Result of fn(*args, **kwargs)
    """
    semaphore = _get_semaphore()
    
    # Acquire semaphore (blocks if at max concurrency)
    semaphore.acquire()
    
    try:
        # Execute the function
        return fn(*args, **kwargs)
    finally:
        # Always release semaphore
        semaphore.release()


def reset_limiter():
    """
    Reset the global semaphore (useful for testing).
    """
    global _semaphore
    with _semaphore_lock:
        _semaphore = None
