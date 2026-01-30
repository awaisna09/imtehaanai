"""
Performance Instrumentation Service
Fine-grained timing and metrics for all agent execution stages
"""

import time
import threading
import functools
from typing import Dict, Any, Optional, Callable
from contextlib import contextmanager
from datetime import datetime
from enum import Enum

# Import structured logging (non-blocking)
try:
    from services.structured_logging import structured_logger
    LOGGING_AVAILABLE = True
except ImportError:
    LOGGING_AVAILABLE = False
    structured_logger = None


class StageType(str, Enum):
    """Types of execution stages for categorization"""
    PROMPT_CONSTRUCTION = "prompt_construction"
    AI_PROVIDER_CALL = "ai_provider_call"
    RESPONSE_PARSING = "response_parsing"
    DATABASE_READ = "database_read"
    DATABASE_WRITE = "database_write"
    CACHE_READ = "cache_read"
    CACHE_WRITE = "cache_write"
    PIPELINE_NODE = "pipeline_node"
    SERVICE_OPERATION = "service_operation"
    OTHER = "other"


class PerformanceTimer:
    """
    Reusable performance timer with structured logging.
    Non-blocking, thread-safe, suitable for metrics aggregation.
    """
    
    def __init__(
        self,
        stage_name: str,
        stage_type: StageType = StageType.OTHER,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize performance timer.
        
        Args:
            stage_name: Name of the execution stage
            stage_type: Type of stage (for categorization)
            job_id: Optional job ID for correlation
            trace_id: Optional trace ID for distributed tracing
            additional_context: Optional additional context fields
        """
        self.stage_name = stage_name
        self.stage_type = stage_type.value
        self.job_id = job_id
        self.trace_id = trace_id
        self.additional_context = additional_context or {}
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration_ms: Optional[float] = None
    
    def __enter__(self):
        """Start timing"""
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and log (non-blocking)"""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        
        # Log asynchronously (non-blocking)
        self._log_timing(exc_type is not None)
    
    def _log_timing(self, error: bool = False):
        """Log timing data (non-blocking, fire-and-forget)"""
        if not LOGGING_AVAILABLE or not structured_logger:
            return
        
        try:
            # Build structured log entry
            log_data = {
                "event": "performance_timing",
                "stage_name": self.stage_name,
                "stage_type": self.stage_type,
                "start_time": (
                    datetime.utcfromtimestamp(self.start_time).isoformat()
                    if self.start_time else None
                ),
                "end_time": (
                    datetime.utcfromtimestamp(self.end_time).isoformat()
                    if self.end_time else None
                ),
                "duration_ms": (
                    round(self.duration_ms, 2)
                    if self.duration_ms else None
                ),
                "job_id": self.job_id,
                "trace_id": self.trace_id,
                "error": error,
                **self.additional_context
            }
            
            # Log asynchronously in background thread (non-blocking)
            def log_async():
                try:
                    structured_logger._log_structured(
                        "INFO" if not error else "ERROR",
                        f"Performance timing: {self.stage_name}",
                        context=log_data
                    )
                except Exception:
                    # Fail silently - instrumentation should never break execution
                    pass
            
            # Fire-and-forget logging
            threading.Thread(target=log_async, daemon=True).start()
            
            # Also track in observability service for aggregation
            try:
                from services.observability import observability
                job_type = self.additional_context.get("job_type")
                if job_type and self.job_id:
                    observability.track_performance_timing(
                        job_id=self.job_id,
                        job_type=job_type,
                        stage_type=self.stage_type,
                        duration_ms=self.duration_ms,
                        stage_name=self.stage_name
                    )
            except Exception:
                pass  # Non-critical, continue
            
        except Exception:
            # Fail silently - instrumentation should never break execution
            pass
    
    def get_duration_ms(self) -> Optional[float]:
        """Get duration in milliseconds"""
        return self.duration_ms


def timed_stage(
    stage_name: Optional[str] = None,
    stage_type: StageType = StageType.OTHER,
    extract_job_id: Optional[Callable] = None,
    extract_trace_id: Optional[Callable] = None,
    additional_context: Optional[Callable] = None
):
    """
    Decorator for timing function execution.
    
    Args:
        stage_name: Name of stage (defaults to function name)
        stage_type: Type of stage
        extract_job_id: Function to extract job_id from function args/kwargs
        extract_trace_id: Function to extract trace_id from function args/kwargs
        additional_context: Function to extract additional context from function args/kwargs
    
    Usage:
        @timed_stage(stage_type=StageType.AI_PROVIDER_CALL)
        def call_llm(prompt, job_id=None):
            ...
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Extract context
            job_id = extract_job_id(*args, **kwargs) if extract_job_id else None
            trace_id = extract_trace_id(*args, **kwargs) if extract_trace_id else None
            extra_context = additional_context(*args, **kwargs) if additional_context else {}
            
            # Use function name if stage_name not provided
            name = stage_name or func.__name__
            
            # Time execution
            with PerformanceTimer(
                stage_name=name,
                stage_type=stage_type,
                job_id=job_id,
                trace_id=trace_id,
                additional_context=extra_context
            ):
                return func(*args, **kwargs)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Extract context
            job_id = extract_job_id(*args, **kwargs) if extract_job_id else None
            trace_id = extract_trace_id(*args, **kwargs) if extract_trace_id else None
            extra_context = additional_context(*args, **kwargs) if additional_context else {}
            
            # Use function name if stage_name not provided
            name = stage_name or func.__name__
            
            # Time execution
            with PerformanceTimer(
                stage_name=name,
                stage_type=stage_type,
                job_id=job_id,
                trace_id=trace_id,
                additional_context=extra_context
            ):
                return await func(*args, **kwargs)
        
        # Return appropriate wrapper based on function type
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


@contextmanager
def timed_operation(
    stage_name: str,
    stage_type: StageType = StageType.OTHER,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    additional_context: Optional[Dict[str, Any]] = None
):
    """
    Context manager for timing operations.
    
    Usage:
        with timed_operation("fetch_user_data", StageType.DATABASE_READ, job_id=job_id):
            result = db.query(...)
    """
    with PerformanceTimer(
        stage_name=stage_name,
        stage_type=stage_type,
        job_id=job_id,
        trace_id=trace_id,
        additional_context=additional_context
    ):
        yield


# Helper functions for common context extraction
def extract_job_id_from_kwargs(*args, **kwargs) -> Optional[str]:
    """Extract job_id from kwargs"""
    return kwargs.get('job_id') or kwargs.get('job_data', {}).get('job_id') if isinstance(kwargs.get('job_data'), dict) else None


def extract_trace_id_from_state(state: Dict) -> Optional[str]:
    """Extract trace_id from LangGraph state"""
    return state.get('trace_id') if isinstance(state, dict) else None


def extract_job_id_from_state(state: Dict) -> Optional[str]:
    """Extract job_id from LangGraph state"""
    return state.get('job_id') if isinstance(state, dict) else None


# Convenience functions for common operations
def time_prompt_construction(
    stage_name: str,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    prompt_size: Optional[int] = None
):
    """Context manager for timing prompt construction"""
    context = {}
    if prompt_size is not None:
        context['prompt_size_tokens'] = prompt_size
    
    return timed_operation(
        stage_name=stage_name,
        stage_type=StageType.PROMPT_CONSTRUCTION,
        job_id=job_id,
        trace_id=trace_id,
        additional_context=context
    )


def time_ai_call(
    stage_name: str,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    model: Optional[str] = None,
    prompt_tokens: Optional[int] = None
):
    """Context manager for timing AI provider calls"""
    context = {}
    if model:
        context['model'] = model
    if prompt_tokens is not None:
        context['prompt_tokens'] = prompt_tokens
    
    return timed_operation(
        stage_name=stage_name,
        stage_type=StageType.AI_PROVIDER_CALL,
        job_id=job_id,
        trace_id=trace_id,
        additional_context=context
    )


def time_response_parsing(
    stage_name: str,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    response_size: Optional[int] = None
):
    """Context manager for timing response parsing"""
    context = {}
    if response_size is not None:
        context['response_size_tokens'] = response_size
    
    return timed_operation(
        stage_name=stage_name,
        stage_type=StageType.RESPONSE_PARSING,
        job_id=job_id,
        trace_id=trace_id,
        additional_context=context
    )


def time_db_read(
    stage_name: str,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    table: Optional[str] = None,
    cache_hit: Optional[bool] = None
):
    """Context manager for timing database reads"""
    context = {}
    if table:
        context['table'] = table
    if cache_hit is not None:
        context['cache_hit'] = cache_hit
    
    return timed_operation(
        stage_name=stage_name,
        stage_type=StageType.DATABASE_READ,
        job_id=job_id,
        trace_id=trace_id,
        additional_context=context
    )


def time_db_write(
    stage_name: str,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    table: Optional[str] = None,
    operation: Optional[str] = None,
    batch_size: Optional[int] = None
):
    """Context manager for timing database writes"""
    context = {}
    if table:
        context['table'] = table
    if operation:
        context['operation'] = operation
    if batch_size is not None:
        context['batch_size'] = batch_size
    
    return timed_operation(
        stage_name=stage_name,
        stage_type=StageType.DATABASE_WRITE,
        job_id=job_id,
        trace_id=trace_id,
        additional_context=context
    )


def time_cache_read(
    stage_name: str,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    cache_key: Optional[str] = None,
    cache_hit: Optional[bool] = None
):
    """Context manager for timing cache reads"""
    context = {}
    if cache_key:
        context['cache_key'] = cache_key
    if cache_hit is not None:
        context['cache_hit'] = cache_hit
    
    return timed_operation(
        stage_name=stage_name,
        stage_type=StageType.CACHE_READ,
        job_id=job_id,
        trace_id=trace_id,
        additional_context=context
    )


def time_cache_write(
    stage_name: str,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    cache_key: Optional[str] = None,
    ttl: Optional[int] = None
):
    """Context manager for timing cache writes"""
    context = {}
    if cache_key:
        context['cache_key'] = cache_key
    if ttl is not None:
        context['ttl'] = ttl
    
    return timed_operation(
        stage_name=stage_name,
        stage_type=StageType.CACHE_WRITE,
        job_id=job_id,
        trace_id=trace_id,
        additional_context=context
    )


def instrument_ai_call(
    prompt_construction_func,
    api_call_func,
    response_parsing_func,
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    model: Optional[str] = None,
    stage_name: Optional[str] = None
):
    """
    Comprehensive AI call instrumentation wrapper.
    Instruments three phases: prompt construction, API call, response parsing.
    
    Args:
        prompt_construction_func: Function that constructs the prompt
        api_call_func: Function that makes the API call
        response_parsing_func: Function that parses the response
        job_id: Optional job ID for correlation
        trace_id: Optional trace ID for distributed tracing
        model: Optional model name
        stage_name: Optional stage name
    
    Returns:
        Tuple of (parsed_response, prompt_construction_time_ms, api_call_time_ms, response_parsing_time_ms)
    """
    import time
    
    # Phase 1: Prompt Construction
    prompt_start = time.time()
    prompt = prompt_construction_func()
    prompt_construction_time_ms = (time.time() - prompt_start) * 1000
    
    # Track prompt construction timing
    with time_prompt_construction(
        stage_name=stage_name or "ai_call_prompt_construction",
        job_id=job_id,
        trace_id=trace_id,
        prompt_size=len(str(prompt)) if prompt else None
    ):
        pass  # Timing already captured above
    
    # Phase 2: API Call
    api_start = time.time()
    raw_response = api_call_func(prompt)
    api_call_time_ms = (time.time() - api_start) * 1000
    
    # Track API call timing
    prompt_tokens = None
    if hasattr(raw_response, 'response_metadata'):
        metadata = raw_response.response_metadata
        if metadata and 'token_usage' in metadata:
            prompt_tokens = metadata['token_usage'].get('prompt_tokens')
    
    with time_ai_call(
        stage_name=stage_name or "ai_call_api",
        job_id=job_id,
        trace_id=trace_id,
        model=model,
        prompt_tokens=prompt_tokens
    ):
        pass  # Timing already captured above
    
    # Phase 3: Response Parsing
    parse_start = time.time()
    parsed_response = response_parsing_func(raw_response)
    response_parsing_time_ms = (time.time() - parse_start) * 1000
    
    # Track response parsing timing
    response_size = None
    if hasattr(raw_response, 'content'):
        response_size = len(str(raw_response.content))
    elif hasattr(raw_response, 'response_metadata'):
        metadata = raw_response.response_metadata
        if metadata and 'token_usage' in metadata:
            response_size = metadata['token_usage'].get('completion_tokens')
    
    with time_response_parsing(
        stage_name=stage_name or "ai_call_response_parsing",
        job_id=job_id,
        trace_id=trace_id,
        response_size=response_size
    ):
        pass  # Timing already captured above
    
    return (
        parsed_response,
        prompt_construction_time_ms,
        api_call_time_ms,
        response_parsing_time_ms
    )


# Global instance for easy import
performance_timer = PerformanceTimer
