"""
Structured Logging Service
Structured logging for background jobs with context and metadata
"""

import json
import logging
import os
import sys
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum
from dotenv import load_dotenv

load_dotenv('config.env')

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "json")  # json or text
ENABLE_DEBUG = os.getenv("ENABLE_DEBUG", "false").lower() == "true"


class LogLevel(str, Enum):
    """Log levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class StructuredLogger:
    """Structured logger for background jobs and API requests"""
    
    def __init__(self, name: str = "imtehaan"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        
        # Set formatter based on format preference
        if LOG_FORMAT == "json":
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
        
        self.logger.addHandler(handler)
    
    def _log_structured(
        self,
        level: str,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """Log with structured context"""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "context": context or {},
            **kwargs
        }
        
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(json.dumps(log_data, default=str))
    
    def log_event(
        self,
        event: str,
        correlation_id: Optional[str] = None,
        job_id: Optional[str] = None,
        **fields
    ):
        """
        Generic event logger for tutor job lifecycle and LangGraph nodes.
        
        Args:
            event: Event name (e.g., 'job_enqueue', 'node_start', 'node_end')
            correlation_id: Correlation ID for end-to-end tracing
            job_id: Job ID
            **fields: Additional fields to include in log (duration_ms, queue_name, worker_id, etc.)
        """
        context = {
            "event": event,
            **fields
        }
        if correlation_id:
            context["correlation_id"] = correlation_id
        if job_id:
            context["job_id"] = job_id
        
        # Determine log level based on event type
        level = "INFO"
        if "error" in event.lower() or "failure" in event.lower() or "timeout" in event.lower():
            level = "ERROR"
        elif "warning" in event.lower() or "retry" in event.lower():
            level = "WARNING"
        
        message = f"Event: {event}"
        if job_id:
            message += f" (job_id: {job_id})"
        if correlation_id:
            message += f" (correlation_id: {correlation_id})"
        
        self._log_structured(level, message, context=context)
    
    def log_job_start(
        self,
        job_id: str,
        job_type: str,
        user_id: Optional[str] = None,
        queue_name: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        queue_wait_seconds: Optional[float] = None,
        **kwargs
    ):
        """Log job start"""
        context = {
            "job_id": job_id,
            "job_type": job_type,
            "user_id": user_id,
            "queue_name": queue_name,
            "event": "job_start"
        }
        if timeout_seconds is not None:
            context["timeout_seconds"] = timeout_seconds
        if queue_wait_seconds is not None:
            context["queue_wait_seconds"] = round(queue_wait_seconds, 3)
        
        self._log_structured(
            "INFO",
            f"Job started: {job_id}",
            context=context,
            **kwargs
        )
    
    def log_job_complete(
        self,
        job_id: str,
        job_type: str,
        duration_seconds: float,
        user_id: Optional[str] = None,
        **kwargs
    ):
        """Log job completion"""
        self._log_structured(
            "INFO",
            f"Job completed: {job_id}",
            context={
                "job_id": job_id,
                "job_type": job_type,
                "user_id": user_id,
                "duration_seconds": duration_seconds,
                "event": "job_complete"
            },
            **kwargs
        )
    
    def log_job_failure(
        self,
        job_id: str,
        job_type: str,
        error: str,
        retry_count: int = 0,
        user_id: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        elapsed_seconds: Optional[float] = None,
        **kwargs
    ):
        """Log job failure"""
        context = {
            "job_id": job_id,
            "job_type": job_type,
            "user_id": user_id,
            "error": error,
            "retry_count": retry_count,
            "event": "job_failure"
        }
        if timeout_seconds is not None:
            context["timeout_seconds"] = timeout_seconds
        if elapsed_seconds is not None:
            context["elapsed_seconds"] = elapsed_seconds
        
        self._log_structured(
            "ERROR",
            f"Job failed: {job_id}",
            context=context,
            **kwargs
        )
    
    def log_job_retry(
        self,
        job_id: str,
        job_type: str,
        retry_count: int,
        delay_seconds: int,
        user_id: Optional[str] = None,
        max_retries: Optional[int] = None,
        **kwargs
    ):
        """Log job retry"""
        context = {
            "job_id": job_id,
            "job_type": job_type,
            "user_id": user_id,
            "retry_count": retry_count,
            "delay_seconds": delay_seconds,
            "event": "job_retry"
        }
        if max_retries is not None:
            context["max_retries"] = max_retries
        
        self._log_structured(
            "WARNING",
            f"Job retry: {job_id}",
            context=context,
            **kwargs
        )
    
    def log_queue_operation(
        self,
        operation: str,
        queue_name: str,
        job_id: Optional[str] = None,
        queue_length: Optional[int] = None,
        **kwargs
    ):
        """Log queue operation"""
        self._log_structured(
            "INFO",
            f"Queue operation: {operation}",
            context={
                "operation": operation,
                "queue_name": queue_name,
                "job_id": job_id,
                "queue_length": queue_length,
                "event": "queue_operation"
            },
            **kwargs
        )
    
    def log_api_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        user_id: Optional[str] = None,
        **kwargs
    ):
        """Log API request (separate from job processing time)"""
        self._log_structured(
            "INFO",
            f"API request: {method} {path}",
            context={
                "method": method,
                "path": path,
                "status_code": status_code,
                "duration_ms": duration_ms,
                "user_id": user_id,
                "event": "api_request"
            },
            **kwargs
        )
    
    def log_worker_event(
        self,
        event: str,
        worker_id: str,
        active_jobs: int = 0,
        processed_count: int = 0,
        error_count: int = 0,
        **kwargs
    ):
        """Log worker event"""
        self._log_structured(
            "INFO",
            f"Worker event: {event}",
            context={
                "event": event,
                "worker_id": worker_id,
                "active_jobs": active_jobs,
                "processed_count": processed_count,
                "error_count": error_count,
                "event_type": "worker_event"
            },
            **kwargs
        )
    
    def log_database_operation(
        self,
        operation: str,
        table: str,
        duration_ms: float,
        cached: bool = False,
        success: Optional[bool] = None,
        error: Optional[str] = None,
        error_code: Optional[str] = None,
        retries: Optional[int] = None,
        request_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        job_id: Optional[str] = None,
        query_name: Optional[str] = None,
        **kwargs
    ):
        """Log database operation with observability data"""
        level = "ERROR" if not success else ("DEBUG" if cached else "INFO")
        message = (
            f"Database operation: {operation} on {table} "
            f"({duration_ms:.2f}ms)"
        )
        if not success:
            message += f" - FAILED: {error}"

        context = {
            "event": "database_operation",
            "operation": operation,
            "table": table,
            "duration_ms": round(duration_ms, 2),
            "cached": cached,
        }
        if success is not None:
            context["success"] = success
        if error:
            context["error"] = error
        if error_code:
            context["error_code"] = error_code
        if retries is not None:
            context["retries"] = retries
        if request_id:
            context["request_id"] = request_id
        if endpoint:
            context["endpoint"] = endpoint
        if job_id:
            context["job_id"] = job_id
        if query_name:
            context["query_name"] = query_name

        self._log_structured(
            level,
            message,
            context=context,
            **kwargs
        )
    
    def log_rate_limit(
        self,
        user_id: str,
        category: str,
        allowed: bool,
        remaining: int,
        **kwargs
    ):
        """Log rate limit check"""
        level = "WARNING" if not allowed else "DEBUG"
        self._log_structured(
            level,
            f"Rate limit check: {category}",
            context={
                "user_id": user_id,
                "category": category,
                "allowed": allowed,
                "remaining": remaining,
                "event": "rate_limit"
            },
            **kwargs
        )
    
    def log_job_timeout(
        self,
        job_id: str,
        job_type: str,
        timeout_seconds: int,
        elapsed_seconds: float,
        user_id: Optional[str] = None,
        **kwargs
    ):
        """Log job timeout (production hardening)"""
        self._log_structured(
            "ERROR",
            f"Job timeout: {job_id}",
            context={
                "job_id": job_id,
                "job_type": job_type,
                "user_id": user_id,
                "timeout_seconds": timeout_seconds,
                "elapsed_seconds": elapsed_seconds,
                "event": "job_timeout"
            },
            **kwargs
        )
    
    def log_limiter_saturation(
        self,
        utilization: float,
        active_count: int,
        limit: int,
        available: int,
        endpoint: Optional[str] = None,
        job_type: Optional[str] = None,
        **kwargs
    ):
        """Log Supabase limiter saturation event"""
        self._log_structured(
            "WARNING",
            f"Supabase limiter saturated: {utilization:.1f}% utilization",
            context={
                "event": "limiter_saturation",
                "utilization": round(utilization, 2),
                "active_count": active_count,
                "limit": limit,
                "available": available,
                "endpoint": endpoint,
                "job_type": job_type
            },
            **kwargs
        )
    
    def log_supabase_budget_saturated(
        self,
        active_count: int,
        limit: int,
        threshold: int,
        saturation_percent: float,
        **kwargs
    ):
        """Log Supabase budget saturation event (backpressure)"""
        self._log_structured(
            "WARNING",
            f"Supabase budget saturated: {active_count}/{limit} "
            f"({saturation_percent:.1f}%)",
            context={
                "event": "supabase_budget_saturated",
                "active_count": active_count,
                "limit": limit,
                "threshold": threshold,
                "saturation_percent": round(saturation_percent, 2)
            },
            **kwargs
        )

    def log_redis_connectivity(
        self,
        event: str,
        available: bool,
        error: Optional[str] = None,
        **kwargs
    ):
        """Log Redis connectivity events (critical for failure isolation)"""
        level = "ERROR" if not available else "INFO"
        context = {
            "event": "redis_connectivity",
            "redis_event": event,
            "available": available
        }
        if error:
            context["error"] = error
        
        self._log_structured(
            level,
            f"Redis connectivity: {event}",
            context=context,
            **kwargs
        )
    
    def log_worker_crash(
        self,
        worker_id: str,
        reason: str,
        active_jobs: int = 0,
        **kwargs
    ):
        """Log worker crash (critical for failure isolation)"""
        self._log_structured(
            "CRITICAL",
            f"Worker crash: {worker_id}",
            context={
                "event": "worker_crash",
                "worker_id": worker_id,
                "reason": reason,
                "active_jobs": active_jobs
            },
            **kwargs
        )
    
    def log_tutor_job_enqueue(
        self,
        job_id: str,
        correlation_id: str,
        queue_name: str,
        user_id: Optional[str] = None,
        conversation_id: Optional[str] = None,
        topic: Optional[str] = None,
        queue_length: Optional[int] = None,
        **kwargs
    ):
        """Log tutor job enqueue"""
        self.log_event(
            event="job_enqueue",
            correlation_id=correlation_id,
            job_id=job_id,
            job_type="tutor_chat",
            queue_name=queue_name,
            user_id=user_id,
            conversation_id=conversation_id,
            topic=topic,
            queue_length=queue_length,
            **kwargs
        )
    
    def log_tutor_job_dequeue(
        self,
        job_id: str,
        correlation_id: str,
        queue_name: str,
        worker_id: Optional[str] = None,
        queue_wait_seconds: Optional[float] = None,
        **kwargs
    ):
        """Log tutor job dequeue"""
        self.log_event(
            event="job_dequeue",
            correlation_id=correlation_id,
            job_id=job_id,
            job_type="tutor_chat",
            queue_name=queue_name,
            worker_id=worker_id,
            queue_wait_seconds=round(queue_wait_seconds, 3) if queue_wait_seconds else None,
            **kwargs
        )
    
    def log_concurrency_decision(
        self,
        job_id: str,
        correlation_id: str,
        decision: str,  # "process" or "requeue"
        active_jobs: int,
        concurrency_limit: int,
        db_connections: Optional[int] = None,
        db_limit: Optional[int] = None,
        reason: Optional[str] = None,
        **kwargs
    ):
        """Log concurrency decision (process or requeue)"""
        self.log_event(
            event="concurrency_decision",
            correlation_id=correlation_id,
            job_id=job_id,
            job_type="tutor_chat",
            decision=decision,
            active_jobs=active_jobs,
            concurrency_limit=concurrency_limit,
            db_connections=db_connections,
            db_limit=db_limit,
            reason=reason,
            **kwargs
        )
    
    def log_tutor_job_start_processing(
        self,
        job_id: str,
        correlation_id: str,
        worker_id: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        **kwargs
    ):
        """Log tutor job start processing"""
        self.log_event(
            event="job_start_processing",
            correlation_id=correlation_id,
            job_id=job_id,
            job_type="tutor_chat",
            worker_id=worker_id,
            timeout_seconds=timeout_seconds,
            **kwargs
        )
    
    def log_langgraph_node(
        self,
        event: str,  # "node_start" or "node_end"
        node_name: str,
        job_id: str,
        correlation_id: str,
        duration_ms: Optional[float] = None,
        error: Optional[str] = None,
        error_type: Optional[str] = None,
        **kwargs
    ):
        """Log LangGraph node start/end with timing"""
        fields = {
            "node_name": node_name,
            "job_type": "tutor_chat",
            **kwargs
        }
        if duration_ms is not None:
            fields["duration_ms"] = round(duration_ms, 2)
        if error:
            fields["error"] = error
        if error_type:
            fields["error_type"] = error_type
        
        self.log_event(
            event=event,
            correlation_id=correlation_id,
            job_id=job_id,
            **fields
        )


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    
    def format(self, record):
        # If message is already JSON, return as-is
        if isinstance(record.msg, str):
            try:
                json.loads(record.msg)
                return record.msg
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Otherwise, format as JSON
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, default=str)


# Global logger instance
structured_logger = StructuredLogger()
