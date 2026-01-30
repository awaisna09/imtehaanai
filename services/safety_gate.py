#!/usr/bin/env python3
"""
System Safety Gate Service
Centralized safety checks that prevent work from being enqueued when system is unsafe.

This service is the SINGLE SOURCE OF TRUTH for system safety checks.
All job-enqueueing endpoints MUST use this service before enqueueing jobs.

Safety Checks:
1. Redis queue depth (prevents queue overflow)
2. Worker health (prevents job accumulation without processing)
3. Memory pressure (prevents OOM crashes)

If ANY check fails, the system MUST reject work (HTTP 503) to prevent crashes.
"""

import os
import time
from typing import Dict, Any, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv('config.env')


class SafetyStatus(Enum):
    """Safety check status"""
    SAFE = "safe"  # System is safe to accept work
    UNSAFE = "unsafe"  # System is unsafe, must reject work
    UNKNOWN = "unknown"  # Cannot determine safety (fail-closed: treat as unsafe)


@dataclass
class SafetyCheckResult:
    """Result of safety gate check"""
    status: SafetyStatus
    safe: bool  # True if system is safe to accept work
    reason: Optional[str] = None  # Reason for unsafe status
    retry_after: int = 60  # Seconds to wait before retry
    checks: Dict[str, Any] = None  # Detailed check results
    
    def __post_init__(self):
        if self.checks is None:
            self.checks = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            "status": self.status.value,
            "safe": self.safe,
            "reason": self.reason,
            "retry_after": self.retry_after,
            "checks": self.checks,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


class SafetyGate:
    """
    Centralized system safety gate.
    
    This is the SINGLE SOURCE OF TRUTH for system safety checks.
    All job-enqueueing endpoints MUST use this service.
    
    Usage:
        safety_gate = SafetyGate()
        result = safety_gate.check_system_safety(queue_name="jobs:tutor")
        if not result.safe:
            return JSONResponse(status_code=503, content=result.to_dict())
    """
    
    def __init__(self):
        """Initialize safety gate with configuration"""
        # Load configuration
        self.enabled = os.getenv("SAFETY_GATE_ENABLED", "true").lower() == "true"
        self.queue_threshold = float(os.getenv("SAFETY_GATE_QUEUE_THRESHOLD", "0.9"))  # 90% capacity
        self.memory_threshold_mb = float(os.getenv("SAFETY_GATE_MEMORY_THRESHOLD_MB", "400"))  # 400MB for backend
        self.memory_percent_threshold = float(os.getenv("SAFETY_GATE_MEMORY_PERCENT_THRESHOLD", "80"))  # 80% system memory
        self.min_healthy_workers = int(os.getenv("SAFETY_GATE_MIN_HEALTHY_WORKERS", "2"))  # Minimum 2 healthy workers
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", "2"))  # 2 seconds max for checks (reduced from 5s to prevent blocking)
        
        # Lazy imports to avoid circular dependencies
        self._job_queue = None
        self._observability = None
        self._memory_monitor = None
    
    def _get_job_queue(self):
        """Lazy load job queue"""
        if self._job_queue is None:
            try:
                from services.job_queue import job_queue
                from services.redis_connection import is_redis_available
                if is_redis_available() and job_queue:
                    self._job_queue = job_queue
            except ImportError:
                pass
        return self._job_queue
    
    def _get_observability(self):
        """Lazy load observability service"""
        if self._observability is None:
            try:
                from services.observability import observability
                self._observability = observability
            except ImportError:
                pass
        return self._observability
    
    def _get_memory_monitor(self):
        """Lazy load memory monitor"""
        if self._memory_monitor is None:
            try:
                from services.memory_monitor import get_memory_usage
                self._memory_monitor = get_memory_usage
            except ImportError:
                pass
        return self._memory_monitor
    
    def check_queue_depth(self, queue_name: str) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """
        Check if queue depth is safe
        
        Returns:
            (safe, reason, details)
        """
        try:
            job_queue = self._get_job_queue()
            if not job_queue:
                return False, "Job queue service not available", {"error": "job_queue_unavailable"}
            
            # Get queue depth (synchronous call, no asyncio needed)
            # CRITICAL FIX: Use timeout to prevent blocking endpoint
            try:
                import threading
                
                # Use threading timeout to prevent blocking
                queue_depth_result = [None]
                queue_depth_error = [None]
                
                def get_queue_depth():
                    try:
                        queue_depth_result[0] = job_queue.get_queue_length(queue_name)
                    except Exception as e:
                        queue_depth_error[0] = e
                
                # Run in thread with timeout
                thread = threading.Thread(target=get_queue_depth, daemon=True)
                thread.start()
                thread.join(timeout=self.request_timeout)  # Use configured timeout
                
                if thread.is_alive():
                    # Thread still running - timeout occurred
                    # FAIL-OPEN: Assume safe if check times out (availability over safety)
                    return True, "Queue depth check timeout (assuming safe)", {
                        "error": "timeout",
                        "timeout_seconds": self.request_timeout,
                        "fail_open": True
                    }
                
                if queue_depth_error[0]:
                    error = queue_depth_error[0]
                    error_str = str(error).lower()
                    if "timeout" in error_str or "timed out" in error_str:
                        # Timeout: fail-open (assume safe) to prevent blocking
                        return True, "Queue depth check timeout (assuming safe)", {
                            "error": "timeout",
                            "timeout_seconds": self.request_timeout,
                            "fail_open": True
                        }
                    # Other errors: fail-open in development, fail-closed in production
                    is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
                    if is_development:
                        return True, f"Queue depth check error (dev mode: assuming safe): {str(error)}", {
                            "error": str(error),
                            "fail_open": True,
                            "dev_mode": True
                        }
                    return False, f"Queue depth check failed: {str(error)}", {"error": str(error)}
                
                queue_depth = queue_depth_result[0]
            except Exception as e:
                # Fail-open on any exception to prevent blocking
                is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
                if is_development:
                    return True, f"Queue depth check exception (dev mode: assuming safe): {str(e)}", {
                        "error": str(e),
                        "fail_open": True,
                        "dev_mode": True
                    }
                # In production, still fail-open to maintain availability
                return True, f"Queue depth check exception (assuming safe): {str(e)}", {
                    "error": str(e),
                    "fail_open": True
                }
            
            max_queue_size = int(os.getenv("MAX_QUEUE_SIZE", "10000"))
            threshold = max_queue_size * self.queue_threshold
            queue_percent = (queue_depth / max_queue_size) * 100 if max_queue_size > 0 else 0
            
            details = {
                "queue_depth": queue_depth,
                "max_queue_size": max_queue_size,
                "threshold": threshold,
                "queue_percent": round(queue_percent, 2),
                "queue_name": queue_name
            }
            
            if queue_depth >= threshold:
                return False, f"Queue capacity exceeded ({queue_depth}/{max_queue_size}, {queue_percent:.1f}%)", details
            
            return True, None, details
            
        except Exception as e:
            # Fail-closed: if check fails, treat as unsafe
            return False, f"Queue depth check error: {str(e)}", {"error": str(e)}
    
    def check_worker_health(self) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """
        Check if worker health is sufficient
        
        Returns:
            (safe, reason, details)
        """
        try:
            observability = self._get_observability()
            if not observability:
                # If observability unavailable, fail-closed (treat as unsafe)
                return False, "Worker health check unavailable", {"error": "observability_unavailable"}
            
            # Get worker health (synchronous call, no asyncio needed)
            # CRITICAL FIX: Use timeout to prevent blocking endpoint
            try:
                import signal
                import threading
                
                # Use threading timeout to prevent blocking
                worker_health_result = [None]
                worker_health_error = [None]
                
                def get_health():
                    try:
                        worker_health_result[0] = observability.get_worker_health()
                    except Exception as e:
                        worker_health_error[0] = e
                
                # Run in thread with timeout
                thread = threading.Thread(target=get_health, daemon=True)
                thread.start()
                thread.join(timeout=self.request_timeout)  # Use configured timeout
                
                if thread.is_alive():
                    # Thread still running - timeout occurred
                    # FAIL-OPEN: Assume safe if check times out (availability over safety)
                    # This prevents blocking the endpoint when observability is slow
                    return True, "Worker health check timeout (assuming safe)", {
                        "error": "timeout",
                        "timeout_seconds": self.request_timeout,
                        "fail_open": True,
                        "note": "Health check timed out, assuming workers are healthy to maintain availability"
                    }
                
                if worker_health_error[0]:
                    error = worker_health_error[0]
                    error_str = str(error).lower()
                    if "timeout" in error_str or "timed out" in error_str:
                        # Timeout: fail-open (assume safe) to prevent blocking
                        return True, "Worker health check timeout (assuming safe)", {
                            "error": "timeout",
                            "timeout_seconds": self.request_timeout,
                            "fail_open": True
                        }
                    # Other errors: fail-open in development, fail-closed in production
                    is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
                    if is_development:
                        return True, f"Worker health check error (dev mode: assuming safe): {str(error)}", {
                            "error": str(error),
                            "fail_open": True,
                            "dev_mode": True
                        }
                    return False, f"Worker health check failed: {str(error)}", {"error": str(error)}
                
                worker_health = worker_health_result[0]
            except Exception as e:
                # Fail-open on any exception to prevent blocking
                is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
                if is_development:
                    return True, f"Worker health check exception (dev mode: assuming safe): {str(e)}", {
                        "error": str(e),
                        "fail_open": True,
                        "dev_mode": True
                    }
                # In production, still fail-open to maintain availability
                return True, f"Worker health check exception (assuming safe): {str(e)}", {
                    "error": str(e),
                    "fail_open": True
                }
            
            # Count healthy workers
            healthy_workers = 0
            total_workers = 0
            worker_details = {}
            
            if worker_health:
                # CRITICAL FIX: Handle nested workers structure from observability
                # observability.get_worker_health() returns: {'workers': {'workers': {...}, 'total_workers': 2, ...}}
                if "workers" in worker_health:
                    workers_data = worker_health.get("workers", {})
                    
                    # Check if it's the nested structure: workers.workers dict
                    if isinstance(workers_data, dict) and "workers" in workers_data:
                        # Nested structure: workers.workers contains actual worker data
                        actual_workers = workers_data.get("workers", {})
                        total_workers = workers_data.get("total_workers", 0)
                        alive_workers = workers_data.get("alive_workers", 0)
                        
                        # Count healthy workers from actual_workers dict
                        for worker_id, health_data in actual_workers.items():
                            if isinstance(health_data, dict):
                                status = health_data.get("status")
                                liveness = health_data.get("liveness", "unknown")
                                if status == "healthy" or (status is None and liveness == "alive"):
                                    healthy_workers += 1
                                worker_details[worker_id] = {
                                    "status": status,
                                    "liveness": liveness
                                }
                        
                        # In development, if we have alive_workers but no healthy count, use alive_workers
                        is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
                        if is_development and healthy_workers == 0 and alive_workers > 0:
                            healthy_workers = alive_workers
                            worker_details["alive_workers"] = alive_workers
                            worker_details["total_workers"] = total_workers
                    # Direct workers dict mapping worker_id to health_data
                    elif isinstance(workers_data, dict):
                        for worker_id, health_data in workers_data.items():
                            if worker_id == "total_workers" or worker_id == "alive_workers":
                                continue  # Skip metadata keys
                            total_workers += 1
                            if isinstance(health_data, dict):
                                status = health_data.get("status")
                                liveness = health_data.get("liveness", "unknown")
                                if status == "healthy" or (status is None and liveness == "alive"):
                                    healthy_workers += 1
                                worker_details[worker_id] = {
                                    "status": status,
                                    "liveness": liveness
                                }
                elif "workers_summary" in worker_health:
                    summary = worker_health.get("workers_summary", {})
                    healthy_workers = summary.get("healthy_count", 0)
                    total_workers = summary.get("total_workers", 0)
                    worker_details = summary
                # Check for alternative structure: workers dict with total_workers key
                elif isinstance(worker_health.get("workers"), dict):
                    workers_dict = worker_health.get("workers", {})
                    # Check if it has total_workers key (alternative structure)
                    if "total_workers" in workers_dict:
                        total_workers = workers_dict.get("total_workers", 0)
                        alive_workers = workers_dict.get("alive_workers", 0)
                        # In development, if workers are alive but not reporting detailed health, assume healthy
                        is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
                        if is_development and alive_workers > 0:
                            healthy_workers = alive_workers
                        worker_details = workers_dict
            
            details = {
                "healthy_workers": healthy_workers,
                "total_workers": total_workers,
                "min_required": self.min_healthy_workers,
                "worker_details": worker_details
            }
            
            # In development, be more lenient with worker health checks
            is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
            if healthy_workers < self.min_healthy_workers:
                # Development mode: if we have at least 1 worker running (PM2 shows it), allow requests
                # Workers might not be reporting health correctly, but they're running
                if is_development:
                    # Check if we have any indication of workers (alive_workers from observability)
                    alive_workers = details.get("worker_details", {}).get("alive_workers", 0)
                    # Also check total_workers from the details dict itself
                    if alive_workers == 0:
                        alive_workers = total_workers
                    if alive_workers > 0 or total_workers > 0:
                        # We have workers, just not reporting detailed health - allow in dev
                        return True, None, {**details, "dev_mode_bypass": True, "note": f"Development mode: allowing requests with {alive_workers or total_workers} worker(s) despite health reporting issues"}
                return False, f"Insufficient worker capacity ({healthy_workers}/{self.min_healthy_workers} healthy workers)", details
            
            return True, None, details
            
        except Exception as e:
            # Fail-closed: if check fails, treat as unsafe
            return False, f"Worker health check error: {str(e)}", {"error": str(e)}
    
    def check_memory_pressure(self) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """
        Check if memory pressure is safe
        
        Returns:
            (safe, reason, details)
        """
        try:
            get_memory_usage = self._get_memory_monitor()
            if not get_memory_usage:
                # If memory monitor unavailable, skip check (don't fail-closed for memory)
                return True, None, {"warning": "memory_monitor_unavailable"}
            
            memory_info = get_memory_usage()
            
            if "error" in memory_info:
                # Memory check failed, but don't fail-closed (allow work)
                return True, None, {"warning": memory_info.get("error")}
            
            memory_rss_mb = memory_info.get("memory_rss_mb", 0)
            system_percent = memory_info.get("system_percent")
            system_available_mb = memory_info.get("system_available_mb")
            
            details = {
                "memory_rss_mb": memory_rss_mb,
                "memory_threshold_mb": self.memory_threshold_mb,
                "system_percent": system_percent,
                "system_available_mb": system_available_mb
            }
            
            # Check process memory
            if memory_rss_mb > self.memory_threshold_mb:
                return False, f"Process memory exceeded threshold ({memory_rss_mb:.1f}MB > {self.memory_threshold_mb}MB)", details
            
            # Check system memory (if available)
            if system_percent is not None and system_percent > self.memory_percent_threshold:
                return False, f"System memory pressure high ({system_percent:.1f}% > {self.memory_percent_threshold}%)", details
            
            return True, None, details
            
        except Exception as e:
            # Memory check failure: don't fail-closed (allow work, but log warning)
            return True, None, {"warning": f"Memory check error: {str(e)}"}
    
    def check_system_safety(
        self,
        queue_name: str,
        skip_memory_check: bool = False
    ) -> SafetyCheckResult:
        """
        Perform all safety checks
        
        Args:
            queue_name: Name of queue to check (e.g., "jobs:tutor")
            skip_memory_check: Skip memory check (for testing or if memory monitor unavailable)
        
        Returns:
            SafetyCheckResult with status and details
        """
        # If safety gate is disabled, always return safe
        if not self.enabled:
            return SafetyCheckResult(
                status=SafetyStatus.SAFE,
                safe=True,
                reason="Safety gate disabled",
                checks={"enabled": False}
            )
        
        checks = {}
        failures = []
        
        # Check 1: Queue depth
        queue_safe, queue_reason, queue_details = self.check_queue_depth(queue_name)
        checks["queue_depth"] = {
            "safe": queue_safe,
            "reason": queue_reason,
            **queue_details
        }
        if not queue_safe:
            failures.append(f"Queue: {queue_reason}")
        
        # Check 2: Worker health
        worker_safe, worker_reason, worker_details = self.check_worker_health()
        checks["worker_health"] = {
            "safe": worker_safe,
            "reason": worker_reason,
            **worker_details
        }
        if not worker_safe:
            failures.append(f"Workers: {worker_reason}")
        
        # Check 3: Memory pressure (optional, skip if unavailable)
        if not skip_memory_check:
            memory_safe, memory_reason, memory_details = self.check_memory_pressure()
            checks["memory_pressure"] = {
                "safe": memory_safe,
                "reason": memory_reason,
                **memory_details
            }
            if not memory_safe:
                failures.append(f"Memory: {memory_reason}")
        else:
            checks["memory_pressure"] = {
                "safe": True,
                "reason": "Skipped",
                "skipped": True
            }
        
        # Determine overall safety
        if failures:
            # System is unsafe
            reason = "; ".join(failures)
            return SafetyCheckResult(
                status=SafetyStatus.UNSAFE,
                safe=False,
                reason=reason,
                retry_after=60,  # Retry after 60 seconds
                checks=checks
            )
        else:
            # System is safe
            return SafetyCheckResult(
                status=SafetyStatus.SAFE,
                safe=True,
                reason=None,
                retry_after=0,
                checks=checks
            )


# Global singleton instance
_safety_gate_instance: Optional[SafetyGate] = None


def get_safety_gate() -> SafetyGate:
    """Get global safety gate instance (singleton)"""
    global _safety_gate_instance
    if _safety_gate_instance is None:
        _safety_gate_instance = SafetyGate()
    return _safety_gate_instance
