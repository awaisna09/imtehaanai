#!/usr/bin/env python3
"""
Circuit Breaker Service for Background Workers
Prevents cascading failures when AI provider is down

Features:
- Redis-backed state (shared across workers)
- Per-job-type circuit breakers
- Automatic recovery with cooldown
- No job loss (jobs remain queued)
- Clear logging
"""

import json
import os
import time
import logging
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from enum import Enum
from services.redis_connection import get_redis_client, is_redis_available

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Too many failures, pausing new jobs
    HALF_OPEN = "half_open"  # Testing recovery (limited jobs allowed)


class CircuitBreaker:
    """
    Circuit breaker for AI provider failures
    
    State Machine:
    CLOSED -> OPEN (on failure threshold)
    OPEN -> HALF_OPEN (after cooldown)
    HALF_OPEN -> CLOSED (on success)
    HALF_OPEN -> OPEN (on failure)
    """
    
    def __init__(
        self,
        job_type: str,
        failure_threshold: int = None,
        time_window_seconds: int = None,
        cooldown_seconds: int = None,
        half_open_max_jobs: int = None
    ):
        """
        Initialize circuit breaker for a job type
        
        Args:
            job_type: Job type identifier (e.g., 'tutor_chat', 'grade_answer')
            failure_threshold: Number of failures to open circuit (default: from env)
            time_window_seconds: Time window for failure counting (default: from env)
            cooldown_seconds: Cooldown period before attempting recovery (default: from env)
            half_open_max_jobs: Max jobs allowed in HALF_OPEN state (default: 1)
        """
        self.job_type = job_type
        self.redis = get_redis_client() if is_redis_available() else None
        
        # Configuration from environment with defaults
        self.failure_threshold = (
            failure_threshold or 
            int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
        )
        self.time_window_seconds = (
            time_window_seconds or 
            int(os.getenv("CIRCUIT_BREAKER_TIME_WINDOW", "60"))
        )
        self.cooldown_seconds = (
            cooldown_seconds or 
            int(os.getenv("CIRCUIT_BREAKER_COOLDOWN", "300"))  # 5 minutes
        )
        self.half_open_max_jobs = (
            half_open_max_jobs or 
            int(os.getenv("CIRCUIT_BREAKER_HALF_OPEN_MAX_JOBS", "1"))
        )
        
        # Redis key prefixes
        self.state_key = f"circuit:state:{job_type}"
        self.failures_key = f"circuit:failures:{job_type}"
        self.half_open_jobs_key = f"circuit:half_open_jobs:{job_type}"
        self.metadata_key = f"circuit:metadata:{job_type}"
        
        # Circuit breaker enabled flag
        self.enabled = os.getenv("CIRCUIT_BREAKER_ENABLED", "true").lower() == "true"
        
        if not self.enabled:
            logger.info(f"Circuit breaker disabled for {job_type}")
    
    def _get_state(self) -> CircuitState:
        """Get current circuit state from Redis"""
        if not self.redis or not self.enabled:
            return CircuitState.CLOSED
        
        try:
            state_str = self.redis.get(self.state_key)
            if state_str:
                return CircuitState(state_str.decode('utf-8'))
            return CircuitState.CLOSED
        except Exception as e:
            logger.warning(f"Failed to get circuit state for {self.job_type}: {e}")
            return CircuitState.CLOSED  # Fail open (allow jobs)
    
    def _set_state(self, state: CircuitState, reason: str = None):
        """Set circuit state in Redis"""
        if not self.redis or not self.enabled:
            return
        
        try:
            self.redis.set(self.state_key, state.value)
            
            # Update metadata
            metadata = {
                'state': state.value,
                'updated_at': datetime.utcnow().isoformat(),
                'reason': reason
            }
            self.redis.setex(
                self.metadata_key,
                self.cooldown_seconds * 2,  # Keep metadata longer
                json.dumps(metadata)
            )
            
            logger.info(
                f"Circuit breaker {self.job_type}: {state.value.upper()} "
                f"{f'({reason})' if reason else ''}"
            )
        except Exception as e:
            logger.error(f"Failed to set circuit state for {self.job_type}: {e}")
    
    def _record_failure(self, error_type: str = None):
        """Record a failure in the time window"""
        if not self.redis or not self.enabled:
            return
        
        try:
            now = time.time()
            failure_record = {
                'timestamp': now,
                'error_type': error_type or 'unknown'
            }
            
            # Add to sorted set (score = timestamp)
            self.redis.zadd(
                self.failures_key,
                {json.dumps(failure_record): now}
            )
            
            # Remove old failures outside time window
            cutoff = now - self.time_window_seconds
            self.redis.zremrangebyscore(self.failures_key, 0, cutoff)
            
            # Count failures in window
            failure_count = self.redis.zcard(self.failures_key)
            
            logger.debug(
                f"Circuit breaker {self.job_type}: Recorded failure "
                f"(total in window: {failure_count}/{self.failure_threshold})"
            )
            
            return failure_count
        except Exception as e:
            logger.error(f"Failed to record failure for {self.job_type}: {e}")
            return 0
    
    def _get_failure_count(self) -> int:
        """Get failure count in current time window"""
        if not self.redis or not self.enabled:
            return 0
        
        try:
            now = time.time()
            cutoff = now - self.time_window_seconds
            count = self.redis.zcount(self.failures_key, cutoff, now)
            return count
        except Exception as e:
            logger.warning(f"Failed to get failure count for {self.job_type}: {e}")
            return 0
    
    def _clear_failures(self):
        """Clear all failure records"""
        if not self.redis or not self.enabled:
            return
        
        try:
            self.redis.delete(self.failures_key)
            logger.debug(f"Circuit breaker {self.job_type}: Cleared failure records")
        except Exception as e:
            logger.warning(f"Failed to clear failures for {self.job_type}: {e}")
    
    def _get_opened_at(self) -> Optional[float]:
        """Get timestamp when circuit was opened"""
        if not self.redis or not self.enabled:
            return None
        
        try:
            metadata_str = self.redis.get(self.metadata_key)
            if metadata_str:
                metadata = json.loads(metadata_str)
                if metadata.get('state') == CircuitState.OPEN.value:
                    opened_at_str = metadata.get('opened_at')
                    if opened_at_str:
                        try:
                            opened_dt = datetime.fromisoformat(opened_at_str.replace('Z', '+00:00'))
                            return opened_dt.timestamp()
                        except (ValueError, TypeError):
                            pass
        except Exception as e:
            logger.warning(f"Failed to get opened_at for {self.job_type}: {e}")
        
        return None
    
    def _increment_half_open_jobs(self) -> int:
        """Increment half-open job counter"""
        if not self.redis or not self.enabled:
            return 0
        
        try:
            count = self.redis.incr(self.half_open_jobs_key)
            # Set TTL to prevent stale counters
            self.redis.expire(self.half_open_jobs_key, self.cooldown_seconds)
            return count
        except Exception as e:
            logger.warning(f"Failed to increment half-open jobs for {self.job_type}: {e}")
            return 0
    
    def _reset_half_open_jobs(self):
        """Reset half-open job counter"""
        if not self.redis or not self.enabled:
            return
        
        try:
            self.redis.delete(self.half_open_jobs_key)
        except Exception as e:
            logger.warning(f"Failed to reset half-open jobs for {self.job_type}: {e}")
    
    def can_process_job(self) -> tuple[bool, str]:
        """
        Check if a job can be processed
        
        Returns:
            (can_process: bool, reason: str)
        """
        if not self.enabled:
            return True, "circuit_breaker_disabled"
        
        if not self.redis:
            # Redis unavailable - fail open (allow jobs)
            return True, "redis_unavailable"
        
        state = self._get_state()
        
        if state == CircuitState.CLOSED:
            return True, "circuit_closed"
        
        elif state == CircuitState.OPEN:
            # Check if cooldown period has passed
            opened_at = self._get_opened_at()
            if opened_at:
                elapsed = time.time() - opened_at
                if elapsed >= self.cooldown_seconds:
                    # Cooldown passed - transition to HALF_OPEN
                    self._set_state(CircuitState.HALF_OPEN, "cooldown_expired")
                    self._reset_half_open_jobs()
                    logger.info(
                        f"Circuit breaker {self.job_type}: Transitioning to HALF_OPEN "
                        f"after {elapsed:.1f}s cooldown"
                    )
                    return True, "half_open_recovery"
                else:
                    remaining = self.cooldown_seconds - elapsed
                    return False, f"circuit_open_cooldown_remaining_{int(remaining)}s"
            else:
                # No opened_at timestamp - allow transition to HALF_OPEN
                self._set_state(CircuitState.HALF_OPEN, "no_timestamp_reset")
                return True, "half_open_reset"
        
        elif state == CircuitState.HALF_OPEN:
            # Check if we've exceeded half-open job limit
            half_open_count = self._increment_half_open_jobs()
            if half_open_count <= self.half_open_max_jobs:
                return True, "half_open_testing"
            else:
                return False, f"half_open_limit_exceeded_{half_open_count}"
        
        # Unknown state - fail open
        return True, "unknown_state"
    
    def record_success(self):
        """Record a successful job completion"""
        if not self.enabled:
            return
        
        state = self._get_state()
        
        if state == CircuitState.HALF_OPEN:
            # Success in HALF_OPEN - close circuit
            self._set_state(CircuitState.CLOSED, "recovery_success")
            self._clear_failures()
            self._reset_half_open_jobs()
            logger.info(
                f"Circuit breaker {self.job_type}: Recovery successful, circuit CLOSED"
            )
        elif state == CircuitState.CLOSED:
            # Success in CLOSED - clear old failures (normal operation)
            # Only clear if we have very few failures (prevent accidental clearing)
            failure_count = self._get_failure_count()
            if failure_count < 2:
                self._clear_failures()
    
    def record_failure(self, error_type: str = None, error_message: str = None):
        """
        Record a job failure
        
        Args:
            error_type: Type of error (e.g., 'APIError', 'TimeoutError')
            error_message: Error message for logging
        """
        if not self.enabled:
            return
        
        state = self._get_state()
        failure_count = self._record_failure(error_type)
        
        if state == CircuitState.CLOSED:
            # Check if we've exceeded threshold
            if failure_count >= self.failure_threshold:
                # Open circuit
                metadata = {
                    'state': CircuitState.OPEN.value,
                    'opened_at': datetime.utcnow().isoformat(),
                    'failure_count': failure_count,
                    'error_type': error_type,
                    'error_message': error_message
                }
                if self.redis:
                    self.redis.setex(
                        self.metadata_key,
                        self.cooldown_seconds * 2,
                        json.dumps(metadata)
                    )
                
                self._set_state(
                    CircuitState.OPEN,
                    f"failure_threshold_exceeded_{failure_count}"
                )
                logger.warning(
                    f"Circuit breaker {self.job_type}: OPENED after {failure_count} failures "
                    f"(threshold: {self.failure_threshold})"
                )
        
        elif state == CircuitState.HALF_OPEN:
            # Failure in HALF_OPEN - reopen circuit
            metadata = {
                'state': CircuitState.OPEN.value,
                'opened_at': datetime.utcnow().isoformat(),
                'failure_count': failure_count,
                'error_type': error_type,
                'error_message': error_message,
                'half_open_failed': True
            }
            if self.redis:
                self.redis.setex(
                    self.metadata_key,
                    self.cooldown_seconds * 2,
                    json.dumps(metadata)
                )
            
            self._set_state(CircuitState.OPEN, "half_open_failure")
            self._reset_half_open_jobs()
            logger.warning(
                f"Circuit breaker {self.job_type}: Reopened after HALF_OPEN failure"
            )
    
    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status"""
        if not self.enabled:
            return {
                'enabled': False,
                'state': 'disabled'
            }
        
        state = self._get_state()
        failure_count = self._get_failure_count()
        
        status = {
            'enabled': True,
            'job_type': self.job_type,
            'state': state.value,
            'failure_count': failure_count,
            'failure_threshold': self.failure_threshold,
            'time_window_seconds': self.time_window_seconds,
            'cooldown_seconds': self.cooldown_seconds
        }
        
        if state == CircuitState.OPEN:
            opened_at = self._get_opened_at()
            if opened_at:
                elapsed = time.time() - opened_at
                remaining = max(0, self.cooldown_seconds - elapsed)
                status['cooldown_remaining_seconds'] = int(remaining)
        
        elif state == CircuitState.HALF_OPEN:
            if self.redis:
                try:
                    count = self.redis.get(self.half_open_jobs_key)
                    status['half_open_jobs_processed'] = int(count) if count else 0
                    status['half_open_max_jobs'] = self.half_open_max_jobs
                except Exception:
                    pass
        
        return status


# Global circuit breaker instances (per job type)
_circuit_breakers: Dict[str, CircuitBreaker] = {}


def get_circuit_breaker(job_type: str) -> CircuitBreaker:
    """Get or create circuit breaker for a job type"""
    if job_type not in _circuit_breakers:
        _circuit_breakers[job_type] = CircuitBreaker(job_type)
    return _circuit_breakers[job_type]


def reset_circuit_breaker(job_type: str):
    """Reset circuit breaker to CLOSED state (for testing/admin)"""
    breaker = get_circuit_breaker(job_type)
    breaker._set_state(CircuitState.CLOSED, "manual_reset")
    breaker._clear_failures()
    breaker._reset_half_open_jobs()
    logger.info(f"Circuit breaker {job_type}: Manually reset to CLOSED")
