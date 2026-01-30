"""
Supabase Circuit Breaker
Prevents retry amplification when Supabase is failing.
Tracks failures and opens circuit to fail fast during outages.
"""

import os
import time
import logging
import threading
from typing import Optional, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Too many failures, failing fast
    HALF_OPEN = "half_open"  # Testing recovery


class SupabaseCircuitBreaker:
    """
    In-memory circuit breaker for Supabase operations.
    Prevents retry amplification during outages.
    """
    
    def __init__(
        self,
        failure_threshold: Optional[int] = None,
        time_window_seconds: Optional[int] = None,
        cooldown_seconds: Optional[int] = None
    ):
        """
        Initialize circuit breaker

        Args:
            failure_threshold: Failures to open circuit (default: from env)
            time_window_seconds: Time window for failures (default: from env)
            cooldown_seconds: Cooldown when open (default: from env)
        """
        self.failure_threshold = (
            failure_threshold or
            int(os.getenv("SUPABASE_CIRCUIT_BREAKER_THRESHOLD", "5"))
        )
        self.time_window_seconds = (
            time_window_seconds or
            int(os.getenv("SUPABASE_CIRCUIT_BREAKER_TIME_WINDOW", "60"))
        )
        self.cooldown_seconds = (
            cooldown_seconds or
            int(os.getenv("SUPABASE_CIRCUIT_BREAKER_COOLDOWN", "60"))
        )
        
        # State tracking
        self.state = CircuitState.CLOSED
        self.failures: list[float] = []  # List of failure timestamps
        self.last_failure_time: float = 0
        self.last_success_time: float = 0
        self.opened_at: Optional[float] = None
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Circuit breaker enabled flag
        enabled_str = os.getenv("SUPABASE_CIRCUIT_BREAKER_ENABLED", "true")
        self.enabled = enabled_str.lower() == "true"

        if not self.enabled:
            logger.info("Supabase circuit breaker disabled")
    
    def _clean_old_failures(self):
        """Remove failures outside the time window"""
        if not self.enabled:
            return
        
        now = time.time()
        cutoff = now - self.time_window_seconds
        self.failures = [f for f in self.failures if f > cutoff]
    
    def _is_breaker_error(self, error: Exception) -> bool:
        """
        Check if error should trigger circuit breaker.
        Only network/PGRST/timeout errors trigger the breaker.
        """
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        breaker_triggers = [
            'connection',
            'timeout',
            'timed out',
            'network',
            'pgrst',
            'postgres',
            'supabase',
            '502',
            '503',
            '504',
            'service unavailable',
            'bad gateway',
            'gateway timeout',
            'connectionerror',
            'timeouterror',
            'httperror',
        ]

        return (
            any(trigger in error_str for trigger in breaker_triggers) or
            any(trigger in error_type for trigger in breaker_triggers)
        )
    
    def can_execute(self) -> tuple[bool, Optional[str]]:
        """
        Check if operation can be executed.

        Returns:
            (can_execute: bool, reason: Optional[str])
        """
        if not self.enabled:
            return True, None

        with self._lock:
            now = time.time()
            self._clean_old_failures()

            if self.state == CircuitState.CLOSED:
                return True, None

            elif self.state == CircuitState.OPEN:
                # Check if cooldown has expired
                if self.opened_at:
                    elapsed = now - self.opened_at
                    if elapsed >= self.cooldown_seconds:
                        # Cooldown expired - transition to HALF_OPEN
                        logger.info(
                            f"Supabase circuit breaker: HALF_OPEN "
                            f"after {elapsed:.1f}s cooldown"
                        )
                        self.state = CircuitState.HALF_OPEN
                        return True, "half_open_recovery"
                    else:
                        remaining = self.cooldown_seconds - elapsed
                        reason = f"circuit_open_cooldown_{int(remaining)}s"
                        return False, reason
                else:
                    # No opened_at timestamp - allow transition to HALF_OPEN
                    self.state = CircuitState.HALF_OPEN
                    return True, "half_open_reset"

            elif self.state == CircuitState.HALF_OPEN:
                # Allow one attempt in HALF_OPEN
                return True, "half_open_testing"

            # Unknown state - fail open (allow execution)
            return True, None
    
    def record_success(self):
        """Record successful operation"""
        if not self.enabled:
            return

        with self._lock:
            now = time.time()
            self.last_success_time = now

            if self.state == CircuitState.HALF_OPEN:
                # Success in HALF_OPEN - close circuit
                logger.info(
                    "Supabase circuit breaker: Recovery successful, closing"
                )
                self.state = CircuitState.CLOSED
                self.failures = []
                self.opened_at = None
            elif self.state == CircuitState.CLOSED:
                # Success in CLOSED - clean old failures
                self._clean_old_failures()
    
    def record_failure(self, error: Exception):
        """
        Record operation failure.
        Only network/PGRST/timeout errors trigger the breaker.
        """
        if not self.enabled:
            return

        # Only trigger on breaker errors
        if not self._is_breaker_error(error):
            return

        with self._lock:
            now = time.time()
            self.failures.append(now)
            self.last_failure_time = now
            self._clean_old_failures()

            failure_count = len(self.failures)

            if self.state == CircuitState.HALF_OPEN:
                # Failure in HALF_OPEN - reopen circuit
                logger.warning(
                    "Supabase circuit breaker: Reopening after HALF_OPEN failure"
                )
                self.state = CircuitState.OPEN
                self.opened_at = now
            elif self.state == CircuitState.CLOSED:
                # Check if threshold exceeded
                if failure_count >= self.failure_threshold:
                    logger.error(
                        f"Supabase circuit breaker: OPENED after "
                        f"{failure_count} failures (threshold: "
                        f"{self.failure_threshold}) within "
                        f"{self.time_window_seconds}s"
                    )
                    self.state = CircuitState.OPEN
                    self.opened_at = now
    
    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status"""
        if not self.enabled:
            return {
                'enabled': False,
                'state': 'disabled'
            }

        with self._lock:
            self._clean_old_failures()
            failure_count = len(self.failures)

            status = {
                'enabled': True,
                'state': self.state.value,
                'failure_count': failure_count,
                'failure_threshold': self.failure_threshold,
                'time_window_seconds': self.time_window_seconds,
                'cooldown_seconds': self.cooldown_seconds
            }

            if self.state == CircuitState.OPEN and self.opened_at:
                elapsed = time.time() - self.opened_at
                remaining = max(0, self.cooldown_seconds - elapsed)
                status['cooldown_remaining_seconds'] = int(remaining)

            return status
    
    def reset(self):
        """Reset circuit breaker to CLOSED state (for testing/admin)"""
        with self._lock:
            self.state = CircuitState.CLOSED
            self.failures = []
            self.opened_at = None
            self.last_failure_time = 0
            logger.info("Supabase circuit breaker: Manually reset to CLOSED")


# Global circuit breaker instance (shared across all Supabase operations)
_supabase_circuit_breaker: Optional[SupabaseCircuitBreaker] = None
_breaker_lock = threading.Lock()


def get_supabase_circuit_breaker() -> SupabaseCircuitBreaker:
    """Get or create global Supabase circuit breaker instance"""
    global _supabase_circuit_breaker
    
    if _supabase_circuit_breaker is None:
        with _breaker_lock:
            if _supabase_circuit_breaker is None:
                _supabase_circuit_breaker = SupabaseCircuitBreaker()
    
    return _supabase_circuit_breaker


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open"""
    pass
