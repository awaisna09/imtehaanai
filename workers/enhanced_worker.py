"""
Enhanced AI Worker Service
Features: Concurrency control, database caching, batched writes, and isolation
"""

import os
import sys
import signal
import time
import traceback
import asyncio
import threading
import random
import logging
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Configure logger
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

# Load environment variables
load_dotenv('config.env')

# Centralized configuration validation (fail-fast)
try:
    from utils.validate_config import validate_and_exit
    # Validate configuration at startup
    # In production, warnings are treated as errors
    env = os.getenv("ENVIRONMENT", "development").lower()
    fail_on_warnings = env == "production"
    validate_and_exit(fail_on_warnings=fail_on_warnings)
except SystemExit:
    # Re-raise system exit from validation
    raise
except Exception as e:
    print(f"[ERROR] Configuration validation failed: {e}")
    print("   The worker cannot start without valid configuration.")
    sys.exit(1)

# Import enhanced services
from services.redis_connection import get_redis_client, is_redis_available
from services.job_queue import (
    job_queue, JobStatus,
    QUEUE_TUTOR, QUEUE_GRADING, QUEUE_MOCK_EXAM, QUEUE_HELPING, QUEUE_LESSON, QUEUE_MASTERY, QUEUE_ROLLUP
)
from services.db_cache import db_cache
from services.batch_writer import batch_writer, execute_batched_write
from services.structured_logging import structured_logger
from services.observability import observability
from services.worker_health import (
    worker_health_reporter,
    WORKER_HEALTH_UPDATE_INTERVAL
)
from services.crash_logger import log_crash, get_active_jobs_count

# Import metrics service for tracking
try:
    from services.metrics import metrics_service
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    metrics_service = None

# Import circuit breaker for failure protection
try:
    from services.circuit_breaker import CircuitBreaker
    CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    CIRCUIT_BREAKER_AVAILABLE = False
    CircuitBreaker = None

# Supabase client will be obtained via singleton factory

# Configuration - Production Hardening
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
ENABLE_DEBUG = os.getenv("ENABLE_DEBUG", "false").lower() == "true"
# Max concurrent jobs per worker (strict limit)
# UPDATED: Increased default to 3 for better throughput (2-4 range recommended)
# Must be <= MAX_DB_CONNECTIONS to prevent connection pool exhaustion
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", 3))
# Max DB connection pool size (strict limit)
# UPDATED: Increased default to 5 to support higher concurrency
# Must exceed WORKER_CONCURRENCY to handle job variations (each job may need 1-2 connections)
MAX_DB_CONNECTIONS = int(os.getenv("MAX_DB_CONNECTIONS", 5))
# Queue poll timeout in seconds
WORKER_POLL_TIMEOUT = int(os.getenv("WORKER_POLL_TIMEOUT", 5))

# Redis connection retry configuration
# Base delay in seconds
REDIS_RETRY_BASE_DELAY = float(os.getenv("REDIS_RETRY_BASE_DELAY", "2.0"))
# Max delay in seconds
REDIS_RETRY_MAX_DELAY = float(os.getenv("REDIS_RETRY_MAX_DELAY", "60.0"))
# 0 = infinite retries
REDIS_RETRY_MAX_ATTEMPTS = int(os.getenv("REDIS_RETRY_MAX_ATTEMPTS", "0"))
# Check Redis every N seconds
REDIS_HEALTH_CHECK_INTERVAL = int(
    os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "10")
)

# Conservative retry policies (production hardening)
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))  # Conservative: max 3 retries
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 60))  # Base delay in seconds
JOB_TIMEOUT = int(os.getenv("JOB_TIMEOUT", 3600))  # 1 hour max per job
STRICT_TIMEOUT_ENFORCEMENT = (
    os.getenv("STRICT_TIMEOUT_ENFORCEMENT", "true").lower() == "true"
)
JOB_TIMEOUT_WARNING = int(os.getenv("JOB_TIMEOUT_WARNING", 1800))  # Warn at 30 min
MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", 10))

# Queue back-pressure
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", 10000))
# 80% capacity
QUEUE_BACK_PRESSURE_THRESHOLD = float(
    os.getenv("QUEUE_BACK_PRESSURE_THRESHOLD", "0.8")
)
# 1 second delay
QUEUE_BACK_PRESSURE_DELAY = float(
    os.getenv("QUEUE_BACK_PRESSURE_DELAY", "1.0")
)

# Initialize Supabase client (for batched writes) - singleton
try:
    from services.supabase_client import get_supabase_client
    supabase_client = get_supabase_client()
except Exception as e:
    print(f"⚠️ Failed to initialize Supabase client: {e}")
    supabase_client = None

# Set up batch writer handler
if supabase_client:
    batch_writer.set_write_handler(
        lambda table, writes: execute_batched_write(table, writes, supabase_client)
    )
    batch_writer.start_periodic_flush()

# Import AI agents and workflows (lazy loading)
AI_WORKFLOWS_AVAILABLE = False
try:
    from langgraph_tutor import run_tutor_graph
    from agents.mock_exam_grading_agent import run_mock_exam_graph
    AI_WORKFLOWS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ AI workflows not available: {e}")


class EnhancedAIWorker:
    """
    Enhanced AI Worker with concurrency control, caching, and batched writes
    """

    def __init__(self, worker_id: str = None, queues: List[str] = None, max_concurrency: int = None):
        self.worker_id = worker_id or f"worker-{os.getpid()}"
        self.queues = queues or [QUEUE_TUTOR, QUEUE_GRADING, QUEUE_MOCK_EXAM, QUEUE_HELPING, QUEUE_LESSON, QUEUE_MASTERY, QUEUE_ROLLUP]
        self.max_concurrency = max_concurrency or WORKER_CONCURRENCY

        # MEMORY SAFETY: Detect memory-triggered restart and log memory
        try:
            from services.memory_monitor import log_memory_usage, get_memory_usage, reset_peak_memory
            
            # Reset peak memory after restart
            reset_peak_memory()
            
            # Get current memory and threshold
            memory_info = get_memory_usage()
            threshold_mb = float(os.getenv("MEMORY_THRESHOLD_MB", "500"))
            memory_mb = memory_info.get("memory_rss_mb", 0)
            
            # Log memory on initialization (includes restarts)
            log_memory_usage(
                service_name=self.worker_id,
                reason="startup",  # Will be "restart" if detected
                context={
                    "memory_threshold_mb": threshold_mb,
                    "memory_mb": memory_mb,
                    "threshold_exceeded": memory_mb > threshold_mb
                }
            )
        except ImportError:
            pass  # Memory monitor not available
        except Exception as e:
            # Non-blocking: log error but don't fail initialization
            if ENABLE_DEBUG:
                print(f"⚠️ Failed to log memory on initialization: {e}")

        # State management
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self.active_jobs = 0  # Currently processing jobs
        self.timed_out_jobs = 0  # Jobs that exceeded timeout
        self.consecutive_failures = 0  # Track consecutive failures for graceful degradation
        
        # Rollup processing thread (for batch_writer rollup queue)
        self.rollup_thread: Optional[threading.Thread] = None
        self.last_rollup_process = 0
        
        # Worker throttling state
        self.jobs_pulled_this_loop = 0
        self.max_jobs_per_loop = int(os.getenv("WORKER_MAX_JOBS_PER_LOOP", "1"))
        self.db_error_count = 0
        self.db_error_window_start = time.time()
        self.db_error_pause_until = 0  # Timestamp when to resume after DB errors
        self.db_error_threshold = int(os.getenv("WORKER_DB_ERROR_THRESHOLD", "5"))
        self.db_error_pause_min = int(os.getenv("WORKER_DB_ERROR_PAUSE_MIN", "30"))
        self.db_error_pause_max = int(os.getenv("WORKER_DB_ERROR_PAUSE_MAX", "60"))
        
        # Per-job-type rate limiting
        self.job_type_rate_limits: Dict[str, Dict[str, Any]] = {}
        self.rate_limit_window = 60  # 60 second window

        # Worker heartbeat for tracking active workers
        self.heartbeat = None

        # Thread pool for concurrent job processing
        self.executor = ThreadPoolExecutor(max_workers=self.max_concurrency, thread_name_prefix=f"worker-{self.worker_id}")

        # Locks for thread safety
        self.stats_lock = threading.Lock()
        self.job_lock = threading.Lock()

        # Agent instances (lazy loading)
        self.tutor_agent = None
        self.grading_agent = None
        self.mock_exam_agent = None
        self.helping_agent = None

        # Database connection pool reuse
        self.db_cache_hits = 0
        self.db_cache_misses = 0

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Redis connection state
        self.redis_available = False
        self.redis_connection_attempts = 0
        self.last_redis_check = 0
        self.redis_retry_delay = REDIS_RETRY_BASE_DELAY

        # Health reporting state
        self.last_health_report = 0

        print(f"🚀 Enhanced AI Worker initialized: {self.worker_id}")
        print(f"📋 Monitoring queues: {self.queues}")
        print(f"⚙️ Max concurrency: {self.max_concurrency}")
        print("💾 DB caching enabled: True")
        print("📦 Batched writes enabled: True")
        print(f"🔄 Redis retry: base_delay={REDIS_RETRY_BASE_DELAY}s, max_delay={REDIS_RETRY_MAX_DELAY}s")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False
        # Unregister worker heartbeat
        if self.heartbeat:
            self.heartbeat.stop_heartbeat()
            self.heartbeat.unregister()
        # Stop batch writer
        batch_writer.stop_periodic_flush()
        # Wait for active jobs to complete
        self.executor.shutdown(wait=True, timeout=30)

    def _initialize_agents(self):
        """Lazy initialization of AI agents"""
        if not AI_WORKFLOWS_AVAILABLE:
            return

        # Get OpenAI API key (required for agents)
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            print("⚠️ OPENAI_API_KEY not found - agents will not be initialized")
            return

        try:
            # Initialize grading agent if not already done
            if self.grading_agent is None:
                from agents.answer_grading_agent import AnswerGradingAgent
                self.grading_agent = AnswerGradingAgent(api_key=api_key)
                print("✅ Grading agent initialized")

            # Initialize mock exam agent if not already done
            if self.mock_exam_agent is None:
                from agents.mock_exam_grading_agent import MockExamGradingAgent
                self.mock_exam_agent = MockExamGradingAgent(api_key=api_key)
                print("✅ Mock exam grading agent initialized")

            # Initialize helping agent if not already done
            if self.helping_agent is None:
                from agents.helping_agent import HelpingAgent
                self.helping_agent = HelpingAgent(api_key=api_key)
                print("✅ Helping agent initialized")

        except Exception as e:
            print(f"⚠️ Failed to initialize agents: {e}")

    def _can_process_job(self) -> bool:
        """
        Check if worker can process another job (strict concurrency control)
        Enforces: concurrency limit, database connection availability
        (conservative: 2x active jobs)
        """
        with self.job_lock:
            # Check concurrency limit (primary constraint)
            if self.active_jobs >= self.max_concurrency:
                return False

            # Check database connection availability (conservative: assume 2 connections per job)
            # Prevents connection pool exhaustion under sustained load
            estimated_connections_needed = (self.active_jobs + 1) * 2
            if estimated_connections_needed > MAX_DB_CONNECTIONS:
                return False

            return True

    def _increment_active_jobs(self):
        """Increment active job counter"""
        with self.job_lock:
            self.active_jobs += 1

    def _decrement_active_jobs(self):
        """Decrement active job counter"""
        with self.job_lock:
            self.active_jobs = max(0, self.active_jobs - 1)

    def _update_stats(self, success: bool = True):
        """Update worker statistics"""
        with self.stats_lock:
            if success:
                self.processed_count += 1
            else:
                self.error_count += 1

    def _wait_for_redis_with_retry(self) -> bool:
        """
        Wait for Redis connection with exponential backoff and jitter
        Returns True when Redis is available, False if max attempts reached
        (only if max_attempts > 0)
        Never exits the worker process - always returns to allow retry loop
        """
        attempt = 0

        while not self.running:
            # If worker is shutting down, stop retrying
            return False

        while True:
            attempt += 1
            self.redis_connection_attempts = attempt

            # Check if Redis is available (non-fatal check)
            try:
                available = is_redis_available()

                if available:
                    # Redis is available - reset retry delay and log success
                    if not self.redis_available:
                        # First successful connection after failure
                        structured_logger.log_redis_connectivity(
                            event="connection_restored",
                            available=True,
                            worker_id=self.worker_id,
                            attempts=attempt
                        )
                        print(
                            f"✅ Redis connection restored after "
                            f"{attempt} attempt(s)"
                        )

                    self.redis_available = True
                    self.redis_retry_delay = REDIS_RETRY_BASE_DELAY  # Reset delay
                    return True
                else:
                    # Redis is not available - log and continue retrying
                    self.redis_available = False
                    error_msg = "Redis connection check returned False"

                    # Log connection attempt failure
                    structured_logger.log_redis_connectivity(
                        event="connection_attempt_failed",
                        available=False,
                        error=error_msg,
                        worker_id=self.worker_id,
                        attempt=attempt,
                        retry_delay=self.redis_retry_delay
                    )

                    if attempt == 1:
                        # First attempt - log initial failure
                        print(f"⚠️ Redis not available, starting retry loop (attempt {attempt})...")
                        structured_logger.log_redis_connectivity(
                            event="connection_lost",
                            available=False,
                            error=error_msg,
                            worker_id=self.worker_id
                        )
                    else:
                        print(
                            f"⚠️ Redis still unavailable (attempt {attempt}), "
                            f"retrying in {self.redis_retry_delay:.1f}s..."
                        )

            except Exception as e:
                # Redis check failed with exception - log and continue retrying
                error_msg = str(e)
                self.redis_available = False

                # Log connection attempt failure
                structured_logger.log_redis_connectivity(
                    event="connection_attempt_failed",
                    available=False,
                    error=error_msg,
                    worker_id=self.worker_id,
                    attempt=attempt,
                    retry_delay=self.redis_retry_delay
                )

                if attempt == 1:
                    # First attempt - log initial failure
                    print(f"⚠️ Redis not available, starting retry loop (attempt {attempt})...")
                    structured_logger.log_redis_connectivity(
                        event="connection_lost",
                        available=False,
                        error=error_msg,
                        worker_id=self.worker_id
                    )
                else:
                    print(f"⚠️ Redis still unavailable (attempt {attempt}), retrying in {self.redis_retry_delay:.1f}s...")

            # Check if we've exceeded max attempts (only if max_attempts > 0)
            if REDIS_RETRY_MAX_ATTEMPTS > 0 and attempt >= REDIS_RETRY_MAX_ATTEMPTS:
                structured_logger.log_redis_connectivity(
                    event="max_retry_attempts_reached",
                    available=False,
                    error=(
                        f"Exceeded max retry attempts "
                        f"({REDIS_RETRY_MAX_ATTEMPTS})"
                    ),
                    worker_id=self.worker_id,
                    attempts=attempt
                )
                print(
                    f"⚠️ Max Redis retry attempts "
                    f"({REDIS_RETRY_MAX_ATTEMPTS}) reached, "
                    f"continuing to retry..."
                )
                # Continue retrying anyway - never give up

            # Calculate exponential backoff with jitter
            # Formula: base_delay * (2 ^ attempt) + random jitter (0-25%)
            exp_power = min(attempt - 1, 10)  # Cap at 2^10
            exponential_delay = REDIS_RETRY_BASE_DELAY * (2 ** exp_power)
            exponential_delay = min(exponential_delay, REDIS_RETRY_MAX_DELAY)
            jitter = random.uniform(0, exponential_delay * 0.25)
            self.redis_retry_delay = exponential_delay + jitter

            # Wait before next attempt (check running flag periodically)
            wait_interval = 0.5  # Check every 0.5 seconds
            waited = 0
            while waited < self.redis_retry_delay and self.running:
                time.sleep(wait_interval)
                waited += wait_interval

            if not self.running:
                # Worker is shutting down
                return False

    def _report_health_status(self):
        """
        Report worker health status (lightweight, passive, read-only from API)
        This does not affect worker operation - it's purely for observability
        """
        try:
            # Determine error state (None if healthy)
            error_state = None
            if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                error_state = (
                    f"degraded: {self.consecutive_failures} "
                    f"consecutive failures"
                )
            elif not self.redis_available:
                error_state = "redis_unavailable"

            # Report health (non-blocking, fails silently)
            worker_health_reporter.report_health(
                worker_id=self.worker_id,
                redis_available=self.redis_available,
                active_jobs=self.active_jobs,
                error_state=error_state,
                processed_count=self.processed_count,
                error_count=self.error_count,
                additional_info={
                    "consecutive_failures": self.consecutive_failures,
                    "redis_connection_attempts": self.redis_connection_attempts,
                    "max_concurrency": self.max_concurrency
                }
            )
        except Exception:
            # Health reporting should never crash worker
            # Fail silently - this is passive observability
            pass

    def _check_redis_health(self) -> bool:
        """
        Check Redis health (non-fatal, never raises exceptions)
        Returns True if Redis is available, False otherwise
        """
        try:
            available = is_redis_available()

            # Track state changes
            if available != self.redis_available:
                if available:
                    # Redis just became available
                    structured_logger.log_redis_connectivity(
                        event="connection_restored",
                        available=True,
                        worker_id=self.worker_id,
                        attempts=self.redis_connection_attempts
                    )
                    print("✅ Redis connection restored")
                    self.redis_retry_delay = REDIS_RETRY_BASE_DELAY  # Reset delay
                else:
                    # Redis just became unavailable
                    structured_logger.log_redis_connectivity(
                        event="connection_lost",
                        available=False,
                        worker_id=self.worker_id
                    )
                    print("⚠️ Redis connection lost, will retry...")

            self.redis_available = available
            return available

        except Exception as e:
            # Non-fatal: log error but don't raise
            if self.redis_available:
                # Was available, now not
                self.redis_available = False
                structured_logger.log_redis_connectivity(
                    event="connection_lost",
                    available=False,
                    error=str(e),
                    worker_id=self.worker_id
                )
                print(f"⚠️ Redis health check failed: {e}")
            return False

    def _cached_db_read(self, table: str, filters: Dict[str, Any], read_func: Callable) -> Any:
        """
        Execute cached database read

        Args:
            table: Table name
            filters: Query filters
            read_func: Function that executes the actual DB read

        Returns:
            Query result (from cache or DB)
        """
        # Try cache first
        cached = db_cache.get(table, filters)
        if cached is not None:
            self.db_cache_hits += 1
            return cached

        # Cache miss, execute read
        self.db_cache_misses += 1
        result = read_func()

        # Cache result
        db_cache.set(table, filters, result)

        return result

    def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single job with strict timeout enforcement and conservative retry policies
        Runs in thread pool with bounded execution time

        Args:
            job_data: Job data from queue

        Returns:
            Job result

        Raises:
            TimeoutError: If job exceeds timeout (strict enforcement)
        
        CRITICAL SCOPING RULE:
        ======================
        Python's scoping rule: If a name is assigned or imported ANYWHERE in a function,
        Python treats it as a LOCAL variable for the ENTIRE function scope.
        
        This means:
        - DO NOT import job_queue inside this function (e.g., in try/except/finally blocks)
        - DO NOT assign to job_queue inside this function
        - ALWAYS use the module-level job_queue import (from line 48-51)
        
        If you add a local import like "from services.job_queue import job_queue" anywhere
        in this function, Python will treat job_queue as local, causing UnboundLocalError
        when it's used before that import statement (e.g., at line 663, 715, etc.).
        
        To catch regressions early, we assert job_queue is not None at the start.
        """
        # PROTECTION: Assert job_queue is available from module-level import
        # This will fail loudly if someone accidentally adds a local import/assignment
        assert job_queue is not None, "job_queue must be imported at module level, not locally in process_job()"
        
        job_id = job_data.get('job_id')
        job_type = job_data.get('job_type')
        data = job_data.get('data', {})
        user_id = data.get('user_id')
        job_timeout = job_data.get('timeout', JOB_TIMEOUT)

        start_time = time.time()

        # Calculate queue wait time (time between job creation and start)
        queue_wait_seconds = None
        created_at = job_data.get('created_at')
        if created_at:
            try:
                from datetime import datetime
                created_dt = datetime.fromisoformat(
                    created_at.replace('Z', '+00:00')
                )
                wait_time = (datetime.utcnow() - created_dt.replace(tzinfo=None)).total_seconds()
                queue_wait_seconds = max(0.0, wait_time)
                
                # METRICS: Track queue wait time (non-blocking, failure-safe)
                if METRICS_AVAILABLE and metrics_service:
                    try:
                        metrics_service.track_queue_wait_time(
                            job_type=job_type,
                            wait_seconds=queue_wait_seconds,
                            job_id=job_id
                        )
                    except Exception as e:
                        # Non-blocking: log but don't fail job
                        print(f"⚠️ Failed to track queue wait time: {e}")
            except (ValueError, TypeError, AttributeError):
                pass  # If created_at parsing fails, skip queue wait tracking

        # CIRCUIT BREAKER: Check if job can be processed (non-blocking, failure-safe)
        circuit_breaker = None
        can_process = True
        circuit_reason = "circuit_breaker_unavailable"
        
        if CIRCUIT_BREAKER_AVAILABLE and CircuitBreaker:
            try:
                circuit_breaker = CircuitBreaker(job_type=job_type)
                can_process, circuit_reason = circuit_breaker.can_process_job()
                
                if not can_process:
                    # Circuit is OPEN or HALF_OPEN limit exceeded - requeue with delay
                    # Use cooldown period as delay (circuit breaker's cooldown_seconds)
                    delay_seconds = circuit_breaker.cooldown_seconds
                    
                    # Log circuit breaker action
                    structured_logger.log_worker_event(
                        event="circuit_breaker_blocked",
                        worker_id=self.worker_id,
                        job_id=job_id,
                        job_type=job_type,
                        reason=circuit_reason,
                        delay_seconds=delay_seconds
                    )
                    
                    print(
                        f"🔌 Circuit breaker {circuit_reason} for {job_type} job {job_id}, "
                        f"requeuing with {delay_seconds}s delay"
                    )
                    
                    # Requeue job with delay (non-blocking, failure-safe)
                    try:
                        # CRITICAL: Clean up processing marker before requeueing
                        # CRITICAL: Processing marker MUST be keyed by job_id, NOT conversation_id
                        # Format: processing:{job_id} - ensures each job has its own lock
                        processing_key = f"{job_queue.processing_prefix}{job_id}"
                        try:
                            marker_set_time = job_queue.redis.get(processing_key)
                            job_queue.redis.delete(processing_key)
                            marker_cleared_time = datetime.utcnow().isoformat()
                            if marker_set_time:
                                logger.info(
                                    f"Processing marker cleared (circuit breaker): job_id={job_id}, "
                                    f"lock_key={processing_key}, set_time={marker_set_time}, "
                                    f"cleared_time={marker_cleared_time}, status=retrying, "
                                    f"key_format=processing:{{job_id}} (per-job lock)"
                                )
                                print(f"[LOCK] Cleared processing marker (circuit breaker): {processing_key} (job_id={job_id})")
                        except Exception as marker_error:
                            logger.warning(f"Failed to cleanup processing marker for circuit breaker requeue: {marker_error}")
                        
                        job_queue.retry_job(job_id, retry_delay=delay_seconds)
                        job_queue.update_job_status(
                            job_id,
                            JobStatus.RETRYING,
                            message=f"Circuit breaker {circuit_reason}, requeued with {delay_seconds}s delay"
                        )
                    except Exception as requeue_error:
                        # If requeue fails, log but don't crash
                        print(f"⚠️ Failed to requeue job {job_id} after circuit breaker block: {requeue_error}")
                        # Mark as failed if requeue fails (this will clean up marker)
                        job_queue.mark_job_failed(
                            job_id,
                            f"Circuit breaker blocked ({circuit_reason}) and requeue failed: {requeue_error}",
                            should_retry=False
                        )
                    
                    # Raise exception to skip processing (non-blocking, graceful)
                    raise Exception(f"Circuit breaker blocked: {circuit_reason}")
                    
            except Exception as circuit_error:
                # Circuit breaker check failed - fail open (allow job to proceed)
                # This ensures circuit breaker doesn't block jobs if it's unavailable
                if "Circuit breaker blocked" in str(circuit_error):
                    # This is our intentional raise - re-raise it
                    raise
                # Otherwise, log and continue (fail open)
                print(f"⚠️ Circuit breaker check failed for {job_type}: {circuit_error}")
                can_process = True  # Fail open
                circuit_breaker = None

        # Structured logging: job queued -> started
        structured_logger.log_job_start(
            job_id=job_id,
            job_type=job_type,
            user_id=user_id,
            queue_name=self._get_queue_name_for_job_type(job_type),
            timeout_seconds=job_timeout,
            queue_wait_seconds=queue_wait_seconds
        )

        # Update job status: pending/queued -> processing
        # CRITICAL: Ensure status transition is consistent: pending -> processing -> completed|failed
        job_queue.update_job_status(job_id, JobStatus.PROCESSING, message='Job processing started')

        # Strict timeout enforcement (production hardening)
        strict_timeout = os.getenv("STRICT_TIMEOUT_ENFORCEMENT", "true").lower() == "true"
        timeout_warning = int(os.getenv("JOB_TIMEOUT_WARNING", 1800))  # Warn at 30 minutes

        # Track if marker was cleaned up (for finally block safety net)
        marker_cleared_in_try = False

        try:
            # Dispatch to appropriate handler with timeout protection
            if strict_timeout:
                # Use Future with timeout for strict enforcement
                from concurrent.futures import TimeoutError as FutureTimeoutError
                future = self.executor.submit(self._dispatch_job_handler, job_type, data)

                try:
                    result = future.result(timeout=job_timeout)
                except FutureTimeoutError:
                    # Job exceeded timeout - cancel and mark as timeout
                    elapsed = time.time() - start_time
                    error_msg = f"Job exceeded timeout of {job_timeout}s (elapsed: {elapsed:.1f}s)"

                    future.cancel()

                    # Mark as timeout (no retry for timeouts - prevents resource exhaustion)
                    # CRITICAL: Use mark_job_failed to ensure processing marker is cleaned up
                    # Status transition: processing -> failed (timeout)
                    job_queue.mark_job_failed(job_id, error_msg, should_retry=False)
                    marker_cleared_in_try = True
                    # Update status to TIMEOUT for clarity (mark_job_failed sets it to FAILED)
                    job_queue.update_job_status(job_id, JobStatus.TIMEOUT, error=error_msg)

                    # Structured logging: timeout (production observability)
                    structured_logger.log_job_timeout(
                        job_id=job_id,
                        job_type=job_type,
                        timeout_seconds=job_timeout,
                        elapsed_seconds=elapsed,
                        user_id=user_id
                    )

                    # Also log as failure for metrics
                    structured_logger.log_job_failure(
                        job_id=job_id,
                        job_type=job_type,
                        error=error_msg,
                        retry_count=job_data.get('retry_count', 0),
                        user_id=user_id,
                        timeout_seconds=job_timeout,
                        elapsed_seconds=elapsed
                    )

                    # Track metrics
                    observability.track_job_processing_time(job_type, elapsed, job_id, success=False)
                    observability.track_job_failure(job_type, error_msg, job_data.get('retry_count', 0), job_id)
                    
                    # CIRCUIT BREAKER: Record timeout failure (non-blocking, failure-safe)
                    if circuit_breaker:
                        try:
                            circuit_breaker.record_failure(
                                error_type="TimeoutError",
                                error_message=error_msg
                            )
                        except Exception as circuit_error:
                            # Non-blocking: log but don't fail job
                            print(f"⚠️ Failed to record circuit breaker timeout for {job_type}: {circuit_error}")
                    
                    # METRICS: Track timeout job execution time (non-blocking, failure-safe)
                    if METRICS_AVAILABLE and metrics_service:
                        try:
                            agent_name_map = {
                                'tutor_chat': 'tutor',
                                'grade_answer': 'grading',
                                'grade_mock_exam': 'mock_exam',
                                'explain_concept': 'helping',
                                'create_lesson': 'lesson'
                            }
                            agent_name = agent_name_map.get(job_type, job_type)
                            metrics_service.track_agent_metric(
                                agent_name=agent_name,
                                metric_name='execution_time',
                                value=elapsed,
                                unit='seconds',
                                metadata={'job_id': job_id, 'job_type': job_type, 'success': False, 'timeout': True}
                            )
                        except Exception as e_metrics:
                            # Non-blocking: log but don't fail job
                            print(f"⚠️ Failed to track timeout job execution time: {e_metrics}")

                    self._update_stats(success=False)
                    # Track timeout (no retry for timeouts)
                    with self.stats_lock:
                        self.timed_out_jobs += 1
                        self.consecutive_failures += 1
                    raise TimeoutError(error_msg)
            else:
                # No strict timeout, but warn if slow (development/testing)
                result = self._dispatch_job_handler(job_type, data)

            # CRITICAL: Mark job as complete and clean up marker - MUST happen before any early returns
            # Status transition: processing -> completed
            # This MUST be in the same function and cannot be skipped by early returns
            elapsed = time.time() - start_time
            
            # CRITICAL: Always call mark_job_complete on success - this updates status and cleans up marker
            # This is the SINGLE POINT where success is handled - no early returns allowed after this
            job_queue.mark_job_complete(job_id, result)
            marker_cleared_in_try = True

            # Log completion with metadata
            user_id = data.get('user_id', 'unknown')
            conversation_id = data.get('conversation_id', 'unknown')
            print(f"[WORKER] Job {job_id} completed successfully - user: {user_id}, conversation: {conversation_id}, elapsed: {elapsed:.1f}s")

            # Warn if job was slow (even if completed)
            if elapsed > timeout_warning:
                structured_logger.log_worker_event(
                    event="job_slow_warning",
                    worker_id=self.worker_id,
                    job_id=job_id,
                    job_type=job_type,
                    elapsed_seconds=elapsed,
                    warning_threshold=timeout_warning
                )

            # Track processing time (separate from request latency)
            observability.track_job_processing_time(
                job_type, elapsed, job_id, success=True,
                queue_wait_seconds=queue_wait_seconds
            )

            # METRICS: Track job execution time per job type (non-blocking, failure-safe)
            if METRICS_AVAILABLE and metrics_service:
                try:
                    # Map job_type to agent name for metrics
                    agent_name_map = {
                        'tutor_chat': 'tutor',
                        'grade_answer': 'grading',
                        'grade_mock_exam': 'mock_exam',
                        'explain_concept': 'helping',
                        'create_lesson': 'lesson'
                    }
                    agent_name = agent_name_map.get(job_type, job_type)
                    metrics_service.track_agent_metric(
                        agent_name=agent_name,
                        metric_name='execution_time',
                        value=elapsed,
                        unit='seconds',
                        metadata={'job_id': job_id, 'job_type': job_type}
                    )
                except Exception as e:
                    # Non-blocking: log but don't fail job
                    print(f"⚠️ Failed to track job execution time: {e}")

            # Structured logging: job complete
            structured_logger.log_job_complete(
                job_id=job_id,
                job_type=job_type,
                duration_seconds=elapsed,
                user_id=user_id
            )

            # CIRCUIT BREAKER: Record success (non-blocking, failure-safe)
            if circuit_breaker:
                try:
                    circuit_breaker.record_success()
                except Exception as e:
                    # Non-blocking: log but don't fail job
                    print(f"⚠️ Failed to record circuit breaker success for {job_type}: {e}")

            self._update_stats(success=True)

            # DB CLIENT LIFECYCLE: Allow loop to yield after job completion
            # This ensures proper connection cleanup and prevents connection buildup
            time.sleep(0.1)  # Allow loop to yield

            return result

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)
            error_trace = traceback.format_exc()

            retry_count = job_data.get('retry_count', 0)

            # Track processing time for failed job
            observability.track_job_processing_time(job_type, elapsed, job_id, success=False)

            # Track failure
            observability.track_job_failure(job_type, error_msg, retry_count, job_id)

            # Structured logging: job failure
            structured_logger.log_job_failure(
                job_id=job_id,
                job_type=job_type,
                error=error_msg,
                retry_count=retry_count,
                user_id=user_id
            )

            if ENVIRONMENT == "development":
                print(f"Traceback: {error_trace}")

            # Conservative retry policy: Only retry if under max retries and not a timeout
            # Timeouts don't retry to prevent resource exhaustion
            max_retries = job_data.get('max_retries', MAX_RETRIES)
            is_timeout = isinstance(e, TimeoutError) or "timeout" in error_msg.lower()
            should_retry = retry_count < max_retries and not is_timeout

            # Status transition: processing -> failed (with optional retry)
            job_queue.mark_job_failed(job_id, error_msg, should_retry=should_retry)
            marker_cleared_in_try = True

            if should_retry:
                # Calculate retry delay with exponential backoff (conservative)
                base_delay = job_data.get('retry_delay', RETRY_DELAY)
                # Max 10 minutes
                max_retry_delay = int(os.getenv("MAX_RETRY_DELAY", 600))
                exponential_backoff = (
                    os.getenv("RETRY_EXPONENTIAL_BACKOFF", "true").lower()
                    == "true"
                )

                if exponential_backoff:
                    retry_delay = min(base_delay * (2 ** retry_count), max_retry_delay)
                else:
                    retry_delay = base_delay

                # Schedule retry with calculated delay
                job_queue.retry_job(job_id, retry_delay=retry_delay)

                # Log retry
                structured_logger.log_job_retry(
                    job_id=job_id,
                    job_type=job_type,
                    retry_count=retry_count + 1,
                    delay_seconds=retry_delay,
                    user_id=user_id,
                    max_retries=max_retries
                )

            self._update_stats(success=False)
            # Track consecutive failures for graceful degradation
            with self.stats_lock:
                if not isinstance(e, TimeoutError):
                    self.consecutive_failures += 1
            raise
        finally:
            # SAFETY NET: Ensure processing marker is ALWAYS cleared, even if mark_job_complete/mark_job_failed fail
            # This is a last resort cleanup to prevent marker leaks and ensure idempotent cleanup
            # CRITICAL: This cleanup must NEVER raise exceptions or crash the worker
            try:
                # Verify job_queue is available (should always be true due to module-level import)
                if job_queue is None:
                    logger.error(f"CRITICAL: job_queue is None in process_job finally block for {job_id}")
                    # Cannot proceed without job_queue, but don't crash - just log and skip cleanup
                # Get job_id (may be None if job_data was invalid)
                elif not job_id:
                    # No job_id means no marker to clean up - skip silently (no logging needed)
                    pass
                else:
                    # Use module-level job_queue import (no local import needed - see docstring)
                    from services.redis_connection import get_redis_client
                    from datetime import datetime
                    
                    # CRITICAL: Processing marker MUST be keyed by job_id, NOT conversation_id or user_id
                    # Format: processing:{job_id} - ensures each job has its own lock
                    processing_key = f"{job_queue.processing_prefix}{job_id}"
                    redis = get_redis_client()
                    
                    if not redis:
                        logger.warning(f"Cannot cleanup processing marker for {job_id}: Redis not available")
                    else:
                        # IDEMPOTENT CLEANUP: Check if marker exists before attempting deletion
                        # This makes cleanup safe to call multiple times
                        try:
                            marker_exists = redis.exists(processing_key)
                            if marker_exists:
                                # Marker still exists - need to clean it up
                                try:
                                    marker_set_time = redis.get(processing_key)
                                    redis.delete(processing_key)
                                    marker_cleared_time = datetime.utcnow().isoformat()
                                    
                                    # Log the cleanup (only if marker actually existed)
                                    if marker_cleared_in_try:
                                        # Marker should have been cleared in try block, but wasn't
                                        logger.warning(
                                            f"Processing marker force-cleared in finally block (should have been cleared earlier): "
                                            f"job_id={job_id}, lock_key={processing_key}, set_time={marker_set_time}, "
                                            f"cleared_time={marker_cleared_time}"
                                        )
                                    else:
                                        # Normal cleanup path (mark_job_complete/mark_job_failed didn't run)
                                        logger.info(
                                            f"Processing marker cleared in finally block (fallback cleanup): "
                                            f"job_id={job_id}, lock_key={processing_key}, set_time={marker_set_time}, "
                                            f"cleared_time={marker_cleared_time}"
                                        )
                                except Exception as delete_error:
                                    # Failed to delete marker - log but don't crash
                                    logger.error(
                                        f"Failed to delete processing marker {processing_key} for {job_id}: {delete_error}"
                                    )
                            else:
                                # Marker already cleared - idempotent: this is fine, no action needed
                                if not marker_cleared_in_try:
                                    # Expected case: marker was cleared by mark_job_complete/mark_job_failed
                                    pass  # No logging needed for normal case
                        except Exception as check_error:
                            # Failed to check if marker exists - log but don't crash
                            logger.error(
                                f"Failed to check processing marker existence for {job_id}: {check_error}"
                            )
            except Exception as finally_cleanup_error:
                # CATCH-ALL: Any exception in cleanup must be caught and logged
                # This ensures the worker never crashes due to cleanup failures
                logger.error(
                    f"CRITICAL: Exception in process_job finally block cleanup for {job_id}: {finally_cleanup_error}",
                    exc_info=True  # Include full traceback for debugging
                )
                # DO NOT re-raise - cleanup failures must not crash the worker
            
            # CRITICAL: Always log that cleanup has completed (or attempted)
            # This provides observability that the finally block executed
            try:
                logger.info(f"process_job cleanup_done=true for job_id={job_id}")
                print(f"[WORKER] Cleanup completed for job {job_id} (cleanup_done=true)")
            except Exception as log_error:
                # Even logging can fail - don't crash, just continue
                pass

    def _dispatch_job_handler(self, job_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch job to appropriate handler (for timeout enforcement)"""
        job_id = data.get('job_id', 'unknown')
        print(f"[DISPATCH] Dispatching job {job_id} of type {job_type}")
        if job_type == 'tutor_chat':
            print(f"[DISPATCH] Calling _process_tutor_job for {job_id}")
            return self._process_tutor_job(data)
        elif job_type == 'grade_answer':
            return self._process_grading_job(data)
        elif job_type == 'grade_mock_exam':
            return self._process_mock_exam_job(data)
        elif job_type == 'explain_concept':
            return self._process_helping_job(data)
        elif job_type == 'create_lesson':
            return self._process_lesson_job(data)
        elif job_type == 'update_mastery':
            return self._process_mastery_update_job(data)
        elif job_type == 'analytics_rollup':
            return self._process_rollup_job(data)
        else:
            raise ValueError(f"Unknown job type: {job_type}")

    def _get_queue_name_for_job_type(self, job_type: str) -> Optional[str]:
        """Get queue name for job type"""
        mapping = {
            'tutor_chat': QUEUE_TUTOR,
            'grade_answer': QUEUE_GRADING,
            'grade_mock_exam': QUEUE_MOCK_EXAM,
            'explain_concept': QUEUE_HELPING,
            'create_lesson': QUEUE_LESSON,
            'update_mastery': QUEUE_MASTERY,
            'analytics_rollup': QUEUE_ROLLUP,
        }
        return mapping.get(job_type)

    def _process_tutor_job(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process tutor chat job with caching"""
        job_id = data.get('job_id', 'unknown')
        print(f"[TUTOR] Starting _process_tutor_job for job {job_id}")
        
        # Use cached subject_id lookup if possible
        topic_id = data.get('topic')
        subject_id = data.get('subject_id')

        if not subject_id and topic_id:
            # Cached DB read for subject_id
            filters = {'topic_id': topic_id}
            subject_data = self._cached_db_read('topics', filters, lambda: None)  # Placeholder
            if subject_data:
                subject_id = subject_data.get('subject_id', 101)
            else:
                subject_id = 101  # Default

        # Execute tutor graph with job_id for instrumentation
        # Note: job_data is passed to _process_tutor_job, but we only have data here
        # The job_id should be passed separately or extracted from context
        print(f"[TUTOR] Calling run_tutor_graph for job {job_id}")
        try:
            result = run_tutor_graph(
                user_id=data['user_id'],
                topic=str(topic_id),
                message=data['message'],
                conversation_id=data.get('conversation_id'),
                explanation_style=data.get('explanation_style', 'default'),
                subject_id=subject_id,
                conversation_history=data.get('conversation_history', []),
                job_id=job_id
            )
            print(f"[TUTOR] run_tutor_graph completed for job {job_id}")
        except Exception as e:
            print(f"[TUTOR] ERROR in run_tutor_graph for job {job_id}: {e}")
            import traceback
            traceback.print_exc()
            raise

        # Batch write conversation history to DB (if needed)
        if result and 'conversation_id' in data:
            batch_writer.enqueue_write(
                'conversations',
                'upsert',
                {
                    'conversation_id': data['conversation_id'],
                    'user_id': data['user_id'],
                    'topic': topic_id,
                    'last_message': data['message'],
                    'updated_at': datetime.utcnow().isoformat()
                }
            )

        return result if isinstance(result, dict) else {'response': str(result)}

    def _process_grading_job(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process answer grading job"""
        self._initialize_agents()
        if not self.grading_agent:
            raise RuntimeError("Grading agent not available")

        # Extract job_id and generate trace_id for instrumentation
        job_id = data.get('job_id')
        import uuid
        trace_id = str(uuid.uuid4()) if not data.get('trace_id') else data.get('trace_id')
        
        result = self.grading_agent.grade_answer(
            question=data['question'],
            model_answer=data['model_answer'],
            student_answer=data['student_answer'],
            user_id=data.get('user_id'),
            max_marks=data.get('max_marks', 10),
            question_id=data.get('question_id'),
            topic_id=data.get('topic_id'),
            topic_name=data.get('topic_name'),
            difficulty_level=data.get('difficulty_level'),
            subject=data.get('subject', 'Business Studies'),
            job_id=job_id,
            trace_id=trace_id
        )

        # Batch write grading result to DB
        if result and data.get('user_id'):
            batch_writer.enqueue_write(
                'grading_results',
                'insert',
                {
                    'user_id': data['user_id'],
                    'question_id': data.get('question_id'),
                    'marks_awarded': result.get('marks_awarded', 0) if isinstance(result, dict) else 0,
                    'percentage_score': result.get('percentage_score', 0) if isinstance(result, dict) else 0,
                    'created_at': datetime.utcnow().isoformat()
                }
            )

        # Convert to dict
        if hasattr(result, 'model_dump'):
            return result.model_dump()
        elif hasattr(result, 'dict'):
            return result.dict()
        return result if isinstance(result, dict) else {'result': str(result)}

    def _process_mock_exam_job(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process mock exam grading job"""
        # KILL SWITCH: Check if mock-exam grading is disabled
        from services.job_kill_switch import job_kill_switch
        if job_kill_switch.is_job_disabled("grade_mock_exam"):
            job_id = data.get('job_id', 'unknown')
            job_kill_switch.log_disabled_job("grade_mock_exam", job_id)
            # Requeue with long delay
            retry_delay = job_kill_switch.get_retry_delay()
            raise RuntimeError(
                f"Mock exam grading disabled via kill switch. "
                f"Will retry after {retry_delay}s"
            )
        
        self._initialize_agents()
        if not self.mock_exam_agent:
            raise RuntimeError("Mock exam grading agent not available")

        # Execute mock exam graph
        report = asyncio.run(run_mock_exam_graph(
            agent=self.mock_exam_agent,
            user_id=data['user_id'],
            attempted_questions=data['attempted_questions'],
            request_id=data.get('request_id'),
            job_id=data.get('job_id'),
            subject=data.get('subject'),
            exam_type=data.get('exam_type')
        ))

        # Batch write exam results to DB
        if report and data.get('user_id'):
            batch_writer.enqueue_write(
                'exam_attempts',
                'insert',
                {
                    'user_id': data['user_id'],
                    'subject': data.get('subject'),
                    'exam_type': data.get('exam_type'),
                    'total_marks': report.total_marks if hasattr(report, 'total_marks') else 0,
                    'marks_obtained': report.marks_obtained if hasattr(report, 'marks_obtained') else 0,
                    'percentage_score': report.percentage_score if hasattr(report, 'percentage_score') else 0,
                    'created_at': datetime.utcnow().isoformat()
                }
            )

        # Convert to dict
        if hasattr(report, 'model_dump'):
            return report.model_dump()
        elif hasattr(report, 'dict'):
            return report.dict()
        return report if isinstance(report, dict) else {'report': str(report)}

    def _process_helping_job(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process helping agent explanation job"""
        self._initialize_agents()
        if not self.helping_agent:
            raise RuntimeError("Helping agent not available")

        # Extract job_id and generate trace_id for instrumentation
        job_id = data.get('job_id')
        import uuid
        trace_id = str(uuid.uuid4()) if not data.get('trace_id') else data.get('trace_id')
        
        explanation = self.helping_agent.explain(
            query=data['query'],
            context=data.get('context'),
            subject=data.get('subject'),
            job_id=job_id,
            trace_id=trace_id
        )

        return {'explanation': explanation, 'success': True}

    def _process_lesson_job(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process lesson creation job"""
        if self.tutor_agent is None:
            from agents.ai_tutor_agent import AITutorAgent
            self.tutor_agent = AITutorAgent()

        services = self.tutor_agent.build_services()
        llm_service = services["llm"]

        lesson_data = llm_service.generate_lesson(
            topic=data['topic'],
            learning_objectives=data['learning_objectives'],
            difficulty_level=data.get('difficulty_level', 'intermediate')
        )

        return {
            'lesson_content': lesson_data.get('lesson_content', ''),
            'key_points': lesson_data.get('key_points', []),
            'practice_questions': lesson_data.get('practice_questions', []),
            'estimated_duration': lesson_data.get('estimated_duration', 30)
        }

    def _process_rollup_job(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process analytics rollup job.
        Calls the rollup handler function from unified_backend.
        """
        try:
            user_id = data.get("user_id")
            if not user_id:
                raise ValueError("user_id is required for rollup job")
            
            # Import rollup handler from unified_backend
            import sys
            import os
            parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            
            from unified_backend import process_rollup_job
            
            # Execute rollup
            result = process_rollup_job(user_id)
            
            return {
                "success": True,
                "result": result,
                "message": "Rollup completed successfully"
            }
        except Exception as e:
            error_msg = f"Rollup job failed: {str(e)}"
            # Record DB error if it's a DB-related error
            error_str = error_msg.lower()
            if any(pattern in error_str for pattern in ['database', 'db', 'supabase', 'connection', 'timeout', '57p01']):
                self._record_db_error()
            if ENABLE_DEBUG:
                print(f"❌ {error_msg}")
                traceback.print_exc()
            return {
                "success": False,
                "error": error_msg
            }
    
    def _is_rate_limited(self, job_type: str, current_time: float) -> bool:
        """
        Check if job type is rate limited.
        
        Args:
            job_type: Job type string
            current_time: Current timestamp
            
        Returns:
            True if rate limited, False otherwise
        """
        # Get rate limit for this job type (default: no limit)
        rate_limit_key = f"WORKER_RATE_LIMIT_{job_type.upper()}"
        rate_limit = int(os.getenv(rate_limit_key, "0"))  # 0 = no limit
        
        if rate_limit <= 0:
            return False  # No rate limit configured
        
        # Get or initialize rate limit tracking for this job type
        if job_type not in self.job_type_rate_limits:
            self.job_type_rate_limits[job_type] = {
                'count': 0,
                'window_start': current_time
            }
        
        rate_data = self.job_type_rate_limits[job_type]
        
        # Reset window if expired
        if current_time - rate_data['window_start'] >= self.rate_limit_window:
            rate_data['count'] = 0
            rate_data['window_start'] = current_time
        
        # Check if limit exceeded
        if rate_data['count'] >= rate_limit:
            return True  # Rate limited
        
        # Increment count
        rate_data['count'] += 1
        return False
    
    def _record_db_error(self):
        """
        Record a DB error for circuit breaker tracking.
        If errors spike (threshold in 60s), pause job pulling.
        """
        self.db_error_count += 1
        current_time = time.time()
        
        # Check if threshold exceeded
        if self.db_error_count >= self.db_error_threshold:
            # Calculate pause duration (random between min and max)
            pause_duration = random.randint(self.db_error_pause_min, self.db_error_pause_max)
            self.db_error_pause_until = current_time + pause_duration
            
            print(
                f"⚠️ DB error spike detected ({self.db_error_count} errors in 60s) - "
                f"pausing job pulling for {pause_duration}s"
            )
            
            # Log structured event
            try:
                structured_logger.log_worker_event(
                    event="db_error_circuit_breaker",
                    worker_id=self.worker_id,
                    db_error_count=self.db_error_count,
                    threshold=self.db_error_threshold,
                    pause_duration=pause_duration
                )
            except Exception:
                pass  # Non-blocking
    
    def _process_batch_writer_rollups(self):
        """
        Process pending rollup jobs from batch_writer queue.
        Called periodically by worker main loop.
        """
        try:
            # Get pending rollups from batch_writer
            rollup_jobs = batch_writer.get_pending_rollups()
            
            if not rollup_jobs:
                return
            
            print(f"📊 Processing {len(rollup_jobs)} rollup job(s) from batch_writer queue")
            
            # Process each rollup job
            for job in rollup_jobs:
                user_id = job.get("user_id")
                if not user_id:
                    continue
                
                try:
                    # Import rollup handler
                    import sys
                    import os
                    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    if parent_dir not in sys.path:
                        sys.path.insert(0, parent_dir)
                    
                    from unified_backend import process_rollup_job
                    
                    # Execute rollup
                    result = process_rollup_job(user_id)
                    print(f"✅ Rollup completed for user {user_id}: {result.get('message', 'success')}")
                except Exception as e:
                    # Log error but continue processing other jobs
                    print(f"❌ Rollup failed for user {user_id}: {e}")
                    # Record DB error if it's a DB-related error
                    error_str = str(e).lower()
                    if any(pattern in error_str for pattern in ['database', 'db', 'supabase', 'connection', 'timeout', '57p01']):
                        self._record_db_error()
                    if ENABLE_DEBUG:
                        traceback.print_exc()
        except Exception as e:
            # Non-blocking: log but don't fail worker
            print(f"⚠️ Error processing batch_writer rollups: {e}")
            # Record DB error if it's a DB-related error
            error_str = str(e).lower()
            if any(pattern in error_str for pattern in ['database', 'db', 'supabase', 'connection', 'timeout', '57p01']):
                self._record_db_error()
            if ENABLE_DEBUG:
                traceback.print_exc()
    
    def _process_mastery_update_job(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process mastery update job (async).
        Applies mastery updates, trends, and weaknesses to database.
        """
        self._initialize_agents()
        
        user_id = data.get('user_id')
        concepts = data.get('concepts', [])
        reasoning_category = data.get('reasoning_category', 'neutral')
        has_misconception = data.get('has_misconception', False)
        max_marks = data.get('max_marks')
        difficulty_level = data.get('difficulty_level')
        topic_id = data.get('topic_id')
        topic_name = data.get('topic_name')
        subject = data.get('subject', 'Business Studies')
        question_id = data.get('question_id')
        concept_deltas = data.get('concept_deltas', {})
        
        if not user_id or not concepts:
            logger.warning(
                f"[MASTERY] Invalid mastery update job: "
                f"user_id={user_id}, concepts={len(concepts) if concepts else 0}"
            )
            return {'success': False, 'error': 'Invalid payload'}
        
        try:
            # Import required services
            from services.supabase_client import get_supabase_client
            from agents.mastery_agent import MasteryAgent
            from agents.answer_grading_agent import SupabaseRepository
            from services.supabase_ops import sb_execute
            from datetime import datetime
            
            supabase_client = get_supabase_client()
            if not supabase_client:
                raise RuntimeError("Supabase client not available")
            
            # Initialize MasteryAgent and SupabaseRepository
            mastery_agent = MasteryAgent(supabase_client=supabase_client)
            repo = SupabaseRepository(supabase_client=supabase_client)
            
            # Build mastery updates list
            # If deltas are pre-computed, use them; otherwise compute
            mastery_updates = []
            trends_batch = []
            weaknesses_batch = []
            
            for concept_id in concepts:
                # Get delta (pre-computed or compute now)
                if concept_id in concept_deltas:
                    delta = concept_deltas[concept_id]
                else:
                    # Compute delta using MasteryEngine
                    from agents.answer_grading_agent import MasteryEngine
                    mastery_engine = MasteryEngine()
                    delta = mastery_engine.compute(
                        reasoning_category,
                        max_marks=max_marks,
                        difficulty_level=difficulty_level
                    )
                
                # Update mastery via repository
                new_mastery = repo.update_mastery(
                    user_id,
                    concept_id,
                    delta,
                    topic_name=topic_name,
                    subject=subject
                )
                
                if new_mastery is not None:
                    mastery_updates.append({
                        "concept_id": concept_id,
                        "delta": delta,
                        "reason": f"{reasoning_category} (marks={max_marks}, diff={difficulty_level})"
                    })
                    
                    trend_entry = {
                        "user_id": user_id,
                        "concept_id": concept_id,
                        "mastery": new_mastery
                    }
                    # Add subject if available
                    if subject and subject.strip():
                        trend_entry["subject"] = subject.strip()
                    trends_batch.append(trend_entry)
                    
                    is_weak = new_mastery < 40 or has_misconception
                    weakness_entry = {
                        "user_id": user_id,
                        "concept_id": concept_id,
                        "is_weak": is_weak
                    }
                    # Add subject if available
                    if subject and subject.strip():
                        weakness_entry["subject"] = subject.strip()
                    weaknesses_batch.append(weakness_entry)
            
            # Write trends and weaknesses
            if trends_batch:
                repo.batch_log_trends(trends_batch)
            
            if weaknesses_batch:
                repo.batch_update_weaknesses(weaknesses_batch)
            
            # Update mastery_states.mastery_micro
            if trends_batch:
                mastery_values = [t["mastery"] for t in trends_batch]
                avg_mastery_micro = sum(mastery_values) / len(mastery_values)
                
                existing_check = sb_execute(
                    supabase_client.table("mastery_states")
                    .select("user_id")
                    .eq("user_id", user_id)
                    .limit(1)
                )
                
                if existing_check.data:
                    sb_execute(
                        supabase_client.table("mastery_states")
                        .update({
                            "mastery_micro": avg_mastery_micro,
                            "updated_at": datetime.now().isoformat()
                        })
                        .eq("user_id", user_id)
                    )
                else:
                    sb_execute(
                        supabase_client.table("mastery_states")
                        .insert({
                            "user_id": user_id,
                            "mastery_concept": 0,
                            "mastery_micro": avg_mastery_micro,
                            "mastery_macro": 0
                        })
                    )
            
            # Invalidate readiness cache
            if mastery_updates and user_id:
                concept_ids_for_invalidation = [u["concept_id"] for u in mastery_updates]
                try:
                    from services.deterministic_cache import (
                        invalidate_cache, CacheOperation
                    )
                    invalidate_cache(
                        CacheOperation.READINESS_ASSESSMENT,
                        user_id,
                        concept_ids_for_invalidation
                    )
                except Exception as e:
                    logger.warning(f"Failed to invalidate cache: {e}")
            
            logger.info(
                f"✅ [MASTERY] Applied mastery updates for user {user_id}: "
                f"{len(mastery_updates)} concepts"
            )
            
            return {
                'success': True,
                'user_id': user_id,
                'concepts_updated': len(mastery_updates),
                'trends_logged': len(trends_batch),
                'weaknesses_updated': len(weaknesses_batch)
            }
            
        except Exception as e:
            # Check if error is due to Supabase circuit breaker or global limit
            error_str = str(e)
            is_circuit_breaker_error = (
                'CircuitBreakerOpenError' in str(type(e).__name__) or
                'circuit breaker' in error_str.lower() or
                'supabase service is temporarily unavailable' in error_str.lower()
            )
            
            is_global_limit_error = (
                'SupabaseGlobalLimitExceeded' in str(type(e).__name__) or
                'global supabase concurrency limit exceeded' in error_str.lower()
            )
            
            if is_circuit_breaker_error:
                logger.warning(
                    f"⚠️ [MASTERY] Supabase circuit breaker is open - "
                    f"job will be retried after cooldown: {e}"
                )
            elif is_global_limit_error:
                logger.warning(
                    f"⚠️ [MASTERY] Global Supabase concurrency limit exceeded - "
                    f"job will be retried with short delay: {e}"
                )
            else:
                logger.error(
                    f"❌ [MASTERY] Error processing mastery update job: {e}",
                    exc_info=True
                )
            
            return {
                'success': False,
                'error': str(e),
                'circuit_breaker_open': is_circuit_breaker_error,
                'global_limit_exceeded': is_global_limit_error
            }

    def run(self):
        """
        Main worker loop with production hardening
        Features: strict concurrency limits, queue back-pressure, health monitoring, graceful degradation
        Resilient Redis connection with automatic retry and recovery
        """
        if not AI_WORKFLOWS_AVAILABLE:
            print("❌ AI workflows not available, cannot start worker")
            return

        # Set running flag before waiting for Redis (so retry loop doesn't exit immediately)
        self.running = True
        
        # RESILIENT REDIS CONNECTION: Wait for Redis with retry (never exit)
        print("🔄 Waiting for Redis connection...")
        max_retry_msg = (
            REDIS_RETRY_MAX_ATTEMPTS if REDIS_RETRY_MAX_ATTEMPTS > 0
            else "infinite"
        )
        structured_logger.log_worker_event(
            event="worker_starting",
            worker_id=self.worker_id,
            redis_retry_enabled=True,
            max_retry_attempts=max_retry_msg
        )

        # Wait for Redis (blocks until available or worker shutdown)
        redis_ready = self._wait_for_redis_with_retry()

        if not redis_ready:
            # Worker is shutting down
            print(
                f"🛑 Worker {self.worker_id} shutting down before "
                f"Redis connection established"
            )
            return

        print("✅ Redis connection established, starting worker loop")

        # Register worker heartbeat
        try:
            from services.worker_heartbeat import WorkerHeartbeat
            self.heartbeat = WorkerHeartbeat(self.worker_id)
            if self.heartbeat.register():
                self.heartbeat.start_heartbeat()
                print(f"✅ Worker {self.worker_id} registered with heartbeat")
            else:
                print(f"⚠️ Failed to register worker heartbeat (continuing anyway)")
        except Exception as e:
            print(f"⚠️ Worker heartbeat error: {e} (continuing anyway)")

        # Validate concurrency vs database connections (production safety check)
        if self.max_concurrency > MAX_DB_CONNECTIONS:
            print(f"⚠️ WARNING: WORKER_CONCURRENCY ({self.max_concurrency}) > MAX_DB_CONNECTIONS ({MAX_DB_CONNECTIONS})")
            print(f"   Reducing concurrency to {MAX_DB_CONNECTIONS} to prevent connection exhaustion")
            self.max_concurrency = MAX_DB_CONNECTIONS

        # self.running is already set to True before waiting for Redis
        self._initialize_agents()

        # Health monitoring and graceful degradation configuration
        health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", 30))
        last_health_check = time.time()
        enable_graceful_degradation = (
            os.getenv("ENABLE_GRACEFUL_DEGRADATION", "true").lower() == "true"
        )
        degradation_threshold = int(
            os.getenv("DEGRADATION_MODE_THRESHOLD", 5)
        )
        consecutive_failures = 0  # Track consecutive failures

        # Log worker start with production hardening details
        structured_logger.log_worker_event(
            event="worker_started",
            worker_id=self.worker_id,
            active_jobs=0,
            processed_count=0,
            error_count=0,
            max_concurrency=self.max_concurrency,
            max_db_connections=MAX_DB_CONNECTIONS,
            job_timeout=JOB_TIMEOUT,
            max_retries=MAX_RETRIES,
            strict_timeout_enforcement=STRICT_TIMEOUT_ENFORCEMENT
        )

        print(f"✅ Hardened Worker {self.worker_id} started")
        print(f"📊 Monitoring {len(self.queues)} queues")
        print(f"⚙️ Max concurrency: {self.max_concurrency} (strict limit)")
        print(f"🔌 Max DB connections: {MAX_DB_CONNECTIONS} (strict limit)")
        print(f"⏱️ Job timeout: {JOB_TIMEOUT}s (strict: {STRICT_TIMEOUT_ENFORCEMENT})")
        print(f"🔄 Max retries: {MAX_RETRIES} (conservative)")
        print(f"🛡️ Graceful degradation: {enable_graceful_degradation}")

        # WATCHDOG: Cleanup stale processing markers on startup
        try:
            print("🧹 Running initial stale processing marker cleanup...")
            self._cleanup_stale_processing_markers()
        except Exception as startup_cleanup_error:
            print(f"⚠️ Initial stale marker cleanup failed: {startup_cleanup_error} (continuing anyway)")

        # Round-robin queue polling
        queue_index = 0

        # MEMORY SAFETY: Periodic memory monitoring
        last_memory_check = 0
        memory_check_interval = int(os.getenv("MEMORY_CHECK_INTERVAL", "300"))  # 5 minutes
        memory_monitoring_enabled = os.getenv("MEMORY_MONITORING_ENABLED", "true").lower() == "true"

        # IDLE TIMEOUT GUARD: Track last DB activity and close connections when idle
        last_idle_check = time.time()
        idle_check_interval = 10.0  # Check every 10 seconds
        idle_timeout_seconds = float(os.getenv("DB_IDLE_TIMEOUT", "60.0"))  # Default 60 seconds

        while self.running:
            try:
                # Periodic Redis health check (non-fatal, never exits worker)
                current_time = time.time()
                if current_time - self.last_redis_check >= REDIS_HEALTH_CHECK_INTERVAL:
                    self._check_redis_health()
                    self.last_redis_check = current_time

                # Periodic health reporting (lightweight, passive)
                if current_time - self.last_health_report >= WORKER_HEALTH_UPDATE_INTERVAL:
                    self._report_health_status()
                    self.last_health_report = current_time

                # PERIODIC CLEANUP: Detect and mark stuck jobs as failed (every 5 minutes)
                if not hasattr(self, 'last_stuck_job_cleanup'):
                    self.last_stuck_job_cleanup = current_time
                stuck_job_cleanup_interval = 300  # 5 minutes
                if current_time - self.last_stuck_job_cleanup >= stuck_job_cleanup_interval:
                    try:
                        self._cleanup_stuck_jobs()
                        self.last_stuck_job_cleanup = current_time
                    except Exception as cleanup_error:
                        # Non-blocking: log but don't fail worker
                        if ENABLE_DEBUG:
                            print(f"⚠️ Stuck job cleanup failed: {cleanup_error}")

                # WATCHDOG: Cleanup stale processing markers (every 60 seconds)
                if not hasattr(self, 'last_stale_marker_cleanup'):
                    self.last_stale_marker_cleanup = current_time
                stale_marker_cleanup_interval = 60  # 60 seconds
                if current_time - self.last_stale_marker_cleanup >= stale_marker_cleanup_interval:
                    try:
                        self._cleanup_stale_processing_markers()
                        self.last_stale_marker_cleanup = current_time
                    except Exception as marker_cleanup_error:
                        # Non-blocking: log but don't fail worker
                        if ENABLE_DEBUG:
                            print(f"⚠️ Stale marker cleanup failed: {marker_cleanup_error}")

                # PERIODIC ROLLUP PROCESSING: Process batch_writer rollup queue (every 60 seconds)
                rollup_process_interval = 60  # 1 minute
                if current_time - self.last_rollup_process >= rollup_process_interval:
                    try:
                        self._process_batch_writer_rollups()
                        self.last_rollup_process = current_time
                    except Exception as rollup_error:
                        # Non-blocking: log but don't fail worker
                        if ENABLE_DEBUG:
                            print(f"⚠️ Batch writer rollup processing failed: {rollup_error}")

                # IDLE TIMEOUT GUARD: Check for idle DB connections and close if idle > 60s
                if current_time - last_idle_check >= idle_check_interval:
                    try:
                        from services.supabase_client import check_idle_timeout, close_db_connections
                        
                        if check_idle_timeout(idle_threshold=idle_timeout_seconds):
                            # Idle for > 60 seconds, close DB connections
                            print(f"⏸️ DB idle for > {idle_timeout_seconds}s, closing connections...")
                            close_db_connections()
                            # Sleep for 5 seconds after closing
                            time.sleep(5)
                            print("✅ DB connections closed (idle timeout guard)")
                    except Exception as idle_error:
                        # Non-blocking: log but don't fail worker
                        if ENABLE_DEBUG:
                            print(f"⚠️ Idle timeout check failed: {idle_error}")
                    
                    last_idle_check = current_time

                # MEMORY SAFETY: Periodic memory check
                if memory_monitoring_enabled and memory_check_interval > 0:
                    if current_time - last_memory_check > memory_check_interval:
                        try:
                            from services.memory_monitor import log_memory_usage, get_memory_usage
                            
                            memory_info = get_memory_usage()
                            threshold_mb = float(os.getenv("MEMORY_THRESHOLD_MB", "500"))
                            memory_mb = memory_info.get("memory_rss_mb", 0)
                            
                            # Log if approaching threshold (80% of limit)
                            if memory_mb > (threshold_mb * 0.8):
                                log_memory_usage(
                                    service_name=self.worker_id,
                                    reason="threshold_warning",
                                    context={
                                        "memory_mb": memory_mb,
                                        "threshold_mb": threshold_mb,
                                        "percent_of_threshold": round((memory_mb / threshold_mb) * 100, 2)
                                    }
                                )
                            
                            last_memory_check = current_time
                        except Exception:
                            # Silently fail - memory monitoring is optional
                            pass

                # If Redis is not available, wait and retry (don't process jobs)
                if not self.redis_available:
                    structured_logger.log_worker_event(
                        event="worker_waiting_for_redis",
                        worker_id=self.worker_id,
                        retry_delay=self.redis_retry_delay
                    )
                    # Use shorter wait interval when Redis is down (check more frequently)
                    time.sleep(min(self.redis_retry_delay, 5.0))
                    # Try to reconnect
                    self._wait_for_redis_with_retry()
                    continue

                # Periodic health check (production hardening)
                if time.time() - last_health_check >= health_check_interval:
                    # Check for graceful degradation conditions
                    if enable_graceful_degradation and consecutive_failures >= degradation_threshold:
                        # Enter degradation mode: reduce activity to protect system
                        structured_logger.log_worker_event(
                            event="degradation_mode",
                            worker_id=self.worker_id,
                            consecutive_failures=consecutive_failures,
                            threshold=degradation_threshold
                        )
                        time.sleep(2.0)  # Longer delay in degradation mode
                        last_health_check = time.time()
                        continue

                    last_health_check = time.time()

                # Reset jobs pulled counter at start of each loop iteration
                self.jobs_pulled_this_loop = 0
                
                # Check concurrency limit before dequeuing (strict enforcement)
                if not self._can_process_job():
                    # Wait a bit if at max concurrency or connection limit
                    time.sleep(0.1)
                    continue

                # Check DB error circuit breaker (pause if DB errors spike)
                current_time = time.time()
                if current_time < self.db_error_pause_until:
                    # Paused due to DB errors - wait
                    remaining_pause = self.db_error_pause_until - current_time
                    if int(remaining_pause) % 10 == 0:  # Log every 10 seconds
                        print(f"⏸️ Worker paused due to DB errors (resume in {int(remaining_pause)}s)")
                    time.sleep(1.0)
                    continue
                
                # Reset DB error count if window expired (60s window)
                if current_time - self.db_error_window_start > 60:
                    self.db_error_count = 0
                    self.db_error_window_start = current_time
                
                # Check job pull limit per loop
                if self.jobs_pulled_this_loop >= self.max_jobs_per_loop:
                    # Reset counter for next loop iteration
                    self.jobs_pulled_this_loop = 0
                    time.sleep(0.1)  # Brief pause before next loop
                    continue
                
                # Poll queues in round-robin fashion
                queue_name = self.queues[queue_index % len(self.queues)]
                queue_index += 1

                # Check queue back-pressure (prevent overload under sustained load)
                # Only check if Redis is available
                try:
                    queue_depth = job_queue.get_queue_length(queue_name)
                except Exception as e:
                    # Redis connection may have been lost
                    error_msg = str(e)
                    if "redis" in error_msg.lower() or "connection" in error_msg.lower():
                        structured_logger.log_redis_connectivity(
                            event="connection_lost_during_operation",
                            available=False,
                            error=error_msg,
                            worker_id=self.worker_id,
                            operation="get_queue_length"
                        )
                        self.redis_available = False
                        time.sleep(1.0)
                        continue
                    else:
                        # Non-Redis error, skip this queue check
                        queue_depth = 0

                back_pressure_threshold = MAX_QUEUE_SIZE * QUEUE_BACK_PRESSURE_THRESHOLD

                if queue_depth >= back_pressure_threshold:
                    # Apply back-pressure: delay before dequeuing to allow system to catch up
                    structured_logger.log_queue_operation(
                        operation="back_pressure_applied",
                        queue_name=queue_name,
                        queue_length=queue_depth,
                        threshold=back_pressure_threshold,
                        worker_id=self.worker_id
                    )
                    time.sleep(QUEUE_BACK_PRESSURE_DELAY)
                    continue

                # Dequeue job (blocking with timeout)
                # Only attempt if Redis is available (checked above)
                try:
                    if ENABLE_DEBUG:
                        print(f"🔄 Worker {self.worker_id} attempting to dequeue from {queue_name} (timeout: {WORKER_POLL_TIMEOUT}s)")
                    job_data = job_queue.dequeue_job(
                        queue_name, timeout=WORKER_POLL_TIMEOUT
                    )
                    if not job_data and ENABLE_DEBUG:
                        print(f"⏳ No job available from {queue_name} after {WORKER_POLL_TIMEOUT}s timeout")
                except Exception as e:
                    # Redis connection may have been lost during dequeue
                    error_msg = str(e)
                    if "redis" in error_msg.lower() or "connection" in error_msg.lower():
                        structured_logger.log_redis_connectivity(
                            event="connection_lost_during_operation",
                            available=False,
                            error=error_msg,
                            worker_id=self.worker_id,
                            operation="dequeue_job"
                        )
                        self.redis_available = False
                        # Will retry on next loop iteration
                        time.sleep(1.0)
                        continue
                    else:
                        # Non-Redis error, log and continue
                        print(f"⚠️ Error dequeuing job: {e}")
                        time.sleep(0.5)
                        continue

                if job_data:
                    # Increment jobs pulled counter
                    self.jobs_pulled_this_loop += 1
                    
                    # Check per-job-type rate limiting
                    job_id = job_data.get('job_id')
                    job_type_str = job_data.get('job_type')
                    
                    # CRITICAL: Skip tutor_enhance jobs - they should only be processed by minimal_tutor_enhance_worker
                    # This prevents re-enqueue loops and workload isolation issues
                    if job_type_str == 'tutor_enhance':
                        logger.warning(
                            f"⚠️ tutor_enhance job {job_id} found in main queue - skipping. "
                            f"tutor_enhance jobs should only be processed by minimal_tutor_enhance_worker. "
                            f"Job will remain in queue but will not be processed by this worker."
                        )
                        # Don't re-enqueue, just skip - let minimal worker handle it
                        continue
                    
                    # Check rate limit for this job type
                    if self._is_rate_limited(job_type_str, current_time):
                        # CRITICAL: Never re-enqueue tutor_chat jobs to prevent loops
                        # tutor_chat is now synchronous by default, async endpoint should be rare
                        if job_type_str == 'tutor_chat':
                            logger.error(
                                f"❌ tutor_chat job {job_id} rate limited - but NOT re-enqueueing to prevent loops. "
                                f"tutor_chat should be synchronous. Marking as failed."
                            )
                            try:
                                job_queue.update_job_status(
                                    job_id,
                                    JobStatus.FAILED,
                                    message="Rate limited but cannot re-enqueue tutor_chat job to prevent loops"
                                )
                            except Exception:
                                pass
                            continue
                        
                        # Rate limited - skip this job, return to queue
                        print(f"⏸️ Job {job_id} ({job_type_str}) rate limited - skipping")
                        # Re-enqueue job (put back at front of queue)
                        try:
                            job_queue.retry_job(job_id, retry_delay=1)
                        except Exception:
                            pass  # Non-blocking
                        continue
                    
                    # CRITICAL: Skip tutor_enhance jobs - they should only be processed by minimal_tutor_enhance_worker
                    # This prevents re-enqueue loops and workload isolation issues
                    if job_type == 'tutor_enhance':
                        logger.warning(
                            f"⚠️ tutor_enhance job {job_id} found in main queue - skipping. "
                            f"tutor_enhance jobs should only be processed by minimal_tutor_enhance_worker. "
                            f"Job will remain in queue but will not be processed by this worker."
                        )
                        # Don't re-enqueue, just skip - let minimal worker handle it
                        continue
                    
                    # Check concurrency limits before starting job
                    # SIMPLIFIED: Use only basic concurrency check to avoid workload isolation bugs

                    # Check if we can process this job (strict concurrency enforcement)
                    with self.job_lock:
                        active_jobs = self.active_jobs
                        concurrency_limit = self.max_concurrency
                        can_process = active_jobs < concurrency_limit

                        # Check database connections (conservative: 2 connections per job)
                        estimated_connections = (active_jobs + 1) * 2
                        db_limit = MAX_DB_CONNECTIONS
                        db_ok = estimated_connections <= db_limit

                        # Final decision: can process only if both checks pass
                        can_start = can_process and db_ok

                        # Unit-level logging for decision tracking
                        decision = "process" if can_start else "requeue"
                        lock_acquired = True  # We have the lock
                        print(f"🔍 Concurrency check: decision={decision}, active_jobs={active_jobs}, limit={concurrency_limit}, db_connections={estimated_connections}/{db_limit}, lock_acquired={lock_acquired}")
                        
                        # Structured logging for concurrency decision
                        try:
                            from services.structured_logging import structured_logger
                            job_data_inner = job_data.get('data', {})
                            correlation_id = job_data_inner.get('correlation_id', 'unknown')
                            reason = None
                            if not can_process:
                                reason = f"Concurrency limit reached ({active_jobs}/{concurrency_limit})"
                            elif not db_ok:
                                reason = f"DB connection limit would be exceeded ({estimated_connections}/{db_limit})"
                            structured_logger.log_concurrency_decision(
                                job_id=job_id,
                                correlation_id=correlation_id,
                                decision=decision,
                                active_jobs=active_jobs,
                                concurrency_limit=concurrency_limit,
                                db_connections=estimated_connections,
                                db_limit=db_limit,
                                reason=reason
                            )
                        except Exception:
                            pass  # Non-critical

                    if not can_start:
                        # CRITICAL: Never re-enqueue tutor-related jobs to prevent loops
                        # tutor_chat jobs are now handled synchronously, tutor_enhance has its own worker
                        if job_type in ['tutor_chat', 'tutor_enhance', 'tutor_enhance']:
                            logger.error(
                                f"❌ Cannot process {job_type} job {job_id} - but NOT re-enqueueing to prevent loops. "
                                f"tutor_chat should be synchronous, tutor_enhance should use minimal worker."
                            )
                            # Mark as failed instead of re-enqueueing
                            try:
                                job_queue.update_job_status(
                                    job_id,
                                    JobStatus.FAILED,
                                    message=f"Worker concurrency limit reached but cannot re-enqueue tutor job (active_jobs={active_jobs}/{concurrency_limit})"
                                )
                            except Exception:
                                pass
                            continue
                        
                        # CRITICAL FIX: Clean up processing marker before re-enqueueing
                        # This prevents stale markers that block job processing
                        # Status transition: processing -> pending (re-enqueued)
                        # CRITICAL: Processing marker MUST be keyed by job_id, NOT conversation_id
                        try:
                            from datetime import datetime
                            # Verify lock key format: processing:{job_id} (NOT conversation_id)
                            processing_key = f"{job_queue.processing_prefix}{job_id}"
                            marker_set_time = job_queue.redis.get(processing_key)
                            job_queue.redis.delete(processing_key)
                            marker_cleared_time = datetime.utcnow().isoformat()
                            
                            if marker_set_time:
                                logger.info(
                                    f"Processing marker cleared (re-enqueue): job_id={job_id}, "
                                    f"lock_key={processing_key}, set_time={marker_set_time}, "
                                    f"cleared_time={marker_cleared_time}, status=pending, "
                                    f"key_format=processing:{{job_id}} (per-job lock)"
                                )
                            else:
                                logger.info(
                                    f"Processing marker cleared (re-enqueue): job_id={job_id}, "
                                    f"lock_key={processing_key}, cleared_time={marker_cleared_time}, "
                                    f"status=pending (marker was missing), "
                                    f"key_format=processing:{{job_id}} (per-job lock)"
                                )
                            print(f"[LOCK] Cleared processing marker for re-enqueued job: {processing_key} (job_id={job_id})")
                        except Exception as cleanup_error:
                            logger.error(f"Failed to cleanup processing marker for re-enqueue: {cleanup_error}")
                            print(f"⚠️ Failed to cleanup processing marker: {cleanup_error}")

                        # Reset job status to pending before re-enqueueing
                        # This ensures frontend doesn't think job is processing when it's actually waiting
                        try:
                            job_queue.update_job_status(
                                job_id,
                                JobStatus.PENDING,
                                message=f"Re-enqueued due to concurrency limit (active_jobs={active_jobs}/{concurrency_limit})"
                            )
                        except Exception as status_error:
                            print(f"⚠️ Failed to reset job status to pending: {status_error}")

                        # Re-enqueue job (put back at front of queue with updated priority)
                        # Only if Redis is available
                        try:
                            # Use workload isolation priority if available
                            try:
                                from services.workload_isolation import workload_isolation, JobType
                                from datetime import datetime
                                
                                job_type_enum = JobType(job_type_str) if job_type_str in [jt.value for jt in JobType] else None
                                
                                if job_type_enum:
                                    created_at_str = job_data.get('created_at')
                                    created_at_dt = None
                                    if created_at_str:
                                        try:
                                            created_at_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                                        except (ValueError, TypeError):
                                            pass
                                    
                                    score = workload_isolation.get_priority_score(job_type_enum, job_id, created_at_dt)
                                else:
                                    # Fallback
                                    priority = job_data.get('priority', 0)
                                    score = priority * 1000000000 + int(time.time() * 1000)
                            except (ImportError, ValueError, AttributeError):
                                # Fallback
                                priority = job_data.get('priority', 0)
                                score = priority * 1000000000 + int(time.time() * 1000)
                            
                            get_redis_client().zadd(
                                queue_name, {job_id: score}
                            )
                        except Exception as e:
                            # Redis connection lost during re-enqueue
                            error_msg = str(e)
                            structured_logger.log_redis_connectivity(
                                event="connection_lost_during_operation",
                                available=False,
                                error=error_msg,
                                worker_id=self.worker_id,
                                operation="re_enqueue_job",
                                job_id=job_data.get('job_id')
                            )
                            self.redis_available = False
                            # Job will be lost, but worker continues
                            job_id_str = job_data.get('job_id')
                            print(
                                f"⚠️ Failed to re-enqueue job "
                                f"{job_id_str}: {e}"
                            )
                        continue

                    # CRITICAL FIX: Mark job as processing IMMEDIATELY after confirming it can start
                    # This ensures frontend knows job is being processed, not stuck in pending
                    # This must happen BEFORE submitting to thread pool to prevent race conditions
                    try:
                        job_queue.update_job_status(
                            job_id,
                            JobStatus.PROCESSING,
                            message='Job started processing'
                        )
                        # Log immediately so frontend can see job is processing
                        if ENABLE_DEBUG:
                            print(f"✅ Job {job_id} marked as PROCESSING and submitted to thread pool")
                    except Exception as status_error:
                        print(f"⚠️ Failed to mark job as processing: {status_error}")
                        # Don't continue if we can't mark as processing - this is critical
                        # Re-enqueue job so it can be retried
                        try:
                            job_queue.update_job_status(job_id, JobStatus.PENDING, message=f"Failed to mark as processing: {status_error}")
                            get_redis_client().zadd(queue_name, {job_id: int(time.time() * 1000)})
                        except:
                            pass
                        continue
                    
                    # Submit job to thread pool
                    self._increment_active_jobs()
                    
                    # Mark job as started in workload isolation
                    try:
                        from services.workload_isolation import workload_isolation, JobType
                        job_type_enum = JobType(job_type_str) if job_type_str in [jt.value for jt in JobType] else None
                        if job_type_enum:
                            workload_isolation.start_job(job_type_enum, job_id)
                    except (ImportError, ValueError, AttributeError):
                        pass  # Non-critical
                    
                    future = self.executor.submit(
                        self._process_job_with_tracking, job_data
                    )

                    # Handle completion asynchronously (non-blocking)
                    def handle_completion(fut):
                        nonlocal consecutive_failures
                        try:
                            self._decrement_active_jobs()
                            
                            # Mark job as completed in workload isolation
                            try:
                                from services.workload_isolation import workload_isolation, JobType
                                job_type_enum = JobType(job_type_str) if job_type_str in [jt.value for jt in JobType] else None
                                if job_type_enum:
                                    # Estimate tokens used (can be improved with actual tracking)
                                    job_chars = workload_isolation.JOB_CHARACTERISTICS.get(job_type_enum)
                                    tokens_used = job_chars.typical_tokens if job_chars else 0
                                    workload_isolation.complete_job(job_type_enum, job_id, tokens_used)
                            except (ImportError, ValueError, AttributeError):
                                pass  # Non-critical
                            
                            result = fut.result()  # Will raise if job failed
                            
                            # CRITICAL FIX: Ensure job is marked as complete IMMEDIATELY when processing finishes
                            # This prevents jobs from getting stuck in processing status and ensures worker is ready for next job
                            try:
                                from services.job_queue import job_queue, JobStatus
                                current_job = job_queue.get_job(job_id)
                                # Mark as complete if still in processing (process_job might have failed to mark it)
                                # OR if status is still pending (edge case where dequeue didn't update status)
                                if current_job:
                                    current_status = current_job.get('status')
                                    if current_status in [JobStatus.PROCESSING, JobStatus.PENDING]:
                                        result_dict = result if isinstance(result, dict) else {'result': result}
                                        job_queue.mark_job_complete(job_id, result_dict)
                                        print(f"✅ Job {job_id} marked as COMPLETED in callback (was {current_status})")
                                        
                                        # CRITICAL: Update stats in callback since process_job runs in thread
                                        # This ensures stats are accurate even if process_job doesn't update them
                                        self._update_stats(success=True)
                                        
                                        # DB CLIENT LIFECYCLE: Allow loop to yield after job completion
                                        # This ensures proper connection cleanup and prevents connection buildup
                                        # Rule: One client per process, not per job - yield after job to allow cleanup
                                        time.sleep(0.1)  # Allow loop to yield
                                        
                                        # Also log job completion for observability
                                        try:
                                            from services.structured_logging import structured_logger
                                            import time
                                            from datetime import datetime
                                            # Calculate duration from created_at
                                            created_at = current_job.get('created_at')
                                            if created_at:
                                                try:
                                                    created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                                    now = datetime.now(created_dt.tzinfo)
                                                    elapsed = (now - created_dt).total_seconds()
                                                except:
                                                    elapsed = 0
                                            else:
                                                elapsed = 0
                                            
                                            user_id = current_job.get('data', {}).get('user_id') if isinstance(current_job.get('data'), dict) else None
                                            structured_logger.log_job_complete(
                                                job_id=job_id,
                                                job_type=job_type_str,
                                                duration_seconds=elapsed,
                                                user_id=user_id
                                            )
                                        except Exception as log_error:
                                            # Non-blocking: log but don't fail
                                            print(f"⚠️ Failed to log job completion: {log_error}")
                                    elif current_status == JobStatus.COMPLETED:
                                        # Already completed by process_job, that's fine
                                        print(f"✅ Job {job_id} already marked as COMPLETED by process_job")
                                        # Still update stats in case process_job didn't
                                        self._update_stats(success=True)
                            except Exception as complete_error:
                                # Non-blocking: log but don't fail
                                print(f"⚠️ Failed to mark job complete in callback: {complete_error}")
                            
                            # Reset consecutive failures on success
                            # (graceful degradation recovery)
                            consecutive_failures = 0
                        except Exception as e:
                            # Error already logged in process_job, but ensure job status is updated
                            # This is a safety net for jobs that might get stuck in processing
                            try:
                                from services.job_queue import job_queue, JobStatus
                                # Check if job is still in processing status
                                current_job = job_queue.get_job(job_id)
                                if current_job and current_job.get('status') == JobStatus.PROCESSING:
                                    # Job is stuck in processing - mark as failed
                                    error_msg = str(e) if e else "Unknown error during job processing"
                                    job_queue.mark_job_failed(
                                        job_id,
                                        f"Job processing failed in thread: {error_msg}",
                                        should_retry=False
                                    )
                            except Exception as update_error:
                                # Non-blocking: log but don't fail
                                print(f"⚠️ Failed to update stuck job {job_id} in completion handler: {update_error}")
                            
                            # Mark job as completed even on failure (cleanup)
                            try:
                                from services.workload_isolation import workload_isolation, JobType
                                job_type_enum = JobType(job_type_str) if job_type_str in [jt.value for jt in JobType] else None
                                if job_type_enum:
                                    workload_isolation.complete_job(job_type_enum, job_id, 0)
                            except (ImportError, ValueError, AttributeError):
                                pass  # Non-critical
                            
                            # Track consecutive failures for graceful degradation
                            consecutive_failures += 1
                            # Update worker-level failure tracking
                            with self.stats_lock:
                                self.consecutive_failures = consecutive_failures

                    future.add_done_callback(handle_completion)
                    
                    # CRITICAL FIX: Continue polling immediately after submitting job to thread pool
                    # This ensures worker is ready to process the next job as soon as capacity is available
                    # Don't wait for current job to complete - worker can handle multiple jobs concurrently
                    continue
                else:
                    # No job available, small sleep to prevent CPU spinning
                    time.sleep(0.1)

                # Print stats every 100 iterations
                if (self.processed_count + self.error_count) % 100 == 0:
                    self._print_stats()

            except KeyboardInterrupt:
                print("\n🛑 Keyboard interrupt received")
                self.running = False
                break
            except Exception as e:
                # Check if this is a fatal exception that should crash the worker
                fatal_exceptions = (MemoryError, SystemError, SystemExit)
                is_fatal = isinstance(e, fatal_exceptions) or not self.running
                
                if is_fatal:
                    # Log fatal crash before exiting
                    context = {
                        'active_jobs': self.active_jobs,
                        'worker_id': self.worker_id,
                        'processed_count': self.processed_count,
                        'error_count': self.error_count,
                        'consecutive_failures': self.consecutive_failures,
                        'redis_available': self.redis_available
                    }
                    log_crash('worker', e, context)
                    raise  # Re-raise fatal exception
                
                # Non-fatal exception - continue processing
                consecutive_failures += 1
                # Update worker-level failure tracking
                with self.stats_lock:
                    self.consecutive_failures = consecutive_failures
                print(f"❌ Worker error: {e}")
                traceback.print_exc()
                # Longer pause on errors to prevent error loops
                time.sleep(1)

        # Shutdown gracefully
        print(f"\n🛑 Worker {self.worker_id} shutting down...")
        # Unregister worker heartbeat
        if self.heartbeat:
            self.heartbeat.stop_heartbeat()
            self.heartbeat.unregister()
        print(f"⏳ Waiting for {self.active_jobs} active jobs to complete...")

        # Report final health status (worker is stopping)
        try:
            worker_health_reporter.report_health(
                worker_id=self.worker_id,
                redis_available=self.redis_available,
                active_jobs=self.active_jobs,
                error_state="shutting_down",
                processed_count=self.processed_count,
                error_count=self.error_count
            )
        except Exception:
            pass  # Health reporting should never block shutdown

        # Log worker shutdown with Redis state
        structured_logger.log_worker_event(
            event="worker_shutting_down",
            worker_id=self.worker_id,
            active_jobs=self.active_jobs,
            processed_count=self.processed_count,
            error_count=self.error_count,
            redis_available=self.redis_available,
            redis_connection_attempts=self.redis_connection_attempts
        )

        # Wait for all jobs to complete
        self.executor.shutdown(wait=True, timeout=60)

        # Flush remaining batched writes
        batch_writer.flush_all()

        self._print_stats()

        # Log worker stopped
        with self.stats_lock:
            final_timed_out = self.timed_out_jobs
            final_failures = self.consecutive_failures

        structured_logger.log_worker_event(
            event="worker_stopped",
            worker_id=self.worker_id,
            active_jobs=0,
            processed_count=self.processed_count,
            error_count=self.error_count,
            timed_out_jobs=final_timed_out,
            consecutive_failures=final_failures,
            redis_available=self.redis_available,
            redis_connection_attempts=self.redis_connection_attempts
        )

        print(f"✅ Hardened Worker {self.worker_id} stopped")
        print(f"📊 Final stats: Processed={self.processed_count}, Errors={self.error_count}, Timeouts={final_timed_out}, Consecutive Failures={final_failures}")
        print(f"📊 Redis stats: Available={self.redis_available}, Connection Attempts={self.redis_connection_attempts}")

    def _process_job_with_tracking(self, job_data: Dict[str, Any]):
        """Wrapper to track job processing with guaranteed marker cleanup"""
        job_id = job_data.get('job_id')
        processing_marker_cleared = False
        
        try:
            return self.process_job(job_data)
        except Exception as e:
            # Ensure job status is updated even if process_job fails to catch the exception
            # This is a safety net for jobs that might get stuck in processing status
            if job_id:
                try:
                    from services.job_queue import job_queue, JobStatus
                    # Check if job is still in processing status
                    current_job = job_queue.get_job(job_id)
                    if current_job and current_job.get('status') == JobStatus.PROCESSING:
                        # Job is stuck in processing - mark as failed (this will clean up marker)
                        job_queue.mark_job_failed(
                            job_id,
                            f"Job processing failed: {str(e)}",
                            should_retry=False
                        )
                        processing_marker_cleared = True
                except Exception as update_error:
                    # Non-blocking: log but don't fail
                    print(f"⚠️ Failed to update stuck job {job_id}: {update_error}")
            raise  # Re-raise the original exception
        finally:
            # SAFETY NET: Ensure processing marker is cleared even if mark_job_failed/mark_job_complete fail
            # This is a last resort cleanup to prevent marker leaks
            if job_id and not processing_marker_cleared:
                try:
                    from services.job_queue import job_queue
                    from services.redis_connection import get_redis_client
                    from datetime import datetime
                    
                    processing_key = f"{job_queue.processing_prefix}{job_id}"
                    redis = get_redis_client()
                    
                    # Check if marker still exists
                    marker_exists = redis.exists(processing_key)
                    if marker_exists:
                        marker_set_time = redis.get(processing_key)
                        redis.delete(processing_key)
                        marker_cleared_time = datetime.utcnow().isoformat()
                        logger.warning(
                            f"Processing marker force-cleared in finally block: job_id={job_id}, "
                            f"lock_key={processing_key}, set_time={marker_set_time}, "
                            f"cleared_time={marker_cleared_time}"
                        )
                except Exception as finally_cleanup_error:
                    # Last resort - log but don't fail
                    logger.error(f"Failed to cleanup processing marker in finally block for {job_id}: {finally_cleanup_error}")
            
            # Ensure active job counter is decremented even on exception
            # (Handled in handle_completion callback, but this is a safety net)

    def _cleanup_stuck_jobs(self):
        """Periodically cleanup jobs stuck in processing status"""
        try:
            from services.job_queue import job_queue, JobStatus
            from services.redis_connection import get_redis_client
            import json
            from datetime import datetime, timezone
            
            redis = get_redis_client()
            if not redis:
                return
            
            # Find jobs stuck in processing for more than 2 hours (increased from 1 hour)
            # This prevents cleanup from marking jobs that are legitimately taking longer
            cutoff_time = datetime.now(timezone.utc).timestamp() - 7200  # 2 hours ago
            stuck_count = 0
            
            for key in redis.keys('job:*'):
                try:
                    data = redis.get(key)
                    if data:
                        job = json.loads(data)
                        status = job.get('status', 'unknown')
                        job_id = job.get('job_id')
                        updated_at = job.get('updated_at', job.get('created_at', ''))
                        
                        if status == JobStatus.PROCESSING and job_id:
                            try:
                                # Parse updated_at timestamp
                                if updated_at:
                                    updated_dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                                    updated_ts = updated_dt.timestamp()
                                    
                                    # If job hasn't been updated in over 2 hours, mark as stuck
                                    if updated_ts < cutoff_time:
                                        job_queue.mark_job_failed(
                                            job_id,
                                            "Job stuck in processing status for over 2 hours - marked as failed by worker cleanup",
                                            should_retry=False
                                        )
                                        stuck_count += 1
                            except Exception as parse_error:
                                # Skip jobs with invalid timestamps
                                continue
                except Exception:
                    # Skip jobs that can't be parsed
                    continue
            
            if stuck_count > 0:
                print(f"🧹 Cleaned up {stuck_count} stuck job(s)")
        except Exception as e:
            # Non-blocking: log but don't fail
            if ENABLE_DEBUG:
                print(f"⚠️ Stuck job cleanup error: {e}")

    def _cleanup_stale_processing_markers(self):
        """
        Periodically cleanup stale processing markers (watchdog)
        Detects and removes processing markers that are:
        - Older than job timeout + buffer
        - Associated with jobs that no longer exist
        - Associated with jobs that are not in processing status
        """
        try:
            from services.job_queue import job_queue, JobStatus, JOB_TIMEOUT
            from services.redis_connection import get_redis_client
            import json
            import re
            from datetime import datetime, timezone
            
            redis = get_redis_client()
            if not redis:
                return
            
            # TTL buffer: add 60 seconds buffer to job timeout to account for processing delays
            ttl_buffer = 60  # seconds
            max_age_seconds = JOB_TIMEOUT + ttl_buffer
            
            # Expected pattern for processing marker value: ISO timestamp string
            # Example: "2026-01-24T18:49:59.908426"
            timestamp_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$')
            
            cleaned_count = 0
            current_time = datetime.now(timezone.utc)
            
            # Scan all processing markers
            processing_keys = redis.keys('processing:*')
            
            for processing_key in processing_keys:
                try:
                    # Extract job_id from key (format: "processing:job_id")
                    job_id = processing_key.replace('processing:', '')
                    
                    # Get marker value and TTL
                    marker_value = redis.get(processing_key)
                    marker_ttl = redis.ttl(processing_key)
                    
                    # SAFEGUARD: Only process markers with expected timestamp pattern
                    if not marker_value or not timestamp_pattern.match(marker_value):
                        # Skip markers that don't match expected pattern (safety check)
                        if ENABLE_DEBUG:
                            print(f"⚠️ Skipping processing marker {processing_key} - value doesn't match expected pattern")
                        continue
                    
                    # Check if marker is stale (TTL expired or negative)
                    is_stale_by_ttl = marker_ttl <= 0
                    
                    # Check if marker is older than max_age
                    is_stale_by_age = False
                    try:
                        marker_timestamp = datetime.fromisoformat(marker_value.replace('Z', '+00:00'))
                        age_seconds = (current_time - marker_timestamp.replace(tzinfo=timezone.utc)).total_seconds()
                        is_stale_by_age = age_seconds > max_age_seconds
                    except (ValueError, TypeError):
                        # Invalid timestamp format - consider stale
                        is_stale_by_age = True
                    
                    # Check if job exists and is in valid processing state
                    job_exists = False
                    job_in_processing = False
                    try:
                        job_key = f"job:{job_id}"
                        job_data = redis.get(job_key)
                        if job_data:
                            job_exists = True
                            job = json.loads(job_data)
                            job_status = job.get('status', 'unknown')
                            job_in_processing = job_status == JobStatus.PROCESSING
                    except Exception:
                        # Job doesn't exist or can't be parsed
                        pass
                    
                    # Delete marker if:
                    # 1. TTL expired (marker should have been auto-deleted but wasn't)
                    # 2. Marker is older than max_age
                    # 3. Job doesn't exist
                    # 4. Job exists but is not in processing status (job completed/failed but marker remains)
                    should_delete = (
                        is_stale_by_ttl or
                        is_stale_by_age or
                        not job_exists or
                        (job_exists and not job_in_processing)
                    )
                    
                    if should_delete:
                        reason = []
                        if is_stale_by_ttl:
                            reason.append("TTL expired")
                        if is_stale_by_age:
                            reason.append(f"age > {max_age_seconds}s")
                        if not job_exists:
                            reason.append("job missing")
                        if job_exists and not job_in_processing:
                            reason.append("job not processing")
                        
                        redis.delete(processing_key)
                        cleaned_count += 1
                        print(f"🧹 Cleaned up stale processing marker: job_id={job_id}, lock_key={processing_key}, reason={', '.join(reason)}")
                        
                except Exception as marker_error:
                    # Skip markers that can't be processed
                    if ENABLE_DEBUG:
                        print(f"⚠️ Error processing marker {processing_key}: {marker_error}")
                    continue
            
            if cleaned_count > 0:
                print(f"✅ Stale marker cleanup: removed {cleaned_count} stale processing marker(s)")
            elif ENABLE_DEBUG:
                print(f"✅ Stale marker cleanup: no stale markers found (checked {len(processing_keys)} markers)")
                
        except Exception as e:
            # Non-blocking: log but don't fail worker
            print(f"⚠️ Stale processing marker cleanup error: {e}")
            if ENABLE_DEBUG:
                import traceback
                traceback.print_exc()

    def _print_stats(self):
        """Print worker statistics with production hardening metrics"""
        cache_hit_rate = 0.0
        if self.db_cache_hits + self.db_cache_misses > 0:
            cache_hit_rate = self.db_cache_hits / (self.db_cache_hits + self.db_cache_misses) * 100

        with self.stats_lock:
            current_failures = self.consecutive_failures
            current_timed_out = self.timed_out_jobs

        print(
            f"📊 Stats: Processed={self.processed_count}, Errors={self.error_count}, "
            f"Timeouts={current_timed_out}, Active={self.active_jobs}/{self.max_concurrency}, "
            f"Cache Hit Rate={cache_hit_rate:.1f}%, "
            f"Consecutive Failures={current_failures}"
        )


def main():
    """Entry point for enhanced worker process"""
    import argparse

    parser = argparse.ArgumentParser(description='Enhanced AI Job Worker')
    parser.add_argument('--worker-id', type=str, help='Worker identifier')
    parser.add_argument(
        '--queues', nargs='+',
        choices=['tutor', 'grading', 'mock_exam', 'helping', 'lesson'],
        default=['tutor', 'grading', 'mock_exam', 'helping', 'lesson'],
        help='Queues to monitor'
    )
    parser.add_argument(
        '--concurrency', type=int, default=WORKER_CONCURRENCY,
        help='Max concurrent jobs per worker'
    )

    args = parser.parse_args()

    # Map queue names
    queue_map = {
        'tutor': QUEUE_TUTOR,
        'grading': QUEUE_GRADING,
        'mock_exam': QUEUE_MOCK_EXAM,
        'helping': QUEUE_HELPING,
        'lesson': QUEUE_LESSON
    }
    queues = [queue_map[q] for q in args.queues]

    # MEMORY SAFETY: Track worker start time for uptime calculation
    worker_start_time = time.time()

    # Check worker limit before starting
    try:
        from services.worker_heartbeat import can_start_worker
        worker_id = args.worker_id or f"worker-{os.getpid()}"
        can_start, reason = can_start_worker(worker_id)
        if not can_start:
            print(reason)
            sys.exit(1)
        print(f"✅ {reason}")
    except Exception as e:
        print(f"⚠️ Worker limit check failed: {e} (continuing anyway)")

    # Create and run enhanced worker with crash handling
    worker = None
    try:
        worker = EnhancedAIWorker(
            worker_id=args.worker_id,
            queues=queues,
            max_concurrency=args.concurrency
        )
        worker.run()
    except KeyboardInterrupt:
        print("\n⚠️ Worker interrupted by user")
        if worker:
            worker.running = False
            # Unregister worker heartbeat
            if worker.heartbeat:
                worker.heartbeat.stop_heartbeat()
                worker.heartbeat.unregister()
        
        # METRICS: Track worker shutdown (non-blocking, failure-safe)
        if METRICS_AVAILABLE and metrics_service:
            try:
                uptime = time.time() - worker_start_time
                worker_id = worker.worker_id if worker else (args.worker_id or f"worker-{os.getpid()}")
                metrics_service.track_worker_restart(
                    worker_id=worker_id,
                    restart_reason="manual_shutdown",
                    uptime_seconds=uptime,
                    metadata={"interrupted": True}
                )
            except Exception:
                pass  # Non-blocking
        
        # MEMORY SAFETY: Log memory on shutdown
        try:
            from services.memory_monitor import log_memory_usage, get_peak_memory
            uptime = time.time() - worker_start_time
            peak = get_peak_memory()
            log_memory_usage(
                service_name=worker.worker_id if worker else (args.worker_id or f"worker-{os.getpid()}"),
                reason="shutdown",
                context={
                    "uptime_seconds": uptime,
                    "peak_memory_mb": peak.get("peak_memory_mb", 0)
                }
            )
        except Exception:
            pass
    except Exception as e:
        # Unregister worker heartbeat on crash
        if worker and worker.heartbeat:
            try:
                worker.heartbeat.stop_heartbeat()
                worker.heartbeat.unregister()
            except Exception:
                pass  # Non-blocking
        # Log fatal crash
        context = {}
        if worker:
            context['active_jobs'] = get_active_jobs_count(worker)
            context['worker_id'] = worker.worker_id
            context['processed_count'] = worker.processed_count
            context['error_count'] = worker.error_count
            context['consecutive_failures'] = worker.consecutive_failures
        
        log_crash('worker', e, context)
        
        # METRICS: Track worker crash restart (non-blocking, failure-safe)
        if METRICS_AVAILABLE and metrics_service:
            try:
                uptime = time.time() - worker_start_time
                worker_id = worker.worker_id if worker else (args.worker_id or f"worker-{os.getpid()}")
                metrics_service.track_worker_restart(
                    worker_id=worker_id,
                    restart_reason="crash",
                    uptime_seconds=uptime,
                    metadata={"error": str(e), "error_type": type(e).__name__}
                )
            except Exception:
                pass  # Non-blocking
        
        # MEMORY SAFETY: Log memory on crash
        try:
            from services.memory_monitor import log_memory_usage, get_peak_memory
            uptime = time.time() - worker_start_time
            peak = get_peak_memory()
            log_memory_usage(
                service_name=worker.worker_id if worker else (args.worker_id or f"worker-{os.getpid()}"),
                reason="crash",
                context={
                    "uptime_seconds": uptime,
                    "peak_memory_mb": peak.get("peak_memory_mb", 0),
                    "error": str(e)
                }
            )
        except Exception:
            pass
        
        raise  # Re-raise to exit with error code


if __name__ == '__main__':
    main()
