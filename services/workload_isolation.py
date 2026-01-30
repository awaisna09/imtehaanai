"""
Workload Isolation Service
Features:
- Job-type specific concurrency limits
- Queue prioritization
- AI provider rate limit protection
- Job starvation prevention (fair scheduling)
"""

import os
import time
import threading
from typing import Dict, Optional, Set, Tuple
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from dotenv import load_dotenv

load_dotenv('config.env')

# Job type definitions
class JobType(str, Enum):
    TUTOR_CHAT = "tutor_chat"
    TUTOR_ENHANCE = "tutor_enhance"  # Bypasses workload isolation - uses minimal worker
    GRADE_ANSWER = "grade_answer"
    GRADE_MOCK_EXAM = "grade_mock_exam"
    EXPLAIN_CONCEPT = "explain_concept"
    CREATE_LESSON = "create_lesson"
    UPDATE_MASTERY = "update_mastery"

# Job characteristics (resource usage patterns)
@dataclass
class JobCharacteristics:
    """Characteristics of a job type"""
    # Typical execution time in seconds
    typical_duration: float
    # Typical AI calls per job
    typical_ai_calls: int
    # Typical tokens per job (estimated)
    typical_tokens: int
    # Is this a long-running job?
    is_long_running: bool
    # Priority class (higher = more important)
    priority_class: int
    # Base concurrency limit for this job type
    base_concurrency_limit: int

# Job type characteristics (based on audit report)
JOB_CHARACTERISTICS = {
    JobType.TUTOR_CHAT: JobCharacteristics(
        typical_duration=30.0,  # ~15-45s average
        typical_ai_calls=3,  # Multiple LLM calls in pipeline
        typical_tokens=2000,
        is_long_running=True,
        priority_class=2,  # Medium priority
        base_concurrency_limit=2
    ),
    JobType.GRADE_ANSWER: JobCharacteristics(
        typical_duration=3.5,  # ~2-5s average
        typical_ai_calls=1,  # Single comprehensive LLM call
        typical_tokens=1500,
        is_long_running=False,
        priority_class=3,  # Higher priority (faster turnaround)
        base_concurrency_limit=4
    ),
    JobType.GRADE_MOCK_EXAM: JobCharacteristics(
        typical_duration=90.0,  # ~30-120s for 10-20 questions
        typical_ai_calls=10,  # One per question (estimated)
        typical_tokens=15000,  # High token usage
        is_long_running=True,
        priority_class=1,  # Lower priority (can wait longer)
        base_concurrency_limit=1  # Very conservative (long-running)
    ),
    JobType.EXPLAIN_CONCEPT: JobCharacteristics(
        typical_duration=2.0,  # ~1-3s average
        typical_ai_calls=1,
        typical_tokens=200,
        is_long_running=False,
        priority_class=4,  # Highest priority (fastest turnaround)
        base_concurrency_limit=5
    ),
    JobType.CREATE_LESSON: JobCharacteristics(
        typical_duration=20.0,  # ~20s average
        typical_ai_calls=1,
        typical_tokens=3000,
        is_long_running=False,
        priority_class=2,  # Medium priority
        base_concurrency_limit=3
    ),
    JobType.UPDATE_MASTERY: JobCharacteristics(
        typical_duration=3.0,  # ~2-5s average (DB writes)
        typical_ai_calls=0,  # No AI calls, just DB writes
        typical_tokens=0,
        is_long_running=False,
        priority_class=3,  # Higher priority (user-facing data)
        base_concurrency_limit=4
    ),
}

# Configuration from environment
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", 3))
MAX_DB_CONNECTIONS = int(os.getenv("MAX_DB_CONNECTIONS", 10))

# Job-type specific concurrency limits (can override base limits)
JOB_TYPE_CONCURRENCY_LIMITS = {
    JobType.TUTOR_CHAT: int(os.getenv("JOB_CONCURRENCY_TUTOR_CHAT", "2")),
    JobType.GRADE_ANSWER: int(os.getenv("JOB_CONCURRENCY_GRADE_ANSWER", "4")),
    JobType.GRADE_MOCK_EXAM: int(os.getenv("JOB_CONCURRENCY_GRADE_MOCK_EXAM", "1")),
    JobType.EXPLAIN_CONCEPT: int(os.getenv("JOB_CONCURRENCY_EXPLAIN_CONCEPT", "5")),
    JobType.CREATE_LESSON: int(os.getenv("JOB_CONCURRENCY_CREATE_LESSON", "3")),
    JobType.UPDATE_MASTERY: int(os.getenv("JOB_CONCURRENCY_UPDATE_MASTERY", "4")),
}

# AI Provider Rate Limit Configuration
# OpenAI rate limits (per minute, per model)
# These are conservative estimates - adjust based on your actual plan
OPENAI_RATE_LIMIT_RPM = int(os.getenv("OPENAI_RATE_LIMIT_RPM", "500"))  # Requests per minute
OPENAI_RATE_LIMIT_TPM = int(os.getenv("OPENAI_RATE_LIMIT_TPM", "200000"))  # Tokens per minute

# Rate limit tracking window (seconds)
RATE_LIMIT_WINDOW = 60  # 1 minute

# Job starvation prevention
# Maximum time a short job can wait before being prioritized
SHORT_JOB_MAX_WAIT_SECONDS = int(os.getenv("SHORT_JOB_MAX_WAIT_SECONDS", "30"))
# Jobs with duration < this are considered "short"
SHORT_JOB_DURATION_THRESHOLD = int(os.getenv("SHORT_JOB_DURATION_THRESHOLD", "5"))


class WorkloadIsolation:
    """
    Manages workload isolation with job-type specific limits,
    queue prioritization, rate limit protection, and starvation prevention
    """
    
    def __init__(self):
        self.lock = threading.Lock()
        
        # Track active jobs per job type
        self.active_jobs: Dict[JobType, Set[str]] = defaultdict(set)
        
        # Track job start times (for starvation detection)
        self.job_start_times: Dict[str, float] = {}
        
        # AI provider rate limit tracking
        self.ai_requests_timeline: list = []  # List of (timestamp, tokens) tuples
        self.ai_tokens_timeline: list = []  # List of (timestamp, tokens) tuples
        
        # Job wait time tracking (for starvation detection)
        self.job_wait_times: Dict[str, float] = {}  # job_id -> wait_start_time
        
        # Statistics
        self.stats = {
            'total_jobs_started': 0,
            'total_jobs_completed': 0,
            'rate_limit_hits': 0,
            'starvation_preventions': 0,
            'concurrency_limit_hits': defaultdict(int),
        }
    
    def can_start_job(
        self, job_type: JobType, job_id: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if a job can start based on:
        1. Job-type specific concurrency limit
        2. Global worker concurrency limit
        3. AI provider rate limits
        4. Database connection availability
        
        Returns:
            (can_start: bool, reason: Optional[str])
        """
        with self.lock:
            # CRITICAL: tutor_enhance jobs bypass workload isolation completely
            # They are processed by minimal_tutor_enhance_worker which has no concurrency limits
            # Always return True for tutor_enhance to prevent re-enqueue loops
            if job_type == JobType.TUTOR_ENHANCE:
                return True, "tutor_enhance bypasses workload isolation"
            
            # Check job-type specific concurrency limit
            job_limit = JOB_TYPE_CONCURRENCY_LIMITS.get(job_type, 1)
            active_count = len(self.active_jobs[job_type])
            
            if active_count >= job_limit:
                self.stats['concurrency_limit_hits'][job_type.value] += 1
                return False, f"Job-type concurrency limit reached ({active_count}/{job_limit})"
            
            # Check global worker concurrency limit
            total_active = sum(len(jobs) for jobs in self.active_jobs.values())
            if total_active >= WORKER_CONCURRENCY:
                return False, f"Global concurrency limit reached ({total_active}/{WORKER_CONCURRENCY})"
            
            # Check AI provider rate limits
            if not self._check_ai_rate_limits(job_type):
                self.stats['rate_limit_hits'] += 1
                return False, "AI provider rate limit would be exceeded"
            
            # Check database connection availability
            # (conservative estimate: 2 connections per job)
            estimated_connections = (total_active + 1) * 2
            if estimated_connections > MAX_DB_CONNECTIONS:
                return (
                    False,
                    f"Database connection pool would be exhausted "
                    f"({estimated_connections}/{MAX_DB_CONNECTIONS})"
                )
            
            # All checks passed
            return True, None
    
    def start_job(self, job_type: JobType, job_id: str):
        """Mark a job as started"""
        with self.lock:
            self.active_jobs[job_type].add(job_id)
            self.job_start_times[job_id] = time.time()
            self.stats['total_jobs_started'] += 1
            
            # Remove from wait tracking
            if job_id in self.job_wait_times:
                del self.job_wait_times[job_id]
    
    def complete_job(self, job_type: JobType, job_id: str, tokens_used: int = 0):
        """Mark a job as completed and update rate limit tracking"""
        with self.lock:
            if job_id in self.active_jobs[job_type]:
                self.active_jobs[job_type].remove(job_id)
            
            if job_id in self.job_start_times:
                del self.job_start_times[job_id]
            
            if job_id in self.job_wait_times:
                del self.job_wait_times[job_id]
            
            self.stats['total_jobs_completed'] += 1
            
            # Track AI usage for rate limiting
            if tokens_used > 0:
                now = time.time()
                self.ai_requests_timeline.append((now, 1))
                self.ai_tokens_timeline.append((now, tokens_used))
                
                # Clean up old entries (keep only last window)
                cutoff = now - RATE_LIMIT_WINDOW
                self.ai_requests_timeline = [
                    (ts, count) for ts, count in self.ai_requests_timeline
                    if ts > cutoff
                ]
                self.ai_tokens_timeline = [
                    (ts, tokens) for ts, tokens in self.ai_tokens_timeline
                    if ts > cutoff
                ]
    
    def _check_ai_rate_limits(self, job_type: JobType) -> bool:
        """
        Check if starting this job would exceed AI provider rate limits
        
        Returns:
            True if within limits, False if would exceed
        """
        now = time.time()
        cutoff = now - RATE_LIMIT_WINDOW
        
        # Count requests in window
        recent_requests = sum(
            count for ts, count in self.ai_requests_timeline
            if ts > cutoff
        )
        
        # Estimate tokens for this job
        job_chars = JOB_CHARACTERISTICS.get(job_type)
        estimated_tokens = job_chars.typical_tokens if job_chars else 1000
        
        # Count tokens in window
        recent_tokens = sum(
            tokens for ts, tokens in self.ai_tokens_timeline
            if ts > cutoff
        )
        
        # Check limits
        if recent_requests + 1 > OPENAI_RATE_LIMIT_RPM:
            return False
        
        if recent_tokens + estimated_tokens > OPENAI_RATE_LIMIT_TPM:
            return False
        
        return True
    
    def should_prioritize_job(self, job_type: JobType, job_id: str, wait_start_time: Optional[float] = None) -> bool:
        """
        Determine if a job should be prioritized to prevent starvation
        
        Returns:
            True if job should be prioritized (short job waiting too long)
        """
        job_chars = JOB_CHARACTERISTICS.get(job_type)
        if not job_chars:
            return False
        
        # Only prioritize short jobs
        if job_chars.is_long_running:
            return False
        
        # Check wait time
        if wait_start_time is None:
            wait_start_time = self.job_wait_times.get(job_id, time.time())
        
        wait_duration = time.time() - wait_start_time
        
        if wait_duration > SHORT_JOB_MAX_WAIT_SECONDS:
            self.stats['starvation_preventions'] += 1
            return True
        
        return False
    
    def track_job_wait(self, job_type: JobType, job_id: str):
        """Track when a job starts waiting (for starvation detection)"""
        with self.lock:
            if job_id not in self.job_wait_times:
                self.job_wait_times[job_id] = time.time()
    
    def get_priority_score(self, job_type: JobType, job_id: str, created_at: Optional[datetime] = None) -> float:
        """
        Calculate priority score for a job (lower = higher priority)
        
        Factors:
        1. Job priority class (from characteristics)
        2. Wait time (starvation prevention)
        3. Age (FIFO within same priority)
        """
        job_chars = JOB_CHARACTERISTICS.get(job_type)
        if not job_chars:
            return 0.0
        
        # Base priority from job class (higher class = lower score = higher priority)
        base_score = job_chars.priority_class * 1000000
        
        # Boost priority if job is starving (short job waiting too long)
        wait_start = self.job_wait_times.get(job_id)
        if wait_start:
            wait_duration = time.time() - wait_start
            if wait_duration > SHORT_JOB_MAX_WAIT_SECONDS:
                # Boost priority significantly
                base_score -= 5000000  # Large boost
        
        # Add age component (FIFO within same priority)
        if created_at:
            age_seconds = (datetime.utcnow() - created_at).total_seconds()
            base_score += age_seconds * 1000  # Age in milliseconds
        
        return base_score
    
    def get_stats(self) -> Dict:
        """Get workload isolation statistics"""
        with self.lock:
            active_counts = {
                job_type.value: len(jobs)
                for job_type, jobs in self.active_jobs.items()
            }
            
            return {
                'active_jobs_by_type': active_counts,
                'total_active_jobs': sum(len(jobs) for jobs in self.active_jobs.values()),
                'stats': self.stats.copy(),
                'rate_limit_status': {
                    'recent_requests': len([
                        ts for ts, _ in self.ai_requests_timeline
                        if ts > time.time() - RATE_LIMIT_WINDOW
                    ]),
                    'recent_tokens': sum(
                        tokens for ts, tokens in self.ai_tokens_timeline
                        if ts > time.time() - RATE_LIMIT_WINDOW
                    ),
                    'limit_rpm': OPENAI_RATE_LIMIT_RPM,
                    'limit_tpm': OPENAI_RATE_LIMIT_TPM,
                },
            }


# Global instance
workload_isolation = WorkloadIsolation()
