"""
Enhanced Job Queue Service
Features: Retries, timeouts, failure handling, and idempotent job design
"""

import json
import os
import hashlib
import time
import random
import logging
from typing import Dict, Optional, Any, List
from datetime import datetime, timedelta
from uuid import uuid4
from dotenv import load_dotenv

from services.redis_connection import get_redis_client, is_redis_available

logger = logging.getLogger(__name__)

load_dotenv('config.env')

# Queue configuration from environment
JOB_RESULT_TTL = int(os.getenv("JOB_RESULT_TTL", 86400))  # 24 hours default
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 60))  # seconds
JOB_TIMEOUT = int(os.getenv("JOB_TIMEOUT", 180))  # 3 minutes default (reduced from 1 hour for faster failure)
ENABLE_IDEMPOTENCY = os.getenv("ENABLE_IDEMPOTENCY", "true").lower() == "true"
IDEMPOTENCY_WINDOW = int(os.getenv("IDEMPOTENCY_WINDOW", 3600))  # 1 hour default

# Back-pressure configuration
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", 10000))  # Max jobs per queue
QUEUE_FULL_POLICY = os.getenv("QUEUE_FULL_POLICY", "reject")  # reject, wait, or drop_oldest

# Queue names
QUEUE_TUTOR = 'jobs:tutor'
QUEUE_GRADING = 'jobs:grading'
QUEUE_MOCK_EXAM = 'jobs:mock_exam'
QUEUE_HELPING = 'jobs:helping'
QUEUE_LESSON = 'jobs:lesson'
QUEUE_MASTERY = 'jobs:mastery'
QUEUE_ROLLUP = 'jobs:rollup'

# Queue priorities
PRIORITY_HIGH = -1000000
PRIORITY_NORMAL = 0
PRIORITY_LOW = 1000000


class JobStatus:
    """Job status constants"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class JobQueue:
    """Enhanced Redis-based job queue with retries, timeouts, and idempotency"""
    
    def __init__(self):
        self.redis = get_redis_client()
        self.job_prefix = "job:"
        self.idempotency_prefix = "idempotency:"
        # CRITICAL: Processing marker prefix - MUST be combined with job_id only
        # Format: processing:{job_id} - NOT conversation_id or user_id
        # This ensures each job has its own lock, preventing "first ok, second stuck" issues
        # where multiple jobs in the same conversation would share a lock
        self.processing_prefix = "processing:"
        
        # Lua script for atomic queue size enforcement
        # Returns: [status, queue_size, dropped_job_id]
        # status: "enqueued" | "rejected" | "dropped_oldest"
        # queue_size: current queue size after operation
        # dropped_job_id: job ID that was dropped (if policy is drop_oldest), nil otherwise
        # This script ensures queue size cannot exceed MAX_QUEUE_SIZE even under concurrent enqueue attempts
        try:
            self._atomic_enqueue_script = self.redis.register_script("""
                local queue_name = KEYS[1]
                local job_id = ARGV[1]
                local score = tonumber(ARGV[2])
                local max_queue_size = tonumber(ARGV[3])
                local policy = ARGV[4]
                
                -- Get current queue size (atomic operation)
                local current_size = redis.call('ZCARD', queue_name)
                
                -- If queue has space, enqueue directly (atomic: check and add in single operation)
                if current_size < max_queue_size then
                    redis.call('ZADD', queue_name, score, job_id)
                    return {'enqueued', current_size + 1, nil}
                end
                
                -- Queue is full - handle according to policy
                if policy == 'reject' then
                    -- Reject: return error status without enqueueing (queue size unchanged)
                    return {'rejected', current_size, nil}
                elseif policy == 'drop_oldest' then
                    -- Drop oldest: remove lowest priority job (highest score = oldest/lowest priority)
                    -- ZRANGE with -1, -1 gets the element with highest score (lowest priority)
                    local oldest = redis.call('ZRANGE', queue_name, -1, -1)
                    if #oldest > 0 then
                        local dropped_job_id = oldest[1]
                        redis.call('ZREM', queue_name, dropped_job_id)
                        -- Now enqueue the new job (atomic: remove and add in single operation)
                        redis.call('ZADD', queue_name, score, job_id)
                        -- Queue size remains the same (dropped one, added one)
                        return {'dropped_oldest', current_size, dropped_job_id}
                    else
                        -- Queue was empty (shouldn't happen since current_size >= max_queue_size, but handle gracefully)
                        redis.call('ZADD', queue_name, score, job_id)
                        return {'enqueued', 1, nil}
                    end
                else
                    -- Unknown policy - reject to be safe
                    return {'rejected', current_size, nil}
                end
            """)
        except Exception as e:
            # If script registration fails, we'll handle it in enqueue_job
            print(f"⚠️ Failed to register atomic enqueue script: {e}")
            self._atomic_enqueue_script = None
    
    def _generate_idempotency_key(self, job_type: str, job_data: Dict[str, Any]) -> str:
        """Generate idempotency key from job type and data"""
        # Sort data for consistent hashing
        sorted_data = json.dumps(job_data, sort_keys=True, default=str)
        # Create hash of job type + data
        hash_input = f"{job_type}:{sorted_data}".encode('utf-8')
        return hashlib.sha256(hash_input).hexdigest()
    
    def _check_idempotency(self, idempotency_key: str) -> Optional[str]:
        """Check if job with same idempotency key exists and is recent"""
        if not ENABLE_IDEMPOTENCY:
            return None
        
        try:
            key = f"{self.idempotency_prefix}{idempotency_key}"
            existing_job_id = self.redis.get(key)
            
            if existing_job_id:
                # Check if existing job is still valid
                job_data = self.get_job(existing_job_id)
                if job_data:
                    status = job_data.get('status')
                    # Return existing job if it's pending, processing, or completed
                    if status in [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.COMPLETED]:
                        return existing_job_id
                    # Check if completed job is within idempotency window
                    if status == JobStatus.COMPLETED:
                        completed_at = job_data.get('completed_at')
                        if completed_at:
                            try:
                                completed_dt = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
                                age = (datetime.utcnow() - completed_dt.replace(tzinfo=None)).total_seconds()
                                if age < IDEMPOTENCY_WINDOW:
                                    return existing_job_id
                            except (ValueError, TypeError):
                                pass
            
            return None
        except Exception as e:
            print(f"⚠️ Error checking idempotency: {e}")
            return None
    
    def _store_idempotency_key(self, idempotency_key: str, job_id: str):
        """Store idempotency key mapping"""
        if not ENABLE_IDEMPOTENCY:
            return
        
        try:
            key = f"{self.idempotency_prefix}{idempotency_key}"
            # Store with TTL matching idempotency window
            self.redis.setex(key, IDEMPOTENCY_WINDOW, job_id)
        except Exception as e:
            print(f"⚠️ Error storing idempotency key: {e}")
    
    def enqueue_job(
        self,
        queue_name: str,
        job_type: str,
        job_data: Dict[str, Any],
        priority: int = PRIORITY_NORMAL,
        max_retries: Optional[int] = None,
        retry_delay: Optional[int] = None,
        timeout: Optional[int] = None,
        idempotency_key: Optional[str] = None
    ) -> str:
        """
        Enqueue a job to Redis queue with idempotency support and atomic queue size enforcement
        
        Args:
            queue_name: Queue name (e.g., QUEUE_TUTOR, QUEUE_GRADING)
            job_type: Type of job ('tutor_chat', 'grade_answer', etc.)
            job_data: Job payload data
            priority: Job priority (lower = higher priority)
            max_retries: Maximum retry attempts (default: MAX_RETRIES from env)
            retry_delay: Delay between retries in seconds (default: RETRY_DELAY from env)
            timeout: Job timeout in seconds (default: JOB_TIMEOUT from env)
            idempotency_key: Optional custom idempotency key (auto-generated if None)
        
        Returns:
            job_id: Unique job identifier (or existing job_id if duplicate)
        
        Raises:
            QueueFullException: If queue is full and policy is 'reject'
        """
        # Generate idempotency key if not provided (check before enqueueing to avoid wasting queue space)
        if idempotency_key is None:
            idempotency_key = self._generate_idempotency_key(job_type, job_data)
        
        # Check for existing job (idempotency) - do this BEFORE atomic enqueue
        existing_job_id = self._check_idempotency(idempotency_key)
        if existing_job_id:
            print(f"♻️ Duplicate job detected, returning existing job_id: {existing_job_id}")
            return existing_job_id
        
        # Generate new job ID
        job_id = f"{job_type}:{uuid4().hex[:12]}"
        timestamp = datetime.utcnow().isoformat()
        
        # Use defaults from environment if not specified
        max_retries = max_retries if max_retries is not None else MAX_RETRIES
        retry_delay = retry_delay if retry_delay is not None else RETRY_DELAY
        timeout = timeout if timeout is not None else JOB_TIMEOUT
        
        job_payload = {
            'job_id': job_id,
            'job_type': job_type,
            'status': JobStatus.PENDING,
            'created_at': timestamp,
            'priority': priority,
            'max_retries': max_retries,
            'retry_delay': retry_delay,
            'timeout': timeout,
            'retry_count': 0,
            'idempotency_key': idempotency_key,
            'data': job_data
        }
        
        try:
            # Store job details with TTL (before enqueueing to queue)
            job_key = f"{self.job_prefix}{job_id}"
            self.redis.setex(
                job_key,
                JOB_RESULT_TTL,
                json.dumps(job_payload, default=str)
            )
            
            # Store idempotency mapping
            self._store_idempotency_key(idempotency_key, job_id)
            
            # ATOMIC ENQUEUE: Use Lua script to atomically check queue size and enqueue
            # This prevents race conditions where multiple concurrent enqueues exceed MAX_QUEUE_SIZE
            
            # Calculate priority score using workload isolation (if available)
            # This includes job-type specific priorities, starvation prevention, and age
            try:
                from services.workload_isolation import workload_isolation, JobType
                
                # Convert job_type string to JobType enum
                job_type_values = [jt.value for jt in JobType]
                job_type_enum = (
                    JobType(job_type) if job_type in job_type_values
                    else None
                )
                
                if job_type_enum:
                    # Use workload isolation priority calculation
                    created_at_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    score = workload_isolation.get_priority_score(job_type_enum, job_id, created_at_dt)
                    
                    # Track job wait time for starvation detection
                    workload_isolation.track_job_wait(job_type_enum, job_id)
                else:
                    # Fallback to simple priority calculation
                    score = priority * 1000000000 + int(time.time() * 1000)
            except (ImportError, ValueError, AttributeError):
                # Fallback to simple priority calculation if workload isolation unavailable
                score = priority * 1000000000 + int(time.time() * 1000)
            
            # Execute atomic enqueue script (fallback to non-atomic if script not registered)
            if self._atomic_enqueue_script is None:
                # Fallback: try to register script again (Redis might have been unavailable at init)
                try:
                    self._atomic_enqueue_script = self.redis.register_script("""
                        local queue_name = KEYS[1]
                        local job_id = ARGV[1]
                        local score = tonumber(ARGV[2])
                        local max_queue_size = tonumber(ARGV[3])
                        local policy = ARGV[4]
                        local current_size = redis.call('ZCARD', queue_name)
                        if current_size < max_queue_size then
                            redis.call('ZADD', queue_name, score, job_id)
                            return {'enqueued', current_size + 1, nil}
                        end
                        if policy == 'reject' then
                            return {'rejected', current_size, nil}
                        elseif policy == 'drop_oldest' then
                            local oldest = redis.call('ZRANGE', queue_name, -1, -1)
                            if #oldest > 0 then
                                local dropped_job_id = oldest[1]
                                redis.call('ZREM', queue_name, dropped_job_id)
                                redis.call('ZADD', queue_name, score, job_id)
                                return {'dropped_oldest', current_size, dropped_job_id}
                            else
                                redis.call('ZADD', queue_name, score, job_id)
                                return {'enqueued', 1, nil}
                            end
                        else
                            return {'rejected', current_size, nil}
                        end
                    """)
                    # Script registration succeeded, execute it
                    result = self._atomic_enqueue_script(
                        keys=[queue_name],
                        args=[job_id, str(score), str(MAX_QUEUE_SIZE), QUEUE_FULL_POLICY]
                    )
                except Exception as e:
                    # If script registration still fails, fall back to non-atomic (not ideal, but better than crashing)
                    print(f"⚠️ Atomic enqueue script unavailable, using non-atomic fallback: {e}")
                    # Use non-atomic fallback (race condition possible, but better than failing completely)
                    current_queue_size = self.get_queue_length(queue_name)
                    if current_queue_size >= MAX_QUEUE_SIZE:
                        if QUEUE_FULL_POLICY == "reject":
                            from services.exceptions import QueueFullException
                            raise QueueFullException(
                                f"Queue {queue_name} is full ({current_queue_size}/{MAX_QUEUE_SIZE}). "
                                "Please try again later."
                            )
                        elif QUEUE_FULL_POLICY == "drop_oldest":
                            oldest = self.redis.zrange(queue_name, -1, -1)
                            if oldest:
                                self.redis.zrem(queue_name, oldest[0])
                                print(f"⚠️ Queue full, dropped oldest job: {oldest[0]}")
                    # Proceed with enqueue (non-atomic, but will work)
                    self.redis.zadd(queue_name, {job_id: score})
                    status = 'enqueued'
                    queue_size = self.get_queue_length(queue_name)
                    dropped_job_id = None
                    # Set result for parsing below
                    result = [status, queue_size, None]
            else:
                # Execute atomic enqueue script (normal path)
                # Returns: [status, queue_size, dropped_job_id]
                result = self._atomic_enqueue_script(
                    keys=[queue_name],
                    args=[job_id, str(score), str(MAX_QUEUE_SIZE), QUEUE_FULL_POLICY]
                )
            
            # Parse result (handles both Lua script results and fallback results)
            # Lua script returns: [status, queue_size, dropped_job_id]
            # Fallback returns: [status, queue_size, None]
            status = result[0].decode('utf-8') if isinstance(result[0], bytes) else result[0]
            queue_size = int(result[1])
            # dropped_job_id can be None (nil in Lua) or a string
            dropped_job_id = None
            if len(result) > 2 and result[2] is not None:
                dropped_job_id = result[2].decode('utf-8') if isinstance(result[2], bytes) else result[2]
            
            # Handle result based on status
            if status == 'rejected':
                # Queue is full and policy is 'reject'
                from services.exceptions import QueueFullException
                
                # Log queue rejection with structured logging
                try:
                    from services.structured_logging import structured_logger
                    structured_logger.log_queue_operation(
                        operation="queue_rejected",
                        queue_name=queue_name,
                        queue_length=queue_size,
                        job_id=job_id,
                        max_queue_size=MAX_QUEUE_SIZE,
                        job_type=job_type,
                        policy=QUEUE_FULL_POLICY
                    )
                except Exception:
                    pass  # Don't fail if logging fails
                
                # Track queue rejection metrics
                try:
                    from services.observability import observability
                    observability.track_queue_rejection(
                        queue_name=queue_name,
                        job_id=job_id,
                        job_type=job_type,
                        queue_size=queue_size,
                        max_queue_size=MAX_QUEUE_SIZE,
                        policy=QUEUE_FULL_POLICY
                    )
                except Exception:
                    pass  # Don't fail if metrics tracking fails
                
                # Clean up job data since it wasn't enqueued
                try:
                    self.redis.delete(job_key)
                    self.redis.delete(f"{self.idempotency_prefix}{idempotency_key}")
                except Exception:
                    pass  # Best effort cleanup
                
                raise QueueFullException(
                    f"Queue {queue_name} is full ({queue_size}/{MAX_QUEUE_SIZE}). "
                    "Please try again later."
                )
            
            elif status == 'dropped_oldest':
                # Queue was full, dropped oldest job to make room
                if dropped_job_id:
                    # Log dropped job with structured logging
                    try:
                        from services.structured_logging import structured_logger
                        structured_logger.log_queue_operation(
                            operation="queue_dropped_oldest",
                            queue_name=queue_name,
                            queue_length=queue_size,
                            job_id=job_id,
                            max_queue_size=MAX_QUEUE_SIZE,
                            job_type=job_type,
                            dropped_job_id=dropped_job_id,
                            policy=QUEUE_FULL_POLICY
                        )
                    except Exception:
                        pass
                    
                    print(f"⚠️ Queue full, dropped oldest job: {dropped_job_id} to make room for {job_id}")
            
            # Job was successfully enqueued (status == 'enqueued')
            # Update job status
            self.update_job_status(job_id, JobStatus.PENDING, message='Job enqueued')
            
            # Log successful enqueue with queue metrics
            try:
                from services.structured_logging import structured_logger
                structured_logger.log_queue_operation(
                    operation="job_enqueued",
                    queue_name=queue_name,
                    queue_length=queue_size,
                    job_id=job_id,
                    max_queue_size=MAX_QUEUE_SIZE,
                    job_type=job_type,
                    priority=priority
                )
            except Exception:
                pass
            
            try:
                print(f"✅ Job enqueued: {job_id} -> {queue_name} (priority: {priority}, timeout: {timeout}s, queue_size: {queue_size}/{MAX_QUEUE_SIZE})")
            except UnicodeEncodeError:
                print(f"[OK] Job enqueued: {job_id} -> {queue_name} (priority: {priority}, timeout: {timeout}s, queue_size: {queue_size}/{MAX_QUEUE_SIZE})")
            return job_id
            
        except Exception as e:
            # Import here to avoid circular dependency
            from services.exceptions import QueueFullException
            
            # Re-raise queue full exceptions (already logged and cleaned up)
            if isinstance(e, QueueFullException):
                raise
            
            # Clean up job data on any other error
            try:
                job_key = f"{self.job_prefix}{job_id}"
                self.redis.delete(job_key)
                self.redis.delete(f"{self.idempotency_prefix}{idempotency_key}")
            except Exception:
                pass  # Best effort cleanup
            
            try:
                print(f"❌ Failed to enqueue job: {e}")
            except UnicodeEncodeError:
                print(f"[ERROR] Failed to enqueue job: {e}")
            raise
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve job details by job_id"""
        try:
            job_key = f"{self.job_prefix}{job_id}"
            job_data = self.redis.get(job_key)
            if job_data:
                return json.loads(job_data)
            return None
        except (Exception, json.JSONDecodeError) as e:
            print(f"❌ Failed to get job {job_id}: {e}")
            return None
    
    def update_job_status(
        self,
        job_id: str,
        status: str,
        message: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        progress: Optional[float] = None,
        correlation_id: Optional[str] = None
    ):
        """Update job status and metadata with structured JSON logging"""
        try:
            job_data = self.get_job(job_id)
            if not job_data:
                print(f"⚠️ Job {job_id} not found for status update")
                return
            
            # Extract correlation_id from job_data if not provided
            if not correlation_id:
                job_payload = job_data.get('data', {})
                correlation_id = job_payload.get('correlation_id')
            
            # Update status fields
            job_data['status'] = status
            job_data['updated_at'] = datetime.utcnow().isoformat()
            
            if message:
                job_data['message'] = message
            
            if result is not None:
                job_data['result'] = result
                if status == JobStatus.COMPLETED:
                    job_data['completed_at'] = datetime.utcnow().isoformat()
            
            if error:
                job_data['error'] = error
                job_data['error_at'] = datetime.utcnow().isoformat()
            
            if progress is not None:
                job_data['progress'] = progress
            
            # Save updated job
            job_key = f"{self.job_prefix}{job_id}"
            ttl = self.redis.ttl(job_key)
            if ttl > 0:
                self.redis.setex(job_key, ttl, json.dumps(job_data, default=str))
            else:
                self.redis.setex(job_key, JOB_RESULT_TTL, json.dumps(job_data, default=str))
            
            # Structured JSON logging for job status write
            try:
                log_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": f"Job status updated: {status}",
                    "event": "job_status_write",
                    "context": {
                        "job_id": job_id,
                        "job_type": job_data.get('job_type'),
                        "status": status,
                        "progress": progress,
                        "has_result": result is not None,
                        "has_error": error is not None,
                        "correlation_id": correlation_id,
                        "user_id": job_payload.get('user_id') if 'data' in job_data else None,
                        "conversation_id": job_payload.get('conversation_id') if 'data' in job_data else None
                    }
                }
                logger.info(json.dumps(log_data, default=str))
            except Exception:
                pass  # Non-critical, don't fail if logging fails
            
            # Publish status update (for real-time updates)
            try:
                self.redis.publish(f"{self.job_prefix}{job_id}:status", json.dumps({
                    'job_id': job_id,
                    'status': status,
                    'message': message,
                    'progress': progress
                }, default=str))
            except Exception:
                pass  # Non-critical
            
        except Exception as e:
            print(f"❌ Failed to update job status {job_id}: {e}")
    
    def publish_streaming_chunk(
        self,
        job_id: str,
        chunk_type: str,
        chunk_data: Any,
        sequence: Optional[int] = None,
        is_final: bool = False
    ) -> bool:
        """
        Publish a streaming chunk for incremental delivery.
        
        This method delegates to the streaming service while maintaining
        job isolation and Redis-first architecture.
        
        Args:
            job_id: Job identifier
            chunk_type: Type of chunk ('text', 'progress', 'metadata', etc.)
            chunk_data: Chunk content
            sequence: Optional sequence number
            is_final: Whether this is the final chunk
        
        Returns:
            True if published successfully
        """
        try:
            from services.streaming_service import get_streaming_service
            streaming_service = get_streaming_service()
            return streaming_service.publish_chunk(
                job_id, chunk_type, chunk_data, sequence, is_final
            )
        except ImportError:
            # Streaming service not available - non-critical
            return False
        except Exception as e:
            print(f"⚠️ Failed to publish streaming chunk for job {job_id}: {e}")
            return False
    
    def dequeue_job(self, queue_name: str, timeout: int = 5) -> Optional[Dict[str, Any]]:
        """
        Dequeue a job from the queue with timeout checking
        
        Args:
            queue_name: Queue name
            timeout: Blocking timeout in seconds
        
        Returns:
            Job data dict or None if timeout
        """
        try:
            # Use BZPOPMIN for priority queue (lowest score = highest priority)
            # BZPOPMIN requires a list of keys, not a single string
            # Check queue size first for debugging
            queue_size = self.redis.zcard(queue_name)
            if queue_size > 0:
                print(f"🔍 Queue {queue_name} has {queue_size} job(s), attempting dequeue (timeout: {timeout}s)")
                # Also check what jobs are in the queue
                sample_jobs = self.redis.zrange(queue_name, 0, 2, withscores=True)
                if sample_jobs:
                    print(f"📋 Sample jobs in queue: {sample_jobs}")
            else:
                # Log when queue is empty to help debug
                if timeout > 0:
                    print(f"⏳ Queue {queue_name} is empty, blocking for {timeout}s...")
            
            # Try bzpopmin - if it fails, log the error
            try:
                result = self.redis.bzpopmin([queue_name], timeout=timeout)
            except Exception as bzpop_error:
                print(f"❌ bzpopmin failed for {queue_name}: {bzpop_error}")
                import traceback
                traceback.print_exc()
                raise
            
            if result:
                queue, job_id, score = result
                print(f"✅ Successfully dequeued job {job_id} from {queue_name} (score: {score})")
                job_data = self.get_job(job_id)
                
                if not job_data:
                    print(f"⚠️ Job {job_id} dequeued but not found in storage")
                    return None
                
                # Check if job has timed out
                created_at = job_data.get('created_at')
                job_timeout = job_data.get('timeout', JOB_TIMEOUT)
                
                if created_at:
                    try:
                        created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        age = (datetime.utcnow() - created_dt.replace(tzinfo=None)).total_seconds()
                        
                        if age > job_timeout:
                            self.update_job_status(
                                job_id,
                                JobStatus.TIMEOUT,
                                error=f'Job timed out after {job_timeout}s',
                                progress=0
                            )
                            print(f"⏱️ Job {job_id} timed out (age: {age:.0f}s, timeout: {job_timeout}s)")
                            return None
                    except (ValueError, TypeError):
                        pass
                
                # CRITICAL FIX: Don't update status here - let worker do it immediately after dequeue
                # This prevents race conditions where job is dequeued but status isn't updated yet
                # Worker will mark as "processing" immediately after concurrency check (which is fast)
                # If concurrency check fails, worker will reset to "pending" before re-enqueueing
                # No status update needed here - worker handles it
                
                # Set processing timestamp for tracking (worker will update status to processing)
                # CRITICAL: Processing marker MUST be keyed by job_id, NOT conversation_id or user_id
                # This ensures each job has its own lock, preventing "first ok, second stuck" issues
                processing_key = f"{self.processing_prefix}{job_id}"
                marker_set_time = datetime.utcnow().isoformat()
                # Use setex (SET with EX) to ensure TTL is set - prevents ghost locks on crashes
                # CRITICAL: Add 60s buffer to TTL (job_timeout + 60) to prevent race condition where
                # marker expires exactly when job completes. This ensures marker exists when cleanup
                # runs in mark_job_complete() or mark_job_failed(), preventing orphaned markers.
                marker_ttl = job_timeout + 60
                self.redis.setex(processing_key, marker_ttl, marker_set_time)
                
                # Log processing marker set with full details
                logger.info(
                    f"Processing marker set: job_id={job_id}, lock_key={processing_key}, "
                    f"set_time={marker_set_time}, ttl={marker_ttl}s (job_timeout={job_timeout}s + 60s buffer), "
                    f"key_format=processing:{{job_id}} (NOT conversation_id or user_id)"
                )
                print(f"[LOCK] Set processing marker: {processing_key} (TTL: {marker_ttl}s = {job_timeout}s + 60s buffer)")
                
                # Structured logging for job dequeue
                try:
                    from services.structured_logging import structured_logger
                    job_data_inner = job_data.get('data', {})
                    correlation_id = job_data_inner.get('correlation_id', 'unknown')
                    created_at = job_data.get('created_at')
                    queue_wait_seconds = None
                    if created_at:
                        try:
                            created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            queue_wait_seconds = (datetime.utcnow() - created_dt.replace(tzinfo=None)).total_seconds()
                        except (ValueError, TypeError):
                            pass
                    structured_logger.log_tutor_job_dequeue(
                        job_id=job_id,
                        correlation_id=correlation_id,
                        queue_name=queue_name,
                        queue_wait_seconds=queue_wait_seconds
                    )
                except Exception:
                    pass  # Non-critical, don't fail if logging fails
                
                # Return job_data - worker will mark as processing after concurrency check
                return job_data
            
            # Timeout - no job available
            if queue_size > 0:
                print(f"⚠️ Dequeue timeout for {queue_name} (has {queue_size} jobs but bzpopmin returned None - possible Redis issue)")
            return None
            
        except Exception as e:
            print(f"❌ Failed to dequeue job from {queue_name}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def mark_job_complete(self, job_id: str, result: Dict[str, Any]):
        """Mark job as complete and clean up processing marker"""
        processing_key = f"{self.processing_prefix}{job_id}"
        marker_cleared_time = None
        
        try:
            # Get marker value before deletion for logging
            marker_set_time = self.redis.get(processing_key)
            
            # Remove processing marker
            self.redis.delete(processing_key)
            marker_cleared_time = datetime.utcnow().isoformat()
            
            # Log processing marker cleared with full details
            if marker_set_time:
                logger.info(
                    f"Processing marker cleared: job_id={job_id}, lock_key={processing_key}, "
                    f"set_time={marker_set_time}, cleared_time={marker_cleared_time}, status=completed, "
                    f"key_format=processing:{{job_id}} (per-job lock)"
                )
                print(f"[LOCK] Cleared processing marker: {processing_key} (completed)")
            else:
                logger.info(
                    f"Processing marker cleared: job_id={job_id}, lock_key={processing_key}, "
                    f"cleared_time={marker_cleared_time}, status=completed (marker was missing), "
                    f"key_format=processing:{{job_id}} (per-job lock)"
                )
                print(f"[LOCK] Cleared processing marker: {processing_key} (completed, was missing)")
            
            # Update job status: pending/queued -> processing -> completed
            self.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                result=result,
                progress=100,
                message='Job completed successfully'
            )
        except Exception as e:
            # Ensure marker is cleared even if status update fails
            try:
                if marker_cleared_time is None:
                    self.redis.delete(processing_key)
                    marker_cleared_time = datetime.utcnow().isoformat()
                    logger.warning(f"Processing marker force-cleared after error: job_id={job_id}, lock_key={processing_key}, cleared_time={marker_cleared_time}")
            except Exception:
                pass  # Non-critical
            logger.error(f"Failed to mark job complete {job_id}: {e}")
            print(f"❌ Failed to mark job complete {job_id}: {e}")
    
    def mark_job_failed(self, job_id: str, error: str, should_retry: bool = True):
        """Mark job as failed and optionally retry"""
        processing_key = f"{self.processing_prefix}{job_id}"
        marker_cleared_time = None
        
        try:
            job_data = self.get_job(job_id)
            if not job_data:
                # Job doesn't exist - still clean up marker if it exists
                try:
                    marker_set_time = self.redis.get(processing_key)
                    self.redis.delete(processing_key)
                    marker_cleared_time = datetime.utcnow().isoformat()
                    if marker_set_time:
                        logger.warning(f"Processing marker cleared for missing job: job_id={job_id}, lock_key={processing_key}, set_time={marker_set_time}, cleared_time={marker_cleared_time}")
                except Exception:
                    pass
                return False
            
            # Get marker value before deletion for logging
            marker_set_time = self.redis.get(processing_key)
            
            # Remove processing marker
            self.redis.delete(processing_key)
            marker_cleared_time = datetime.utcnow().isoformat()
            
            # Log processing marker cleared with full details
            if marker_set_time:
                logger.info(
                    f"Processing marker cleared: job_id={job_id}, lock_key={processing_key}, "
                    f"set_time={marker_set_time}, cleared_time={marker_cleared_time}, status=failed, "
                    f"key_format=processing:{{job_id}} (per-job lock)"
                )
                print(f"[LOCK] Cleared processing marker: {processing_key} (failed)")
            else:
                logger.info(
                    f"Processing marker cleared: job_id={job_id}, lock_key={processing_key}, "
                    f"cleared_time={marker_cleared_time}, status=failed (marker was missing), "
                    f"key_format=processing:{{job_id}} (per-job lock)"
                )
                print(f"[LOCK] Cleared processing marker: {processing_key} (failed, was missing)")
            
            retry_count = job_data.get('retry_count', 0)
            max_retries = job_data.get('max_retries', MAX_RETRIES)
            
            # Check if error is due to Supabase circuit breaker or global limit
            is_circuit_breaker_error = (
                'CircuitBreakerOpenError' in error or
                'circuit breaker' in error.lower() or
                'supabase service is temporarily unavailable' in error.lower()
            )
            
            is_global_limit_error = (
                'SupabaseGlobalLimitExceeded' in error or
                'global supabase concurrency limit exceeded' in error.lower()
            )
            
            is_budget_saturated_error = (
                'SupabaseBudgetSaturated' in error or
                'supabase request budget saturated' in error.lower()
            )
            
            if should_retry and retry_count < max_retries:
                # Calculate retry delay
                base_delay = job_data.get('retry_delay', RETRY_DELAY)
                max_retry_delay = int(os.getenv("MAX_RETRY_DELAY", 600))
                
                if is_circuit_breaker_error:
                    # Circuit breaker error: use cooldown period + buffer
                    from services.supabase_circuit_breaker import get_supabase_circuit_breaker
                    circuit_breaker = get_supabase_circuit_breaker()
                    status = circuit_breaker.get_status()
                    cooldown_remaining = status.get('cooldown_remaining_seconds', 60)
                    delay_seconds = max(cooldown_remaining + 10, 60)  # At least 60s
                    delay_seconds = min(delay_seconds, max_retry_delay)
                elif is_global_limit_error or is_budget_saturated_error:
                    # Global limit or budget saturated error: exponential backoff with jitter
                    # Start with 15-30s, then increase
                    base_delay = 15 + random.randint(0, 15)  # 15-30s jitter
                    delay_seconds = min(base_delay * (2 ** min(retry_count, 3)), max_retry_delay)
                else:
                    # Regular error: exponential backoff
                    delay_seconds = min(base_delay * (2 ** retry_count), max_retry_delay)
                
                return self._retry_job(job_id, job_data, delay_seconds)
            else:
                # Mark as permanently failed
                self.update_job_status(
                    job_id,
                    JobStatus.FAILED,
                    error=error,
                    progress=0,
                    message=f'Job failed after {retry_count} retries'
                )
                return False
        except Exception as e:
            print(f"❌ Failed to mark job failed {job_id}: {e}")
            return False
    
    def retry_job(self, job_id: str, retry_delay: Optional[int] = None) -> bool:
        """
        Retry a failed job with optional custom delay (conservative exponential backoff)
        
        Args:
            job_id: Job ID to retry
            retry_delay: Optional custom retry delay in seconds (uses exponential backoff if None)
        
        Returns:
            True if job was queued for retry, False otherwise
        """
        try:
            job_data = self.get_job(job_id)
            if not job_data:
                return False
            
            # Use provided delay or calculate exponential backoff (conservative)
            if retry_delay is None:
                retry_count = job_data.get('retry_count', 0)
                base_delay = job_data.get('retry_delay', RETRY_DELAY)
                # Exponential backoff with max cap (conservative: max 10 minutes)
                max_retry_delay = int(os.getenv("MAX_RETRY_DELAY", 600))  # Max 10 minutes
                exponential_backoff = os.getenv("RETRY_EXPONENTIAL_BACKOFF", "true").lower() == "true"
                if exponential_backoff:
                    delay_seconds = min(base_delay * (2 ** retry_count), max_retry_delay)
                else:
                    delay_seconds = base_delay
            else:
                delay_seconds = retry_delay
            
            return self._retry_job(job_id, job_data, delay_seconds)
        except Exception as e:
            print(f"❌ Failed to retry job {job_id}: {e}")
            return False
    
    def _retry_job(self, job_id: str, job_data: Dict[str, Any], delay_seconds: int) -> bool:
        """
        Internal method to retry a job with specified delay (conservative exponential backoff)
        
        Args:
            job_id: Job ID to retry
            job_data: Job data dictionary
            delay_seconds: Retry delay in seconds (calculated by retry_job)
        
        Returns:
            True if job was queued for retry, False otherwise
        """
        try:
            retry_count = job_data.get('retry_count', 0) + 1
            max_retries = job_data.get('max_retries', MAX_RETRIES)
            
            # Conservative retry policy: don't retry if exceeded max retries
            if retry_count > max_retries:
                return False
            
            # Update retry count
            job_data['retry_count'] = retry_count
            job_data['status'] = JobStatus.RETRYING
            job_data['last_retry_at'] = datetime.utcnow().isoformat()
            job_data['next_retry_at'] = (datetime.utcnow() + timedelta(seconds=delay_seconds)).isoformat()
            
            # Re-enqueue with delay (using sorted set score for delayed execution)
            # Score = priority * large_number + (current_time + delay) in milliseconds
            retry_time = int((time.time() + delay_seconds) * 1000)
            priority = job_data.get('priority', PRIORITY_NORMAL)
            score = priority * 1000000000 + retry_time
            
            # Save updated job
            job_key = f"{self.job_prefix}{job_id}"
            ttl = self.redis.ttl(job_key)
            if ttl > 0:
                self.redis.setex(job_key, ttl, json.dumps(job_data, default=str))
            else:
                self.redis.setex(job_key, JOB_RESULT_TTL, json.dumps(job_data, default=str))
            
            # Re-add to queue with delayed score (ensures delayed execution)
            queue_name = self._get_queue_name_for_job_type(job_data.get('job_type', ''))
            if queue_name:
                self.redis.zadd(queue_name, {job_id: score})
            
            self.update_job_status(
                job_id,
                JobStatus.RETRYING,
                message=f'Job queued for retry {retry_count}/{max_retries} (delay: {delay_seconds}s)'
            )
            
            print(f"🔄 Job {job_id} queued for retry {retry_count}/{max_retries} (delay: {delay_seconds}s)")
            return True
        except Exception as e:
            print(f"❌ Failed to retry job {job_id}: {e}")
            return False
    
    def _get_queue_name_for_job_type(self, job_type: str) -> Optional[str]:
        """Get queue name for job type"""
        mapping = {
            'tutor_chat': QUEUE_TUTOR,
            'grade_answer': QUEUE_GRADING,
            'grade_mock_exam': QUEUE_MOCK_EXAM,
            'explain_concept': QUEUE_HELPING,
            'create_lesson': QUEUE_LESSON,
        }
        return mapping.get(job_type)
    
    def get_queue_length(self, queue_name: str) -> int:
        """Get current queue length (only pending jobs)"""
        try:
            return self.redis.zcard(queue_name)
        except Exception:
            return 0
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get statistics for all queues"""
        try:
            stats = {
                'tutor_queue': self.get_queue_length(QUEUE_TUTOR),
                'grading_queue': self.get_queue_length(QUEUE_GRADING),
                'mock_exam_queue': self.get_queue_length(QUEUE_MOCK_EXAM),
                'helping_queue': self.get_queue_length(QUEUE_HELPING),
                'lesson_queue': self.get_queue_length(QUEUE_LESSON),
                'redis_connected': is_redis_available(),
            }
            return stats
        except Exception:
            return {
                'tutor_queue': 0,
                'grading_queue': 0,
                'mock_exam_queue': 0,
                'helping_queue': 0,
                'lesson_queue': 0,
                'redis_connected': False,
            }
    
    def cleanup_stale_jobs(self, max_age_hours: int = 24):
        """Clean up stale processing jobs (jobs stuck in processing state)"""
        try:
            pattern = f"{self.processing_prefix}*"
            cursor = 0
            cleaned = 0
            cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
            
            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    job_id = key.replace(self.processing_prefix, '')
                    job_data = self.get_job(job_id)
                    
                    if job_data:
                        status = job_data.get('status')
                        # Check if job is stuck in processing
                        if status == JobStatus.PROCESSING:
                            created_at = job_data.get('created_at')
                            if created_at:
                                try:
                                    created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                    if created_dt.replace(tzinfo=None) < cutoff:
                                        # Job is stale, mark as failed
                                        self.mark_job_failed(
                                            job_id,
                                            'Job stuck in processing state',
                                            should_retry=False
                                        )
                                        cleaned += 1
                                except (ValueError, TypeError):
                                    pass
                
                if cursor == 0:
                    break
            
            if cleaned > 0:
                print(f"🧹 Cleaned up {cleaned} stale processing jobs")
            
            return cleaned
        except Exception as e:
            print(f"❌ Failed to cleanup stale jobs: {e}")
            return 0


# Global queue instance
job_queue = JobQueue()
