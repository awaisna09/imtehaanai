"""
Redis-based Job Queue Service (Legacy - for backward compatibility)
New code should use services/job_queue.py instead
"""

import json
import os
from typing import Dict, Optional, Any, List
from datetime import datetime, timedelta
from uuid import uuid4
from dotenv import load_dotenv

# Import new connection module
from services.redis_connection import get_redis_client

load_dotenv('config.env')

# Queue names
QUEUE_TUTOR = 'jobs:tutor'
QUEUE_GRADING = 'jobs:grading'
QUEUE_MOCK_EXAM = 'jobs:mock_exam'
QUEUE_HELPING = 'jobs:helping'
QUEUE_LESSON = 'jobs:lesson'

# Job result storage TTL (24 hours)
JOB_RESULT_TTL = 86400

# Use new connection module
# get_redis_client is imported from services.redis_connection


class JobQueue:
    """Redis-based job queue manager (Legacy - use services.job_queue.JobQueue instead)"""
    
    def __init__(self):
        try:
            self.redis = get_redis_client()
        except Exception as e:
            print(f"⚠️ Warning: Redis connection failed: {e}")
            print("   This is likely due to Redis not being available")
            raise
    
    def enqueue_job(
        self,
        queue_name: str,
        job_type: str,
        job_data: Dict[str, Any],
        priority: int = 0,
        max_retries: int = 3,
        retry_delay: int = 60
    ) -> str:
        """
        Enqueue a job to Redis queue
        
        Args:
            queue_name: Queue name (e.g., QUEUE_TUTOR, QUEUE_GRADING)
            job_type: Type of job ('tutor_chat', 'grade_answer', 'grade_mock_exam')
            job_data: Job payload data
            priority: Job priority (lower = higher priority, default: 0)
            max_retries: Maximum retry attempts
            retry_delay: Delay between retries in seconds
        
        Returns:
            job_id: Unique job identifier
        """
        job_id = f"{job_type}:{uuid4().hex[:12]}"
        timestamp = datetime.utcnow().isoformat()
        
        job_payload = {
            'job_id': job_id,
            'job_type': job_type,
            'status': 'pending',
            'created_at': timestamp,
            'priority': priority,
            'max_retries': max_retries,
            'retry_delay': retry_delay,
            'retry_count': 0,
            'data': job_data
        }
        
        try:
            # Store job details with TTL
            job_key = f"job:{job_id}"
            self.redis.setex(
                job_key,
                JOB_RESULT_TTL,
                json.dumps(job_payload, default=str)
            )
            
            # Add to queue (sorted set for priority support)
            score = priority * 1000000 + int(datetime.utcnow().timestamp() * 1000)
            self.redis.zadd(queue_name, {job_id: score})
            
            # Update job status
            self.update_job_status(job_id, 'pending', message='Job enqueued')
            
            print(f"✅ Job enqueued: {job_id} -> {queue_name} (priority: {priority})")
            return job_id
            
        except redis.RedisError as e:
            print(f"❌ Failed to enqueue job: {e}")
            raise
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve job details by job_id"""
        try:
            job_key = f"job:{job_id}"
            job_data = self.redis.get(job_key)
            if job_data:
                return json.loads(job_data)
            return None
        except (redis.RedisError, json.JSONDecodeError) as e:
            print(f"❌ Failed to get job {job_id}: {e}")
            return None
    
    def update_job_status(
        self,
        job_id: str,
        status: str,
        message: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        progress: Optional[float] = None
    ):
        """
        Update job status and metadata
        
        Args:
            job_id: Job identifier
            status: New status ('pending', 'processing', 'completed', 'failed', 'retrying')
            message: Status message
            result: Job result data (for completed jobs)
            error: Error message (for failed jobs)
            progress: Progress percentage (0-100)
        """
        try:
            job_key = f"job:{job_id}"
            job_data = self.get_job(job_id)
            
            if not job_data:
                print(f"⚠️ Job {job_id} not found for status update")
                return
            
            # Update status fields
            job_data['status'] = status
            job_data['updated_at'] = datetime.utcnow().isoformat()
            
            if message:
                job_data['message'] = message
            
            if result is not None:
                job_data['result'] = result
                if status == 'completed':
                    job_data['completed_at'] = datetime.utcnow().isoformat()
            
            if error:
                job_data['error'] = error
                job_data['error_at'] = datetime.utcnow().isoformat()
            
            if progress is not None:
                job_data['progress'] = progress
            
            # Save updated job
            ttl = self.redis.ttl(job_key)
            if ttl > 0:
                self.redis.setex(job_key, ttl, json.dumps(job_data, default=str))
            else:
                self.redis.setex(job_key, JOB_RESULT_TTL, json.dumps(job_data, default=str))
            
            # Publish status update (for real-time updates if needed)
            self.redis.publish(f"job:{job_id}:status", json.dumps({
                'job_id': job_id,
                'status': status,
                'message': message,
                'progress': progress
            }, default=str))
            
        except (redis.RedisError, json.JSONDecodeError) as e:
            print(f"❌ Failed to update job status {job_id}: {e}")
    
    def dequeue_job(self, queue_name: str, timeout: int = 5) -> Optional[Dict[str, Any]]:
        """
        Dequeue a job from the queue (BLPOP for blocking)
        
        Args:
            queue_name: Queue name
            timeout: Blocking timeout in seconds
        
        Returns:
            Job data dict or None if timeout
        """
        try:
            # Use BZPOPMIN for priority queue (lowest score = highest priority)
            # BZPOPMIN requires a list of keys, not a single string
            result = self.redis.bzpopmin([queue_name], timeout=timeout)
            
            if result:
                queue, job_id, score = result
                job_data = self.get_job(job_id)
                
                if job_data:
                    # Update status to processing
                    self.update_job_status(job_id, 'processing', message='Job started processing')
                    return job_data
                else:
                    print(f"⚠️ Job {job_id} dequeued but not found in storage")
                    return None
            
            return None
            
        except redis.RedisError as e:
            print(f"❌ Failed to dequeue job: {e}")
            return None
    
    def retry_job(self, job_id: str, queue_name: str):
        """Retry a failed job by re-enqueueing it"""
        try:
            job_data = self.get_job(job_id)
            if not job_data:
                return False
            
            retry_count = job_data.get('retry_count', 0)
            max_retries = job_data.get('max_retries', 3)
            
            if retry_count >= max_retries:
                self.update_job_status(
                    job_id,
                    'failed',
                    error=f'Max retries ({max_retries}) exceeded'
                )
                return False
            
            # Increment retry count
            job_data['retry_count'] = retry_count + 1
            job_data['status'] = 'retrying'
            job_data['last_retry_at'] = datetime.utcnow().isoformat()
            
            # Re-enqueue with delay (using sorted set score)
            delay_seconds = job_data.get('retry_delay', 60) * (retry_count + 1)
            score = int((datetime.utcnow() + timedelta(seconds=delay_seconds)).timestamp() * 1000)
            
            job_key = f"job:{job_id}"
            self.redis.setex(job_key, JOB_RESULT_TTL, json.dumps(job_data, default=str))
            self.redis.zadd(queue_name, {job_id: score})
            
            self.update_job_status(
                job_id,
                'retrying',
                message=f'Job queued for retry {retry_count + 1}/{max_retries}'
            )
            
            print(f"🔄 Job {job_id} queued for retry {retry_count + 1}/{max_retries}")
            return True
            
        except redis.RedisError as e:
            print(f"❌ Failed to retry job {job_id}: {e}")
            return False
    
    def get_queue_length(self, queue_name: str) -> int:
        """Get current queue length"""
        try:
            return self.redis.zcard(queue_name)
        except redis.RedisError:
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
                'redis_connected': self.redis.ping() if self.redis else False
            }
            return stats
        except redis.RedisError:
            return {
                'tutor_queue': 0,
                'grading_queue': 0,
                'mock_exam_queue': 0,
                'helping_queue': 0,
                'lesson_queue': 0,
                'redis_connected': False
            }
    
    def cleanup_old_jobs(self, days: int = 7):
        """Clean up jobs older than specified days"""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            cutoff_timestamp = cutoff.timestamp()
            
            pattern = "job:*"
            cursor = 0
            cleaned = 0
            
            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    job_data = self.redis.get(key)
                    if job_data:
                        job = json.loads(job_data)
                        created_at = job.get('created_at')
                        if created_at:
                            created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            if created_dt.timestamp() < cutoff_timestamp:
                                self.redis.delete(key)
                                cleaned += 1
                
                if cursor == 0:
                    break
            
            print(f"🧹 Cleaned up {cleaned} old jobs (older than {days} days)")
            return cleaned
            
        except redis.RedisError as e:
            print(f"❌ Failed to cleanup old jobs: {e}")
            return 0


# Global queue instance
job_queue = JobQueue()
