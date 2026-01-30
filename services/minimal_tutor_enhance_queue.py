"""
Minimal Redis Queue for Tutor Enhance Jobs

This is a simple, lightweight queue implementation specifically for tutor_enhance jobs.
It bypasses all workload isolation, processing markers, and complex queue logic.
Uses a simple Redis list with BRPOP for atomic blocking dequeue.
"""

import json
import time
import logging
from typing import Dict, Optional, Any
from datetime import datetime
from uuid import uuid4

from services.redis_connection import get_redis_client, is_redis_available

logger = logging.getLogger(__name__)

# Queue name for tutor_enhance jobs
QUEUE_TUTOR_ENHANCE = 'queue:tutor_enhance'

# Job storage prefix
JOB_PREFIX = 'job:tutor_enhance:'


class MinimalTutorEnhanceQueue:
    """
    Minimal Redis queue for tutor_enhance jobs.
    
    Uses a simple Redis list (LPUSH/BRPOP) for FIFO queue behavior.
    No workload isolation, no processing markers, no complex logic.
    """
    
    def __init__(self):
        self.redis = get_redis_client()
        self.queue_name = QUEUE_TUTOR_ENHANCE
        self.job_prefix = JOB_PREFIX
    
    def is_available(self) -> bool:
        """Check if Redis is available"""
        return is_redis_available() and self.redis is not None
    
    def enqueue_job(
        self,
        user_id: str,
        conversation_id: str,
        topic_id: str,
        subject_id: Optional[int],
        user_message_id: Optional[str] = None,
        assistant_message_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> str:
        """
        Enqueue a tutor_enhance job.
        
        Args:
            user_id: User identifier
            conversation_id: Conversation identifier
            topic_id: Topic identifier
            subject_id: Subject identifier
            user_message_id: Optional user message ID
            assistant_message_id: Optional assistant message ID
            correlation_id: Optional correlation ID
            
        Returns:
            job_id: Unique job identifier
        """
        if not self.is_available():
            raise RuntimeError("Redis not available")
        
        # Generate job ID
        job_id = f"tutor_enhance:{uuid4().hex[:12]}"
        
        # Create job payload
        job_data = {
            'job_id': job_id,
            'job_type': 'tutor_enhance',
            'created_at': datetime.utcnow().isoformat(),
            'data': {
                'user_id': user_id,
                'conversation_id': conversation_id,
                'topic_id': topic_id,
                'subject_id': subject_id,
                'user_message_id': user_message_id,
                'assistant_message_id': assistant_message_id,
                'correlation_id': correlation_id
            }
        }
        
        # Store job data in Redis (with TTL of 7 days)
        job_key = f"{self.job_prefix}{job_id}"
        self.redis.setex(
            job_key,
            7 * 24 * 3600,  # 7 days TTL
            json.dumps(job_data)
        )
        
        # Enqueue job ID to list (LPUSH for FIFO with BRPOP)
        self.redis.lpush(self.queue_name, job_id)
        
        logger.info(f"Enqueued tutor_enhance job: {job_id}")
        return job_id
    
    def dequeue_job(self, timeout: int = 5) -> Optional[Dict[str, Any]]:
        """
        Dequeue a job from the queue (blocking).
        
        Uses BRPOP for atomic blocking pop from list.
        No processing markers, no workload isolation checks.
        
        Args:
            timeout: Blocking timeout in seconds
            
        Returns:
            Job data dict or None if timeout
        """
        if not self.is_available():
            return None
        
        try:
            # BRPOP: blocking right pop (FIFO behavior with LPUSH)
            # Returns: (queue_name, job_id) or None if timeout
            result = self.redis.brpop(self.queue_name, timeout=timeout)
            
            if result:
                queue_name, job_id = result
                job_id = job_id.decode('utf-8') if isinstance(job_id, bytes) else job_id
                
                # Retrieve job data
                job_key = f"{self.job_prefix}{job_id}"
                job_data_str = self.redis.get(job_key)
                
                if job_data_str:
                    job_data_str = job_data_str.decode('utf-8') if isinstance(job_data_str, bytes) else job_data_str
                    job_data = json.loads(job_data_str)
                    
                    # Add status
                    job_data['status'] = 'processing'
                    
                    logger.info(f"Dequeued tutor_enhance job: {job_id}")
                    return job_data
                else:
                    logger.warning(f"Job {job_id} dequeued but data not found")
                    return None
            
            return None
            
        except Exception as e:
            logger.error(f"Error dequeuing job: {e}")
            return None
    
    def mark_job_completed(self, job_id: str, result: Dict[str, Any]):
        """
        Mark a job as completed and store result.
        
        Args:
            job_id: Job identifier
            result: Result data to store
        """
        if not self.is_available():
            return
        
        try:
            job_key = f"{self.job_prefix}{job_id}"
            job_data_str = self.redis.get(job_key)
            
            if job_data_str:
                job_data_str = job_data_str.decode('utf-8') if isinstance(job_data_str, bytes) else job_data_str
                job_data = json.loads(job_data_str)
                
                # Extract assistant_message_id from job data to include in result
                assistant_message_id = None
                if job_data.get('data') and isinstance(job_data['data'], dict):
                    assistant_message_id = job_data['data'].get('assistant_message_id')
                
                # Update job data with result
                job_data['status'] = 'completed'
                job_data['completed_at'] = datetime.utcnow().isoformat()
                
                # Include assistant_message_id in result if available
                if assistant_message_id and isinstance(result, dict):
                    result = result.copy()
                    result['assistant_message_id'] = assistant_message_id
                
                job_data['result'] = result
                
                # Store updated job data (keep TTL)
                self.redis.setex(
                    job_key,
                    7 * 24 * 3600,  # 7 days TTL
                    json.dumps(job_data)
                )
                
                logger.info(f"Marked job {job_id} as completed, assistant_message_id: {assistant_message_id or 'none'}")
        except Exception as e:
            logger.error(f"Error marking job completed: {e}")
    
    def mark_job_failed(self, job_id: str, error: str):
        """
        Mark a job as failed and store error.
        
        Args:
            job_id: Job identifier
            error: Error message
        """
        if not self.is_available():
            return
        
        try:
            job_key = f"{self.job_prefix}{job_id}"
            job_data_str = self.redis.get(job_key)
            
            if job_data_str:
                job_data_str = job_data_str.decode('utf-8') if isinstance(job_data_str, bytes) else job_data_str
                job_data = json.loads(job_data_str)
                
                # Update job data with error
                job_data['status'] = 'failed'
                job_data['failed_at'] = datetime.utcnow().isoformat()
                job_data['error'] = error
                
                # Store updated job data (keep TTL)
                self.redis.setex(
                    job_key,
                    7 * 24 * 3600,  # 7 days TTL
                    json.dumps(job_data)
                )
                
                logger.info(f"Marked job {job_id} as failed: {error}")
        except Exception as e:
            logger.error(f"Error marking job failed: {e}")
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job data by job_id.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job data dict or None if not found
        """
        if not self.is_available():
            return None
        
        try:
            job_key = f"{self.job_prefix}{job_id}"
            job_data_str = self.redis.get(job_key)
            
            if job_data_str:
                job_data_str = job_data_str.decode('utf-8') if isinstance(job_data_str, bytes) else job_data_str
                return json.loads(job_data_str)
            
            return None
        except Exception as e:
            logger.error(f"Error getting job: {e}")
            return None
    
    def get_queue_length(self) -> int:
        """Get current queue length"""
        if not self.is_available():
            return 0
        
        try:
            return self.redis.llen(self.queue_name)
        except Exception as e:
            logger.error(f"Error getting queue length: {e}")
            return 0
