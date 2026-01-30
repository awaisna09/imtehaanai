"""
Mastery Update Service
Handles asynchronous mastery update job creation and processing.
"""

import logging
import hashlib
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Import job queue
try:
    from services.job_queue import job_queue, QUEUE_MASTERY
    from services.redis_connection import is_redis_available
    JOB_QUEUE_AVAILABLE = is_redis_available()
except ImportError:
    JOB_QUEUE_AVAILABLE = False
    job_queue = None
    QUEUE_MASTERY = None


def build_mastery_update_payload(
    user_id: str,
    concepts: List[str],
    reasoning_category: str,
    has_misconception: bool,
    max_marks: Optional[int],
    difficulty_level: Optional[int],
    topic_id: Optional[str] = None,
    topic_name: Optional[str] = None,
    subject: Optional[str] = None,
    question_id: Optional[str] = None,
    attempt_id: Optional[str] = None,
    concept_deltas: Optional[Dict[str, float]] = None
) -> Dict[str, Any]:
    """
    Build a payload for mastery update job.
    
    Args:
        user_id: User ID
        concepts: List of concept IDs (normalized)
        reasoning_category: 'good', 'neutral', or 'confused'
        has_misconception: Whether student has misconception
        max_marks: Maximum marks for the question
        difficulty_level: Difficulty level (1-3)
        topic_id: Optional topic ID
        topic_name: Optional topic name
        subject: Optional subject name
        question_id: Optional question ID
        attempt_id: Optional attempt ID for idempotency
        concept_deltas: Optional pre-computed deltas per concept (concept_id -> delta)
    
    Returns:
        Payload dictionary for mastery update job
    """
    payload = {
        "user_id": user_id,
        "concepts": concepts,
        "reasoning_category": reasoning_category,
        "has_misconception": has_misconception,
        "max_marks": max_marks,
        "difficulty_level": difficulty_level,
        "topic_id": topic_id,
        "topic_name": topic_name,
        "subject": subject or "Business Studies",
        "question_id": question_id,
        "timestamp": datetime.now().isoformat()
    }
    
    # Include pre-computed deltas if provided (for faster worker processing)
    if concept_deltas:
        payload["concept_deltas"] = concept_deltas
    
    # Generate idempotency key if attempt_id provided
    if attempt_id:
        payload["idempotency_key"] = f"mastery_update:{user_id}:{attempt_id}"
    
    return payload


def enqueue_mastery_update_job(
    payload: Dict[str, Any],
    idempotency_key: Optional[str] = None
) -> Optional[str]:
    """
    Enqueue a mastery update job to Redis queue.
    
    Args:
        payload: Mastery update payload from build_mastery_update_payload()
        idempotency_key: Optional idempotency key (auto-generated if not provided)
    
    Returns:
        Job ID if successful, None if enqueue failed
    """
    if not JOB_QUEUE_AVAILABLE or not job_queue:
        logger.warning(
            "[MASTERY] Job queue not available - mastery update will be skipped"
        )
        return None
    
    try:
        # Use provided idempotency key or generate from payload
        if not idempotency_key and "idempotency_key" in payload:
            idempotency_key = payload["idempotency_key"]
        elif not idempotency_key:
            # Generate from user_id + question_id + timestamp (hour granularity)
            hour_key = datetime.now().strftime("%Y%m%d%H")
            idempotency_key = (
                f"mastery_update:{payload['user_id']}:"
                f"{payload.get('question_id', 'unknown')}:{hour_key}"
            )
        
        job_id = job_queue.enqueue_job(
            queue_name=QUEUE_MASTERY,
            job_type="update_mastery",
            job_data=payload,
            idempotency_key=idempotency_key
        )
        
        logger.info(
            f"✅ [MASTERY] Enqueued mastery update job: {job_id} "
            f"for user {payload['user_id']} ({len(payload['concepts'])} concepts)"
        )
        
        return job_id
        
    except Exception as e:
        logger.error(
            f"❌ [MASTERY] Failed to enqueue mastery update job: {e}",
            exc_info=True
        )
        # Don't block user - log error and continue
        return None
