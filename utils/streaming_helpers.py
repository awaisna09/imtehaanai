#!/usr/bin/env python3
"""
Streaming Helpers - Utilities for workers to publish streaming chunks
Maintains job isolation and Redis-first architecture
"""

import logging
from typing import Any, Optional
from services.job_queue import job_queue

logger = logging.getLogger(__name__)


def publish_text_chunk(
    job_id: str,
    text: str,
    sequence: Optional[int] = None,
    is_final: bool = False
) -> bool:
    """
    Publish a text chunk for streaming.
    
    Args:
        job_id: Job identifier
        text: Text content to stream
        sequence: Optional sequence number (auto-generated if None)
        is_final: Whether this is the final chunk
    
    Returns:
        True if published successfully
    """
    try:
        return job_queue.publish_streaming_chunk(
            job_id=job_id,
            chunk_type='text',
            chunk_data={'text': text},
            sequence=sequence,
            is_final=is_final
        )
    except Exception as e:
        logger.warning(f"Failed to publish text chunk for job {job_id}: {e}")
        return False


def publish_progress_chunk(
    job_id: str,
    progress: float,
    message: Optional[str] = None
) -> bool:
    """
    Publish a progress update chunk.
    
    Args:
        job_id: Job identifier
        progress: Progress percentage (0-100)
        message: Optional progress message
    
    Returns:
        True if published successfully
    """
    try:
        return job_queue.publish_streaming_chunk(
            job_id=job_id,
            chunk_type='progress',
            chunk_data={
                'progress': progress,
                'message': message
            }
        )
    except Exception as e:
        logger.warning(
            f"Failed to publish progress chunk for job {job_id}: {e}"
        )
        return False


def publish_metadata_chunk(
    job_id: str,
    metadata: dict,
    is_final: bool = False
) -> bool:
    """
    Publish a metadata chunk.
    
    Args:
        job_id: Job identifier
        metadata: Metadata dictionary
        is_final: Whether this is the final chunk
    
    Returns:
        True if published successfully
    """
    try:
        return job_queue.publish_streaming_chunk(
            job_id=job_id,
            chunk_type='metadata',
            chunk_data=metadata,
            is_final=is_final
        )
    except Exception as e:
        logger.warning(
            f"Failed to publish metadata chunk for job {job_id}: {e}"
        )
        return False


def publish_final_result(
    job_id: str,
    result: dict
) -> bool:
    """
    Publish final result chunk and mark streaming as complete.
    
    Args:
        job_id: Job identifier
        result: Final result dictionary
    
    Returns:
        True if published successfully
    """
    try:
        return job_queue.publish_streaming_chunk(
            job_id=job_id,
            chunk_type='result',
            chunk_data=result,
            is_final=True
        )
    except Exception as e:
        logger.warning(
            f"Failed to publish final result for job {job_id}: {e}"
        )
        return False
