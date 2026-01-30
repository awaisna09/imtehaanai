#!/usr/bin/env python3
"""
Streaming Service - Handles incremental delivery of long-running AI responses
Maintains Redis-first architecture and job isolation
"""

import json
import logging
import time
from typing import Dict, Optional, Any, List
from datetime import datetime
from services.redis_connection import get_redis_client, is_redis_available

logger = logging.getLogger(__name__)


class StreamingService:
    """
    Service for streaming incremental job outputs to clients.
    
    Architecture:
    - Workers publish chunks to Redis pub/sub channels
    - API layer streams chunks to clients via SSE
    - Final completion semantics preserved
    - Graceful fallback to polling if streaming unavailable
    """
    
    def __init__(self):
        """Initialize streaming service"""
        import os
        self.redis = get_redis_client() if is_redis_available() else None
        self.chunk_prefix = "stream:chunks:"
        self.channel_prefix = "stream:channel:"
        # Load TTL from environment, default to 1 hour
        self.chunk_ttl = int(os.getenv("STREAM_CHUNK_TTL", "3600"))
    
    def publish_chunk(
        self,
        job_id: str,
        chunk_type: str,
        chunk_data: Any,
        sequence: Optional[int] = None,
        is_final: bool = False
    ) -> bool:
        """
        Publish a streaming chunk for a job.
        
        Args:
            job_id: Job identifier
            chunk_type: Type of chunk ('text', 'progress', 'metadata', etc.)
            chunk_data: Chunk content (will be JSON serialized)
            sequence: Optional sequence number for ordering
            is_final: Whether this is the final chunk
        
        Returns:
            True if published successfully, False otherwise
        """
        if not self.redis:
            logger.warning("Redis not available for streaming")
            return False
        
        try:
            # Generate sequence number if not provided
            if sequence is None:
                sequence = int(time.time() * 1000)  # Millisecond timestamp
            
            # Create chunk payload
            chunk = {
                'job_id': job_id,
                'chunk_type': chunk_type,
                'data': chunk_data,
                'sequence': sequence,
                'timestamp': datetime.utcnow().isoformat(),
                'is_final': is_final
            }
            
            # Store chunk in Redis (for recovery/replay)
            chunk_key = f"{self.chunk_prefix}{job_id}:{sequence}"
            self.redis.setex(
                chunk_key,
                self.chunk_ttl,
                json.dumps(chunk, default=str)
            )
            
            # Publish to pub/sub channel
            channel = f"{self.channel_prefix}{job_id}"
            self.redis.publish(
                channel,
                json.dumps(chunk, default=str)
            )
            
            logger.debug(
                f"Published chunk for job {job_id}: "
                f"type={chunk_type}, sequence={sequence}, final={is_final}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish chunk for job {job_id}: {e}")
            return False
    
    def get_chunks(
        self,
        job_id: str,
        since_sequence: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve stored chunks for a job (for recovery/replay).
        
        Args:
            job_id: Job identifier
            since_sequence: Optional sequence number to get chunks after
        
        Returns:
            List of chunk dictionaries, sorted by sequence
        """
        if not self.redis:
            return []
        
        try:
            # Find all chunk keys for this job
            pattern = f"{self.chunk_prefix}{job_id}:*"
            chunk_keys = []
            
            # Scan for keys (Redis SCAN for efficiency)
            cursor = 0
            while True:
                cursor, keys = self.redis.scan(
                    cursor,
                    match=pattern,
                    count=100
                )
                chunk_keys.extend(keys)
                if cursor == 0:
                    break
            
            # Retrieve and parse chunks
            chunks = []
            for key in chunk_keys:
                try:
                    chunk_data = self.redis.get(key)
                    if chunk_data:
                        chunk = json.loads(chunk_data)
                        # Filter by sequence if specified
                        if (since_sequence is None or
                                chunk.get('sequence', 0) > since_sequence):
                            chunks.append(chunk)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Failed to parse chunk {key}: {e}")
                    continue
            
            # Sort by sequence
            chunks.sort(key=lambda x: x.get('sequence', 0))
            
            return chunks
            
        except Exception as e:
            logger.error(f"Failed to retrieve chunks for job {job_id}: {e}")
            return []
    
    def subscribe_to_chunks(
        self,
        job_id: str,
        timeout: int = 300
    ):
        """
        Subscribe to chunks for a job (generator for SSE).
        
        Args:
            job_id: Job identifier
            timeout: Maximum time to wait for chunks (seconds)
        
        Yields:
            Chunk dictionaries as they arrive
        """
        if not self.redis:
            logger.warning("Redis not available for streaming subscription")
            return
        
        channel = f"{self.channel_prefix}{job_id}"
        pubsub = self.redis.pubsub()
        
        try:
            # Subscribe to channel
            pubsub.subscribe(channel)
            
            # First, send any existing chunks (replay)
            existing_chunks = self.get_chunks(job_id)
            for chunk in existing_chunks:
                yield chunk
                if chunk.get('is_final'):
                    return  # Already completed
            
            # Then listen for new chunks
            start_time = time.time()
            for message in pubsub.listen():
                if time.time() - start_time > timeout:
                    logger.warning(
                        f"Streaming timeout for job {job_id} "
                        f"after {timeout}s"
                    )
                    break
                
                if message['type'] == 'message':
                    try:
                        chunk = json.loads(message['data'])
                        yield chunk
                        
                        # Stop if final chunk
                        if chunk.get('is_final'):
                            break
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(
                            f"Failed to parse chunk message: {e}"
                        )
                        continue
                        
        except Exception as e:
            logger.error(
                f"Error in streaming subscription for job {job_id}: {e}"
            )
        finally:
            try:
                pubsub.unsubscribe(channel)
                pubsub.close()
            except Exception:
                pass
    
    def cleanup_chunks(self, job_id: str) -> bool:
        """
        Clean up stored chunks for a job.
        
        Args:
            job_id: Job identifier
        
        Returns:
            True if cleanup successful
        """
        if not self.redis:
            return False
        
        try:
            pattern = f"{self.chunk_prefix}{job_id}:*"
            cursor = 0
            deleted = 0
            
            while True:
                cursor, keys = self.redis.scan(
                    cursor,
                    match=pattern,
                    count=100
                )
                if keys:
                    deleted += self.redis.delete(*keys)
                if cursor == 0:
                    break
            
            logger.debug(
                f"Cleaned up {deleted} chunks for job {job_id}"
            )
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to cleanup chunks for job {job_id}: {e}"
            )
            return False


# Global instance
_streaming_service: Optional[StreamingService] = None


def get_streaming_service() -> StreamingService:
    """Get or create global streaming service instance"""
    global _streaming_service
    if _streaming_service is None:
        _streaming_service = StreamingService()
    return _streaming_service
