#!/usr/bin/env python3
"""
Embedding Usage Tracker
Tracks embedding generation patterns to identify high-traffic topics/concepts.
"""

import os
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

# Import Redis for persistent tracking
try:
    from services.redis_connection import get_redis_client, is_redis_available
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)


class EmbeddingUsageTracker:
    """
    Tracks embedding generation patterns to identify high-traffic topics/concepts.
    Uses Redis for persistent storage across worker restarts.
    """

    def __init__(self):
        """Initialize the usage tracker"""
        self.redis_client = None
        if REDIS_AVAILABLE and is_redis_available():
            try:
                self.redis_client = get_redis_client()
            except Exception as e:
                logger.warning(f"Failed to initialize Redis for usage tracking: {e}")

        # In-memory fallback (if Redis unavailable)
        self._in_memory_counts: Dict[str, int] = defaultdict(int)
        self._in_memory_timestamps: Dict[str, List[float]] = defaultdict(list)

        # Configuration
        self.TRACKING_WINDOW_HOURS = int(
            os.getenv("EMBEDDING_TRACKING_WINDOW_HOURS", 24)
        )  # 24 hours default
        self.MIN_ACCESS_COUNT = int(
            os.getenv("EMBEDDING_MIN_ACCESS_COUNT", 10)
        )  # Min accesses to be "high-traffic"

    def track_embedding_generation(
        self,
        text: str,
        context: Optional[Dict] = None
    ) -> None:
        """
        Track an embedding generation event.

        Args:
            text: The text that was embedded
            context: Optional context dict with:
                - topic_id: Topic ID if available
                - subject_id: Subject ID if available
                - concept_id: Concept ID if available
                - query_type: Type of query (e.g., "user_message", "question", "concept_name")
        """
        try:
            timestamp = time.time()
            context = context or {}

            # Create tracking keys
            # 1. By text hash (exact text matches)
            text_hash = self._hash_text(text)
            text_key = f"embedding_usage:text:{text_hash}"

            # 2. By topic (if available)
            topic_key = None
            if context.get("topic_id"):
                topic_id = str(context["topic_id"])
                subject_id = str(context.get("subject_id", "all"))
                topic_key = (
                    f"embedding_usage:topic:{subject_id}:{topic_id}"
                )

            # 3. By concept (if available)
            concept_key = None
            if context.get("concept_id"):
                concept_id = str(context["concept_id"])
                concept_key = f"embedding_usage:concept:{concept_id}"

            # 4. By query type (e.g., "user_message", "question")
            query_type = context.get("query_type", "unknown")
            query_type_key = f"embedding_usage:type:{query_type}"

            # Track in Redis if available
            if self.redis_client:
                try:
                    # Use sorted sets for efficient time-window queries
                    # Score = timestamp, member = text_hash or topic_id
                    now = int(timestamp)

                    # Track text usage
                    self.redis_client.zadd(
                        text_key,
                        {text_hash: now}
                    )
                    # Set expiry on key (tracking window)
                    self.redis_client.expire(
                        text_key,
                        self.TRACKING_WINDOW_HOURS * 3600
                    )

                    # Track topic usage
                    if topic_key:
                        self.redis_client.zadd(
                            topic_key,
                            {topic_id: now}
                        )
                        self.redis_client.expire(
                            topic_key,
                            self.TRACKING_WINDOW_HOURS * 3600
                        )

                    # Track concept usage
                    if concept_key:
                        self.redis_client.zadd(
                            concept_key,
                            {concept_id: now}
                        )
                        self.redis_client.expire(
                            concept_key,
                            self.TRACKING_WINDOW_HOURS * 3600
                        )

                    # Track query type usage
                    self.redis_client.zadd(
                        query_type_key,
                        {query_type: now}
                    )
                    self.redis_client.expire(
                        query_type_key,
                        self.TRACKING_WINDOW_HOURS * 3600
                    )

                except Exception as e:
                    logger.warning(
                        f"Failed to track embedding usage in Redis: {e}"
                    )
                    # Fallback to in-memory
                    self._track_in_memory(
                        text_hash, timestamp, topic_key, concept_key
                    )

            else:
                # Fallback to in-memory tracking
                self._track_in_memory(text_hash, timestamp, topic_key, concept_key)

        except Exception as e:
            logger.warning(f"Error tracking embedding usage: {e}")

    def _track_in_memory(
        self,
        text_hash: str,
        timestamp: float,
        topic_key: Optional[str],
        concept_key: Optional[str]
    ) -> None:
        """Fallback in-memory tracking"""
        # Track text
        self._in_memory_counts[text_hash] += 1
        self._in_memory_timestamps[text_hash].append(timestamp)

        # Clean old timestamps (outside tracking window)
        cutoff = timestamp - (self.TRACKING_WINDOW_HOURS * 3600)
        self._in_memory_timestamps[text_hash] = [
            ts for ts in self._in_memory_timestamps[text_hash]
            if ts > cutoff
        ]

    def _hash_text(self, text: str) -> str:
        """Generate a stable hash for text"""
        import hashlib
        # Normalize text (lowercase, strip whitespace)
        normalized = text.lower().strip()
        return hashlib.md5(normalized.encode()).hexdigest()[:16]

    def get_high_traffic_topics(
        self,
        limit: int = 100,
        min_access_count: Optional[int] = None
    ) -> List[Tuple[str, int, Optional[int]]]:
        """
        Get high-traffic topics based on embedding generation frequency.

        Args:
            limit: Maximum number of topics to return
            min_access_count: Minimum access count (defaults to self.MIN_ACCESS_COUNT)

        Returns:
            List of tuples: (topic_id, access_count, subject_id)
            Sorted by access count (descending)
        """
        min_count = min_access_count or self.MIN_ACCESS_COUNT
        cutoff_time = time.time() - (self.TRACKING_WINDOW_HOURS * 3600)

        high_traffic = []

        if self.redis_client:
            try:
                # Scan for all topic keys
                pattern = "embedding_usage:topic:*"
                cursor = 0
                topic_keys = []

                while True:
                    cursor, keys = self.redis_client.scan(
                        cursor,
                        match=pattern,
                        count=100
                    )
                    topic_keys.extend(keys)
                    if cursor == 0:
                        break

                # Count accesses per topic
                for key in topic_keys:
                    try:
                        # Count members in time window
                        count = self.redis_client.zcount(
                            key, cutoff_time, time.time()
                        )
                        if count >= min_count:
                            # Extract topic_id and subject_id from key
                            # Format: embedding_usage:topic:{subject_id}:{topic_id}
                            parts = key.split(":")
                            if len(parts) >= 4:
                                subject_id = (
                                    parts[3] if parts[3] != "all" else None
                                )
                                topic_id = (
                                    parts[4] if len(parts) > 4 else parts[3]
                                )
                                subject_id_int = (
                                    int(subject_id)
                                    if subject_id and subject_id.isdigit()
                                    else None
                                )
                                high_traffic.append((
                                    topic_id,
                                    count,
                                    subject_id_int
                                ))
                    except Exception as e:
                        logger.warning(
                            f"Error processing topic key {key}: {e}"
                        )
                        continue

            except Exception as e:
                logger.warning(f"Error getting high-traffic topics from Redis: {e}")

        # Sort by access count (descending)
        high_traffic.sort(key=lambda x: x[1], reverse=True)

        return high_traffic[:limit]

    def get_high_traffic_texts(
        self,
        limit: int = 200,
        min_access_count: Optional[int] = None
    ) -> List[Tuple[str, int]]:
        """
        Get high-traffic texts based on embedding generation frequency.

        Args:
            limit: Maximum number of texts to return
            min_access_count: Minimum access count

        Returns:
            List of tuples: (text_hash, access_count)
            Sorted by access count (descending)
        """
        min_count = min_access_count or self.MIN_ACCESS_COUNT
        cutoff_time = time.time() - (self.TRACKING_WINDOW_HOURS * 3600)

        high_traffic = []

        if self.redis_client:
            try:
                # Scan for all text keys
                pattern = "embedding_usage:text:*"
                cursor = 0
                text_keys = []

                while True:
                    cursor, keys = self.redis_client.scan(
                        cursor,
                        match=pattern,
                        count=100
                    )
                    text_keys.extend(keys)
                    if cursor == 0:
                        break

                # Count accesses per text
                for key in text_keys:
                    try:
                        count = self.redis_client.zcount(
                            key, cutoff_time, time.time()
                        )
                        if count >= min_count:
                            # Extract text_hash from key
                            text_hash = key.split(":")[-1]
                            high_traffic.append((text_hash, count))
                    except Exception as e:
                        logger.warning(
                            f"Error processing text key {key}: {e}"
                        )
                        continue

            except Exception as e:
                logger.warning(f"Error getting high-traffic texts from Redis: {e}")

        # Sort by access count (descending)
        high_traffic.sort(key=lambda x: x[1], reverse=True)

        return high_traffic[:limit]

    def get_usage_stats(self) -> Dict[str, Any]:  # noqa: F821
        """
        Get overall usage statistics.

        Returns:
            Dict with:
                - total_embeddings_generated: Total count
                - high_traffic_topics_count: Number of high-traffic topics
                - high_traffic_texts_count: Number of high-traffic texts
                - tracking_window_hours: Current tracking window
        """
        high_traffic_topics = self.get_high_traffic_topics(limit=1000)
        high_traffic_texts = self.get_high_traffic_texts(limit=1000)

        return {
            "high_traffic_topics_count": len(high_traffic_topics),
            "high_traffic_texts_count": len(high_traffic_texts),
            "tracking_window_hours": self.TRACKING_WINDOW_HOURS,
            "min_access_count": self.MIN_ACCESS_COUNT,
            "top_topics": high_traffic_topics[:10],
            "top_texts": high_traffic_texts[:10]
        }


# Singleton instance
_usage_tracker: Optional[EmbeddingUsageTracker] = None


def get_usage_tracker() -> EmbeddingUsageTracker:
    """Get or create the singleton usage tracker"""
    global _usage_tracker
    if _usage_tracker is None:
        _usage_tracker = EmbeddingUsageTracker()
    return _usage_tracker
