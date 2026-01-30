#!/usr/bin/env python3
"""
Embedding Pre-generation Service
Pre-generates embeddings for high-traffic topics/concepts in the background.
"""

import os
import json
import time
import logging
import hashlib
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

# Import cache
try:
    from cache import cache_get, cache_set, cache_delete
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    def cache_get(key): return None
    def cache_set(key, value, ttl=3600): return False
    def cache_delete(key): return False

# Import usage tracker
try:
    from services.embedding_usage_tracker import get_usage_tracker
    USAGE_TRACKER_AVAILABLE = True
except ImportError:
    USAGE_TRACKER_AVAILABLE = False
    def get_usage_tracker(): return None

# Import concept agent for embedding generation
try:
    from agents.concept_agent import ConceptAgent
    CONCEPT_AGENT_AVAILABLE = True
except ImportError:
    CONCEPT_AGENT_AVAILABLE = False

logger = logging.getLogger(__name__)


class EmbeddingPreGenerator:
    """
    Pre-generates embeddings for high-traffic topics/concepts.
    Runs as a background job, rate-limited and safe for incremental execution.
    """

    def __init__(
        self,
        concept_agent: Optional[ConceptAgent] = None,
        supabase_client: Optional[Any] = None
    ):
        """
        Initialize the embedding pre-generator.

        Args:
            concept_agent: ConceptAgent instance for embedding generation
            supabase_client: Supabase client for fetching topic/concept data
        """
        self.concept_agent = concept_agent
        self.supabase = supabase_client
        self.usage_tracker = (
            get_usage_tracker() if USAGE_TRACKER_AVAILABLE else None
        )

        # Configuration
        self.MAX_EMBEDDINGS_PER_BATCH = int(
            os.getenv("EMBEDDING_PREGEN_BATCH_SIZE", 50)
        )  # Generate 50 embeddings per batch
        self.RATE_LIMIT_DELAY_SECONDS = float(
            os.getenv("EMBEDDING_PREGEN_RATE_LIMIT_DELAY", 0.1)
        )  # 100ms delay between embeddings (10 per second)
        self.EMBEDDING_CACHE_TTL = int(
            os.getenv("EMBEDDING_PREGEN_CACHE_TTL", 604800)
        )  # 7 days default
        self.EMBEDDING_VERSION_KEY = "embedding_version"
        self.EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

    def generate_embedding_key(self, text: str) -> str:
        """
        Generate a cache key for a pre-generated embedding.

        Args:
            text: Text to generate key for

        Returns:
            Cache key string
        """
        # Normalize text (lowercase, strip whitespace)
        normalized = text.lower().strip()
        text_hash = hashlib.md5(normalized.encode()).hexdigest()[:16]
        model_version = self.EMBEDDING_MODEL.replace("-", "_")
        return f"pregenerated_embedding:{model_version}:{text_hash}"

    def get_pre_generated_embedding(self, text: str) -> Optional[List[float]]:
        """
        Get a pre-generated embedding if available.

        Args:
            text: Text to get embedding for

        Returns:
            Embedding vector (list of floats) or None if not pre-generated
        """
        if not CACHE_AVAILABLE:
            return None

        try:
            cache_key = self.generate_embedding_key(text)
            cached = cache_get(cache_key)
            if cached:
                # Handle both dict format (with metadata) and list format (legacy)
                if isinstance(cached, dict):
                    embedding = cached.get("embedding")
                    if embedding and isinstance(embedding, list) and len(embedding) > 0:
                        logger.debug(
                            f"[EMBEDDING CACHE HIT] Pre-generated embedding found "
                            f"for text hash: {cache_key.split(':')[-1][:8]}"
                        )
                        return embedding
                elif isinstance(cached, list) and len(cached) > 0:
                    # Legacy format: direct list
                    logger.debug(
                        f"[EMBEDDING CACHE HIT] Pre-generated embedding found "
                        f"for text hash: {cache_key.split(':')[-1][:8]}"
                    )
                    return cached
        except Exception as e:
            logger.warning(f"Error getting pre-generated embedding: {e}")

        return None

    def store_pre_generated_embedding(
        self,
        text: str,
        embedding: List[float],
        version: Optional[str] = None
    ) -> bool:
        """
        Store a pre-generated embedding in cache.

        Args:
            text: Text that was embedded
            embedding: Embedding vector
            version: Optional version string for invalidation

        Returns:
            bool: True if stored successfully
        """
        if not CACHE_AVAILABLE:
            return False

        try:
            cache_key = self.generate_embedding_key(text)
            # Store with metadata
            cache_data = {
                "embedding": embedding,
                "text_hash": hashlib.md5(text.lower().strip().encode()).hexdigest()[:16],
                "model": self.EMBEDDING_MODEL,
                "version": version or "1.0",
                "generated_at": datetime.now().isoformat()
            }
            cache_set(cache_key, cache_data, ttl=self.EMBEDDING_CACHE_TTL)
            logger.debug(
                f"[EMBEDDING PREGEN] Stored pre-generated embedding for "
                f"text hash: {cache_key.split(':')[-1][:8]}"
            )
            return True
        except Exception as e:
            logger.warning(f"Error storing pre-generated embedding: {e}")
            return False

    def invalidate_embedding(self, text: str) -> bool:
        """
        Invalidate a pre-generated embedding (e.g., when content changes).

        Args:
            text: Text whose embedding should be invalidated

        Returns:
            bool: True if invalidated successfully
        """
        if not CACHE_AVAILABLE:
            return False

        try:
            cache_key = self.generate_embedding_key(text)
            cache_delete(cache_key)
            logger.info(
                f"[EMBEDDING INVALIDATE] Invalidated pre-generated embedding "
                f"for text hash: {cache_key.split(':')[-1][:8]}"
            )
            return True
        except Exception as e:
            logger.warning(f"Error invalidating embedding: {e}")
            return False

    def pre_generate_for_topics(
        self,
        topic_ids: List[Tuple[str, Optional[int]]],
        max_embeddings: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Pre-generate embeddings for high-traffic topics.

        Args:
            topic_ids: List of (topic_id, subject_id) tuples
            max_embeddings: Maximum number of embeddings to generate (defaults to batch size)

        Returns:
            Dict with:
                - generated_count: Number of embeddings generated
                - skipped_count: Number skipped (already cached)
                - failed_count: Number that failed
                - topics_processed: Number of topics processed
        """
        if not self.concept_agent or not self.supabase:
            logger.warning(
                "Cannot pre-generate embeddings: concept_agent or supabase not available"
            )
            return {
                "generated_count": 0,
                "skipped_count": 0,
                "failed_count": 0,
                "topics_processed": 0
            }

        max_count = max_embeddings or self.MAX_EMBEDDINGS_PER_BATCH
        generated = 0
        skipped = 0
        failed = 0
        topics_processed = 0

        logger.info(
            f"[EMBEDDING PREGEN] Starting pre-generation for {len(topic_ids)} topics "
            f"(max {max_count} embeddings)"
        )

        for topic_id, subject_id in topic_ids:
            if generated >= max_count:
                break

            try:
                # Fetch topic name
                topic_name = self._fetch_topic_name(topic_id, subject_id)
                if not topic_name:
                    logger.warning(
                        f"[EMBEDDING PREGEN] Could not fetch topic name for "
                        f"topic_id: {topic_id}, subject_id: {subject_id}"
                    )
                    failed += 1
                    continue

                # Check if already pre-generated
                cached = self.get_pre_generated_embedding(topic_name)
                if cached:
                    skipped += 1
                    continue

                # Generate embedding
                embedding = self.concept_agent.generate_embedding(topic_name)
                if embedding is None:
                    logger.warning(
                        f"[EMBEDDING PREGEN] Failed to generate embedding for "
                        f"topic: {topic_name}"
                    )
                    failed += 1
                    continue

                # Store pre-generated embedding
                stored = self.store_pre_generated_embedding(
                    topic_name,
                    embedding,
                    version=self._get_content_version(topic_id, subject_id)
                )
                if stored:
                    generated += 1
                    logger.info(
                        f"[EMBEDDING PREGEN] Pre-generated embedding for "
                        f"topic: {topic_name} (topic_id: {topic_id})"
                    )
                else:
                    failed += 1

                # Rate limiting: delay between embeddings
                time.sleep(self.RATE_LIMIT_DELAY_SECONDS)

                topics_processed += 1

            except Exception as e:
                logger.error(
                    f"[EMBEDDING PREGEN] Error processing topic {topic_id}: {e}",
                    exc_info=True
                )
                failed += 1
                continue

        logger.info(
            f"[EMBEDDING PREGEN] Completed: generated={generated}, "
            f"skipped={skipped}, failed={failed}, topics={topics_processed}"
        )

        return {
            "generated_count": generated,
            "skipped_count": skipped,
            "failed_count": failed,
            "topics_processed": topics_processed
        }

    def pre_generate_for_concepts(
        self,
        concept_ids: List[str],
        max_embeddings: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Pre-generate embeddings for concept names/descriptions.

        Args:
            concept_ids: List of concept IDs
            max_embeddings: Maximum number of embeddings to generate

        Returns:
            Dict with generation statistics
        """
        if not self.concept_agent or not self.supabase:
            return {
                "generated_count": 0,
                "skipped_count": 0,
                "failed_count": 0,
                "concepts_processed": 0
            }

        max_count = max_embeddings or self.MAX_EMBEDDINGS_PER_BATCH
        generated = 0
        skipped = 0
        failed = 0
        concepts_processed = 0

        logger.info(
            f"[EMBEDDING PREGEN] Starting pre-generation for {len(concept_ids)} concepts "
            f"(max {max_count} embeddings)"
        )

        for concept_id in concept_ids:
            if generated >= max_count:
                break

            try:
                # Fetch concept details
                concept_details = self.concept_agent.fetch_concept_details([concept_id])
                if not concept_details or concept_id not in concept_details:
                    logger.warning(
                        f"[EMBEDDING PREGEN] Could not fetch concept details for "
                        f"concept_id: {concept_id}"
                    )
                    failed += 1
                    continue

                concept = concept_details[concept_id]
                name = concept.get("name", "")
                description = concept.get("description", "")

                # Generate embeddings for both name and combined text
                texts_to_embed = []
                if name:
                    texts_to_embed.append(("concept_name", name))
                if name and description:
                    combined = f"{name} {description}".strip()
                    texts_to_embed.append(("concept_combined", combined))

                for text_type, text in texts_to_embed:
                    if generated >= max_count:
                        break

                    # Check if already pre-generated
                    cached = self.get_pre_generated_embedding(text)
                    if cached:
                        skipped += 1
                        continue

                    # Generate embedding
                    embedding = self.concept_agent.generate_embedding(text)
                    if embedding is None:
                        logger.warning(
                            f"[EMBEDDING PREGEN] Failed to generate embedding for "
                            f"concept {concept_id}, text_type: {text_type}"
                        )
                        failed += 1
                        continue

                    # Store pre-generated embedding
                    stored = self.store_pre_generated_embedding(
                        text,
                        embedding,
                        version=self._get_content_version(concept_id=concept_id)
                    )
                    if stored:
                        generated += 1
                        logger.info(
                            f"[EMBEDDING PREGEN] Pre-generated embedding for "
                            f"concept: {concept_id}, type: {text_type}"
                        )

                    # Rate limiting
                    time.sleep(self.RATE_LIMIT_DELAY_SECONDS)

                concepts_processed += 1

            except Exception as e:
                logger.error(
                    f"[EMBEDDING PREGEN] Error processing concept {concept_id}: {e}",
                    exc_info=True
                )
                failed += 1
                continue

        logger.info(
            f"[EMBEDDING PREGEN] Completed: generated={generated}, "
            f"skipped={skipped}, failed={failed}, concepts={concepts_processed}"
        )

        return {
            "generated_count": generated,
            "skipped_count": skipped,
            "failed_count": failed,
            "concepts_processed": concepts_processed
        }

    def pre_generate_for_common_queries(
        self,
        common_queries: List[str],
        max_embeddings: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Pre-generate embeddings for common query patterns.

        Args:
            common_queries: List of common query texts
            max_embeddings: Maximum number of embeddings to generate

        Returns:
            Dict with generation statistics
        """
        if not self.concept_agent:
            return {
                "generated_count": 0,
                "skipped_count": 0,
                "failed_count": 0,
                "queries_processed": 0
            }

        max_count = max_embeddings or self.MAX_EMBEDDINGS_PER_BATCH
        generated = 0
        skipped = 0
        failed = 0
        queries_processed = 0

        logger.info(
            f"[EMBEDDING PREGEN] Starting pre-generation for {len(common_queries)} queries "
            f"(max {max_count} embeddings)"
        )

        for query_text in common_queries:
            if generated >= max_count:
                break

            try:
                # Check if already pre-generated
                cached = self.get_pre_generated_embedding(query_text)
                if cached:
                    skipped += 1
                    queries_processed += 1
                    continue

                # Generate embedding
                embedding = self.concept_agent.generate_embedding(query_text)
                if embedding is None:
                    logger.warning(
                        f"[EMBEDDING PREGEN] Failed to generate embedding for query: "
                        f"{query_text[:50]}..."
                    )
                    failed += 1
                    queries_processed += 1
                    continue

                # Store pre-generated embedding
                stored = self.store_pre_generated_embedding(
                    query_text,
                    embedding,
                    version="1.0"
                )
                if stored:
                    generated += 1
                    logger.info(
                        f"[EMBEDDING PREGEN] Pre-generated embedding for query: "
                        f"{query_text[:50]}..."
                    )

                # Rate limiting
                time.sleep(self.RATE_LIMIT_DELAY_SECONDS)

                queries_processed += 1

            except Exception as e:
                logger.error(
                    f"[EMBEDDING PREGEN] Error processing query: {e}",
                    exc_info=True
                )
                failed += 1
                queries_processed += 1
                continue

        logger.info(
            f"[EMBEDDING PREGEN] Completed: generated={generated}, "
            f"skipped={skipped}, failed={failed}, queries={queries_processed}"
        )

        return {
            "generated_count": generated,
            "skipped_count": skipped,
            "failed_count": failed,
            "queries_processed": queries_processed
        }

    def _fetch_topic_name(
        self,
        topic_id: str,
        subject_id: Optional[int]
    ) -> Optional[str]:
        """Fetch topic name from database"""
        if not self.supabase:
            return None

        try:
            # Determine table name based on subject_id
            table_name = "topics"
            if subject_id == 102:  # Islamiyat
                table_name = "topics_isl"
            elif subject_id == 113:  # Geography
                table_name = "topics_geography"
            elif subject_id == 114:  # History
                table_name = "topics_history"
            elif subject_id == 119:  # Economics
                table_name = "topics_economics"

            topic_id_int = int(topic_id) if isinstance(topic_id, str) else topic_id
            result = (
                self.supabase.table(table_name)
                .select("topic")
                .eq("topic_id", topic_id_int)
                .limit(1)
                .execute()
            )

            if result.data and len(result.data) > 0:
                return result.data[0].get("topic")
        except Exception as e:
            logger.warning(f"Error fetching topic name: {e}")

        return None

    def _get_content_version(
        self,
        topic_id: Optional[str] = None,
        subject_id: Optional[int] = None,
        concept_id: Optional[str] = None
    ) -> str:
        """
        Get content version for embedding invalidation.
        Uses updated_at timestamp from database if available.

        Args:
            topic_id: Optional topic ID
            concept_id: Optional concept ID

        Returns:
            Version string (timestamp-based)
        """
        if not self.supabase:
            return "1.0"

        try:
            if topic_id and subject_id:
                # Get topic updated_at
                table_name = "topics"
                if subject_id == 102:
                    table_name = "topics_isl"
                elif subject_id == 113:
                    table_name = "topics_geography"
                elif subject_id == 114:
                    table_name = "topics_history"
                elif subject_id == 119:
                    table_name = "topics_economics"

                topic_id_int = int(topic_id) if isinstance(topic_id, str) else topic_id
                result = (
                    self.supabase.table(table_name)
                    .select("updated_at")
                    .eq("topic_id", topic_id_int)
                    .limit(1)
                    .execute()
                )
                if result.data and result.data[0].get("updated_at"):
                    return result.data[0]["updated_at"]

            elif concept_id:
                # Get concept updated_at
                result = (
                    self.supabase.table("concepts")
                    .select("updated_at")
                    .eq("concept_id", concept_id)
                    .limit(1)
                    .execute()
                )
                if result.data and result.data[0].get("updated_at"):
                    return result.data[0]["updated_at"]

        except Exception as e:
            logger.debug(f"Error getting content version: {e}")

        # Fallback: use current timestamp
        return datetime.now().isoformat()

    def run_background_pre_generation(
        self,
        max_embeddings: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Run background pre-generation for high-traffic topics/concepts.

        Args:
            max_embeddings: Maximum total embeddings to generate

        Returns:
            Dict with overall statistics
        """
        if not self.usage_tracker:
            logger.warning(
                "Usage tracker not available - cannot identify high-traffic items"
            )
            return {
                "total_generated": 0,
                "total_skipped": 0,
                "total_failed": 0,
                "topics_processed": 0,
                "concepts_processed": 0,
                "queries_processed": 0
            }

        max_total = max_embeddings or self.MAX_EMBEDDINGS_PER_BATCH
        total_generated = 0
        total_skipped = 0
        total_failed = 0

        # 1. Pre-generate for high-traffic topics
        high_traffic_topics = self.usage_tracker.get_high_traffic_topics(
            limit=50,
            min_access_count=5  # Lower threshold for pre-generation
        )
        if high_traffic_topics:
            topic_ids = [(topic_id, subject_id) for topic_id, _, subject_id in high_traffic_topics]
            remaining = max_total - total_generated
            if remaining > 0:
                topic_stats = self.pre_generate_for_topics(
                    topic_ids,
                    max_embeddings=remaining
                )
                total_generated += topic_stats["generated_count"]
                total_skipped += topic_stats["skipped_count"]
                total_failed += topic_stats["failed_count"]
                topics_processed = topic_stats["topics_processed"]
            else:
                topics_processed = 0
        else:
            topics_processed = 0

        # 2. Pre-generate for common query patterns
        # (Could be extended to track common user messages)
        common_queries = [
            "What is",
            "Explain",
            "How does",
            "Define",
            "Describe",
            "Compare",
            "What are the",
            "Why is",
            "How can",
            "What are"
        ]
        remaining = max_total - total_generated
        if remaining > 0:
            query_stats = self.pre_generate_for_common_queries(
                common_queries,
                max_embeddings=min(remaining, 10)  # Limit common queries
            )
            total_generated += query_stats["generated_count"]
            total_skipped += query_stats["skipped_count"]
            total_failed += query_stats["failed_count"]
            queries_processed = query_stats["queries_processed"]
        else:
            queries_processed = 0

        logger.info(
            f"[EMBEDDING PREGEN] Background pre-generation completed: "
            f"generated={total_generated}, skipped={total_skipped}, "
            f"failed={total_failed}"
        )

        return {
            "total_generated": total_generated,
            "total_skipped": total_skipped,
            "total_failed": total_failed,
            "topics_processed": topics_processed,
            "concepts_processed": 0,  # Could be extended
            "queries_processed": queries_processed
        }


# Singleton instance
_pregen_service: Optional[EmbeddingPreGenerator] = None


def get_pregen_service(
    concept_agent: Optional[ConceptAgent] = None,
    supabase_client: Optional[Any] = None
) -> EmbeddingPreGenerator:
    """Get or create the singleton pre-generation service"""
    global _pregen_service
    if _pregen_service is None:
        _pregen_service = EmbeddingPreGenerator(concept_agent, supabase_client)
    return _pregen_service
