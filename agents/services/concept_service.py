#!/usr/bin/env python3
"""
Concept Service - Handles concept-related operations
Wraps ConceptAgent internally.
"""

import hashlib
import logging
import os
from typing import Optional, List, Dict

# Import centralized caching
try:
    from services.deterministic_cache import (
        cached_operation, CacheOperation, CacheTTL, CacheMetrics,
        invalidate_cache, generate_cache_key
    )
    DETERMINISTIC_CACHE_AVAILABLE = True
except ImportError:
    DETERMINISTIC_CACHE_AVAILABLE = False
    def cached_operation(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    CacheOperation = None
    CacheTTL = None

logger = logging.getLogger(__name__)


class ConceptService:
    """
    Handles embeddings, concept similarity search, and concept metadata.
    Wraps ConceptAgent internally.
    """

    def __init__(self, concept_agent, cache_get=None, cache_set=None):
        """
        Initialize ConceptService.

        Args:
            concept_agent: ConceptAgent instance to wrap
            cache_get: Optional cache get function
            cache_set: Optional cache set function
        """
        self.concept_agent = concept_agent
        self.cache_get = cache_get
        self.cache_set = cache_set
        self.logger = logging.getLogger(__name__)

    def generate_embedding(
        self,
        text: str,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> Optional[List[float]]:
        """
        Generate an embedding vector for a text string.
        Delegates to ConceptAgent.

        Args:
            text: Text to generate embedding for
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for distributed tracing

        Returns:
            List of floats (embedding vector) or None
        """
        if not self.concept_agent:
            return None
        return self.concept_agent.generate_embedding(
            text, job_id=job_id, trace_id=trace_id
        )

    def find_related_concepts(
        self,
        message_text: str,
        subject_id: Optional[str] = None,
        topic_id: Optional[str] = None,
        k: int = 5,
        min_similarity: Optional[float] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> List[Dict]:
        """
        Find related concepts using pgvector similarity search.
        Delegates to ConceptAgent.retrieve_concepts().
        Uses centralized caching with configurable TTL.

        Args:
            message_text: User message to search for
            subject_id: Optional subject ID filter
            topic_id: Optional topic ID filter
            k: Number of concepts to retrieve (default: 5)
            min_similarity: Minimum similarity threshold (default: None)
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for instrumentation

        Returns:
            List of concept dicts with concept_id, name, description, distance
        """
        if not self.concept_agent:
            return []

        # Use centralized caching if available
        if DETERMINISTIC_CACHE_AVAILABLE:
            @cached_operation(
                CacheOperation.CONCEPT_SEARCH,
                ttl=CacheTTL.CONCEPT_SEARCH,
                job_id=job_id,
                trace_id=trace_id
            )
            def _fetch_concepts():
                return self.concept_agent.retrieve_concepts(
                    message_text, subject_id, topic_id, k, min_similarity
                )
            
            return _fetch_concepts()
        
        # Fallback to legacy caching
        if self.cache_get:
            message_hash = hashlib.md5(
                message_text.encode()
            ).hexdigest()[:8]
            subject_key = subject_id or "all"
            topic_key = topic_id or "all"
            similarity_key = f"{min_similarity}" if min_similarity else "none"
            cache_key = (
                f"concept_rag:{subject_key}:{topic_key}:"
                f"{similarity_key}:{k}:{message_hash}"
            )
            cached = self.cache_get(cache_key)
            if cached is not None:
                if os.getenv("DEBUG", "0") == "1":
                    self.logger.info(f"Cache hit for concept RAG: {cache_key}")
                return cached

        # Fetch from ConceptAgent
        concepts = self.concept_agent.retrieve_concepts(
            message_text, subject_id, topic_id, k, min_similarity
        )

        # Cache the result (60 seconds TTL for RAG)
        if self.cache_set and concepts:
            message_hash = hashlib.md5(
                message_text.encode()
            ).hexdigest()[:8]
            subject_key = subject_id or "all"
            topic_key = topic_id or "all"
            similarity_key = f"{min_similarity}" if min_similarity else "none"
            cache_key = (
                f"concept_rag:{subject_key}:{topic_key}:"
                f"{similarity_key}:{k}:{message_hash}"
            )
            self.cache_set(cache_key, concepts, ttl=60)

        return concepts

    def keyword_match(
        self,
        message_text: str,
        subject_id: Optional[str] = None,
        topic_id: Optional[str] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> List[Dict]:
        """
        Fallback keyword-based concept search when embedding search returns
        no results. Searches for concepts by matching keywords in message
        against concept and explanation columns.
        Uses subject-specific concept tables.
        Uses centralized caching with configurable TTL.

        Args:
            message_text: User message to search for
            subject_id: Optional subject ID filter (can be str or int)
            topic_id: Optional topic ID filter (from topic selection)
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for instrumentation

        Returns:
            List of concept dicts with concept_id, name, description, distance
        """
        if not self.concept_agent:
            return []

        # Use centralized caching if available
        if DETERMINISTIC_CACHE_AVAILABLE:
            @cached_operation(
                CacheOperation.CONCEPT_KEYWORD_MATCH,
                ttl=CacheTTL.CONCEPT_KEYWORD_MATCH,
                job_id=job_id,
                trace_id=trace_id
            )
            def _keyword_match():
                return self.concept_agent.keyword_match(
                    message_text, subject_id, topic_id
                )
            
            return _keyword_match()
        
        # Fallback to direct call
        return self.concept_agent.keyword_match(
            message_text, subject_id, topic_id
        )

    def fetch_concepts_by_topic(
        self,
        topic_id: str,
        limit: int = 10,
        random_order: bool = True,
        subject_id: Optional[int] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> List[Dict]:
        """
        Fetch concepts directly from the database by topic_id.
        Returns concepts in random order for variety.
        Uses subject-specific concept tables.
        Uses centralized caching with configurable TTL.

        Args:
            topic_id: Topic ID to fetch concepts for
            limit: Maximum number of concepts to return (default: 5)
            random_order: Whether to return concepts in random order
                (default: True)
            subject_id: Optional subject ID to determine which concept table to use
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for instrumentation

        Returns:
            List of concept dicts with concept_id, name (concept),
            description (explanation)
        """
        if not self.concept_agent:
            self.logger.warning(f"[CONCEPT SERVICE] ConceptAgent is None - cannot fetch concepts for topic_id={topic_id}, subject_id={subject_id}")
            return []
        
        self.logger.info(f"[CONCEPT SERVICE] Fetching concepts for topic_id={topic_id}, subject_id={subject_id}, limit={limit}")

        # Use centralized caching if available
        if DETERMINISTIC_CACHE_AVAILABLE:
            # CRITICAL FIX: Pass topic_id and subject_id as arguments to the decorator
            # so they're included in the cache key. This prevents cache collisions
            # between different subjects for the same topic_id.
            @cached_operation(
                CacheOperation.CONCEPT_BY_TOPIC,
                ttl=CacheTTL.CONCEPT_BY_TOPIC,
                job_id=job_id,
                trace_id=trace_id
            )
            def _fetch_by_topic(topic_id_arg, subject_id_arg, limit_arg, random_order_arg):
                return self.concept_agent.fetch_concepts_by_topic(
                    topic_id_arg, limit_arg, random_order_arg, subject_id_arg
                )
            
            # Call with explicit arguments so they're included in cache key
            return _fetch_by_topic(topic_id, subject_id, limit, random_order)
        
        # Fallback to direct call
        return self.concept_agent.fetch_concepts_by_topic(
            topic_id, limit, random_order, subject_id
        )

    def fetch_concept_details(
        self, concept_ids: List[str]
    ) -> Dict[str, Dict]:
        """
        Fetch metadata (name, description) for given concept_ids.
        Delegates to ConceptAgent.

        Args:
            concept_ids: List of concept IDs to fetch

        Returns:
            Dict mapping concept_id to {"name": str, "description": str}
        """
        if not self.concept_agent:
            return {}
        return self.concept_agent.fetch_concept_details(concept_ids)

    def get_prerequisites_and_next_concepts(
        self, concept_ids: List[str]
    ) -> Dict:
        """
        Fetch prerequisite concepts and next-step concepts.
        Delegates to ConceptAgent.

        Args:
            concept_ids: List of concept IDs to get prerequisites/next for

        Returns:
            Dict with "prerequisites" and "next_concepts" lists
        """
        if not self.concept_agent:
            return {
                "prerequisites": [],
                "next_concepts": []
            }
        return self.concept_agent.get_prerequisites_and_next_concepts(
            concept_ids
        )

    def refresh_embedding(self, concept_id: str) -> bool:
        """
        Refresh the embedding for a concept by regenerating it.

        Args:
            concept_id: Concept ID to refresh embedding for

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.concept_agent or not self.concept_agent.supabase:
            return False

        try:
            # Fetch concept details
            concept_details = self.concept_agent.fetch_concept_details(
                [concept_id]
            )
            if not concept_details or concept_id not in concept_details:
                return False

            concept = concept_details[concept_id]
            # Combine name and description for embedding
            name = concept.get('name', '')
            desc = concept.get('description', '')
            text_to_embed = f"{name} {desc}".strip()

            if not text_to_embed:
                return False

            # Generate new embedding
            embedding = self.concept_agent.generate_embedding(text_to_embed)
            if embedding is None:
                return False

            # Update in database
            self.concept_agent.supabase.table("concepts").update({
                "embedding": embedding,
                "updated_at": "now()"
            }).eq("concept_id", concept_id).execute()

            # Invalidate pre-generated embeddings for this concept
            try:
                from services.embedding_pregen import get_pregen_service
                pregen_service = get_pregen_service(
                    self.concept_agent,
                    self.concept_agent.supabase
                )
                # Invalidate embeddings for concept name and description
                concept = concept_details[concept_id]
                name = concept.get('name', '')
                desc = concept.get('description', '')
                if name:
                    pregen_service.invalidate_embedding(name)
                if name and desc:
                    combined = f"{name} {desc}".strip()
                    pregen_service.invalidate_embedding(combined)
            except Exception as e:
                self.logger.warning(
                    f"Failed to invalidate pre-generated embeddings: {e}"
                )

            if os.getenv("DEBUG", "0") == "1":
                self.logger.info(
                    f"Refreshed embedding for concept: {concept_id}"
                )
            return True

        except Exception as e:
            self.logger.error(f"Error refreshing embedding: {e}")
            return False
