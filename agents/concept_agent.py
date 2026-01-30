#!/usr/bin/env python3
"""
Concept Agent - Handles concept retrieval, embeddings, and concept graph
"""

import os
from typing import Any, Dict, List, Optional
from openai import OpenAI
import logging
import hashlib

# Import cache
try:
    from cache import cache_get, cache_set, _hash_string
except ImportError:
    # Fallback if cache not available
    def cache_get(key): return None
    def cache_set(key, value, ttl=3600): return False
    def _hash_string(text): return hashlib.md5(text.encode()).hexdigest()[:12]

logger = logging.getLogger(__name__)


class ConceptAgent:
    """
    Agent responsible for concept-related operations:
    - Embedding generation
    - Concept retrieval via pgvector
    - Lesson chunk retrieval
    - Concept graph (prerequisites/next concepts)
    """

    def __init__(
        self,
        api_key: str = None,
        supabase_client: Optional[Any] = None
    ):
        """
        Initialize Concept Agent

        Args:
            api_key: OpenAI API key for embeddings
            supabase_client: Supabase client instance
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self._embed_client = OpenAI(api_key=self.api_key)
        self.supabase = supabase_client

    def generate_embedding(
        self,
        text: str,
        context: Optional[Dict] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> Optional[List[float]]:
        """
        Generate an embedding vector for a text string.
        Checks for pre-generated embeddings first, then generates if needed.
        Returns a list of floats compatible with Supabase pgvector.

        Args:
            text: Text to generate embedding for
            context: Optional context dict with topic_id, subject_id, etc.
                    (used for usage tracking and cache key generation)
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for distributed tracing
        """
        # Import performance instrumentation
        try:
            from services.performance_instrumentation import (
                time_ai_call,
                time_response_parsing
            )
            INSTRUMENTATION_AVAILABLE = True
        except ImportError:
            INSTRUMENTATION_AVAILABLE = False
            # Create no-op context managers if instrumentation unavailable
            from contextlib import nullcontext
            time_ai_call = lambda *args, **kwargs: nullcontext()
            time_response_parsing = lambda *args, **kwargs: nullcontext()

        # Check Redis cache for embedding first (fastest path)
        try:
            import json
            import hashlib
            from services.redis_connection import get_redis_client, is_redis_available
            
            if is_redis_available():
                redis_client = get_redis_client()
                # Generate cache key from text hash
                text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
                cache_key = f"embedding:{text_hash}"
                
                # Try to get from cache
                cached_embedding = redis_client.get(cache_key)
                if cached_embedding:
                    embedding = json.loads(cached_embedding)
                    logger.debug(
                        f"[EMBEDDING] Using cached embedding from Redis "
                        f"(hash: {text_hash[:8]})"
                    )
                    # Track usage of cached embedding
                    if context:
                        try:
                            from services.embedding_usage_tracker import get_usage_tracker
                            usage_tracker = get_usage_tracker()
                            if usage_tracker:
                                usage_tracker.track_embedding_generation(text, context)
                        except Exception:
                            pass  # Non-critical, continue
                    return embedding
        except Exception as e:
            logger.debug(f"Error checking Redis cache for embedding: {e}")
            # Continue to pre-generation check on error
        
        # Check for pre-generated embedding second
        try:
            from services.embedding_pregen import get_pregen_service
            pregen_service = get_pregen_service(self, self.supabase)
            pre_generated = pregen_service.get_pre_generated_embedding(text)
            if pre_generated:
                logger.debug(
                    f"[EMBEDDING] Using pre-generated embedding for text "
                    f"(hash: {hashlib.md5(text.encode()).hexdigest()[:8]})"
                )
                # Cache in Redis for future use
                try:
                    import json
                    import hashlib
                    from services.redis_connection import get_redis_client, is_redis_available
                    if is_redis_available():
                        redis_client = get_redis_client()
                        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
                        cache_key = f"embedding:{text_hash}"
                        # Cache for 1 hour
                        redis_client.setex(cache_key, 3600, json.dumps(pre_generated))
                except Exception:
                    pass  # Non-critical, continue
                # Track usage of pre-generated embedding
                if context:
                    try:
                        from services.embedding_usage_tracker import get_usage_tracker
                        usage_tracker = get_usage_tracker()
                        if usage_tracker:
                            usage_tracker.track_embedding_generation(text, context)
                    except Exception:
                        pass  # Non-critical, continue
                return pre_generated
        except ImportError:
            # Pre-generation service not available, continue to generation
            pass
        except Exception as e:
            logger.debug(f"Error checking pre-generated embedding: {e}")
            # Continue to generation on error

        # Generate embedding (not pre-generated or pregen service unavailable)
        # Note: Embeddings API doesn't have separate prompt construction phase
        # PHASE 1: API Call (embedding generation)
        try:
            with time_ai_call(
                stage_name="concept_agent_embedding_api_call",
                job_id=job_id,
                trace_id=trace_id,
                model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
                prompt_tokens=len(text) // 4  # Rough estimate
            ):
                resp = self._embed_client.embeddings.create(
                    model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
                    input=text
                )

            # PHASE 2: Response Parsing (extract embedding vector)
            with time_response_parsing(
                stage_name="concept_agent_embedding_parsing",
                job_id=job_id,
                trace_id=trace_id,
                response_size=len(resp.data[0].embedding) if resp.data else None
            ):
                embedding = resp.data[0].embedding

                # Cache embedding in Redis for future use (1 hour TTL)
                try:
                    import json
                    import hashlib
                    from services.redis_connection import get_redis_client, is_redis_available
                    if is_redis_available():
                        redis_client = get_redis_client()
                        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
                        cache_key = f"embedding:{text_hash}"
                        # Cache for 1 hour
                        redis_client.setex(cache_key, 3600, json.dumps(embedding))
                        logger.debug(f"[EMBEDDING] Cached embedding in Redis (hash: {text_hash[:8]})")
                except Exception as e:
                    logger.debug(f"Error caching embedding in Redis: {e}")
                    # Non-critical, continue

                # Track usage for future pre-generation
                if context:
                    try:
                        from services.embedding_usage_tracker import get_usage_tracker
                        usage_tracker = get_usage_tracker()
                        if usage_tracker:
                            usage_tracker.track_embedding_generation(text, context)
                    except Exception:
                        pass  # Non-critical, continue

            return embedding
        except Exception:
            return None

    def generate_embeddings_batch(
        self,
        texts: List[str],
        context: Optional[Dict] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> List[Optional[List[float]]]:
        """
        Generate embeddings for multiple texts in a single batch API call.
        This is more efficient than calling generate_embedding() multiple times.
        
        Args:
            texts: List of texts to generate embeddings for
            context: Optional context dict for usage tracking
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for distributed tracing
            
        Returns:
            List of embedding vectors (same order as input texts)
            None values indicate failed embedding generation for that text
        """
        if not texts:
            return []
        
        # Check for pre-generated embeddings first
        pregen_results = []
        texts_to_generate = []
        text_indices = []
        
        try:
            from services.embedding_pregen import get_pregen_service
            pregen_service = get_pregen_service(self, self.supabase)
            
            for i, text in enumerate(texts):
                pre_generated = pregen_service.get_pre_generated_embedding(text) if pregen_service else None
                if pre_generated:
                    pregen_results.append((i, pre_generated))
                else:
                    texts_to_generate.append(text)
                    text_indices.append(i)
        except ImportError:
            # Pre-generation service not available, generate all
            texts_to_generate = texts
            text_indices = list(range(len(texts)))
        except Exception as e:
            logger.debug(f"Error checking pre-generated embeddings: {e}")
            # Continue to generation on error
            texts_to_generate = texts
            text_indices = list(range(len(texts)))
        
        # Initialize result list with None
        results = [None] * len(texts)
        
        # Fill in pre-generated embeddings
        for idx, embedding in pregen_results:
            results[idx] = embedding
        
        # Generate embeddings for remaining texts in batch
        if texts_to_generate:
            try:
                # Import performance instrumentation
                try:
                    from services.performance_instrumentation import (
                        time_ai_call,
                        time_response_parsing
                    )
                    INSTRUMENTATION_AVAILABLE = True
                except ImportError:
                    INSTRUMENTATION_AVAILABLE = False
                    from contextlib import nullcontext
                    time_ai_call = lambda *args, **kwargs: nullcontext()
                    time_response_parsing = lambda *args, **kwargs: nullcontext()
                
                # Batch API call
                with time_ai_call(
                    stage_name="concept_agent_batch_embedding_api_call",
                    job_id=job_id,
                    trace_id=trace_id,
                    model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
                    prompt_tokens=sum(len(text) // 4 for text in texts_to_generate)  # Rough estimate
                ):
                    resp = self._embed_client.embeddings.create(
                        model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
                        input=texts_to_generate
                    )
                
                # Parse responses
                with time_response_parsing(
                    stage_name="concept_agent_batch_embedding_parsing",
                    job_id=job_id,
                    trace_id=trace_id,
                    response_size=len(resp.data) if resp.data else None
                ):
                    for i, embedding_data in enumerate(resp.data):
                        if i < len(text_indices):
                            results[text_indices[i]] = embedding_data.embedding
                
                # Track usage for future pre-generation
                if context:
                    try:
                        from services.embedding_usage_tracker import get_usage_tracker
                        usage_tracker = get_usage_tracker()
                        if usage_tracker:
                            for text in texts_to_generate:
                                usage_tracker.track_embedding_generation(text, context)
                    except Exception:
                        pass  # Non-critical, continue
                        
            except Exception as e:
                logger.error(f"Error generating batch embeddings: {e}")
                # Return partial results (pre-generated ones are already filled)
        
        return results

    def retrieve_concepts(
        self,
        message_text: str,
        subject_id: Optional[str] = None,
        topic_id: Optional[str] = None,
        k: int = 5,
        min_similarity: Optional[float] = None
    ) -> List[Dict]:
        """
        Given a user message, return the top-k related concepts from Supabase
        using pgvector similarity search.
        Uses cache with 10 minute TTL.

        Args:
            message_text: User message to search for
            subject_id: Optional subject ID filter
            topic_id: Optional topic ID filter
            k: Number of concepts to retrieve (default: 5)
            min_similarity: Minimum similarity threshold (default: None)

        Returns a list of dicts:
            {
                "concept_id": str,
                "name": str,  # Maps to "concept" column
                "description": str,  # Maps to "explanation" column
                "distance": float
            }
        """
        # Check cache first
        message_hash = _hash_string(
            f"{message_text}:{subject_id}:{topic_id}:{k}:{min_similarity}"
        )
        cache_key = (
            f"concepts:{subject_id or 'all'}:{topic_id or 'all'}:"
            f"{k}:{min_similarity or 'none'}:{message_hash}"
        )
        cached = cache_get(cache_key)
        if cached is not None:
            logger.info(f"Cache hit for concepts:{message_hash}")
            return cached

        if not self.supabase:
            return []

        # Generate embedding with context for usage tracking
        embedding = self.generate_embedding(
            message_text,
            context={
                "topic_id": topic_id,
                "subject_id": subject_id,
                "query_type": "user_message"
            }
        )
        if embedding is None:
            return []

        try:
            # Build RPC parameters - start with minimal required params
            rpc_params = {
                "query_embedding": embedding,
                "match_count": k
            }

            # Add optional parameters if provided
            if subject_id is not None:
                rpc_params["subject_filter"] = subject_id
            if topic_id is not None:
                # Ensure topic_id is converted to int if it's a string
                topic_id_int = (
                    int(topic_id) if isinstance(topic_id, str)
                    else topic_id
                )
                rpc_params["topic_filter"] = topic_id_int
            if min_similarity is not None:
                rpc_params["min_similarity"] = min_similarity

            # Import timeout wrapper (with fallback if not available)
            try:
                from langgraph_tutor import safe_supabase_query
            except ImportError:
                # Fallback: define inline if import fails
                import threading

                def safe_supabase_query(
                    query_func, timeout=5, default_return=None
                ):
                    if timeout <= 0:
                        try:
                            return query_func()
                        except Exception:
                            return default_return

                    result_container = {
                        "value": None,
                        "error": None,
                        "completed": False
                    }

                    def execute_query():
                        try:
                            result_container["value"] = query_func()
                            result_container["completed"] = True
                        except Exception as e:
                            result_container["error"] = e
                            result_container["completed"] = True

                    query_thread = threading.Thread(
                        target=execute_query, daemon=True
                    )
                    query_thread.start()
                    query_thread.join(timeout=timeout)
                    if (not result_container["completed"] or
                            result_container["error"]):
                        return default_return
                    return result_container["value"]

            # Use Supabase RPC for pgvector search with timeout (5 seconds)
            # Try with all parameters first, then fall back to minimal

            def query_func():
                try:
                    return self.supabase.rpc(
                        "match_concepts",
                        rpc_params
                    ).execute()
                except Exception as rpc_error:
                    # If RPC fails with optional params,
                    # try with minimal params
                    logger.warning(
                        f"RPC call failed with optional params, "
                        f"trying minimal: {rpc_error}"
                    )
                    minimal_params = {
                        "query_embedding": embedding,
                        "match_count": k
                    }
                    return self.supabase.rpc(
                        "match_concepts",
                        minimal_params
                    ).execute()

            response = safe_supabase_query(
                query_func, timeout=5, default_return=None
            )

            if response is None:
                logger.warning(
                    "pgvector similarity search timed out or failed, "
                    "falling back to keyword search"
                )
                # Fallback to keyword search if pgvector fails
                return self.keyword_match(
                    message_text, subject_id, topic_id
                )

            rows = response.data or []
            concepts = []
            for row in rows:
                # Handle both column name formats:
                # - New format: "concept" and "explanation"
                # - Old format: "name" and "description"
                concept_name = (
                    row.get("concept") or row.get("name") or ""
                )
                concept_desc = (
                    row.get("explanation") or row.get("description") or ""
                )
                concepts.append({
                    "concept_id": row.get("concept_id"),
                    "name": concept_name,
                    "description": concept_desc,
                    "distance": row.get("distance"),
                    "updated_at": row.get("updated_at"),
                    # Store topic_id if available for filtering
                    "topic_id": row.get("topic_id")
                })

            # Filter by topic_id in Python if RPC didn't filter and we have it
            if topic_id and concepts:
                topic_id_int = (
                    int(topic_id) if isinstance(topic_id, str)
                    else topic_id
                )
                # Only filter if topic_id is present in the rows
                concepts_with_topic = [
                    c for c in concepts
                    if c.get("topic_id") is not None
                ]
                if concepts_with_topic:
                    concepts = [
                        c for c in concepts_with_topic
                        if c.get("topic_id") == topic_id_int
                    ]
                # If no topic_id in rows, we can't filter - rely on RPC

            # Cache for 10 minutes (600 seconds)
            if concepts:
                cache_set(cache_key, concepts, ttl=600)

            return concepts

        except Exception as e:
            logger.warning(
                f"Error in retrieve_concepts (pgvector search failed): {e}, "
                "falling back to keyword search"
            )
            # Fallback to keyword search if pgvector search fails
            try:
                return self.keyword_match(
                    message_text, subject_id, topic_id
                )
            except Exception as keyword_error:
                logger.error(f"Keyword fallback also failed: {keyword_error}")
                return []

    def keyword_match(
        self,
        message_text: str,
        subject_id: Optional[str] = None,
        topic_id: Optional[str] = None
    ) -> List[Dict]:
        """
        Fallback keyword-based concept search when embedding search returns
        no results. Searches for concepts by matching keywords in message
        against concept and explanation columns using SQL LIKE queries.
        Uses subject-specific concept tables.

        Args:
            message_text: User message to search for
            subject_id: Optional subject ID filter (can be str or int)
            topic_id: Optional topic ID filter (from topic selection)

        Returns:
            List of concept dicts with concept_id, name, description, distance
        """
        """
        Fallback keyword-based concept search when embedding search returns
        no results. Searches for concepts by matching keywords in message
        against concept and explanation columns using SQL LIKE queries.

        Args:
            message_text: User message to search for
            subject_id: Optional subject ID filter
            topic_id: Optional topic ID filter (from topic selection)

        Returns:
            List of concept dicts with concept_id, name, description, distance
        """
        if not self.supabase:
            return []

        try:
            # Extract keywords from message (simple: split by spaces)
            # Remove common stop words and short words
            words = message_text.lower().split()
            keywords = [
                w for w in words
                if len(w) > 3 and w not in [
                    'the', 'and', 'for', 'are', 'but', 'what', 'is', 'in',
                    'the', 'of', 'to', 'a', 'an'
                ]
            ][:5]  # Limit to top 5 keywords

            if not keywords:
                return []

            # Get the correct concept table name based on subject_id
            # Convert subject_id to int if it's a string
            subject_id_int = None
            if subject_id:
                try:
                    subject_id_int = int(subject_id) if isinstance(subject_id, str) else subject_id
                except (ValueError, TypeError):
                    subject_id_int = None
            
            table_name = self._get_concept_table_name(subject_id_int)
            
            logger.info(
                f"[KEYWORD MATCH] Using table '{table_name}' "
                f"for subject_id: {subject_id_int}, keywords: {keywords[:3]}"
            )
            
            # Search for keywords in "concept" or "explanation" columns
            # Use OR conditions for multiple keywords
            results = []
            for keyword in keywords:
                try:
                    # Build fresh query for each keyword search
                    base_query = self.supabase.table(table_name).select(
                        "concept_id, concept, explanation, "
                        "topic_id, updated_at"
                    )

                    # Apply topic_id filter if provided (from topic selection)
                    if topic_id:
                        # Ensure topic_id is converted to int if it's a string
                        topic_id_int = (
                            int(topic_id) if isinstance(topic_id, str)
                            else topic_id
                        )
                        base_query = base_query.eq("topic_id", topic_id_int)

                    # Search in "concept" column with timeout protection
                    try:
                        from langgraph_tutor import safe_supabase_query
                    except ImportError:
                        import threading

                        def safe_supabase_query(
                            query_func, timeout=10, default_return=None
                        ):
                            if timeout <= 0:
                                try:
                                    return query_func()
                                except Exception:
                                    return default_return
                            result_container = {
                                "value": None,
                                "error": None,
                                "completed": False
                            }

                            def execute_query():
                                try:
                                    result_container["value"] = query_func()
                                    result_container["completed"] = True
                                except Exception as e:
                                    result_container["error"] = e
                                    result_container["completed"] = True
                            query_thread = threading.Thread(
                                target=execute_query, daemon=True
                            )
                            query_thread.start()
                            query_thread.join(timeout=timeout)
                            if (not result_container["completed"] or
                                    result_container["error"]):
                                return default_return
                            return result_container["value"]

                    def concept_query():
                        return (
                            base_query.ilike("concept", f"%{keyword}%")
                            .limit(10)
                            .execute()
                        )
                    concept_results = safe_supabase_query(
                        concept_query, timeout=10, default_return={"data": []}
                    )
                    if concept_results is None:
                        concept_results = {"data": []}
                    if concept_results.data:
                        results.extend(concept_results.data)

                    # Build fresh query for explanation search
                    expl_base_query = (
                        self.supabase.table("concepts").select(
                            "concept_id, concept, explanation, "
                            "topic_id, updated_at"
                        )
                    )
                    if topic_id:
                        # Ensure topic_id is converted to int if it's a string
                        topic_id_int = (
                            int(topic_id) if isinstance(topic_id, str)
                            else topic_id
                        )
                        expl_base_query = expl_base_query.eq(
                            "topic_id", topic_id_int
                        )

                    # Search in "explanation" column with timeout protection
                    def expl_query():
                        return (
                            expl_base_query.ilike(
                                "explanation", f"%{keyword}%"
                            )
                            .limit(10)
                            .execute()
                        )
                    expl_results = safe_supabase_query(
                        expl_query,
                        timeout=10,
                        default_return={"data": []}
                    )
                    if expl_results is None:
                        expl_results = {"data": []}
                    if expl_results.data:
                        results.extend(expl_results.data)
                except Exception as e:
                    logger.warning(
                        f"Error searching for keyword '{keyword}': {e}"
                    )
                    continue

            # Deduplicate by concept_id
            seen = set()
            concepts = []
            for row in results:
                concept_id = row.get("concept_id")
                if concept_id and concept_id not in seen:
                    seen.add(concept_id)
                    concepts.append({
                        "concept_id": concept_id,
                        # Map "concept" to "name"
                        "name": row.get("concept", ""),
                        # Map "explanation" to "description"
                        "description": row.get("explanation", ""),
                        "distance": 0.5,  # Placeholder for keyword match
                        "updated_at": row.get("updated_at")
                    })

            # Limit to top 7 results
            return concepts[:7]

        except Exception as e:
            logger.error(f"Error in keyword_match: {e}")
            return []
    
    def _get_concept_table_name(self, subject_id: Optional[int] = None) -> str:
        """
        Get the correct concept table name based on subject_id.
        
        Args:
            subject_id: Subject ID (101=Business Studies, 102=Islamiyat, etc.)
        
        Returns:
            Table name string (e.g., "concepts", "concepts_economics", "concepts_history")
        """
        if subject_id is None:
            return "concepts"  # Default to main concepts table
        
        subject_table_map = {
            101: "concepts",  # Business Studies - main concepts table
            102: "concepts_isl",  # Islamiyat
            113: "concepts_geography",  # Pak Studies Geography
            114: "concepts_history",  # Pak Studies History
            119: "concepts_economics",  # Economics - using concepts_economics table
        }
        
        return subject_table_map.get(subject_id, "concepts")

    def fetch_concepts_by_topic(
        self,
        topic_id: str,
        limit: int = 10,
        random_order: bool = True,
        subject_id: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch concepts directly from the database by topic_id.
        Returns concepts in random order for variety.
        Uses subject-specific concept tables.

        Args:
            topic_id: Topic ID to fetch concepts for
            limit: Maximum number of concepts to return (default: 10)
            random_order: Whether to return concepts in random order
                (default: True)
            subject_id: Optional subject ID to determine which concept table to use

        Returns:
            List of concept dicts with concept_id, name (concept),
            description (explanation)
        """
        if not self.supabase:
            return []

        try:
            # Ensure topic_id is converted to int if it's a string
            topic_id_int = (
                int(topic_id) if isinstance(topic_id, str) else topic_id
            )

            # Check cache first
            # CACHE KEY: Strictly based on topic_id for 24-hour caching
            # When entering a topic, ALL concepts are fetched once and cached for 24 hours
            # Cache key is topic_id + subject_id only (no limit, no order preference)
            # This ensures one cache entry per topic, regardless of limit/order requests
            subject_key = f"subject_{subject_id}" if subject_id else "no_subject"
            # Primary cache key: strictly topic_id-based (24-hour cache)
            # Store ALL concepts for the topic, not limited
            cache_key = f"concepts_by_topic:{topic_id_int}:{subject_key}"
            
            # CRITICAL DEBUG: Log cache key and subject_id for Islamiyat topics
            if topic_id_int and 100 <= topic_id_int <= 199:
                logger.info(
                    f"[CONCEPT FETCH] 🔍 DEBUG: topic_id={topic_id_int} (Islamiyat range), "
                    f"subject_id={subject_id}, cache_key={cache_key}"
                )
                if subject_id != 102:
                    logger.error(
                        f"[CONCEPT FETCH] ⚠️ CRITICAL: topic_id={topic_id_int} is Islamiyat "
                        f"but subject_id={subject_id} is NOT 102! Cache key will be wrong!"
                    )
            
            cached = cache_get(cache_key)
            if cached is not None:
                logger.info(
                    f"[CONCEPT FETCH] Cache HIT for topic_id={topic_id_int}, "
                    f"subject_id={subject_id}, cache_key={cache_key}, "
                    f"returning {min(limit, len(cached))} of {len(cached)} cached concepts"
                )
                # CRITICAL: Verify cached concepts are from correct table
                if topic_id_int and 100 <= topic_id_int <= 199:
                    # Check if cached concepts look like Business Studies concepts
                    # (This is a heuristic - Business Studies concepts might have different patterns)
                    sample_concept = cached[0] if cached else None
                    if sample_concept:
                        logger.info(
                            f"[CONCEPT FETCH] 🔍 DEBUG: Cached concept sample: "
                            f"concept_id={sample_concept.get('concept_id')}, "
                            f"name={sample_concept.get('name', '')[:50]}"
                        )
                # Apply limit and order from cached concepts
                if not random_order:
                    # Return in cached order (already sorted by concept_id)
                    return cached[:limit]
                # If random_order is True, shuffle cached concepts
                import random
                import time
                random.seed(int(time.time() * 1000000) % 1000000)
                cached_copy = cached.copy()
                random.shuffle(cached_copy)
                random.seed()
                return cached_copy[:limit]

            # Import timeout wrapper (with fallback if not available)
            try:
                from langgraph_tutor import safe_supabase_query
            except ImportError:
                # Fallback: define inline if import fails
                import threading

                def safe_supabase_query(
                    query_func, timeout=3, default_return=None
                ):
                    if timeout <= 0:
                        try:
                            return query_func()
                        except Exception:
                            return default_return

                    result_container = {
                        "value": None,
                        "error": None,
                        "completed": False
                    }

                    def execute_query():
                        try:
                            result_container["value"] = query_func()
                            result_container["completed"] = True
                        except Exception as e:
                            result_container["error"] = e
                            result_container["completed"] = True

                    query_thread = threading.Thread(
                        target=execute_query, daemon=True
                    )
                    query_thread.start()
                    query_thread.join(timeout=timeout)
                    if (not result_container["completed"] or
                            result_container["error"]):
                        return default_return
                    return result_container["value"]

            # Get the correct concept table name based on subject_id
            table_name = self._get_concept_table_name(subject_id)
            
            logger.info(
                f"[CONCEPT FETCH] Using table '{table_name}' "
                f"for topic_id: {topic_id_int}, subject_id: {subject_id}"
            )
            
            # CRITICAL DEBUG: Log if we're using wrong table for Islamiyat
            if topic_id_int and 100 <= topic_id_int <= 199:
                if subject_id != 102:
                    logger.error(
                        f"[CONCEPT FETCH] ⚠️ CRITICAL ERROR: topic_id={topic_id_int} "
                        f"is in Islamiyat range (100-199) but subject_id={subject_id} "
                        f"is NOT 102! Using table '{table_name}' which may be WRONG!"
                    )
                elif table_name != "concepts_isl":
                    logger.error(
                        f"[CONCEPT FETCH] ⚠️ CRITICAL ERROR: topic_id={topic_id_int} "
                        f"is in Islamiyat range (100-199), subject_id={subject_id} is correct, "
                        f"but table_name='{table_name}' is NOT 'concepts_isl'! "
                        f"This will fetch from the WRONG table!"
                    )
                else:
                    logger.info(
                        f"[CONCEPT FETCH] ✅ Correctly using 'concepts_isl' table "
                        f"for Islamiyat topic_id={topic_id_int}"
                    )
            
            # DEBUG: Log topic_id range check for Economics
            if subject_id == 119:  # Economics
                logger.info(
                    f"[CONCEPT FETCH] Economics topic check: topic_id={topic_id_int}, "
                    f"expected range: 500-699, in_range: {500 <= topic_id_int <= 699}, "
                    f"table_name: {table_name}"
                )
                if table_name != "concepts_economics":
                    logger.error(
                        f"[CONCEPT FETCH] ⚠️ CRITICAL ERROR: Economics (subject_id=119) "
                        f"is using table '{table_name}' instead of 'concepts_economics'!"
                    )
                elif table_name == "concepts_economics":
                    logger.info(
                        f"[CONCEPT FETCH] ✅ Correctly using 'concepts_economics' table "
                        f"for Economics topic_id={topic_id_int}"
                    )
            
            # Wrap query with timeout (10 seconds - increased for reliability)
            def query_func():
                # Always order by concept_id for consistent ordering
                # CRITICAL: Log the actual query for Economics
                if subject_id == 119:
                    logger.info(
                        f"[CONCEPT FETCH] Economics: Executing query on table '{table_name}': "
                        f"SELECT concept_id, concept, explanation, topic_id "
                        f"FROM {table_name} WHERE topic_id = {topic_id_int}"
                    )
                return (
                    self.supabase.table(table_name)
                    .select("concept_id, concept, explanation, topic_id")
                    .eq("topic_id", topic_id_int)
                    .order("concept_id", desc=False)
                    .execute()
                )

            # Execute query with timeout
            result = safe_supabase_query(
                query_func, timeout=10, default_return=None
            )
            
            # If safe_supabase_query returns None, try direct query as fallback
            if result is None:
                logger.warning(f"[CONCEPT FETCH] safe_supabase_query returned None, trying direct query for topic_id: {topic_id_int}, table: {table_name}")
                try:
                    # Fetch ALL concepts (no limit) - they will be cached and limited later
                    result = (
                        self.supabase.table(table_name)
                        .select("concept_id, concept, explanation, topic_id")
                        .eq("topic_id", topic_id_int)
                        .order("concept_id", desc=False)
                        .execute()
                    )
                    logger.info(f"[CONCEPT FETCH] Direct query succeeded, found {len(result.data) if result and result.data else 0} concepts")
                except Exception as direct_error:
                    logger.error(f"[CONCEPT FETCH] Direct query also failed: {direct_error}")
                    result = None

            if result is None or not result.data:
                if result is None:
                    logger.error(
                        f"[CONCEPT FETCH] Query timeout or error for "
                        f"topic_id: {topic_id_int}, subject_id: {subject_id}, "
                        f"table: {table_name}"
                    )
                    logger.error(
                        f"[CONCEPT FETCH] Supabase client status: "
                        f"{'Connected' if self.supabase else 'Not connected'}"
                    )
                    # Try direct query without timeout wrapper to see actual error
                    try:
                        direct_result = (
                            self.supabase.table(table_name)
                            .select("concept_id, concept, explanation, topic_id")
                            .eq("topic_id", topic_id_int)
                            .limit(1)
                            .execute()
                        )
                        logger.info(f"[CONCEPT FETCH] Direct query result: {len(direct_result.data) if direct_result and direct_result.data else 0} concepts found")
                    except Exception as direct_error:
                        logger.error(f"[CONCEPT FETCH] Direct query error: {direct_error}")
                else:
                    # IMPORTANT: Enhanced logging for missing concepts
                    # This helps identify data gaps in the database
                    logger.warning(
                        f"[CONCEPT FETCH] ⚠️ NO CONCEPTS FOUND for "
                        f"topic_id: {topic_id_int}, subject_id: {subject_id}, "
                        f"table: {table_name}"
                    )
                    logger.warning(
                        f"[CONCEPT FETCH] Query returned result but result.data is empty. "
                        f"Result type: {type(result)}, Has data attr: {hasattr(result, 'data')}"
                    )
                    if result and hasattr(result, 'data'):
                        logger.warning(
                            f"[CONCEPT FETCH] result.data value: {result.data}, "
                            f"type: {type(result.data)}, length: {len(result.data) if result.data else 0}"
                        )
                    logger.warning(
                        f"[CONCEPT FETCH] This may indicate: "
                        f"1) Missing data in {table_name} for topic_id={topic_id_int}, "
                        f"2) Incorrect subject_id={subject_id} for this topic_id, or "
                        f"3) Topic_id doesn't exist in the topics table"
                    )
                    # For Economics, log the actual query being executed
                    if subject_id == 119:
                        logger.info(
                            f"[CONCEPT FETCH] Economics query details: "
                            f"SELECT concept_id, concept, explanation, topic_id "
                            f"FROM {table_name} WHERE topic_id = {topic_id_int}"
                        )
                    # Log the actual query for debugging
                    logger.debug(
                        f"[CONCEPT FETCH] Query executed: SELECT concept_id, concept, explanation, topic_id "
                        f"FROM {table_name} WHERE topic_id = {topic_id_int}"
                    )
                    # Try to verify if topic exists in topics table
                    try:
                        # Check if topic exists in topics_history (for History)
                        if subject_id == 114:  # History
                            check_table = "topics_history"
                        elif subject_id == 113:  # Geography
                            check_table = "topics_geography"
                        elif subject_id == 119:  # Economics
                            check_table = "topics_economics"
                        elif subject_id == 102:  # Islamiyat
                            check_table = "topics_isl"
                        else:
                            check_table = "topics"  # Business Studies or default
                        
                        topic_check = (
                            self.supabase.table(check_table)
                            .select("topic_id, subject_id")
                            .eq("topic_id", topic_id_int)
                            .limit(1)
                            .execute()
                        )
                        if topic_check.data and len(topic_check.data) > 0:
                            found_subject_id = topic_check.data[0].get("subject_id")
                            logger.info(
                                f"[CONCEPT FETCH] ✓ Topic EXISTS in {check_table} "
                                f"with subject_id: {found_subject_id}"
                            )
                            if found_subject_id != subject_id:
                                logger.warning(
                                    f"[CONCEPT FETCH] ⚠️ MISMATCH: Topic has "
                                    f"subject_id={found_subject_id} but we queried "
                                    f"with subject_id={subject_id}. This explains "
                                    f"why no concepts were found!"
                                )
                        else:
                            logger.warning(
                                f"[CONCEPT FETCH] ⚠️ Topic NOT FOUND in {check_table} "
                                f"for topic_id: {topic_id_int}"
                            )
                    except Exception as e:
                        logger.debug(
                            f"[CONCEPT FETCH] Could not verify topic existence: {e}"
                        )
                return []

            # Convert to expected format
            logger.info(f"[CONCEPT FETCH] Query succeeded! Found {len(result.data) if result.data else 0} concepts in result.data")
            
            # CRITICAL DEBUG for Economics
            if subject_id == 119 and result.data:
                logger.info(
                    f"[CONCEPT FETCH] Economics: Found {len(result.data)} concepts. "
                    f"Sample row keys: {list(result.data[0].keys()) if result.data else []}, "
                    f"Sample row: {result.data[0] if result.data else 'None'}"
                )
            
            concepts = []
            for row in result.data:
                concept_name = row.get("concept", "")
                concept_id = row.get("concept_id")
                
                # CRITICAL DEBUG for Economics - log if name is missing
                if subject_id == 119 and not concept_name:
                    logger.warning(
                        f"[CONCEPT FETCH] ⚠️ Economics concept missing 'concept' field! "
                        f"Row keys: {list(row.keys())}, Row: {row}"
                    )
                
                concepts.append({
                    "concept_id": concept_id,
                    # Map "concept" to "name"
                    "name": concept_name,
                    # Map "explanation" to "description"
                    "description": row.get("explanation", ""),
                    # No distance for direct topic query
                    "distance": 0.0,
                    "topic_id": row.get("topic_id")
                })
            
            # CRITICAL DEBUG for Economics - log final concepts
            if subject_id == 119:
                logger.info(
                    f"[CONCEPT FETCH] Economics: Mapped {len(concepts)} concepts. "
                    f"Sample mapped concept: {concepts[0] if concepts else 'None'}"
                )

            # Sort by concept_id for consistent ordering
            # This ensures the same order every time when random_order=False
            concepts.sort(key=lambda x: x.get("concept_id", 0))

            # Cache ALL concepts (not limited) for 24 hours - strictly based on topic_id
            # When entering a topic, ALL concepts are fetched once and cached for 24 hours
            # Cache key is topic_id + subject_id only (no limit, no order preference)
            if concepts:
                cache_set(cache_key, concepts, ttl=86400)  # 24 hours
                logger.info(
                    f"[CONCEPT FETCH] Cached {len(concepts)} concepts (ALL) for "
                    f"topic_id={topic_id_int}, subject_id={subject_id} "
                    f"for 24 hours (cache_key: {cache_key})"
                )

            # Apply limit and random_order to the result (cache stores all concepts)
            if random_order and len(concepts) > 1:
                import random
                import time
                random.seed(int(time.time() * 1000000) % 1000000)
                concepts_copy = concepts.copy()
                random.shuffle(concepts_copy)
                random.seed()
                concepts = concepts_copy

            # Limit results for return (cache has all concepts)
            concepts = concepts[:limit]

            logger.info(
                f"Fetched {len(concepts)} concepts for "
                f"topic_id: {topic_id_int}"
            )

            return concepts

        except Exception as e:
            logger.error(f"Error in fetch_concepts_by_topic: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    def retrieve_lesson_chunks(
        self, question: str, lesson_id: str, k: int = 3
    ) -> List[Dict]:
        """
        Retrieve top-k relevant lesson chunks using pgvector similarity search.

        Args:
            question: Student's question text
            lesson_id: Lesson ID to search within
            k: Number of chunks to retrieve (default: 3)

        Returns:
            List of dicts with chunk_text and distance
        """
        if not self.supabase:
            return []

        # Generate embedding for question
        query_embedding = self.generate_embedding(question)
        if query_embedding is None:
            return []

        try:
            # Use Supabase RPC for pgvector similarity search
            response = self.supabase.rpc(
                "match_lesson_chunks",
                {
                    "query_embedding": query_embedding,
                    "lesson_id_filter": lesson_id,
                    "match_count": k
                }
            ).execute()

            rows = response.data or []
            chunks = []
            for row in rows:
                chunks.append({
                    "chunk_text": row.get("chunk_text", ""),
                    "distance": row.get("distance", 1.0)
                })

            return chunks

        except Exception:
            # Fallback: try direct table query if RPC doesn't exist
            try:
                try:
                    from langgraph_tutor import safe_supabase_query
                except ImportError:
                    import threading

                    def safe_supabase_query(
                        query_func, timeout=10, default_return=None
                    ):
                        if timeout <= 0:
                            try:
                                return query_func()
                            except Exception:
                                return default_return
                        result_container = {
                            "value": None,
                            "error": None,
                            "completed": False
                        }

                        def execute_query():
                            try:
                                result_container["value"] = query_func()
                                result_container["completed"] = True
                            except Exception as e:
                                result_container["error"] = e
                                result_container["completed"] = True

                        query_thread = threading.Thread(
                            target=execute_query, daemon=True
                        )
                        query_thread.start()
                        query_thread.join(timeout=timeout)
                        if (not result_container["completed"] or
                                result_container["error"]):
                            return default_return
                        return result_container["value"]

                def table_query():
                    return (
                        self.supabase.table("lesson_embeddings")
                        .select("chunk_text, embedding")
                        .eq("lesson_id", lesson_id)
                        .limit(k * 2)
                        .execute()
                    )
                response = safe_supabase_query(
                    table_query, timeout=10, default_return={"data": []}
                )
                if response is None:
                    response = {"data": []}

                rows = response.data or []
                chunks = []
                for row in rows:
                    chunks.append({
                        "chunk_text": row.get("chunk_text", ""),
                        "distance": 0.5  # Placeholder
                    })

                return chunks[:k]

            except Exception as e:
                logger.error(f"Error retrieving lesson chunks: {e}")
                return []

    def generate_lesson_embeddings(
        self, lesson_id: str, lesson_content: str
    ) -> bool:
        """
        Generate embeddings for lesson content and store in lesson_embeddings
        table.

        If lesson content > 1000 chars, split into chunks of 500-800 tokens.
        Generate embeddings for each chunk and upsert into database.

        Args:
            lesson_id: The lesson ID from lessons table
            lesson_content: Full lesson text content

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or not lesson_content:
            return False

        # Only process if content is substantial
        if len(lesson_content) < 1000:
            return False

        try:
            # Simple chunking: split by paragraphs
            # Target: 500-800 tokens per chunk (~2000-3200 chars)
            chunks = []
            paragraphs = lesson_content.split("\n\n")

            current_chunk = ""
            for para in paragraphs:
                # If adding this paragraph would exceed ~3000 chars, save chunk
                if len(current_chunk) + len(para) > 3000 and current_chunk:
                    chunks.append(current_chunk.strip())
                    current_chunk = para
                else:
                    current_chunk += "\n\n" + para if current_chunk else para

            # Add final chunk
            if current_chunk.strip():
                chunks.append(current_chunk.strip())

            # Generate embeddings and upsert
            rows_to_upsert = []
            for idx, chunk_text in enumerate(chunks):
                embedding = self.generate_embedding(chunk_text)
                if embedding is None:
                    continue

                chunk_id = f"{lesson_id}_chunk_{idx}"
                rows_to_upsert.append({
                    "lesson_id": lesson_id,
                    "chunk_id": chunk_id,
                    "chunk_text": chunk_text,
                    "embedding": embedding
                })

            if rows_to_upsert:
                # Upsert into lesson_embeddings table with timeout
                try:
                    from langgraph_tutor import safe_supabase_query
                except ImportError:
                    import threading

                    def safe_supabase_query(
                        query_func, timeout=10, default_return=None
                    ):
                        if timeout <= 0:
                            try:
                                return query_func()
                            except Exception:
                                return default_return
                        result_container = {
                            "value": None,
                            "error": None,
                            "completed": False
                        }

                        def execute_query():
                            try:
                                result_container["value"] = query_func()
                                result_container["completed"] = True
                            except Exception as e:
                                result_container["error"] = e
                                result_container["completed"] = True

                        query_thread = threading.Thread(
                            target=execute_query, daemon=True
                        )
                        query_thread.start()
                        query_thread.join(timeout=timeout)
                        if (not result_container["completed"] or
                                result_container["error"]):
                            return default_return
                        return result_container["value"]

                def upsert_query():
                    return (
                        self.supabase.table("lesson_embeddings")
                        .upsert(rows_to_upsert)
                        .execute()
                    )
                safe_supabase_query(
                    upsert_query, timeout=10, default_return=None
                )
                logger.info(
                    f"Generated {len(rows_to_upsert)} embeddings for "
                    f"lesson_id: {lesson_id}"
                )
                return True

            return False

        except Exception as e:
            logger.error(f"Error generating lesson embeddings: {e}")
            return False

    def fetch_concept_details(
        self, concept_ids: List[str]
    ) -> Dict[str, Dict]:
        """
        Fetch metadata (concept, explanation) for given concept_ids from
        public.concepts.
        Return mapping: { concept_id: {"name":..., "description":...} }
        Note: Maps "concept" column to "name" and
        "explanation" to "description"
        """
        if not self.supabase or len(concept_ids) == 0:
            return {}

        try:
            # Add timeout protection
            try:
                from langgraph_tutor import safe_supabase_query
            except ImportError:
                import threading

                def safe_supabase_query(
                    query_func, timeout=10, default_return=None
                ):
                    if timeout <= 0:
                        try:
                            return query_func()
                        except Exception:
                            return default_return
                    result_container = {
                        "value": None,
                        "error": None,
                        "completed": False
                    }

                    def execute_query():
                        try:
                            result_container["value"] = query_func()
                            result_container["completed"] = True
                        except Exception as e:
                            result_container["error"] = e
                            result_container["completed"] = True
                    query_thread = threading.Thread(
                        target=execute_query, daemon=True
                    )
                    query_thread.start()
                    query_thread.join(timeout=timeout)
                    if (not result_container["completed"] or
                            result_container["error"]):
                        return default_return
                    return result_container["value"]

            def fetch_query():
                return (
                    self.supabase.table("concepts")
                    .select("concept_id, concept, explanation")
                    .in_("concept_id", concept_ids)
                    .execute()
                )
            res = safe_supabase_query(
                fetch_query, timeout=10, default_return={"data": []}
            )
            if res is None:
                res = {"data": []}
            rows = res.data or []

            details_map = {}
            for row in rows:
                cid = row.get("concept_id")
                if cid:
                    details_map[cid] = {
                        # Map "concept" to "name"
                        "name": row.get("concept", ""),
                        # Map "explanation" to "description"
                        "description": row.get("explanation", "")
                    }

            logger.info(
                f"Fetched details for {len(details_map)} concept(s)"
            )
            return details_map

        except Exception as e:
            logger.error(f"Error fetching concept details: {e}")
            return {}

    def get_prerequisites_and_next_concepts(
        self, concept_ids: List[str]
    ) -> Dict:
        """
        Fetch prerequisite concepts and next-step concepts for a list of
        concept_ids.

        Returns:
        {
            "prerequisites": [
                # "name" maps to "concept" column
                {"concept_id": str, "name": str}
            ],
            "next_concepts": [
                # "name" maps to "concept" column
                {"concept_id": str, "name": str}
            ]
        }
        """
        if not self.supabase or len(concept_ids) == 0:
            return {
                "prerequisites": [],
                "next_concepts": []
            }

        prereq_ids = []
        next_ids = []

        try:
            # Fetch prerequisites with timeout protection
            try:
                from langgraph_tutor import safe_supabase_query
            except ImportError:
                import threading

                def safe_supabase_query(
                    query_func, timeout=10, default_return=None
                ):
                    if timeout <= 0:
                        try:
                            return query_func()
                        except Exception:
                            return default_return
                    result_container = {
                        "value": None,
                        "error": None,
                        "completed": False
                    }

                    def execute_query():
                        try:
                            result_container["value"] = query_func()
                            result_container["completed"] = True
                        except Exception as e:
                            result_container["error"] = e
                            result_container["completed"] = True
                    query_thread = threading.Thread(
                        target=execute_query, daemon=True
                    )
                    query_thread.start()
                    query_thread.join(timeout=timeout)
                    if (not result_container["completed"] or
                            result_container["error"]):
                        return default_return
                    return result_container["value"]

            def prereq_query():
                return (
                    self.supabase.table("concept_prerequisites")
                    .select("prerequisite_concept_id")
                    .in_("concept_id", concept_ids)
                    .execute()
                )
            prereq_res = safe_supabase_query(
                prereq_query, timeout=10, default_return={"data": []}
            )
            if prereq_res is None:
                prereq_res = {"data": []}
            # Handle both Supabase response objects and dicts
            if hasattr(prereq_res, 'data'):
                prereq_rows = prereq_res.data or []
            elif isinstance(prereq_res, dict):
                prereq_rows = prereq_res.get("data", [])
            else:
                prereq_rows = []
            prereq_ids = [
                row["prerequisite_concept_id"]
                for row in prereq_rows
                if row.get("prerequisite_concept_id")
            ]

        except Exception as e:
            logger.error(f"Error fetching prerequisites: {e}")

        try:
            # Fetch next concepts with timeout protection
            def next_query():
                return (
                    self.supabase.table("concept_next")
                    .select("next_concept_id")
                    .in_("concept_id", concept_ids)
                    .execute()
                )
            next_res = safe_supabase_query(
                next_query, timeout=10, default_return={"data": []}
            )
            if next_res is None:
                next_res = {"data": []}
            # Handle both Supabase response objects and dicts
            if hasattr(next_res, 'data'):
                next_rows = next_res.data or []
            elif isinstance(next_res, dict):
                next_rows = next_res.get("data", [])
            else:
                next_rows = []
            next_ids = [
                row["next_concept_id"]
                for row in next_rows
                if row.get("next_concept_id")
            ]

        except Exception as e:
            logger.error(f"Error fetching next concepts: {e}")

        # Deduplicate both lists
        prereq_ids = list(set(prereq_ids))
        next_ids = list(set(next_ids))

        # Fetch concept details
        all_concept_ids = prereq_ids + next_ids
        details = self.fetch_concept_details(all_concept_ids)

        # Build structured results
        prerequisites = [
            {
                "concept_id": cid,
                "name": details.get(cid, {}).get("name", "")
            }
            for cid in prereq_ids
        ]

        next_concepts = [
            {
                "concept_id": cid,
                "name": details.get(cid, {}).get("name", "")
            }
            for cid in next_ids
        ]

        logger.info(
            f"Found {len(prerequisites)} prerequisite(s) and "
            f"{len(next_concepts)} next concept(s)"
        )

        return {
            "prerequisites": prerequisites,
            "next_concepts": next_concepts
        }
