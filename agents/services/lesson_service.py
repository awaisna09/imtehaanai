#!/usr/bin/env python3
"""
Lesson Service - Handles lesson content operations
"""

import hashlib
import logging
import os
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)


class LessonService:
    """
    Provides lesson retrieval and management logic.
    Handles lesson content fetching, embedding generation, and chunk retrieval.
    """

    def __init__(
        self, supabase_client, concept_agent, cache_get=None, cache_set=None
    ):
        """
        Initialize LessonService.

        Args:
            supabase_client: Supabase client instance
            concept_agent: ConceptAgent instance for embedding generation
            cache_get: Optional cache get function
            cache_set: Optional cache set function
        """
        self.supabase = supabase_client
        self.concept_agent = concept_agent
        self.cache_get = cache_get
        self.cache_set = cache_set
        self.logger = logging.getLogger(__name__)

    def get_lesson_id_from_topic(self, topic_id: str) -> str:
        """
        Get lesson_id from topic_id by querying Supabase lessons table.

        Args:
            topic_id: Topic ID to look up

        Returns:
            str: Lesson ID (lessons_id) or topic_id as fallback
        """
        if not self.supabase:
            return topic_id

        try:
            res = (
                self.supabase.table("lessons")
                .select("lessons_id")
                .eq("topic_id", topic_id)
                .limit(1)
                .execute()
            )
            rows = res.data or []
            if rows and rows[0].get("lessons_id"):
                return str(rows[0]["lessons_id"])
            # Fallback: return topic_id if no lesson found
            return topic_id
        except Exception as e:
            self.logger.error(f"Error getting lesson_id from topic: {e}")
            # Fallback: return topic_id on error
            return topic_id

    def get_subject_id_from_topic(self, topic_id: int) -> Optional[int]:
        """
        Return the subject_id for a given topic_id.

        Args:
            topic_id: Topic ID to look up

        Returns:
            Optional[int]: Subject ID or None if not found
        """
        if not self.supabase:
            return None

        try:
            res = (
                self.supabase.table("topics")
                .select("subject_id")
                .eq("topic_id", topic_id)
                .single()
                .execute()
            )
            if res.data:
                return res.data.get("subject_id")
            return None
        except Exception as e:
            self.logger.error(
                f"[ERROR] get_subject_id_from_topic failed: {e}"
            )
            return None

    def fetch_lesson_content(self, topic_id: str) -> Optional[str]:
        """
        Fetch full lesson content for a given topic_id.
        Uses caching (1 day TTL).

        Args:
            topic_id: Topic ID to fetch lessons for

        Returns:
            Concatenated lesson content or None
        """
        # Check cache first
        if self.cache_get:
            cache_key = f"lesson_text:{topic_id}"
            cached = self.cache_get(cache_key)
            if cached is not None:
                if os.getenv("DEBUG", "0") == "1":
                    self.logger.info(f"Cache hit for lesson_text:{topic_id}")
                return cached

        if not self.supabase:
            return None

        try:
            # Import timeout wrapper (with fallback if not available)
            try:
                from langgraph_tutor import safe_supabase_query
            except ImportError:
                # Fallback: define inline if import fails
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

            # Determine which table to use based on topic_id
            # Check if topic exists in topics_history (for Pak Studies History)
            # or topics_geography (for Pak Studies Geography)
            table_name = "lessons"
            try:
                topic_id_int = int(topic_id)
                # Check if topic exists in topics_history
                history_check = (
                    self.supabase.table("topics_history")
                    .select("topic_id")
                    .eq("topic_id", topic_id_int)
                    .eq("subject_id", 114)
                    .limit(1)
                    .execute()
                )
                if history_check.data and len(history_check.data) > 0:
                    table_name = "lessons_history"
                    self.logger.info(f"Using lessons_history table for topic_id: {topic_id}")
                else:
                    # Check if topic exists in topics_geography
                    geography_check = (
                        self.supabase.table("topics_geography")
                        .select("topic_id")
                        .eq("topic_id", topic_id_int)
                        .eq("subject_id", 113)
                        .limit(1)
                        .execute()
                    )
                    if geography_check.data and len(geography_check.data) > 0:
                        table_name = "lessons_geography"
                        self.logger.info(f"Using lessons_geography table for topic_id: {topic_id}")
            except (ValueError, Exception) as e:
                # If check fails, use default lessons table
                self.logger.debug(f"Could not check topics_history/topics_geography, using default: {e}")
            
            # Wrap query with timeout (10 seconds)
            def query_func():
                return (
                    self.supabase.table(table_name)
                    .select("content")
                    .eq("topic_id", topic_id)
                    .execute()
                )

            res = safe_supabase_query(
                query_func, timeout=10, default_return=None
            )

            if res is None:
                self.logger.error(
                    f"[LESSON FETCH] Query timeout or error for "
                    f"topic_id: {topic_id}"
                )
                self.logger.error(
                    "[LESSON FETCH] Supabase client status: "
                    f"{'Connected' if self.supabase else 'Not connected'}"
                )

                return None

            rows = res.data or []

            contents = []
            for row in rows:
                content = row.get("content")
                if content:
                    # Handle jsonb content (dict/list) by converting to string
                    if isinstance(content, (dict, list)):
                        import json
                        content = json.dumps(content, ensure_ascii=False)
                    # Handle string content
                    if isinstance(content, str):
                        contents.append(content)

            lesson_text = (
                "\n\n".join(contents)
                if contents else "No lesson content available."
            )

            # PHASE 1: Cache for 3600 seconds (1 hour) - increased from 300 for better performance
            if self.cache_set:
                cache_key = f"lesson_text:{topic_id}"
                self.cache_set(cache_key, lesson_text, ttl=3600)

            if os.getenv("DEBUG", "0") == "1":
                self.logger.info(
                    f"Fetched lesson content for topic_id: {topic_id} "
                    f"({len(rows)} lesson(s))"
                )
            return lesson_text

        except Exception as e:
            self.logger.error(f"Error fetching lesson content: {e}")
            return None

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

        if not self.concept_agent:
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
                embedding = self.concept_agent.generate_embedding(chunk_text)
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
                # Upsert into lesson_embeddings table
                self.supabase.table("lesson_embeddings").upsert(
                    rows_to_upsert
                ).execute()
                if os.getenv("DEBUG", "0") == "1":
                    self.logger.info(
                        f"Generated {len(rows_to_upsert)} embeddings for "
                        f"lesson_id: {lesson_id}"
                    )
                return True

            return False

        except Exception as e:
            self.logger.error(f"Error generating lesson embeddings: {e}")
            return False

    def retrieve_lesson_chunks(
        self, question: str, lesson_id: str, k: int = 3
    ) -> List[Dict]:
        """
        Retrieve top-k relevant lesson chunks using pgvector similarity search.
        Uses caching (300 seconds TTL).

        Args:
            question: Student's question text
            lesson_id: Lesson ID to search within
            k: Number of chunks to retrieve (default: 3)

        Returns:
            List of dicts with chunk_text and distance
        """
        if not self.supabase:
            return []

        if not self.concept_agent:
            return []

        # Check cache first
        if self.cache_get:
            question_hash = hashlib.md5(question.encode()).hexdigest()[:8]
            cache_key = f"lesson_chunks:{lesson_id}:{question_hash}"
            cached = self.cache_get(cache_key)
            if cached is not None:
                if os.getenv("DEBUG", "0") == "1":
                    self.logger.info(
                        f"Cache hit for lesson_chunks: {lesson_id}"
                    )
                return cached

        # Generate embedding for question
        query_embedding = self.concept_agent.generate_embedding(question)
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

            # Cache the result (300 seconds TTL)
            if self.cache_set and chunks:
                question_hash = hashlib.md5(question.encode()).hexdigest()[:8]
                cache_key = f"lesson_chunks:{lesson_id}:{question_hash}"
                self.cache_set(cache_key, chunks, ttl=300)

            return chunks

        except Exception:
            # Fallback: try direct table query if RPC doesn't exist
            try:
                response = (
                    self.supabase.table("lesson_embeddings")
                    .select("chunk_text, embedding")
                    .eq("lesson_id", lesson_id)
                    .limit(k * 2)
                    .execute()
                )

                rows = response.data or []
                chunks = []
                for row in rows:
                    chunks.append({
                        "chunk_text": row.get("chunk_text", ""),
                        "distance": 0.5  # Placeholder
                    })

                result = chunks[:k]

                # Cache the result (300 seconds TTL)
                if self.cache_set and result:
                    question_hash = (
                        hashlib.md5(question.encode()).hexdigest()[:8]
                    )
                    cache_key = f"lesson_chunks:{lesson_id}:{question_hash}"
                    self.cache_set(cache_key, result, ttl=300)

                return result

            except Exception as e:
                self.logger.error(f"Error retrieving lesson chunks: {e}")
                return []

    def refresh_lesson_chunks(self, lesson_id: str) -> bool:
        """
        Refresh lesson chunk embeddings by regenerating them.

        Args:
            lesson_id: Lesson ID to refresh chunks for

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.supabase or not self.concept_agent:
            return False

        try:
            # Fetch lesson content
            res = (
                self.supabase.table("lessons")
                .select("content")
                .eq("lessons_id", lesson_id)
                .limit(1)
                .execute()
            )

            rows = res.data or []
            if not rows or not rows[0].get("content"):
                return False

            lesson_content = rows[0]["content"]

            # Regenerate embeddings using existing method
            return self.generate_lesson_embeddings(lesson_id, lesson_content)

        except Exception as e:
            self.logger.error(f"Error refreshing lesson chunks: {e}")
            return False
