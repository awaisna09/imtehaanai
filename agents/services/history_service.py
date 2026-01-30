#!/usr/bin/env python3
"""
History Service - Handles conversation history operations
"""

import logging
import os
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class HistoryService:
    """
    Manages conversation history, caching, and retrieval.
    """

    def __init__(self, supabase_client, cache_get, cache_set, cache_delete):
        """
        Initialize HistoryService.

        Args:
            supabase_client: Supabase client instance
            cache_get: Cache get function
            cache_set: Cache set function
            cache_delete: Cache delete function
        """
        self.supabase = supabase_client
        self.cache_get = cache_get
        self.cache_set = cache_set
        self.cache_delete = cache_delete
        self.logger = logging.getLogger(__name__)

    def get_recent_messages(
        self,
        conversation_id: str,
        limit: int = 10,
        subject: Optional[str] = None
    ) -> List[Dict]:
        """
        Fetch recent conversation messages from Supabase.
        Returns a list of dicts with:
            { "role": "user" | "assistant", "content": str }
        Uses cache with 30 second TTL (chat is dynamic).

        Args:
            conversation_id: Conversation ID to fetch messages for
            limit: Maximum number of messages to fetch (default: 10)
            subject: Optional subject name to filter messages by
                    (e.g., "Business Studies", "Economics")

        Returns:
            List of message dicts in chronological order
        """
        # Check cache first (include subject in cache key if filtering)
        cache_key = f"history:{conversation_id}:{subject or 'all'}"
        if self.cache_get:
            cached = self.cache_get(cache_key)
            if cached is not None:
                if os.getenv("DEBUG", "0") == "1":
                    self.logger.info(
                        f"Cache hit for history:{conversation_id}:"
                        f"{subject or 'all'}"
                    )
                return cached

        # If supabase is not configured, fall back to empty list
        if not self.supabase:
            return []

        try:
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

            # Wrap query with timeout (3 seconds)
            def query_func():
                query = (
                    self.supabase
                    .table("tutor_messages")
                    .select("role, message_text, subject")
                    .eq("conversation_id", conversation_id)
                )

                # Filter by subject if provided
                if subject:
                    query = query.eq("subject", subject)

                return (
                    query
                    .order("created_at", desc=True)
                    .limit(limit)
                    .execute()
                )

            res = safe_supabase_query(
                query_func, timeout=3, default_return=None
            )

            if res is None:
                return []

            rows = res.data or []

            # Convert to the structure expected by the LLM code
            messages = [
                {"role": row["role"], "content": row["message_text"]}
                for row in rows
            ]

            # Reverse to chronological order
            result = list(reversed(messages))

            # PHASE 1: Cache for 300 seconds (5 minutes) - increased from 30 for better performance
            if self.cache_set:
                self.cache_set(cache_key, result, ttl=300)

            return result

        except Exception:
            # Fail silently, return no messages
            return []

    def invalidate_cache(self, conversation_id: str):
        """
        Invalidate the cache for a conversation.

        Args:
            conversation_id: Conversation ID to invalidate cache for
        """
        cache_key = f"history:{conversation_id}"
        if self.cache_delete:
            try:
                self.cache_delete(cache_key)
            except Exception:
                pass
