#!/usr/bin/env python3
"""
Message Service - Handles message logging operations
"""

import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


class MessageService:
    """
    Stores messages (user + assistant) and manages conversation state.
    Handles logging messages to Supabase and cache invalidation.
    """

    def __init__(self, supabase_client, cache_delete):
        """
        Initialize MessageService.

        Args:
            supabase_client: Supabase client instance
            cache_delete: Cache delete function for invalidating history cache
        """
        self.supabase = supabase_client
        self.cache_delete = cache_delete
        self.logger = logging.getLogger(__name__)

    def log(
        self,
        user_id: Optional[str],
        lesson_topic: Optional[str],
        conversation_id: str,
        role: str,
        content: str,
        concept_ids: Optional[List[str]] = None,
        subject: Optional[str] = None,
        subject_id: Optional[int] = None,
        correlation_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Store a single tutor or student message in Supabase.
        Invalidates history cache for this conversation.
        
        Returns:
            message_id (str) if successful, None otherwise
        """
        """
        Store a single tutor or student message in Supabase.
        Invalidates history cache for this conversation.

        Args:
            user_id: Student/user ID
            lesson_topic: Topic/lesson ID
            conversation_id: Conversation ID
            role: Message role ('user' or 'assistant')
            content: Message content/text
            concept_ids: Optional list of related concept IDs
            subject: Optional subject name (e.g., "Business Studies",
                    "Economics")
            subject_id: Optional subject ID (e.g., 101, 119).
                       If subject is None but subject_id is provided,
                       subject name will be derived from subject_id
        """
        if not self.supabase:
            return

        # Determine subject name from subject_id if subject not provided
        subject_name = subject
        if not subject_name and subject_id is not None:
            subject_map = {
                101: "Business Studies",
                102: "Islamiyat",
                103: "Mathematics",
                104: "Physics",
                105: "Chemistry",
                113: "Pak Studies Geography",
                114: "Pak Studies History",
                119: "Economics"
            }
            subject_name = subject_map.get(subject_id)

        payload = {
            "user_id": user_id,
            "lesson_id": lesson_topic,
            "conversation_id": conversation_id,
            "role": role,
            "message_text": content,
            "concept_ids": concept_ids or [],
        }

        # Add subject to payload if available
        if subject_name:
            payload["subject"] = subject_name
        
        # Add correlation_id to payload if available
        if correlation_id:
            payload["correlation_id"] = correlation_id
        
        # Structured JSON logging for message persistence
        try:
            import json
            from datetime import datetime
            log_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "INFO",
                "message": f"Persisting {role} message to tutor_messages",
                "event": "message_persistence",
                "context": {
                    "user_id": user_id,
                    "conversation_id": conversation_id,
                    "role": role,
                    "message_length": len(content),
                    "concept_ids_count": len(concept_ids) if concept_ids else 0,
                    "correlation_id": correlation_id,
                    "subject_id": subject_id
                }
            }
            self.logger.info(json.dumps(log_data, default=str))
        except Exception:
            pass  # Non-critical, don't fail if logging fails

        try:
            # Add timeout protection for Supabase insert
            try:
                from langgraph_tutor import safe_supabase_query
            except ImportError:
                # Fallback if import fails
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
                        "value": None, "error": None, "completed": False
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

            def insert_func():
                result = (
                    self.supabase.table("tutor_messages")
                    .insert(payload)
                    .execute()
                )
                # Return message_id from inserted record
                if result.data and len(result.data) > 0:
                    return result.data[0].get("message_id")
                return None

            # Use async write pattern - don't block on message logging
            # This is already called via async_write in langgraph_tutor
            # But add timeout protection just in case
            message_id = safe_supabase_query(insert_func, timeout=10, default_return=None)
            return message_id

            # Invalidate history cache when new message is added
            cache_key = f"history:{conversation_id}"
            if self.cache_delete:
                try:
                    self.cache_delete(cache_key)
                except Exception:
                    pass
        except Exception:
            # Fail silently (logging can be added later)
            return None
