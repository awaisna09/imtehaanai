#!/usr/bin/env python3
"""
Answer Grading Agent for Multiple Subjects (Business Studies, Economics, Geography, History, Islamiyat)
This LangChain agent grades student answers against model answers
and provides detailed feedback. Supports questions with or without case studies/context.
"""

import os
import json
import asyncio
import hashlib
import time
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from pydantic import BaseModel, Field
import logging

# Import Supabase operations helper for concurrency limiting
from services.supabase_ops import sb_execute

# Mastery updates are now processed synchronously (no workers needed)

# Import cache if available
try:
    from cache import cache_get, cache_set
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False

    def cache_get(key): return None
    def cache_set(key, value, ttl=3600): return False

# Import centralized caching
try:
    from services.deterministic_cache import (
        cached_operation, CacheOperation, CacheTTL, CacheMetrics,
        invalidate_cache
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


# Configure logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Debug mode - set via environment variable
DEBUG_MODE = os.getenv("GRADING_DEBUG", "false").lower() == "true"


def _normalize_text(text: str) -> str:
    """
    Normalize text for comparison by:
    - Converting to lowercase
    - Removing extra whitespace
    - Removing punctuation (optional, but helps with comparison)
    """
    if not text:
        return ""
    # Convert to lowercase and strip
    normalized = text.lower().strip()
    # Remove extra whitespace
    normalized = " ".join(normalized.split())
    return normalized


def _extract_question_text(full_question: str) -> str:
    """
    Extract the actual question text from a question that might include context.
    Looks for patterns like "Q\n", "Question:", etc.
    """
    if not full_question:
        return ""
    
    # Try to find the question part after context
    # Common patterns: "Q\n", "Question:", "Q:", etc.
    lines = full_question.split('\n')
    question_started = False
    question_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        # Skip empty lines at start
        if not question_started and not line_stripped:
            continue
        # Check if this line starts the question
        if line_stripped.upper().startswith('Q') and len(line_stripped) <= 3:
            question_started = True
            continue
        # If we've started collecting question, add the line
        if question_started:
            question_lines.append(line_stripped)
        # If line looks like a question (ends with ? or starts with question words)
        elif any(line_stripped.lower().startswith(qw) for qw in ['outline', 'explain', 'describe', 'discuss', 'analyze', 'evaluate', 'what', 'how', 'why']):
            question_started = True
            question_lines.append(line_stripped)
    
    # If we found question lines, use them; otherwise use the whole text
    if question_lines:
        return ' '.join(question_lines)
    return full_question


def _check_answer_similarity_to_question(question: str, student_answer: str) -> tuple[bool, str]:
    """
    Check if the student answer is identical or too similar to the question.
    
    Returns:
        (is_similar, reason): Tuple indicating if answer is too similar and why
    """
    if not question or not student_answer:
        return False, ""
    
    # Extract the actual question text (in case it includes context)
    question_text = _extract_question_text(question)
    
    # Normalize both texts
    q_normalized = _normalize_text(question_text)
    a_normalized = _normalize_text(student_answer)
    
    # Also normalize the full question for substring checks
    q_full_normalized = _normalize_text(question)
    
    # Check 1: Exact match (after normalization) - answer matches question text
    if q_normalized == a_normalized:
        return True, "Your answer is identical to the question. Please provide your own answer."
    
    # Check 2: Answer is a substring of question (or vice versa)
    # This catches cases where student copies part of the question
    if len(a_normalized) > 10:  # Only check if answer has some content
        # Check if answer is in the question text
        if a_normalized in q_normalized:
            return True, "Your answer appears to be copied from the question. Please answer carefully."
        # Check if answer is in the full question (including context)
        if a_normalized in q_full_normalized:
            return True, "Your answer appears to be copied from the question. Please answer carefully."
        # CRITICAL: Check if question text appears anywhere in the answer
        # This catches cases where student pastes question at the end of their answer
        # We check if the normalized question text (at least 20 chars) appears in the answer
        if len(q_normalized) >= 20 and q_normalized in a_normalized:
            # Question text found in answer - this is not acceptable
            return True, "Your answer contains the question text. Please provide only your own answer without copying the question."
        # Also check if the full question (with context) appears in answer
        if len(q_full_normalized) >= 20 and q_full_normalized in a_normalized:
            return True, "Your answer contains the question text. Please provide only your own answer without copying the question."
        # Check if question is in answer (with minimal additional text) - for shorter questions
        if len(q_normalized) < 20 and q_normalized in a_normalized and len(a_normalized) - len(q_normalized) < 30:
            return True, "Your answer is too similar to the question. Please provide your own thoughtful answer."
    
    # Check 3: High word overlap (>70% of answer words are in question)
    q_words = set(q_normalized.split())
    a_words = set(a_normalized.split())
    
    # Remove common stop words for better comparison
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'should', 'could', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those'}
    q_words = q_words - stop_words
    a_words = a_words - stop_words
    
    if len(a_words) > 0:
        overlap = len(a_words & q_words) / len(a_words)
        # Lower threshold to 70% and require at least 5 words for more accuracy
        if overlap > 0.7 and len(a_words) >= 5:
            return True, "Your answer is too similar to the question. Please answer carefully with your own understanding."
        # For shorter answers, use higher threshold
        if overlap > 0.85 and len(a_words) >= 3:
            return True, "Your answer is too similar to the question. Please answer carefully with your own understanding."
    
    # Check 4: Answer is very short and mostly matches question words
    if len(a_normalized) < 100 and len(a_words) > 0:
        overlap_ratio = len(a_words & q_words) / len(a_words)
        if overlap_ratio > 0.6:
            return True, "Your answer appears to be copied from the question. Please provide a proper answer."
    
    # Check 5: Similarity ratio using sequence matching
    # If answer length is similar to question length and high overlap
    length_ratio = min(len(a_normalized), len(q_normalized)) / max(len(a_normalized), len(q_normalized)) if max(len(a_normalized), len(q_normalized)) > 0 else 0
    if length_ratio > 0.8 and len(a_words) > 0:
        overlap_ratio = len(a_words & q_words) / len(a_words)
        if overlap_ratio > 0.75:
            return True, "Your answer is too similar to the question. Please answer carefully."
    
    return False, ""


def async_write(fn, *args, **kwargs):
    """
    Fire-and-forget wrapper for database writes.
    Executes the function in a background thread to avoid blocking.
    Includes error handling to log exceptions.
    """
    def wrapped_fn():
        try:
            fn(*args, **kwargs)
        except Exception as e:
            # Get function name - handle both regular functions and bound methods
            fn_name = 'unknown'
            if hasattr(fn, '__name__'):
                fn_name = fn.__name__
            elif hasattr(fn, '__func__'):
                fn_name = fn.__func__.__name__
            elif hasattr(fn, '__qualname__'):
                fn_name = fn.__qualname__
            
            logger.error(
                f"[ERROR] async_write failed for {fn_name}: {e}",
                exc_info=True
            )
    
    threading.Thread(
        target=wrapped_fn, daemon=True
    ).start()


class GradingCriteria(BaseModel):
    """Criteria for grading answers across all subjects (Business Studies, Economics, Geography, History, Islamiyat)"""
    content_accuracy: float = Field(
        description="Accuracy of business concepts and terminology (0-10)"
    )
    structure_clarity: float = Field(
        description="Logical structure and clarity of argument (0-10)"
    )
    examples_relevance: float = Field(
        description="Relevance and quality of examples provided (0-10)"
    )
    critical_thinking: float = Field(
        description="Depth of analysis and critical thinking (0-10)"
    )
    business_terminology: float = Field(
        description="Proper use of business terminology (0-10)"
    )


class GradingResult(BaseModel):
    """Result of the grading process"""
    overall_score: float = Field(
        description="Overall score out of max_marks for the question"
    )
    percentage: float = Field(description="Percentage score")
    grade: str = Field(description="Letter grade (A, B, C, D, F)")
    strengths: List[str] = Field(
        description="List of strengths in the answer"
    )
    areas_for_improvement: List[str] = Field(
        description="List of areas that need improvement"
    )
    specific_feedback: str = Field(
        description="Detailed feedback on the answer"
    )
    suggestions: List[str] = Field(
        description="Specific suggestions for improvement"
    )
    reasoning_category: str = Field(
        default="unknown",
        description=(
            "One of: correct, partial, mild_confusion, wrong, "
            "high_confusion, misconception"
        )
    )
    has_misconception: bool = Field(
        default=False,
        description=(
            "True if the student answer contains a conceptual "
            "misconception"
        )
    )
    topic_name: Optional[str] = Field(
        default=None,
        description="Name of the topic for this question"
    )
    primary_concept_ids: List[str] = Field(
        default_factory=list,
        description="List of primary syllabus concept IDs"
    )
    secondary_concept_ids: List[str] = Field(
        default_factory=list,
        description="List of secondary or related concept IDs"
    )
    mastery_deltas: Dict[str, float] = Field(
        default_factory=dict,
        description="Mapping of concept_id to mastery delta change"
    )
    max_marks: Optional[int] = Field(
        default=None,
        description="Total marks possible for this question"
    )


class SupabaseRepository:
    """Repository for Supabase database operations"""

    def __init__(self, supabase_client=None):
        """
        Initialize Supabase repository with shared client.
        
        Args:
            supabase_client: Optional Supabase client instance.
                           If None, will use singleton factory.
        """
        if supabase_client is not None:
            self.client = supabase_client
            self.enabled = True
        else:
            # Use singleton factory
            try:
                from services.supabase_client import get_supabase_client
                self.client = get_supabase_client()
                self.enabled = self.client is not None
            except Exception as e:
                if DEBUG_MODE:
                    logger.warning(
                        f"[WARNING] Failed to get Supabase client: {e}"
                    )
                self.client = None
                self.enabled = False
        
        if DEBUG_MODE:
            if self.enabled:
                logger.info(
                    "✅ [SUPABASE] SupabaseRepository initialized with shared client"
                )
            else:
                logger.warning(
                    "[WARNING] SupabaseRepository disabled - "
                    "no Supabase client available"
                )

    def log_question_attempt(self, **data):
        """
        Insert a row into question_attempts if Supabase is enabled.

        Handles JSONB arrays (primary_concept_ids, secondary_concept_ids)
        automatically via Supabase client.
        """
        if not self.enabled:
            return None
        try:
            if DEBUG_MODE:
                logger.info(
                    "📝 [SUPABASE] Inserting into table: question_attempts"
                )
                logger.info("   Data:")
                logger.info(f"     user_id: {data.get('user_id')}")
                logger.info(f"     question_id: {data.get('question_id')}")
                logger.info(f"     topic_id: {data.get('topic_id')}")
                logger.info(f"     raw_score: {data.get('raw_score')}")
                logger.info(f"     percentage: {data.get('percentage')}")
                logger.info(f"     grade: {data.get('grade')}")
                logger.info(
                    f"     reasoning_category: "
                    f"{data.get('reasoning_category')}"
                )
                logger.info(
                    f"     has_misconception: "
                    f"{data.get('has_misconception')}"
                )
                logger.info(
                    f"     primary_concept_ids: "
                    f"{data.get('primary_concept_ids')}"
                )
                logger.info(
                    f"     secondary_concept_ids: "
                    f"{data.get('secondary_concept_ids')}"
                )
            result = sb_execute(
                self.client.table("question_attempts").insert(data)
            )

            if DEBUG_MODE and result.data:
                logger.info("=" * 80)
                logger.info(
                    "✅ [SUPABASE] Entry created in question_attempts:"
                )
                created_entry = result.data[0]
                entry_id = created_entry.get('attempt_id', 'N/A')
                logger.info(f"   Entry ID: {entry_id}")
                logger.info(f"   user_id: {created_entry.get('user_id')}")
                q_id = created_entry.get('question_id')
                logger.info(f"   question_id: {q_id}")
                logger.info(f"   topic_id: {created_entry.get('topic_id')}")
                logger.info(
                    f"   raw_score: {created_entry.get('raw_score')}"
                )
                logger.info(
                    f"   percentage: {created_entry.get('percentage')}"
                )
                logger.info(f"   grade: {created_entry.get('grade')}")
                reasoning = created_entry.get('reasoning_category')
                logger.info(f"   reasoning_category: {reasoning}")
                misconception = created_entry.get('has_misconception')
                logger.info(f"   has_misconception: {misconception}")
                primary = created_entry.get('primary_concept_ids')
                logger.info(f"   primary_concept_ids: {primary}")
                secondary = created_entry.get('secondary_concept_ids')
                logger.info(f"   secondary_concept_ids: {secondary}")
                created = created_entry.get('created_at', 'N/A')
                logger.info(f"   created_at: {created}")
                logger.info("=" * 80)

            return result
        except Exception as e:
            logger.error(f"Error logging question attempt: {e}")
            return None

    def fetch_question_by_id(self, question_id: str, subject: str = None):
        """
        Fetch question + model answer + optional metadata
        (max_marks, difficulty_level) if exists.
        
        Supports all subjects:
        - Business Studies: business_activity_questions
        - Economics: questions_economics
        - Geography: questions_geography
        - History: questions_history
        - Islamiyat: questions_islamiyat
        """
        if not self.enabled:
            return None
        
        # Determine table name based on subject
        subject_normalized = (subject or "Business Studies").strip().lower()
        if "economics" in subject_normalized:
            table_name = "questions_economics"
        elif "geography" in subject_normalized or "pak studies geography" in subject_normalized:
            table_name = "questions_geography"
        elif "history" in subject_normalized or "pak studies history" in subject_normalized:
            table_name = "questions_history"
        elif "islamiyat" in subject_normalized or "islamiat" in subject_normalized:
            table_name = "questions_islamiyat"
        else:
            # Default to Business Studies
            table_name = "business_activity_questions"
        
        if DEBUG_MODE:
            logger.info(
                f"📊 [SUPABASE] Reading from table: {table_name}"
            )
            logger.info(
                f"   Query: SELECT * WHERE question_id = {question_id}"
            )
            logger.info(
                f"   Subject: {subject or 'Business Studies (default)'}"
            )
        
        res = sb_execute(
            self.client.table(table_name)
            .select("*")
            .eq("question_id", question_id)
        )
        if DEBUG_MODE and res.data:
            logger.info(
                f"✅ [SUPABASE] Retrieved question data: "
                f"question_id={question_id}, "
                f"marks={res.data[0].get('marks')}, "
                f"topic_id={res.data[0].get('topic_id')}, "
                f"table={table_name}"
            )
        return res.data[0] if res.data else None

    def fetch_topic_name_by_id(self, topic_id: str | int | None):
        """
        Fetch topic name from topics table using topic_id.

        Returns topic name string or None if not found.
        """
        if not self.enabled:
            logger.warning(
                "⚠️  [TOPIC] Cannot fetch topic_name: Supabase disabled"
            )
            return None

        if not topic_id:
            logger.warning(
                "⚠️  [TOPIC] Cannot fetch topic_name: topic_id is None"
            )
            return None

        try:
            # Convert topic_id to int for query
            topic_id_int = int(topic_id) if topic_id else None
            if not topic_id_int:
                logger.warning(
                    f"⚠️  [TOPIC] Invalid topic_id: {topic_id} "
                    f"(cannot convert to int)"
                )
                return None

            logger.info(
                f"📊 [TOPIC] Fetching topic_name from topics table: "
                f"topic_id={topic_id_int}"
            )

            # Query topics table - try 'topic' column first, then 'title'
            # as fallback
            res = sb_execute(
                self.client.table("topics")
                .select("*")  # Select all columns to see what's available
                .eq("topic_id", topic_id_int)
            )

            if res.data and len(res.data) > 0:
                topic_data = res.data[0]
                logger.info(
                    f"📊 [TOPIC] Raw topic data: {topic_data}"
                )
                # Try 'topic' column first (most common)
                topic_name = topic_data.get("topic")
                if not topic_name:
                    # Fallback to 'title' column
                    topic_name = topic_data.get("title")
                if not topic_name:
                    # Try 'name' as another fallback
                    topic_name = topic_data.get("name")

                if topic_name:
                    logger.info(
                        f"✅ [TOPIC] Successfully fetched topic_name: "
                        f"'{topic_name}' for topic_id={topic_id_int}"
                    )
                    return topic_name
                else:
                    logger.warning(
                        f"⚠️  [TOPIC] Topic found but no name in data: "
                        f"topic_id={topic_id_int}, data={topic_data}"
                    )
                    return None
            else:
                logger.warning(
                    f"⚠️  [TOPIC] No topic found in database for "
                    f"topic_id={topic_id_int}"
                )

            return None
        except ValueError as e:
            logger.error(
                f"❌ [TOPIC] Error converting topic_id {topic_id} to int: {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"❌ [TOPIC] Error fetching topic name for topic_id "
                f"{topic_id}: {e}"
            )
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def update_mastery(
        self,
        user_id: str,
        concept_id: str,
        delta: float,
        topic_name: str | None = None,
        subject: str | None = None
    ):
        """
        Update or insert mastery (0–100 clamp).

        Args:
            user_id: User identifier
            concept_id: Concept identifier
            delta: Mastery delta to apply
            topic_name: Optional topic name to store
            subject: Optional subject name to store (e.g., "Business Studies", "Economics")

        Returns the new mastery value, or None if Supabase is disabled.
        """
        if not self.enabled:
            logger.error(
                "❌ [SUPABASE] Supabase is DISABLED - cannot update mastery. "
                "Check SUPABASE_URL and "
                "SUPABASE_SERVICE_ROLE_KEY/SUPABASE_ANON_KEY"
            )
            return None

        if not self.client:
            logger.error(
                "❌ [SUPABASE] Supabase client is None - cannot update "
                "mastery. Check Supabase initialization."
            )
            return None

        logger.info(
            f"🔍 [SUPABASE] update_mastery called: user_id={user_id}, "
            f"concept_id={concept_id}, delta={delta}, "
            f"topic_name={topic_name}, subject={subject}"
        )

        try:
            if DEBUG_MODE:
                logger.info(
                    "📊 [SUPABASE] Reading from table: user_mastery"
                )
                logger.info(
                    f"   Query: SELECT * WHERE user_id={user_id} "
                    f"AND concept_id={concept_id}"
                )
            # Normalize concept_id to title case for consistency
            # This ensures all new entries use consistent casing
            def normalize_to_title_case(text: str) -> str:
                """Convert text to title case, handling multi-word concepts"""
                if not text:
                    return ""
                words = text.strip().split()
                return " ".join(word.capitalize() for word in words)
            
            concept_id_normalized = concept_id.lower().strip() if concept_id else ""
            concept_id_title_case = normalize_to_title_case(concept_id) if concept_id else ""
            
            # First try exact match with the provided concept_id
            res = sb_execute(
                self.client.table("user_mastery")
                .select("*")
                .eq("user_id", user_id)
                .eq("concept_id", concept_id)
            )
            
            # If no exact match, try case-insensitive lookup
            if not res.data or len(res.data) == 0:
                # Get all user_mastery entries for this user and find case-insensitive match
                all_user_mastery = sb_execute(
                    self.client.table("user_mastery")
                    .select("*")
                    .eq("user_id", user_id)
                )
                if all_user_mastery.data:
                    for entry in all_user_mastery.data:
                        existing_cid = entry.get("concept_id", "")
                        if existing_cid and existing_cid.lower().strip() == concept_id_normalized:
                            # Found case-insensitive match - use existing entry
                            res.data = [entry]
                            # Use the existing entry's concept_id for the update
                            # but for new inserts, we'll use title case
                            existing_concept_id = existing_cid
                            logger.info(
                                f"🔄 [MASTERY] Found case-insensitive match: existing='{existing_cid}', "
                                f"searching='{concept_id}' - will update existing entry"
                            )
                            break
            
            # If we found an existing entry, use its concept_id for update
            # If not found, we'll insert with title case concept_id
            if res.data and len(res.data) > 0:
                existing_entry = res.data[0]
                existing_concept_id = existing_entry.get("concept_id", concept_id)
                # Use existing concept_id for consistency with database
                concept_id_for_db = existing_concept_id
                current = existing_entry.get("mastery", 50)
            else:
                # New entry - use title case for consistency
                concept_id_for_db = concept_id_title_case if concept_id_title_case else concept_id
                current = 50
            new_mastery = max(0, min(100, current + delta))

            if DEBUG_MODE:
                logger.info(
                    f"📈 [MASTERY] Level Update for Concept {concept_id} "
                    f"(using '{concept_id_for_db}' in database):"
                )
                logger.info(f"   Previous Mastery: {current:.2f}")
                logger.info(f"   Delta Applied: {delta:+.2f}")
                logger.info(f"   New Mastery: {new_mastery:.2f}")
                logger.info(
                    f"   Clamped: {new_mastery != current + delta}"
                )
                if topic_name:
                    logger.info(f"   Topic Name: {topic_name}")

            update_data = {"mastery": new_mastery}
            insert_data = {
                "user_id": user_id,
                "concept_id": concept_id_for_db,  # Use normalized/converted concept_id
                "mastery": new_mastery
            }

            # Add topic_name if provided (explicitly check for
            # non-empty string)
            topic_name_clean = None
            if topic_name and topic_name.strip():
                topic_name_clean = topic_name.strip()
                update_data["topic_name"] = topic_name_clean
                insert_data["topic_name"] = topic_name_clean
                logger.info(
                    f"📝 [TOPIC_NAME] Adding topic_name to "
                    f"update/insert: '{topic_name_clean}'"
                )
            else:
                logger.warning(
                    f"⚠️  [TOPIC_NAME] topic_name is None or empty: "
                    f"'{topic_name}' - will not be stored"
                )

            # Add subject if provided (explicitly check for non-empty string)
            # If no subject provided, leave as null
            subject_clean = None
            if subject and subject.strip():
                subject_clean = subject.strip()
                update_data["subject"] = subject_clean
                insert_data["subject"] = subject_clean
                logger.info(
                    f"📚 [SUBJECT] Adding subject to "
                    f"update/insert: '{subject_clean}'"
                )
            else:
                logger.info(
                    f"📚 [SUBJECT] No subject provided - will store as NULL"
                )

            # Use UPSERT to ensure we update if exists, insert if not
            # This prevents duplicates even if the lookup missed an existing entry
            upsert_data = insert_data.copy()  # Has all fields needed for insert/update
            
            # Determine if this is an update or insert for logging
            is_update = res.data and len(res.data) > 0
            operation = "Updating" if is_update else "Inserting"
            
            logger.info(
                f"📝 [SUPABASE] {operation} user_mastery: "
                f"user_id={user_id}, concept_id={concept_id_for_db}"
            )
            logger.info(
                f"   Upsert data: {upsert_data}"
            )
            
            if DEBUG_MODE:
                logger.info(
                    f"   Full data: {{user_id: {user_id}, "
                    f"concept_id: {concept_id_for_db}, "
                    f"mastery: {new_mastery:.2f}, "
                    f"topic_name: {topic_name or 'None'}, "
                    f"subject: {subject or 'None'}}}"
                )
            
            try:
                # Use upsert with explicit conflict resolution on (user_id, concept_id)
                # This ensures we update existing entries instead of creating duplicates
                # First, try to find existing entry with exact match
                # Note: user_mastery table uses composite primary key (user_id, concept_id), not id column
                existing_check = sb_execute(
                    self.client.table("user_mastery")
                    .select("concept_id")
                    .eq("user_id", user_id)
                    .eq("concept_id", concept_id_for_db)
                    .limit(1)
                )
                
                if existing_check.data and len(existing_check.data) > 0:
                    # Entry exists - use UPDATE to avoid duplicates
                    logger.info(
                        f"🔄 [MASTERY] Existing entry found, using UPDATE: "
                        f"concept_id='{concept_id_for_db}'"
                    )
                    result = sb_execute(
                        self.client.table("user_mastery")
                        .update(update_data)
                        .eq("user_id", user_id)
                        .eq("concept_id", concept_id_for_db)
                    )
                else:
                    # No entry found - use INSERT
                    logger.info(
                        f"➕ [MASTERY] No existing entry found, using INSERT: "
                        f"concept_id='{concept_id_for_db}'"
                    )
                    result = sb_execute(
                        self.client.table("user_mastery")
                        .insert(insert_data)
                    )
            except Exception as upsert_error:
                # If upsert with on_conflict fails, fall back to manual update/insert
                error_str = str(upsert_error).lower()
                if any(keyword in error_str for keyword in ["on_conflict", "conflict", "unique", "constraint"]):
                    logger.warning(
                        f"⚠️  [SUPABASE] Upsert with on_conflict not supported, "
                        f"falling back to manual update/insert: {upsert_error}"
                    )
                    # Fallback: Use the existing logic
                    if res.data and len(res.data) > 0:
                        # Update existing
                        try:
                            result = (
                                self.client.table("user_mastery")
                                .update(update_data)
                                .eq("user_id", user_id)
                                .eq("concept_id", concept_id_for_db)
                            )
                        except Exception as update_error:
                            logger.error(
                                f"❌ [SUPABASE] Error updating user_mastery: {update_error}"
                            )
                            raise
                    else:
                        # Insert new
                        try:
                            result = (
                                self.client.table("user_mastery")
                                .insert(insert_data)
                            )
                            # If insert doesn't return data, fetch it
                            if not result.data or len(result.data) == 0:
                                fetch_result = (
                                    self.client.table("user_mastery")
                                    .select("*")
                                    .eq("user_id", user_id)
                                    .eq("concept_id", concept_id_for_db)
                                )
                                if fetch_result.data:
                                    result.data = fetch_result.data
                        except Exception as insert_error:
                            # Check if it's a duplicate key error
                            if "duplicate" in str(insert_error).lower() or "unique" in str(insert_error).lower():
                                # Entry already exists, try update instead
                                logger.info(
                                    f"🔄 [MASTERY] Insert failed due to duplicate, "
                                    f"attempting update instead"
                                )
                                try:
                                    result = (
                                        self.client.table("user_mastery")
                                        .update(update_data)
                                        .eq("user_id", user_id)
                                        .eq("concept_id", concept_id_for_db)
                                    )
                                except Exception as update_error2:
                                    logger.error(
                                        f"❌ [SUPABASE] Error updating after duplicate: {update_error2}"
                                    )
                                    raise
                            else:
                                logger.error(
                                    f"❌ [SUPABASE] Error inserting into user_mastery: {insert_error}"
                                )
                                raise
                else:
                    logger.error(
                        f"❌ [SUPABASE] Error upserting into user_mastery: {upsert_error}"
                    )
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    raise
            
            # Verify the result
            if result.data and len(result.data) > 0:
                saved_entry = result.data[0]
                stored_topic_name = saved_entry.get('topic_name')
                stored_subject = saved_entry.get('subject')
                
                if DEBUG_MODE:
                    logger.info(f"   ✅ Entry saved ({'updated' if is_update else 'inserted'}):")
                    logger.info(
                        f"      user_id: {saved_entry.get('user_id')}"
                    )
                    logger.info(
                        f"      concept_id: {saved_entry.get('concept_id')}"
                    )
                    logger.info(
                        f"      mastery: {saved_entry.get('mastery')}"
                    )
                    logger.info(
                        f"      topic_name: {stored_topic_name or 'NULL'}"
                    )
                    logger.info(
                        f"      subject: {stored_subject or 'NULL'}"
                    )
                
                # Verify topic_name was saved correctly
                if topic_name_clean:
                    if stored_topic_name != topic_name_clean:
                        logger.warning(
                            f"⚠️  [TOPIC_NAME] Mismatch! "
                            f"Expected: '{topic_name_clean}', "
                            f"Got: '{stored_topic_name}'"
                        )
                    else:
                        logger.info(
                            f"✅ [TOPIC_NAME] Verified: topic_name "
                            f"saved correctly: '{stored_topic_name}'"
                        )
                
                # Verify subject was saved correctly
                if subject_clean:
                    if stored_subject != subject_clean:
                        logger.warning(
                            f"⚠️  [SUBJECT] Mismatch! "
                            f"Expected: '{subject_clean}', "
                            f"Got: '{stored_subject}'"
                        )
                    else:
                        logger.info(
                            f"✅ [SUBJECT] Verified: subject "
                            f"saved correctly: '{stored_subject}'"
                        )
                
                # Update mastery_states.mastery_micro with the same mastery value
                # Match by user_id and subject name (e.g., if Economics mastery is updated, update Economics row)
                # Supports both INSERT (new row) and UPSERT/UPDATE (existing row) operations
                if subject_clean and new_mastery is not None:
                    try:
                        # Validate user_id is a valid UUID (required for mastery_states)
                        import uuid
                        try:
                            uuid.UUID(user_id)
                        except (ValueError, TypeError):
                            logger.debug(
                                f"[DEBUG] Skipping mastery_states update - "
                                f"user_id '{user_id}' is not a valid UUID"
                            )
                        else:
                            # Check if mastery_states row exists for this user and subject
                            existing_mastery_states = sb_execute(
                                self.client.table("mastery_states")
                                .select("user_id, mastery_micro, mastery_concept, mastery_macro, subject")
                                .eq("user_id", user_id)
                                .eq("subject", subject_clean)
                                .limit(1)
                            )
                            
                            # Determine if row exists
                            row_exists = existing_mastery_states.data and len(existing_mastery_states.data) > 0
                            
                            # Prepare update/insert data
                            # For updates: only update mastery_micro (preserve mastery_concept and mastery_macro)
                            # For inserts: set defaults for mastery_concept and mastery_macro
                            if row_exists:
                                existing_row = existing_mastery_states.data[0]
                                # Only update mastery_micro, preserve other fields
                                update_data = {
                                    "mastery_micro": new_mastery,  # Same mastery as stored in user_mastery
                                    "subject": subject_clean,  # Ensure subject is set
                                    "updated_at": datetime.now().isoformat()
                                }
                                
                                # Try UPDATE first (more efficient for existing rows)
                                try:
                                    result = sb_execute(
                                        self.client.table("mastery_states")
                                        .update(update_data)
                                        .eq("user_id", user_id)
                                        .eq("subject", subject_clean)
                                    )
                                    logger.info(
                                        f"✅ [MASTERY_STATES] Updated mastery_states.mastery_micro (UPDATE): "
                                        f"user_id={user_id}, subject={subject_clean}, mastery_micro={new_mastery}"
                                    )
                                except Exception as update_error:
                                    # If update fails, try upsert as fallback (preserve existing values)
                                    logger.debug(
                                        f"[DEBUG] Update failed, trying upsert: {update_error}"
                                    )
                                    try:
                                        # Preserve existing mastery_concept and mastery_macro when upserting
                                        upsert_data = {
                                            "user_id": user_id,
                                            "mastery_micro": new_mastery,
                                            "mastery_concept": existing_row.get("mastery_concept", 0),
                                            "mastery_macro": existing_row.get("mastery_macro", 0),
                                            "subject": subject_clean,
                                            "updated_at": datetime.now().isoformat()
                                        }
                                        sb_execute(
                                            self.client.table("mastery_states")
                                            .upsert(upsert_data, on_conflict="user_id,subject")
                                        )
                                        logger.info(
                                            f"✅ [MASTERY_STATES] Updated mastery_states.mastery_micro (UPSERT fallback): "
                                            f"user_id={user_id}, subject={subject_clean}, mastery_micro={new_mastery}"
                                        )
                                    except Exception as upsert_error:
                                        logger.warning(
                                            f"⚠️  [MASTERY_STATES] Both update and upsert failed: {upsert_error}"
                                        )
                            else:
                                # Row doesn't exist - INSERT new row with defaults
                                insert_data = {
                                    "user_id": user_id,
                                    "mastery_micro": new_mastery,  # Same mastery as stored in user_mastery
                                    "mastery_concept": 0,  # Default for new row
                                    "mastery_macro": 0,  # Default for new row
                                    "subject": subject_clean,
                                    "updated_at": datetime.now().isoformat()
                                }
                                
                                # Try INSERT first
                                try:
                                    sb_execute(
                                        self.client.table("mastery_states")
                                        .insert(insert_data)
                                    )
                                    logger.info(
                                        f"✅ [MASTERY_STATES] Inserted mastery_states row (INSERT): "
                                        f"user_id={user_id}, subject={subject_clean}, mastery_micro={new_mastery}"
                                    )
                                except Exception as insert_error:
                                    # If insert fails (e.g., duplicate), try upsert as fallback
                                    error_str = str(insert_error).lower()
                                    if any(keyword in error_str for keyword in ["duplicate", "unique", "constraint", "conflict"]):
                                        logger.debug(
                                            f"[DEBUG] Insert failed due to duplicate, trying upsert: {insert_error}"
                                        )
                                        try:
                                            sb_execute(
                                                self.client.table("mastery_states")
                                                .upsert(insert_data, on_conflict="user_id,subject")
                                            )
                                            logger.info(
                                                f"✅ [MASTERY_STATES] Inserted mastery_states row (UPSERT fallback): "
                                                f"user_id={user_id}, subject={subject_clean}, mastery_micro={new_mastery}"
                                            )
                                        except Exception as upsert_error:
                                            logger.warning(
                                                f"⚠️  [MASTERY_STATES] Both insert and upsert failed: {upsert_error}"
                                            )
                                    else:
                                        logger.warning(
                                            f"⚠️  [MASTERY_STATES] Insert failed: {insert_error}"
                                        )
                    except Exception as mastery_states_error:
                        logger.warning(
                            f"⚠️  [MASTERY_STATES] Error updating mastery_states.mastery_micro: {mastery_states_error}",
                            exc_info=True
                        )
                        # Don't fail the entire mastery update if mastery_states update fails
            else:
                logger.error(
                    "❌ [SUPABASE] Upsert returned no data - "
                    "cannot verify entry was saved"
                )
            return new_mastery
        except Exception as e:
            logger.error(
                f"Error updating mastery for user {user_id}, "
                f"concept {concept_id}: {e}"
            )
            return None

    # Removed log_trend, batch_log_trends, update_weakness, and batch_update_weaknesses methods
    # user_trends and user_weaknesses tables are not used anywhere in the application

    def search_concepts_by_question_embedding(
        self,
        question_id: str,
        match_limit: int = 5,
    ):
        """
        Search for related concepts using question_embeddings ->
        concept_embeddings.

        Uses caching to avoid repeated database queries.
        """
        if not self.enabled:
            return []

        cache_key = f"concepts_for_question:{question_id}:{match_limit}"
        if CACHE_AVAILABLE:
            cached = cache_get(cache_key)
            if cached is not None:
                if DEBUG_MODE:
                    logger.info(
                        f"🔍 [CACHE HIT] Concepts for question {question_id}"
                    )
                return cached

        try:
            if DEBUG_MODE:
                logger.info(
                    "📊 [SUPABASE] Calling RPC function: "
                    "match_concepts_for_question"
                )
                logger.info(
                    f"   Parameters: question_id={question_id}, "
                    f"match_limit={match_limit}"
                )
            res = self.client.rpc(
                "match_concepts_for_question",
                {
                    "q_question_id": question_id,
                    "match_count": match_limit,
                },
            )
            result = res.data or []

            if DEBUG_MODE:
                logger.info(
                    f"✅ [SUPABASE] RPC returned {len(result)} concepts"
                )
                if result:
                    concept_ids = [r.get("concept_id") for r in result[:5]]
                    logger.info(f"   Concept IDs: {concept_ids}")

            if CACHE_AVAILABLE and result:
                cache_set(cache_key, result, ttl=600)
                if DEBUG_MODE:
                    logger.info(
                        f"💾 [CACHE SET] Concepts for question {question_id}"
                    )

            return result
        except Exception as e:
            if DEBUG_MODE:
                logger.error(f"Error searching concepts: {e}")
            return []

    def fetch_lesson_context_for_question(self, question_id: str, subject: str = None):
        """
        Fetch lesson/context/case study text linked to a question.

        Supports all subjects:
        - Business Studies: queries business_activity_questions for 'context'
        - Economics: queries questions_economics for 'case_study'
        - Islamiyat: queries questions_islamiyat for 'context'
        - History: returns empty (no context/case study)
        - Geography: returns empty (no context/case study)
        
        Uses caching to avoid repeated database queries.
        """
        if not self.enabled:
            return ""

        # Determine table and column based on subject
        subject_normalized = (subject or "Business Studies").strip().lower()
        
        # History and Geography don't have context/case study
        if "history" in subject_normalized or "pak studies history" in subject_normalized:
            if DEBUG_MODE:
                logger.info(
                    f"ℹ️  [CONTEXT] History questions don't have context/case study - returning empty"
                )
            return ""
        
        if "geography" in subject_normalized or "pak studies geography" in subject_normalized:
            if DEBUG_MODE:
                logger.info(
                    f"ℹ️  [CONTEXT] Geography questions don't have context/case study - returning empty"
                )
            return ""
        
        # Determine table and column for subjects that have context/case study
        if "economics" in subject_normalized:
            table_name = "questions_economics"
            column_name = "case_study"
        elif "islamiyat" in subject_normalized or "islamiat" in subject_normalized:
            table_name = "questions_islamiyat"
            column_name = "context"
        else:
            # Default to Business Studies
            table_name = "business_activity_questions"
            column_name = "context"

        cache_key = f"lesson_context:{question_id}:{subject or 'business'}"
        if CACHE_AVAILABLE:
            cached = cache_get(cache_key)
            if cached is not None:
                if DEBUG_MODE:
                    logger.info(
                        f"🔍 [CACHE HIT] Lesson context for question "
                        f"{question_id} (subject: {subject})"
                    )
                return cached

        try:
            if DEBUG_MODE:
                logger.info(
                    f"📊 [SUPABASE] Reading from table: {table_name}"
                )
                logger.info(
                    f"   Query: SELECT {column_name} WHERE question_id = "
                    f"{question_id}"
                )
                logger.info(
                    f"   Subject: {subject or 'Business Studies (default)'}"
                )
            qb_res = (
                self.client.table(table_name)
                .select(column_name)
                .eq("question_id", question_id)
            )
            if not qb_res.data:
                if DEBUG_MODE:
                    logger.info(
                        f"⚠️  [SUPABASE] No context/case study found for question "
                        f"{question_id} in table {table_name}"
                    )
                return ""

            context = qb_res.data[0].get(column_name) or ""
            if DEBUG_MODE:
                logger.info(
                    f"✅ [SUPABASE] Retrieved {column_name}: "
                    f"{len(context)} characters from {table_name}"
                )

            if CACHE_AVAILABLE and context:
                cache_set(cache_key, context, ttl=3600)
                if DEBUG_MODE:
                    logger.info(
                        f"💾 [CACHE SET] Lesson context for question "
                        f"{question_id}"
                    )

            return context

        except Exception as e:
            if DEBUG_MODE:
                logger.error(f"Error fetching lesson context: {e}")
            return ""


class ConceptDetector:
    """Detect primary/secondary concepts using embeddings (LLM fallback)."""

    def __init__(
        self,
        repo: SupabaseRepository,
        llm: ChatOpenAI,
        question_searcher: "QuestionEmbeddingsSearcher | None" = None,
    ):
        self.repo = repo
        self.llm = llm
        self.question_searcher = question_searcher

    async def detect_async(
        self,
        question: str,
        student_answer: str,
        question_id: str | None = None
    ):
        """Async version of detect for parallel execution"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.detect, question, student_answer, question_id
        )

    def detect(
        self,
        question: str,
        student_answer: str,
        question_id: str | None = None
    ):
        """
        Return {
            "primary": [concept_id1, concept_id2],
            "secondary": [concept_id3]
        }
        Uses embeddings first, falls back to LLM.
        """
        start_time = time.time()

        if (
            self.repo.enabled
            and self.question_searcher
            and question_id
        ):
            concept_ids = (
                self.question_searcher.get_concept_ids_for_question(
                    question_id=question_id,
                    limit=5,
                )
            )
            if concept_ids:
                elapsed = time.time() - start_time
                if DEBUG_MODE:
                    logger.info(
                        f"🔍 [ConceptDetector] Found {len(concept_ids)} "
                        f"concepts via embeddings in {elapsed:.2f}s"
                    )
                return {"primary": concept_ids, "secondary": []}

        if not self.repo.enabled:
            return {"primary": [], "secondary": []}

        question_answer_hash = hashlib.md5(
            f'{question}:{student_answer}'.encode()
        ).hexdigest()
        cache_key = f"concepts_llm:{question_answer_hash}"
        if CACHE_AVAILABLE:
            cached = cache_get(cache_key)
            if cached is not None:
                elapsed = time.time() - start_time
                if DEBUG_MODE:
                    logger.info(
                        f"🔍 [CACHE HIT] Concepts detected in {elapsed:.2f}s"
                    )
                return cached

        q_short = question[:250] + "..." if len(question) > 250 else question
        a_short = (
            student_answer[:250] + "..."
            if len(student_answer) > 250
            else student_answer
        )
        prompt = (
            f"Identify subject concepts. Return JSON:\n"
            f'{{"primary": ["id1"], "secondary": ["id2"]}}\n'
            f"Q: {q_short}\nA: {a_short}"
        )
        out = self.llm.invoke(prompt).content

        try:
            data = json.loads(out)
            elapsed = time.time() - start_time

            if CACHE_AVAILABLE:
                cache_set(cache_key, data, ttl=1800)
                if DEBUG_MODE:
                    logger.info(
                        f"💾 [CACHE SET] Concepts detected in {elapsed:.2f}s"
                    )

            if DEBUG_MODE:
                logger.info(
                    f"🔍 [ConceptDetector] Detected concepts in "
                    f"{elapsed:.2f}s: {data}"
                )

            return data
        except Exception as e:
            if DEBUG_MODE:
                logger.error(f"Error in concept detection: {e}")
            return {"primary": [], "secondary": []}


class ReasoningClassifier:
    """Classify reasoning quality of student answers"""

    def __init__(self, llm: ChatOpenAI):
        self.llm = llm

    async def classify_async(
        self, question: str, model_answer: str, student_answer: str
    ) -> str:
        """Async version for parallel execution"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.classify, question, model_answer, student_answer
        )

    def classify(
        self, question: str, model_answer: str, student_answer: str
    ) -> str:
        start_time = time.time()

        reasoning_hash = hashlib.md5(
            f'{question}:{model_answer}:{student_answer}'.encode()
        ).hexdigest()
        cache_key = f"reasoning:{reasoning_hash}"
        if CACHE_AVAILABLE:
            cached = cache_get(cache_key)
            if cached is not None:
                elapsed = time.time() - start_time
                if DEBUG_MODE:
                    logger.info(
                        f"🔍 [CACHE HIT] Reasoning classified in "
                        f"{elapsed:.2f}s: {cached}"
                    )
                return cached

        q_short = question[:250] + "..." if len(question) > 250 else question
        m_short = (
            model_answer[:300] + "..."
            if len(model_answer) > 300
            else model_answer
        )
        a_short = (
            student_answer[:250] + "..."
            if len(student_answer) > 250
            else student_answer
        )
        prompt = (
            f"Classify reasoning. Return JSON:\n"
            f'{{"category": "<correct|partial|mild_confusion|wrong|'
            f'high_confusion|misconception>"}}\n'
            f"Q: {q_short}\nModel: {m_short}\nAnswer: {a_short}"
        )
        out = self.llm.invoke(prompt).content
        try:
            category = json.loads(out)["category"]
            elapsed = time.time() - start_time

            if CACHE_AVAILABLE:
                cache_set(cache_key, category, ttl=1800)
                if DEBUG_MODE:
                    logger.info(
                        f"💾 [CACHE SET] Reasoning classified in "
                        f"{elapsed:.2f}s: {category}"
                    )

            if DEBUG_MODE:
                logger.info(
                    f"🔍 [ReasoningClassifier] Classified in {elapsed:.2f}s: "
                    f"{category}"
                )

            return category
        except Exception as e:
            if DEBUG_MODE:
                logger.error(f"Error in reasoning classification: {e}")
            return "partial"


class MisconceptionDetector:
    """Detect misconceptions in student answers"""

    def __init__(self, llm: ChatOpenAI):
        self.llm = llm

    async def detect_async(self, question: str, student_answer: str) -> bool:
        """Async version for parallel execution"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.detect, question, student_answer
        )

    def detect(
        self,
        question: str,
        student_answer: str,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> bool:
        """
        Detect misconceptions in student answers.
        Uses centralized caching with configurable TTL.
        
        Args:
            question: Question text
            student_answer: Student answer text
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for instrumentation
        
        Returns:
            bool: True if misconception detected, False otherwise
        """
        start_time = time.time()

        # Use centralized caching if available
        if DETERMINISTIC_CACHE_AVAILABLE:
            @cached_operation(
                CacheOperation.MISCONCEPTION_DETECTION,
                ttl=CacheTTL.MISCONCEPTION_DETECTION,
                job_id=job_id,
                trace_id=trace_id
            )
            def _detect_misconception():
                return self._detect_misconception_impl(question, student_answer)
            
            result = _detect_misconception()
            elapsed = time.time() - start_time
            if DEBUG_MODE:
                logger.info(
                    f"🔍 [MisconceptionDetector] Detected in {elapsed:.2f}s: "
                    f"{result}"
                )
            return result
        
        # Fallback to legacy caching
        misconception_hash = hashlib.md5(
            f'{question}:{student_answer}'.encode()
        ).hexdigest()
        cache_key = f"misconception:{misconception_hash}"
        if CACHE_AVAILABLE:
            cached = cache_get(cache_key)
            if cached is not None:
                elapsed = time.time() - start_time
                if DEBUG_MODE:
                    logger.info(
                        f"🔍 [CACHE HIT] Misconception detected in "
                        f"{elapsed:.2f}s: {cached}"
                    )
                return cached
        
        result = self._detect_misconception_impl(question, student_answer)
        elapsed = time.time() - start_time
        
        if CACHE_AVAILABLE:
            cache_set(cache_key, result, ttl=1800)
            if DEBUG_MODE:
                logger.info(
                    f"💾 [CACHE SET] Misconception detected in "
                    f"{elapsed:.2f}s: {result}"
                )
        
        if DEBUG_MODE:
            logger.info(
                f"🔍 [MisconceptionDetector] Detected in {elapsed:.2f}s: "
                f"{result}"
            )
        
        return result
    
    def _detect_misconception_impl(self, question: str, student_answer: str) -> bool:
        """Internal implementation of misconception detection"""
        q_short = question[:250] + "..." if len(question) > 250 else question
        a_short = (
            student_answer[:250] + "..."
            if len(student_answer) > 250
            else student_answer
        )
        prompt = (
            f"Misconception? Return JSON:\n"
            f'{{"misconception": <true|false>}}\n'
            f"Q: {q_short}\nA: {a_short}"
        )
        out = self.llm.invoke(prompt).content
        try:
            has_misconception = bool(json.loads(out)["misconception"])
            return has_misconception
        except Exception as e:
            if DEBUG_MODE:
                logger.error(f"Error in misconception detection: {e}")
            return False


class MasteryEngine:
    """Compute mastery deltas from reasoning category and difficulty"""

    base_map = {
        "correct": 4,
        "partial": 2,
        "mild_confusion": 0,
        "wrong": -2,
        "high_confusion": -3,
        "misconception": -8,
    }

    def difficulty_weight(self, max_marks: int | None):
        if not max_marks:
            return 1.0
        if max_marks <= 2:
            return 0.8
        if max_marks <= 4:
            return 1.0
        if max_marks <= 7:
            return 1.2
        return 1.4

    def compute(
        self,
        reasoning_category: str,
        max_marks: int | None = None,
        difficulty_level: int | None = None,
    ):
        base = self.base_map.get(reasoning_category, 0)
        weight = self.difficulty_weight(max_marks)

        if difficulty_level is not None:
            if difficulty_level == 1:
                extra = 0.8
            elif difficulty_level == 2:
                extra = 1.0
            elif difficulty_level == 3:
                extra = 1.2
            else:
                extra = 1.0
            weight *= extra

        if DEBUG_MODE:
            marks_weight = self.difficulty_weight(max_marks)
            extra = 1.0
            if difficulty_level is not None:
                if difficulty_level == 1:
                    extra = 0.8
                elif difficulty_level == 2:
                    extra = 1.0
                elif difficulty_level == 3:
                    extra = 1.2
            final_delta = base * weight
            logger.info(
                f"🔍 [MasteryEngine] Computing delta for "
                f"reasoning={reasoning_category}:"
            )
            logger.info(f"   Base value: {base}")
            logger.info(f"   Marks weight: {marks_weight:.2f}")
            if difficulty_level is not None:
                logger.info(f"   Difficulty multiplier: {extra:.2f}")
            logger.info(f"   Final weight: {weight:.2f}")
            logger.info(f"   Final delta: {final_delta:+.2f}")

        return base * weight


# Removed WeaknessEngine and TrendUpdater classes - user_weaknesses and user_trends tables are not used


class QuestionEmbeddingsSearcher:
    """
    Search for related concepts using question_embeddings +
    concept_embeddings.
    """

    def __init__(self, repo: SupabaseRepository):
        self.repo = repo

    def get_concept_ids_for_question(
        self, question_id: str, limit: int = 5
    ) -> List[str]:
        """
        Use SupabaseRepository.search_concepts_by_question_embedding to get
        related concepts.

        Returns a list of concept_id strings.
        """
        if not self.repo.enabled:
            return []

        rows = self.repo.search_concepts_by_question_embedding(
            question_id=question_id,
            match_limit=limit,
        )
        concept_ids = []
        for row in rows:
            cid = row.get("concept_id")
            if cid:
                concept_ids.append(cid)
        return concept_ids


class RAGRetriever:
    """
    Retrieve question text, model answer, and lesson/context for RAG.

    NOTE: There is no mark scheme. We only use model_answer +
    lesson/context.
    """

    def __init__(self, repo: SupabaseRepository):
        self.repo = repo

    def get_bundle(
        self,
        question: str,
        model_answer: str,
        question_id: str | None = None,
        subject: str | None = None,
    ) -> Dict[str, str]:
        """
        Return a dict with:
        - question: str
        - model_answer: str
        - lesson_context: str (or case_study for Economics)
        
        Supports all subjects by querying appropriate tables.
        """
        final_question = question
        final_model_answer = model_answer
        lesson_context = ""

        if self.repo.enabled and question_id:
            qb = self.repo.fetch_question_by_id(question_id, subject=subject)
            if qb:
                final_question = qb.get("question", final_question)
                final_model_answer = qb.get(
                    "model_answer", final_model_answer
                )
                lesson_context = (
                    self.repo.fetch_lesson_context_for_question(question_id, subject=subject)
                )

        return {
            "question": final_question,
            "model_answer": final_model_answer,
            "lesson_context": lesson_context,
        }


class AnswerGradingAgent:
    """LangChain agent for grading answers across all subjects (Business Studies, Economics, Geography, History, Islamiyat).
    Handles questions with or without case studies/context."""

    def __init__(
        self,
        api_key: str,
        model: str = None,
        temperature: float = None,
        max_tokens: int = None
    ):
        """Initialize the grading agent with configuration"""
        load_dotenv('config.env')

        self.model = model or os.getenv(
            'GRADING_MODEL', 'gpt-4o-mini'
        )
        self.temperature = temperature or float(
            os.getenv('GRADING_TEMPERATURE', '0.1')
        )
        # Slightly lower default for speed, still enough for feedback
        self.max_tokens = max_tokens or int(
            os.getenv('GRADING_MAX_TOKENS', '1500')
        )

        if os.getenv('LANGSMITH_TRACING', 'false').lower() == 'true':
            os.environ['LANGSMITH_TRACING'] = 'true'
            os.environ['LANGSMITH_ENDPOINT'] = os.getenv(
                'LANGSMITH_ENDPOINT', 'https://api.smith.langchain.com'
            )
            os.environ['LANGSMITH_API_KEY'] = os.getenv(
                'LANGSMITH_API_KEY', ''
            )
            os.environ['LANGSMITH_PROJECT'] = os.getenv(
                'LANGSMITH_PROJECT', 'imtehaan-ai-tutor'
            )
            print("🔍 LangSmith tracing enabled for grading system")

        self.llm = ChatOpenAI(
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            openai_api_key=api_key,
            timeout=30,
            max_retries=1
        )
        self.repo = SupabaseRepository()

        self.question_searcher = QuestionEmbeddingsSearcher(self.repo)
        self.rag_retriever = RAGRetriever(self.repo)

        self.concept_detector = ConceptDetector(
            self.repo,
            self.llm,
            question_searcher=self.question_searcher,
        )
        self.reasoning_classifier = ReasoningClassifier(self.llm)
        self.misconception_detector = MisconceptionDetector(self.llm)
        self.mastery_engine = MasteryEngine()
        self._setup_agent()

    def _setup_agent(self):
        """Setup the LangChain agent with tools and prompts"""
        pass

    def _clamp_score(self, score: float, max_marks: int) -> float:
        """Clamp a raw score to the range [0, max_marks]."""
        return max(0.0, min(float(max_marks), float(score)))

    def _get_system_prompt(self, subject: str = "Business Studies") -> str:
        """Get the system prompt for the grading agent"""
        # VERSION: 2025-12-12 - Updated with CRITICAL: MARKS AND SCORING
        # VERSION: 2025-01-XX - Updated to support multiple subjects (Business Studies, Economics)
        # VERSION: 2025-01-XX - Updated to support all subjects (Geography, History, Islamiyat)
        
        # Normalize subject name for consistency
        subject_normalized = subject.strip() if subject else "Business Studies"
        subject_lower = subject_normalized.lower()
        
        # Map subject to proper display name
        if "economics" in subject_lower:
            subject_display = "Economics"
        elif "geography" in subject_lower or "pak studies geography" in subject_lower:
            subject_display = "Geography"
        elif "history" in subject_lower or "pak studies history" in subject_lower:
            subject_display = "History"
        elif "islamiyat" in subject_lower or "islamiyat" in subject_lower:
            subject_display = "Islamiyat"
        elif "business" in subject_lower:
            subject_display = "Business Studies"
        else:
            # Default to Business Studies if unclear
            subject_display = "Business Studies"
        
        return f"""You are an expert Cambridge IGCSE {subject_display} examiner.

Your job is to:
1. Grade student answers using the JSON schema provided.
2. Compare the student response to the model answer.
3. Detect the presence or absence of:
   • Knowledge
   • Application
   • Analysis
   • Evaluation

────────────────────────────────────────
CRITICAL: MARKS AND SCORING
────────────────────────────────────────
• For every question, the TOTAL MARKS will be given in the user message.
• You MUST treat that total as the ONLY maximum for the score.
• You MUST return:
  - overall_score = OBTAINED MARKS out of the given total marks
  - percentage = (overall_score / total_marks) * 100
• NEVER assume a 0–50 scale or any other fixed maximum.
• NEVER return a score higher than the total marks given for that question.

Examples:
• If the question is worth 2 marks and the answer is perfect,
  return overall_score = 2.
• If the question is worth 4 marks and the answer deserves 75%,
  return overall_score = 3.
• If the question is worth 6 marks and the answer deserves 50%,
  return overall_score = 3.

────────────────────────────────────────
MARK-BASED EXPECTATION RULES
────────────────────────────────────────
You must evaluate whether the student included the correct
components based on the mark allocation:

• For 2-mark questions, the student is expected to include
  Knowledge only.
• For 4-mark questions, the student must include
  Knowledge + Application.
• For 6-mark questions, the student must include
  Knowledge + Application + Analysis.
• For 8+ marks, the student must include
  Knowledge + Application + Analysis + Evaluation.

If the student misses any component required for that mark level,
explicitly mention it as a weakness.

────────────────────────────────────────
WORD COUNT GUIDELINES FOR OPTIMAL GRADING
────────────────────────────────────────
The length of the student's answer is an important factor in grading.
Use these word count ranges as a guide when evaluating responses:

Marks | Ideal Word Count | What This Looks Like in the Exam
------|-------------------|----------------------------------
2 marks | 15–25 words | One clear definition or two brief points
4 marks | 40–60 words | Two developed points or a short explanation
6 marks | 70–100 words | 2 explained points with application
7 marks | 90–120 words | 2–3 explained points + some analysis
8 marks | 110–140 words | Balanced analysis, cause–effect shown
10 marks | 140–180 words | Analysis + evaluation (judgement)
14 marks | 220–280 words | Full evaluation, both sides, justified conclusion

IMPORTANT WORD COUNT EVALUATION RULES:
• The word count will be provided for the student's answer.
• Answers significantly below the ideal range may lack depth and detail.
• Answers significantly above the ideal range may be repetitive or unfocused.
• Consider word count when determining if the answer has sufficient:
  - Detail for the mark allocation
  - Development of points
  - Analysis and evaluation (for higher mark questions)
• If the answer is too short for the mark allocation, mention this in
  "areas_for_improvement" and suggest expanding with more detail.
• If the answer is too long and repetitive, mention this in feedback.
• Word count is a GUIDE, not a strict requirement - content quality matters most.
  However, answers that are far outside the ideal range should be flagged.

────────────────────────────────────────
FEEDBACK REQUIREMENTS
────────────────────────────────────────
When writing feedback:
• Explicitly mention strengths and weaknesses in Knowledge,
  Application, Analysis, Evaluation.
• Be specific and contextual: e.g. "Your Application was weak because
  you did not use the relevant {subject_display.lower()} examples/context."
• If case study or context is provided, reference it in feedback when relevant.
• If no case study/context is provided, focus on general {subject_display.lower()}
  concepts, principles, and examples.
• Identify misunderstandings.
• Encourage the student but remain academically strict.
• Make the connection between the required marks and the missing
  components clear.

────────────────────────────────────────
CRITICAL: DETECT COPIED ANSWERS
────────────────────────────────────────
BEFORE grading, you MUST check if the student's answer is identical or 
too similar to the question itself. This is a critical validation step.

CHECK FOR:
1. **Exact Match**: If the student's answer is identical to the question 
   text (after ignoring case and extra spaces), this is NOT a valid answer.
   
2. **Question Text in Answer**: If the question text appears ANYWHERE in 
   the student's answer (at the beginning, middle, or end), this is NOT 
   acceptable. The student should provide ONLY their own answer, not include 
   the question text.
   
3. **Question Copying**: If the student has copied the question text 
   (or a significant portion of it) as their answer, this is NOT acceptable.
   
4. **High Similarity**: If the student's answer contains 70% or more of 
   the same words as the question, it is likely copied and should be rejected.

5. **Context Copying**: If the question includes context (like a case study) 
   and the student copies text from that context as their answer, this 
   should be detected.

IF YOU DETECT THAT THE ANSWER IS COPIED FROM THE QUESTION:
• Return overall_score = 0.0
• Return percentage = 0.0
• Return grade = "F"
• Set reasoning_category = "wrong"
• Set has_misconception = false
• In specific_feedback, write: "Your answer is identical or too similar 
  to the question. Please answer carefully and provide your own response 
  based on your understanding."
• In areas_for_improvement, include: "Please provide your own answer 
  based on your understanding of the topic, rather than copying the question."
• In suggestions, include:
  - "Read the question carefully and understand what is being asked"
  - "Provide your own answer based on your knowledge and understanding"
  - "Avoid copying the question text or context as your answer"

EXAMPLES OF COPIED ANSWERS TO REJECT:
• Question: "Outline two ways the economic problem could influence 
  Amina's stock decisions."
  Student Answer: "Outline two ways the economic problem could influence 
  Amina's stock decisions." → REJECT (F grade, 0 marks)
  
• Question: "Outline two examples of capital that Nida might use in her 
  flower shop."
  Student Answer: "Nida is planning to open a small flower shop. She will 
  need land for the shop, workers to arrange flowers, tools like scissors 
  and fridges, and she will take the risk of starting the business.
  Q
  Outline two examples of capital that Nida might use in her flower shop." 
  → REJECT (F grade, 0 marks) - Question text appears in answer!
  
• Question includes context about "Amina manages a bookstore..."
  Student Answer: "Amina manages a bookstore..." → REJECT (F grade, 0 marks)

• Question: "Explain the concept of opportunity cost."
  Student Answer: "Explain the concept of opportunity cost" → REJECT (F grade, 0 marks)
  
• Question: "What is inflation?"
  Student Answer: "Inflation is when prices rise. What is inflation?" 
  → REJECT (F grade, 0 marks) - Question text appears at the end!

DO NOT grade these as regular answers. They must receive 0 marks and 
grade F with appropriate feedback.

IMPORTANT:
• DO NOT modify any JSON fields or output structure.
• DO NOT add new fields.
• Maintain the JSON schema exactly as requested in the user message.
• You are ONLY grading. Do not generate a full model answer."""

    def grade_answer(
        self,
        question: str,
        model_answer: str,
        student_answer: str,
        user_id: str = None,
        max_marks: int = None,
        question_id: str = None,
        topic_id: str = None,
        topic_name: str = None,
        difficulty_level: int = None,
        subject: str = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> GradingResult:
        """
        Grade a student answer against the model answer.

        Single LLM call on the hot path.
        """
        total_start = time.time()

        if DEBUG_MODE:
            logger.info("=" * 80)
            logger.info("🎯 [GRADING] Starting answer grading")
            logger.info("=" * 80)
            logger.info(f"📝 Question ID: {question_id}")
            logger.info(f"👤 User ID: {user_id}")
            logger.info(f"📚 Topic ID: {topic_id}")
            logger.info(f"📚 Topic Name: {topic_name}")
            logger.info(f"📖 Subject: {subject if subject else 'Business Studies (default)'}")
            logger.info(f"📊 Max Marks: {max_marks}")
            logger.info(f"🎚️  Difficulty Level: {difficulty_level}")
            logger.info(f"📏 Question Length: {len(question)} chars")
            logger.info(
                f"📏 Student Answer Length: {len(student_answer)} chars"
            )
            logger.info("-" * 80)

        # VALIDATION: Check if answer is identical or too similar to question
        is_similar, similarity_reason = _check_answer_similarity_to_question(
            question, student_answer
        )
        if is_similar:
            logger.warning(
                f"⚠️  [GRADING] Answer is identical/similar to question: {similarity_reason}"
            )
            # Return F grade with appropriate feedback instead of raising error
            # Ensure max_marks is available for the result
            max_marks_value = max_marks if max_marks and max_marks > 0 else 10
            
            return GradingResult(
                overall_score=0.0,
                percentage=0.0,
                grade="F",
                strengths=[],
                areas_for_improvement=[
                    similarity_reason,
                    "Please provide your own answer based on your understanding of the topic."
                ],
                specific_feedback=similarity_reason,
                suggestions=[
                    "Read the question carefully and understand what is being asked",
                    "Provide your own answer based on your knowledge and understanding",
                    "Avoid copying the question text as your answer"
                ],
                reasoning_category="wrong",
                has_misconception=False,
                topic_name=topic_name,
                primary_concept_ids=[],
                secondary_concept_ids=[],
                mastery_deltas={},
                max_marks=max_marks_value
            )

        try:
            # RAG: get final question, model_answer, and any lesson/context
            rag_start = time.time()
            # Get subject from parameter or default to "Business Studies"
            subject_name = subject if subject else "Business Studies"
            bundle = self.rag_retriever.get_bundle(
                question=question,
                model_answer=model_answer,
                question_id=question_id,
                subject=subject_name,
            )
            rag_elapsed = time.time() - rag_start
            rag_question = bundle["question"]
            rag_model_answer = bundle["model_answer"]
            lesson_context = bundle["lesson_context"]

            if DEBUG_MODE:
                logger.info(
                    f"🔍 [RAG] Retrieval completed in {rag_elapsed:.2f}s"
                )
                logger.info(
                    f"   Question from RAG: {rag_question[:100]}..."
                )
                logger.info(
                    f"   Lesson Context Length: {len(lesson_context)} chars"
                )
                if not lesson_context or not lesson_context.strip():
                    logger.info(
                        "   ℹ️  No case study/context available - grading "
                        "will proceed without context"
                    )
                logger.info(
                    f"   Using RAG Question: {rag_question != question}"
                )
                logger.info(
                    f"   Using RAG Model Answer: "
                    f"{rag_model_answer != model_answer}"
                )
                logger.info("-" * 80)

            # Truncate inputs to reduce token usage and speed
            q_trunc = (
                rag_question[:350] + "..."
                if len(rag_question) > 350
                else rag_question
            )
            m_trunc = (
                rag_model_answer[:500] + "..."
                if len(rag_model_answer) > 500
                else rag_model_answer
            )
            a_trunc = (
                student_answer[:350] + "..."
                if len(student_answer) > 350
                else student_answer
            )
            ctx_trunc = (
                lesson_context[:150] + "..."
                if lesson_context and len(lesson_context) > 150
                else (lesson_context or "")
            )
            
            # Calculate word count for student answer (for word count evaluation)
            student_answer_word_count = len(student_answer.split()) if student_answer else 0

            # Require max_marks - no default to 50
            if max_marks is None or max_marks <= 0:
                logger.error(
                    "❌ [GRADING] max_marks is required and must be > 0. "
                    f"Received: {max_marks}"
                )
                raise ValueError(
                    "max_marks is required for grading. "
                    "Cannot grade without knowing total marks."
                )

            max_marks_value = max_marks

            # Import performance instrumentation
            try:
                from services.performance_instrumentation import (
                    time_prompt_construction,
                    time_ai_call,
                    time_response_parsing
                )
                INSTRUMENTATION_AVAILABLE = True
            except ImportError:
                INSTRUMENTATION_AVAILABLE = False
                # Create no-op context managers if instrumentation unavailable
                from contextlib import nullcontext
                time_prompt_construction = lambda *args, **kwargs: nullcontext()
                time_ai_call = lambda *args, **kwargs: nullcontext()
                time_response_parsing = lambda *args, **kwargs: nullcontext()

            # PHASE 1: Prompt Construction
            with time_prompt_construction(
                stage_name="answer_grading_prompt_construction",
                job_id=job_id,
                trace_id=trace_id
            ):
                # Get subject from parameter or default to "Business Studies"
                subject_name = subject if subject else "Business Studies"
                system_prompt = self._get_system_prompt(subject=subject_name)

                # Build user prompt (grading instructions and content)
                # VERSION: 2025-12-12 - Updated with dynamic max_marks, no 0-50
                # Include max_marks prominently at the start
                # Get subject from parameter or default to "Business Studies"
                subject_name = subject if subject else "Business Studies"
                
                user_prompt = (
                    f"Max marks for this question: {max_marks_value}.\n\n"
                    f"You MUST:\n"
                    f"- Return overall_score as the obtained marks OUT OF "
                    f"{max_marks_value}.\n"
                    f"- Ensure overall_score is between 0 and {max_marks_value} "
                    f"(never higher than {max_marks_value}).\n"
                    f"- Calculate percentage as "
                    f"(overall_score / {max_marks_value}) * 100.\n"
                    f"- Never assume a 0–50 scale or any other fixed maximum.\n\n"
                    f"Subject: {subject_name}\n\n"
                    f"IMPORTANT: Student Answer Word Count: {student_answer_word_count} words\n"
                    f"Refer to the word count guidelines in the system prompt to evaluate "
                    f"if the answer length is appropriate for {max_marks_value} marks.\n\n"
                    f"Now grade the following answer. Return ONLY JSON.\n\n"
                    f"Question: {q_trunc}\n"
                    f"Model Answer: {m_trunc}\n"
                    f"Student Answer: {a_trunc}\n"
                )
                # Only include context/case study if it exists
                if ctx_trunc and ctx_trunc.strip():
                    user_prompt += f"\nAdditional Context/Case Study (if relevant): {ctx_trunc}\n"

                user_prompt += (
                    f'\nReturn JSON exactly in this shape:\n'
                    f'{{"overall_score": <0-{max_marks_value}>, '
                    f'"percentage": <0-100>, '
                    f'"grade": "<A|B|C|D|F>", '
                    '"strengths": ["s1"], '
                    '"areas_for_improvement": ["a1"], '
                    '"specific_feedback": "<brief>", '
                    '"suggestions": ["s1"], '
                    '"reasoning_category": "<correct|partial|mild_confusion|'
                    'wrong|high_confusion|misconception>", '
                    '"has_misconception": <true|false>, '
                    '"primary_concepts": ["id1"], '
                    '"secondary_concepts": ["id2"]}'
                )
                prompt_size = len(system_prompt) + len(user_prompt)

            llm_start = time.time()

            # Use proper message format: SystemMessage + HumanMessage
            # CRITICAL: Both messages MUST be sent to the LLM
            system_msg = SystemMessage(content=system_prompt)
            human_msg = HumanMessage(content=user_prompt)
            messages = [system_msg, human_msg]

            # Explicit verification that both messages are present
            if len(messages) != 2:
                error_msg = (
                    f"CRITICAL ERROR: Expected 2 messages, got {len(messages)}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            if not isinstance(messages[0], SystemMessage):
                error_msg = (
                    "CRITICAL ERROR: First message is not a SystemMessage"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            if not isinstance(messages[1], HumanMessage):
                error_msg = (
                    "CRITICAL ERROR: Second message is not a HumanMessage"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            if DEBUG_MODE:
                logger.info("🤖 [LLM] Invoking grading LLM (single call)...")
                logger.info(
                    "   System prompt length: {} chars".format(
                        len(system_prompt)
                    )
                )
                logger.info(
                    "   User prompt length: {} chars".format(len(user_prompt))
                )
                logger.info("=" * 80)
                logger.info("📤 VERIFYING MESSAGES BEFORE SENDING:")
                logger.info("=" * 80)
                logger.info(
                    "   ✅ Message 1: {} ({} chars)".format(
                        type(messages[0]).__name__,
                        len(messages[0].content)
                    )
                )
                logger.info(
                    "   ✅ Message 2: {} ({} chars)".format(
                        type(messages[1]).__name__,
                        len(messages[1].content)
                    )
                )
                logger.info("   ✅ Total messages: {}".format(len(messages)))
                logger.info("=" * 80)
                logger.info("📋 SYSTEM MESSAGE PREVIEW (first 200 chars):")
                logger.info("   " + messages[0].content[:200] + "...")
                logger.info("=" * 80)
                logger.info("📋 USER MESSAGE PREVIEW (first 200 chars):")
                logger.info("   " + messages[1].content[:200] + "...")
                logger.info("=" * 80)
                logger.info("🚀 Sending BOTH messages to LLM now...")

            # Invoke LLM with both messages
            # CRITICAL: ChatOpenAI.invoke() MUST receive both SystemMessage
            # and HumanMessage. LangChain's tracing may show simplified view,
            # but both are sent to OpenAI API
            if DEBUG_MODE:
                logger.info("=" * 80)
                logger.info("🔍 FINAL MESSAGE VERIFICATION BEFORE LLM CALL:")
                logger.info("=" * 80)
                for i, msg in enumerate(messages, 1):
                    msg_type = type(msg).__name__
                    base_type = (
                        msg.__class__.__bases__[0].__name__
                        if msg.__class__.__bases__ else "N/A"
                    )
                    logger.info(
                        "   Message {}: {} (base: {})".format(
                            i, msg_type, base_type
                        )
                    )
                    logger.info(
                        "   Content preview: {}...".format(
                            msg.content[:100]
                        )
                    )
                logger.info("=" * 80)
                logger.info(
                    "📤 Calling llm.invoke() with {} messages".format(
                        len(messages)
                    )
                )
                logger.info(
                    "   Note: LangChain tracing may show simplified view, "
                    "but both messages are sent to OpenAI API"
                )
                logger.info("=" * 80)

            # CRITICAL: Pass messages list directly to invoke()
            # This ensures SystemMessage goes as 'system' role and
            # HumanMessage goes as 'user' role in the OpenAI API call
            #
            # IMPORTANT: If LangChain tracing shows only HumanMessage, it may
            # be a display issue. The actual OpenAI API call should include
            # both messages. However, to ensure both are sent, we'll verify
            # the message structure one more time before invoking.
            if DEBUG_MODE:
                # Log the actual message objects being sent
                logger.info("=" * 80)
                logger.info(
                    "🔍 FINAL CHECK - Messages being sent to invoke():"
                )
                logger.info("=" * 80)
                for i, msg in enumerate(messages, 1):
                    msg_type = type(msg).__name__
                    msg_role = getattr(msg, 'type', 'N/A')
                    msg_length = len(msg.content)
                    logger.info(
                        "   [{}] Type: {}, Role: {}, Length: {} chars".format(
                            i, msg_type, msg_role, msg_length
                        )
                    )
                    logger.info("-" * 80)
                    logger.info("   FULL CONTENT:")
                    logger.info("-" * 80)
                    # Show full content, but split into chunks if too long
                    content = msg.content
                    if len(content) > 2000:
                        logger.info("   " + content[:2000])
                        logger.info(
                            "   ... (truncated, total: {} chars)".format(
                                len(content)
                            )
                        )
                    else:
                        logger.info("   " + content)
                    logger.info("=" * 80)

            # PHASE 2: API Call
            with time_ai_call(
                stage_name="answer_grading_api_call",
                job_id=job_id,
                trace_id=trace_id,
                model="gpt-4o-mini",
                prompt_tokens=prompt_size // 4  # Rough estimate
            ):
                try:
                    # Ensure we're passing the list of messages, not a single
                    # message. ChatOpenAI.invoke() should handle
                    # [SystemMessage, HumanMessage] correctly and send both to
                    # OpenAI API
                    result = self.llm.invoke(messages)
                except Exception as e:
                    if DEBUG_MODE:
                        logger.error(
                            "❌ [LLM] Error invoking LLM with messages: {}".format(
                                e
                            )
                        )
                        logger.error(
                            "   Messages sent: {} (types: {})".format(
                                len(messages),
                                [type(m).__name__ for m in messages]
                            )
                        )
                    raise
            llm_elapsed = time.time() - llm_start

            if DEBUG_MODE:
                logger.info(f"✅ [LLM] Response received in {llm_elapsed:.2f}s")
                logger.info(f"   Response Length: {len(result.content)} chars")
                logger.info("-" * 80)

            # PHASE 3: Response Parsing and Validation
            with time_response_parsing(
                stage_name="answer_grading_response_parsing",
                job_id=job_id,
                trace_id=trace_id,
                response_size=len(result.content) if hasattr(result, 'content') else None
            ):
                # Direct JSON parsing path (fast path)
                parse_start = time.time()
                grading_result: GradingResult
                try:
                    if DEBUG_MODE:
                        logger.info(
                            "🔍 [PARSING] Attempting direct JSON parsing..."
                        )
                    content = result.content.strip()
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        json_str = content[json_start:json_end]
                        parsed_data = json.loads(json_str)

                        required_keys = ['overall_score', 'percentage', 'grade']
                        if all(key in parsed_data for key in required_keys):
                            parse_elapsed = time.time() - parse_start

                        # Validate and convert score to ensure it's out of
                        # max_marks
                        if max_marks is None or max_marks <= 0:
                            logger.error(
                                "❌ max_marks is None or invalid. "
                                "Cannot process score."
                            )
                            raise ValueError("max_marks is required")

                        raw_score = parsed_data.get('overall_score', 0)
                        max_marks_for_calc = max_marks

                        # Detect if score is out of 50 and convert
                        # Common scores out of 50: 25, 30, 35, 40, 45, 50
                        # Also check if score is between max_marks and 50
                        score_needs_conversion = False
                        if raw_score > max_marks_for_calc:
                            # If score > max_marks and <= 50, likely out of 50
                            if raw_score <= 50:
                                score_needs_conversion = True
                            else:
                                # Score > 50, just clamp
                                parsed_data['overall_score'] = (
                                    max_marks_for_calc
                                )
                                logger.warning(
                                    f"   ⚠️  LLM returned {raw_score} "
                                    f"(exceeds max_marks "
                                    f"{max_marks_for_calc}). "
                                    f"Clamped to {max_marks_for_calc}"
                                )

                        if score_needs_conversion:
                            # Convert from 50-point scale to actual marks
                            converted_score = (
                                (raw_score / 50.0) * max_marks_for_calc
                            )
                            parsed_data['overall_score'] = round(
                                converted_score, 1
                            )
                            logger.warning(
                                f"   ⚠️  LLM returned {raw_score} "
                                f"(out of 50, max_marks="
                                f"{max_marks_for_calc}). "
                                f"Converted to "
                                f"{parsed_data['overall_score']}/"
                                f"{max_marks_for_calc}"
                            )
                        elif (raw_score > max_marks_for_calc and
                              raw_score > 50):
                            # Already handled above (clamped)
                            pass

                        # Hard clamp score to valid range [0, max_marks]
                        # This ensures overall_score can never exceed max_marks
                        final_score = self._clamp_score(
                            parsed_data.get('overall_score', 0),
                            max_marks_for_calc
                        )
                        parsed_data['overall_score'] = final_score

                        # Recalculate percentage from clamped score
                        # Percentage = (obtained marks / total marks) * 100
                        if max_marks_for_calc > 0:
                            parsed_data['percentage'] = round(
                                (final_score / max_marks_for_calc) * 100, 1
                            )
                            if DEBUG_MODE:
                                logger.info(
                                    f"   📊 Recalculated percentage: "
                                    f"{final_score}/{max_marks_for_calc} = "
                                    f"{parsed_data['percentage']}%"
                                )

                        if DEBUG_MODE:
                            logger.info(
                                f"✅ [PARSING] Direct JSON parsing successful "
                                f"in {parse_elapsed:.2f}s"
                            )
                            score = parsed_data.get('overall_score')
                            logger.info(
                                f"   Score: {score}/{max_marks}"
                            )
                            percentage = parsed_data.get('percentage')
                            logger.info(f"   Percentage: {percentage}%")
                            grade = parsed_data.get('grade')
                            logger.info(f"   Grade: {grade}")
                            reasoning = parsed_data.get('reasoning_category')
                            logger.info(f"   Reasoning: {reasoning}")
                            misconception = parsed_data.get(
                                'has_misconception'
                            )
                            logger.info(f"   Misconception: {misconception}")
                            primary = parsed_data.get('primary_concepts', [])
                            logger.info(f"   Primary Concepts: {primary}")
                            secondary = parsed_data.get(
                                'secondary_concepts', []
                            )
                            logger.info(f"   Secondary Concepts: {secondary}")
                            logger.info("-" * 80)

                        grading_result = GradingResult(
                            overall_score=parsed_data['overall_score'],
                            percentage=parsed_data['percentage'],
                            grade=parsed_data['grade'],
                            strengths=parsed_data.get('strengths', []),
                            areas_for_improvement=parsed_data.get(
                                'areas_for_improvement', []
                            ),
                            specific_feedback=parsed_data.get(
                                'specific_feedback', ''
                            ),
                            suggestions=parsed_data.get('suggestions', []),
                            reasoning_category=parsed_data.get(
                                'reasoning_category', 'partial'
                            ),
                            has_misconception=parsed_data.get(
                                'has_misconception', False
                            ),
                            primary_concept_ids=parsed_data.get(
                                'primary_concepts', []
                            ),
                            secondary_concept_ids=parsed_data.get(
                                'secondary_concepts', []
                            ),
                            max_marks=max_marks
                        )

                        if DEBUG_MODE:
                            logger.info(
                                "🔍 [MASTERY] Processing mastery updates..."
                            )
                        # Use topic_name from frontend if provided,
                        # otherwise fetch it
                        final_topic_name = topic_name
                        if not final_topic_name and topic_id:
                            final_topic_name = (
                                self.repo.fetch_topic_name_by_id(topic_id)
                            )
                        # Get subject from parameter or default to "Business Studies"
                        subject_name = subject if subject else "Business Studies"
                        
                        # Get attempt_id from question_attempt log if available
                        attempt_id = None
                        if user_id and question_id:
                            # Attempt will be logged before mastery processing
                            # We'll extract attempt_id after logging
                            pass
                        
                        self._process_mastery_and_analytics(
                            grading_result, user_id, max_marks,
                            difficulty_level, question_id,
                            topic_id=topic_id, topic_name=final_topic_name,
                            subject=subject_name, attempt_id=attempt_id
                        )
                        # Set topic_name in the result
                        grading_result.topic_name = final_topic_name

                        # Extract attempt_id from question_attempt log for idempotency
                        attempt_id = None
                        if user_id and question_id:
                            if DEBUG_MODE:
                                logger.info("=" * 80)
                                logger.info(
                                    "💾 [LOGGING] Logging question attempt..."
                                )
                                logger.info("=" * 80)
                            # OPTIMIZED: Move database writes to background (non-blocking)
                            # Get subject from parameter or default to "Business Studies"
                            subject_name = subject if subject else "Business Studies"
                            
                            # Log question attempt in background (non-blocking)
                            def log_attempt():
                                try:
                                    attempt_result = self.repo.log_question_attempt(
                                        user_id=user_id,
                                        question_id=question_id,
                                        topic_id=topic_id,
                                        raw_score=grading_result.overall_score,
                                        percentage=grading_result.percentage,
                                        grade=grading_result.grade,
                                        reasoning_category=(
                                            grading_result.reasoning_category
                                        ),
                                        has_misconception=(
                                            grading_result.has_misconception
                                        ),
                                        primary_concept_ids=(
                                            grading_result.primary_concept_ids
                                        ),
                                        secondary_concept_ids=(
                                            grading_result.secondary_concept_ids
                                        )
                                    )
                                    attempt_id = None
                                    if attempt_result and attempt_result.data:
                                        attempt_id = attempt_result.data[0].get('attempt_id')
                                    
                                    if DEBUG_MODE:
                                        logger.info(
                                            "✅ [LOGGING] Question attempt "
                                            "logged successfully (background)"
                                        )
                                        if attempt_id:
                                            logger.info(
                                                f"   Attempt ID: {attempt_id} "
                                                "(will be used for mastery update idempotency)"
                                            )
                                    
                                    # Process mastery updates after getting attempt_id
                                    self._process_mastery_and_analytics(
                                        grading_result, user_id, max_marks,
                                        difficulty_level, question_id,
                                        topic_id=topic_id, topic_name=final_topic_name,
                                        subject=subject_name, attempt_id=attempt_id
                                    )
                                except Exception as e:
                                    logger.warning(
                                        f"Failed to log question attempt (background): {e}. "
                                        "Grading will continue without logging."
                                    )
                            
                            # Execute DB writes in background thread (non-blocking)
                            async_write(log_attempt)

                        total_elapsed = time.time() - total_start
                        if DEBUG_MODE:
                            logger.info("=" * 80)
                            logger.info(
                                f"✅ [GRADING] Completed in "
                                f"{total_elapsed:.2f}s"
                            )
                            logger.info(
                                f"   Breakdown: RAG={rag_elapsed:.2f}s, "
                                f"LLM={llm_elapsed:.2f}s, "
                                f"Parse={parse_elapsed:.2f}s"
                            )
                            logger.info("=" * 80)

                            return grading_result
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    parse_elapsed = time.time() - parse_start
                    if DEBUG_MODE:
                        logger.warning(
                            f"⚠️  [PARSING] Direct JSON parsing failed in "
                            f"{parse_elapsed:.2f}s: {e}"
                        )
                        logger.info("   Falling back to structured parsing...")
                    logger.warning(
                        f"Direct JSON parsing failed: {e}. "
                        f"Falling back to structured parsing."
                    )
                    # fall through

                # Fallback: structured parsing without extra LLM calls
                if DEBUG_MODE:
                    logger.info("🔍 [PARSING] Using structured parsing fallback...")
                grading_result = self._parse_grading_result(
                    {"output": result.content},
                    question,
                    model_answer,
                    student_answer,
                    user_id,
                    max_marks,
                    question_id,
                    rag_question=rag_question,
                    rag_model_answer=rag_model_answer,
                    difficulty_level=difficulty_level,
                    topic_id=topic_id,
                    topic_name=topic_name,
                    subject=subject_name,
                )

            # OPTIMIZED: Move database writes to background (non-blocking)
            if user_id and question_id:
                # Get subject from parameter or default to "Business Studies"
                subject_name_for_mastery = subject if subject else "Business Studies"
                
                # Log question attempt in background (non-blocking)
                def log_attempt_fallback():
                    try:
                        attempt_result = self.repo.log_question_attempt(
                            user_id=user_id,
                            question_id=question_id,
                            topic_id=topic_id,
                            raw_score=grading_result.overall_score,
                            percentage=grading_result.percentage,
                            grade=grading_result.grade,
                            reasoning_category=grading_result.reasoning_category,
                            has_misconception=grading_result.has_misconception,
                            primary_concept_ids=(
                                grading_result.primary_concept_ids
                            ),
                            secondary_concept_ids=(
                                grading_result.secondary_concept_ids
                            )
                        )
                        attempt_id = None
                        if attempt_result and attempt_result.data:
                            attempt_id = attempt_result.data[0].get('attempt_id')
                        
                        # Process mastery updates after getting attempt_id
                        self._process_mastery_and_analytics(
                            grading_result, user_id, max_marks, difficulty_level,
                            question_id, topic_id=topic_id, topic_name=topic_name,
                            subject=subject_name_for_mastery, attempt_id=attempt_id
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to log question attempt (background): {e}. "
                            "Grading will continue without logging."
                        )
                
                # Execute DB writes in background thread (non-blocking)
                async_write(log_attempt_fallback)

            total_elapsed = time.time() - total_start
            if DEBUG_MODE:
                logger.info("=" * 80)
                logger.info(
                    f"✅ [GRADING] Completed in {total_elapsed:.2f}s"
                )
                logger.info(
                    f"   Final Score: "
                    f"{grading_result.overall_score}/{max_marks} "
                    f"({grading_result.percentage}%)"
                )
                logger.info(f"   Grade: {grading_result.grade}")
                logger.info("=" * 80)

            return grading_result

        except Exception as e:
            total_elapsed = time.time() - total_start
            if DEBUG_MODE:
                logger.error("=" * 80)
                logger.error(
                    f"❌ [GRADING] Error after {total_elapsed:.2f}s: {e}"
                )
                logger.error("=" * 80)
            logger.error(f"Error during grading: {e}")
            return self._create_fallback_result(
                question, model_answer, student_answer, max_marks
            )

    def _parse_grading_result(
        self,
        agent_result: Dict,
        question: str,
        model_answer: str,
        student_answer: str,
        user_id: str = None,
        max_marks: int = None,
        question_id: str | None = None,
        rag_question: str | None = None,
        rag_model_answer: str | None = None,
        difficulty_level: int | None = None,
        topic_id: str | None = None,
        topic_name: str | None = None,
        subject: str | None = None
    ) -> GradingResult:
        """
        Parse the agent result into a structured GradingResult.

        IMPORTANT: This fallback path does NOT make extra LLM calls.
        """
        try:
            output = agent_result.get("output", "").strip()

            # Try to salvage JSON from the output (again) without LLM
            json_start = output.find('{')
            json_end = output.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                try:
                    json_str = output[json_start:json_end]
                    parsed_data = json.loads(json_str)

                    # Validate and convert score to ensure it's out of
                    # max_marks
                    if max_marks is None or max_marks <= 0:
                        logger.error(
                            "❌ [FALLBACK] max_marks is None or invalid. "
                            "Cannot process score."
                        )
                        raise ValueError("max_marks is required")

                    raw_score = parsed_data.get('overall_score', 0)
                    max_marks_for_calc = max_marks

                    # Detect if score is out of 50 and convert
                    score_needs_conversion = False
                    if raw_score > max_marks_for_calc:
                        # If score > max_marks and <= 50, likely out of 50
                        if raw_score <= 50:
                            score_needs_conversion = True
                        else:
                            # Score > 50, just clamp
                            parsed_data['overall_score'] = max_marks_for_calc
                            logger.warning(
                                f"   ⚠️  [FALLBACK] LLM returned score "
                                f"{raw_score} (exceeds max_marks "
                                f"{max_marks_for_calc}). "
                                f"Clamped to {max_marks_for_calc}"
                            )

                    if score_needs_conversion:
                        # Convert from 50-point scale to actual marks
                        converted_score = (
                            (raw_score / 50.0) * max_marks_for_calc
                        )
                        parsed_data['overall_score'] = round(
                            converted_score, 1
                        )
                        logger.warning(
                            f"   ⚠️  [FALLBACK] LLM returned {raw_score} "
                            f"(out of 50, max_marks="
                            f"{max_marks_for_calc}). "
                            f"Converted to "
                            f"{parsed_data['overall_score']}/"
                            f"{max_marks_for_calc}"
                        )

                    # Hard clamp score to valid range [0, max_marks]
                    # This ensures overall_score can never exceed max_marks
                    final_score = self._clamp_score(
                        parsed_data.get('overall_score', 0),
                        max_marks_for_calc
                    )
                    parsed_data['overall_score'] = final_score

                    # Recalculate percentage from clamped score
                    # Percentage = (obtained marks / total marks) * 100
                    if max_marks_for_calc > 0:
                        parsed_data['percentage'] = round(
                            (final_score / max_marks_for_calc) * 100, 1
                        )
                        if DEBUG_MODE:
                            logger.info(
                                f"   📊 [FALLBACK] Recalculated percentage: "
                                f"{final_score}/{max_marks_for_calc} = "
                                f"{parsed_data['percentage']}%"
                            )

                    grading_result = GradingResult(
                        overall_score=parsed_data.get('overall_score', 0),
                        percentage=parsed_data.get('percentage', 0.0),
                        grade=parsed_data.get('grade', 'F'),
                        strengths=parsed_data.get('strengths', []),
                        areas_for_improvement=parsed_data.get(
                            'areas_for_improvement', []
                        ),
                        specific_feedback=parsed_data.get(
                            'specific_feedback',
                            "Feedback could not be fully parsed."
                        ),
                        suggestions=parsed_data.get('suggestions', []),
                        reasoning_category=parsed_data.get(
                            'reasoning_category', 'partial'
                        ),
                        has_misconception=parsed_data.get(
                            'has_misconception', False
                        ),
                        primary_concept_ids=parsed_data.get(
                            'primary_concepts', []
                        ),
                        secondary_concept_ids=parsed_data.get(
                            'secondary_concepts', []
                        ),
                        max_marks=max_marks
                    )
                except Exception:
                    # If even that fails, use a safe default
                    grading_result = self._create_structured_result(
                        output, question, model_answer, student_answer,
                        max_marks
                    )
            else:
                grading_result = self._create_structured_result(
                    output, question, model_answer, student_answer, max_marks
                )

            # Process mastery and analytics (no extra LLM)
            # Use topic_name from frontend if provided, otherwise fetch it
            final_topic_name = topic_name
            if not final_topic_name and topic_id:
                final_topic_name = self.repo.fetch_topic_name_by_id(topic_id)
            # Get subject from parameter or default to "Business Studies"
            subject_name_for_mastery = subject if subject else "Business Studies"
            # Note: attempt_id not available in this path (parse_grading_result)
            # Idempotency will use question_id + timestamp instead
            self._process_mastery_and_analytics(
                grading_result, user_id, max_marks, difficulty_level,
                question_id, topic_id=topic_id, topic_name=final_topic_name,
                subject=subject_name_for_mastery, attempt_id=None
            )
            # Set topic_name in the result
            grading_result.topic_name = final_topic_name

            return grading_result

        except Exception as e:
            logger.error(f"Error parsing grading result: {e}")
            return self._create_fallback_result(
                question, model_answer, student_answer, max_marks
            )

    def _process_mastery_and_analytics(
        self,
        result: GradingResult,
        user_id: str | None,
        max_marks: int | None,
        difficulty_level: int | None,
        question_id: str | None,
        topic_id: str | None = None,
        topic_name: str | None = None,
        subject: str | None = None,
        attempt_id: str | None = None
    ):
        """
        Process mastery updates and analytics ASYNCHRONOUSLY.
        Computes deltas and enqueues mastery update job instead of writing synchronously.
        
        IMPORTANT: This function should only be called ONCE per grading result.
        Uses a marker to prevent duplicate processing.
        """
        # CRITICAL: Prevent duplicate processing of the same grading result
        # Check if mastery has already been processed for this result
        if hasattr(result, '_mastery_processed') and result._mastery_processed:
            logger.warning(
                f"⚠️  [MASTERY] Mastery already processed for this result. "
                f"Skipping duplicate call to prevent multiple entries."
            )
            return
        
        # Mark this result as being processed
        result._mastery_processed = True
        
        mastery_start = time.time()

        result.mastery_deltas = {}
        if not user_id:
            logger.warning(
                "⚠️  [MASTERY] Skipped - no user_id provided. "
                "Mastery updates will not be processed."
            )
            return

        # Combine and deduplicate concepts (case-insensitive) to ensure each concept is only processed once
        # Normalize all concept IDs to title case for consistent handling
        all_concepts_raw = result.primary_concept_ids + result.secondary_concept_ids
        logger.info(
            f"🔍 [MASTERY] Concept detection: "
            f"primary_concept_ids={result.primary_concept_ids}, "
            f"secondary_concept_ids={result.secondary_concept_ids}, "
            f"all_concepts_raw={all_concepts_raw}"
        )
        normalized_concepts = {}  # Map: normalized_lowercase -> normalized_titlecase
        
        def normalize_to_title_case(text: str) -> str:
            """Convert text to title case, handling multi-word concepts"""
            if not text:
                return ""
            # Handle multi-word concepts (e.g., "seasonal demand" -> "Seasonal Demand")
            words = text.strip().split()
            return " ".join(word.capitalize() for word in words)
        
        for cid in all_concepts_raw:
            if not cid or not cid.strip():
                continue
            cid_normalized = cid.lower().strip()
            cid_title_case = normalize_to_title_case(cid)
            
            # Keep only one entry per normalized concept (case-insensitive)
            # Prefer title case version for consistency
            if cid_normalized not in normalized_concepts:
                normalized_concepts[cid_normalized] = cid_title_case
            else:
                # If we've seen this concept before, prefer the title case version
                # (both should be title case now, but keep existing)
                pass
        
        all_concepts = list(normalized_concepts.values())
        logger.info(
            f"📋 [MASTERY] Normalized concepts: "
            f"all_concepts={all_concepts}, count={len(all_concepts)}"
        )
        
        if not all_concepts:
            if DEBUG_MODE:
                logger.info("🔍 [Mastery] Skipped - no concepts detected")
            return
        
        # Log deduplication and normalization if needed
        total_concepts = len(all_concepts_raw)
        if len(all_concepts) < total_concepts:
            logger.info(
                f"📊 [MASTERY] Deduplicated/normalized concepts: {total_concepts} -> {len(all_concepts)} "
                f"(removed {total_concepts - len(all_concepts)} duplicates/case-variants)"
            )
            if DEBUG_MODE:
                logger.info(
                    f"   Normalized concepts: {all_concepts}"
                )

        # Always fetch topic_name from topic_id if available
        # (even if provided, we verify it's correct from database)
        logger.info(
            f"🔍 [TOPIC] _process_mastery_and_analytics called with: "
            f"topic_name={topic_name}, topic_id={topic_id}"
        )

        # If we have topic_id, always fetch topic_name from database
        # to ensure we have the correct value
        if topic_id:
            logger.info(
                f"📚 [TOPIC] Fetching topic_name from topics table "
                f"using topic_id: {topic_id}"
            )
            fetched_topic_name = self.repo.fetch_topic_name_by_id(topic_id)
            if fetched_topic_name:
                topic_name = fetched_topic_name  # Use fetched value
                logger.info(
                    f"✅ [TOPIC] Using fetched topic_name: '{topic_name}' "
                    f"for topic_id: {topic_id}"
                )
            else:
                logger.warning(
                    f"⚠️  [TOPIC] Could not fetch topic_name for "
                    f"topic_id: {topic_id}"
                )
                # Keep the provided topic_name if fetch failed
                if not topic_name:
                    logger.warning(
                        "⚠️  [TOPIC] No topic_name available - "
                        "will not be stored in user_mastery"
                    )
        elif question_id and not topic_name:
            # Try to get topic_id from question data
            question_data = self.repo.fetch_question_by_id(question_id)
            if question_data and question_data.get('topic_id'):
                topic_id_from_question = question_data.get('topic_id')
                topic_name = (
                    self.repo.fetch_topic_name_by_id(
                        topic_id_from_question
                    )
                )
                if DEBUG_MODE:
                    if topic_name:
                        logger.info(
                            f"📚 [TOPIC] Fetched topic_name: "
                            f"{topic_name} from question_id: "
                            f"{question_id}, topic_id: "
                            f"{topic_id_from_question}"
                        )

        reasoning = result.reasoning_category
        miscon = result.has_misconception

        logger.info(
            f"🔍 [Mastery] Processing {len(all_concepts)} concepts for "
            f"user {user_id}"
        )
        logger.info(
            f"   Reasoning: {reasoning}, Misconception: {miscon}, "
            f"Max Marks: {max_marks}, Difficulty: {difficulty_level}"
        )
        if topic_name:
            logger.info(
                f"   ✅ Topic Name available: '{topic_name}' "
                f"(will be stored in user_mastery)"
            )
        else:
            logger.warning(
                "   ⚠️  Topic Name is None - will not be stored in "
                "user_mastery"
            )
            logger.warning(
                f"   ⚠️  topic_id was: {topic_id}, "
                f"question_id was: {question_id}"
            )
        if DEBUG_MODE:
            logger.info("=" * 80)
            logger.info("📊 [DIFFICULTY] Calculation Breakdown:")
            if max_marks:
                if max_marks <= 2:
                    logger.info(f"   Max Marks: {max_marks} → Low (1)")
                elif max_marks <= 4:
                    logger.info(f"   Max Marks: {max_marks} → Medium (2)")
                else:
                    logger.info(f"   Max Marks: {max_marks} → High (3)")
            else:
                logger.info("   Max Marks: None (using default)")
            if difficulty_level:
                diff_map = {1: "Low", 2: "Medium", 3: "High"}
                logger.info(
                    f"   Difficulty Level: {difficulty_level} "
                    f"({diff_map.get(difficulty_level, 'Unknown')})"
                )
            else:
                logger.info("   Difficulty Level: None")
            logger.info("=" * 80)

        # PARALLELIZED: Process concepts in parallel for better performance
        # Each concept update is independent and can run concurrently
        try:
            from services.batch_parallelization import (
                run_batch_parallel_sync
            )
            BATCH_PARALLEL_AVAILABLE = True
        except ImportError:
            BATCH_PARALLEL_AVAILABLE = False
            if DEBUG_MODE:
                logger.warning(
                    "[WARNING] Batch parallelization not available, "
                    "using sequential processing"
                )
        
        def compute_concept_delta(cid: str) -> Optional[Dict]:
            """
            Compute delta for a single concept (NO DB WRITE).
            Returns delta information for enqueueing.
            """
            if not cid:
                return None
            
            try:
                delta = self.mastery_engine.compute(
                    reasoning,
                    max_marks=max_marks,
                    difficulty_level=difficulty_level,
                )

                if DEBUG_MODE:
                    logger.info(
                        f"   Concept {cid}: Delta = {delta:.2f} "
                        f"(reasoning={reasoning}, marks={max_marks}, "
                        f"difficulty={difficulty_level})"
                    )
                
                return {
                    "concept_id": cid,
                    "delta": delta
                }
            except Exception as e:
                logger.warning(
                    f"[WARNING] Failed to compute delta for concept {cid}: {e}",
                    exc_info=True
                )
                return None
        
        # Compute deltas for all concepts (NO DB WRITES - just computation)
        concept_deltas = {}
        if BATCH_PARALLEL_AVAILABLE and len(all_concepts) > 1:
            # Use parallel batch processing for delta computation
            concept_results = run_batch_parallel_sync(
                items=all_concepts,
                process_func=compute_concept_delta,
                job_type="concept_delta_computation",
                job_id=None,
                trace_id=None,
                base_limit=None,
                error_handler=None
            )
            
            # Aggregate deltas
            for _, concept_result, exc in concept_results:
                if exc is None and concept_result is not None:
                    cid = concept_result["concept_id"]
                    delta = concept_result["delta"]
                    result.mastery_deltas[cid] = delta
                    concept_deltas[cid] = delta
        else:
            # Fallback to sequential processing
            for cid in all_concepts:
                concept_result = compute_concept_delta(cid)
                if concept_result:
                    cid = concept_result["concept_id"]
                    delta = concept_result["delta"]
                    result.mastery_deltas[cid] = delta
                    concept_deltas[cid] = delta

        # Process mastery updates synchronously (no workers needed)
        logger.info(
            f"🔍 [MASTERY] Processing mastery updates: "
            f"concept_deltas={len(concept_deltas)} concepts, "
            f"all_concepts={len(all_concepts)}, "
            f"user_id={user_id}, "
            f"subject={subject}, "
            f"repo_enabled={self.repo.enabled if self.repo else False}"
        )
        
        if not concept_deltas:
            logger.warning(
                f"⚠️  [MASTERY] No concept_deltas to process - "
                f"mastery updates will be skipped. "
                f"all_concepts={all_concepts}, "
                f"primary_concept_ids={result.primary_concept_ids}, "
                f"secondary_concept_ids={result.secondary_concept_ids}"
            )
            return
        
        if not self.repo or not self.repo.enabled:
            logger.error(
                f"❌ [MASTERY] Repository is disabled - cannot update mastery. "
                f"repo={self.repo}, enabled={self.repo.enabled if self.repo else False}"
            )
            return
        
        try:
            # Process each concept synchronously
            # Use a set to track processed concepts and prevent duplicates
            processed_concepts = set()
            for concept_id in all_concepts:
                # Skip if we've already processed this concept (case-insensitive check)
                concept_id_lower = concept_id.lower().strip() if concept_id else ""
                if concept_id_lower in processed_concepts:
                    logger.warning(
                        f"⚠️  [MASTERY] Skipping duplicate concept: "
                        f"'{concept_id}' (already processed)"
                    )
                    continue
                processed_concepts.add(concept_id_lower)
                
                delta = concept_deltas.get(concept_id, 0)
                
                logger.info(
                    f"📝 [MASTERY] Queuing mastery update: "
                    f"concept_id={concept_id}, delta={delta}, "
                    f"user_id={user_id}, subject={subject or 'Business Studies'}, "
                    f"topic_name={topic_name}"
                )
                
                # OPTIMIZED: Update mastery via repository in background (non-blocking)
                async_write(
                    self.repo.update_mastery,
                    user_id=user_id,
                    concept_id=concept_id,
                    delta=delta,
                    topic_name=topic_name,
                    subject=subject or "Business Studies"
                )
            
            logger.info(
                f"✅ [MASTERY] Queued mastery updates (background) "
                f"for {len(processed_concepts)} unique concepts "
                f"(skipped {len(all_concepts) - len(processed_concepts)} "
                f"duplicates)"
            )
        except Exception as e:
            logger.error(
                f"❌ [MASTERY] Error applying mastery updates: {e}",
                exc_info=True
            )
            # Don't block user - continue without mastery update

        mastery_elapsed = time.time() - mastery_start
        if DEBUG_MODE:
            logger.info("=" * 80)
            logger.info(
                f"✅ [Mastery] Processing completed in "
                f"{mastery_elapsed:.2f}s (background writes queued)"
            )
            logger.info(
                f"   Mastery deltas computed: {result.mastery_deltas}"
            )
            mastery_count = len(result.mastery_deltas)
            logger.info(
                f"   ✅ {mastery_count} concept mastery updates applied"
            )
            logger.info("=" * 80)

    def _create_structured_result(
        self,
        output: str,
        question: str,
        model_answer: str,
        student_answer: str,
        max_marks: int = None
    ) -> GradingResult:
        """
        Create a structured result when JSON parsing fails.

        IMPORTANT: No extra LLM calls here.
        """
        if DEBUG_MODE:
            logger.warning(
                "⚠️  [FALLBACK] Using static structured result fallback. "
                "No extra LLM call."
            )

        # Use max_marks if provided, otherwise default to 1
        if max_marks is None or max_marks <= 0:
            max_marks = 1
            logger.warning(
                "⚠️  [FALLBACK] max_marks not provided, using default of 1"
            )

        # Calculate fallback score as 50% of max_marks
        # Clamp to ensure it never exceeds max_marks
        fallback_score_raw = max_marks * 0.5
        fallback_score = self._clamp_score(fallback_score_raw, max_marks)
        if max_marks > 0:
            fallback_percentage = round(
                (fallback_score / max_marks) * 100, 1
            )
        else:
            fallback_percentage = 50.0

        return GradingResult(
            overall_score=round(fallback_score, 1),
            percentage=fallback_percentage,
            grade="D",
            strengths=[
                "You attempted the question and provided a relevant response.",
            ],
            areas_for_improvement=[
                "Your answer did not fully match the expected structure.",
                "Key subject concepts were not clearly explained.",
                "Application, analysis or evaluation may be missing."
            ],
            specific_feedback=(
                "The grading system could not reliably parse the detailed "
                "feedback from the AI. This is a technical fallback result. "
                "Please retry the question later or contact support if this "
                "keeps happening."
            ),
            suggestions=[
                "Write your answer in full sentences with clear points.",
                (
                    "Make sure you define key terms and link them to the "
                    "case/context."
                ),
                (
                    "Add at least one explanation and one consequence or "
                    "evaluation."
                )
            ],
            max_marks=max_marks
        )

    def _create_fallback_result(
        self,
        question: str,
        model_answer: str,
        student_answer: str,
        max_marks: int = None
    ) -> GradingResult:
        """Create a fallback result when grading fully fails"""
        if DEBUG_MODE:
            logger.error("❌ [FALLBACK] Creating hard fallback result.")

        # Use max_marks if provided, otherwise default to 1
        if max_marks is None or max_marks <= 0:
            max_marks = 1
            logger.warning(
                "⚠️  [FALLBACK] max_marks not provided, using default of 1"
            )

        # Clamp score to ensure it never exceeds max_marks (even though it's 0)
        clamped_score = self._clamp_score(0.0, max_marks)

        return GradingResult(
            overall_score=clamped_score,
            percentage=0.0,
            grade="F",
            strengths=["Answer submitted successfully"],
            areas_for_improvement=[
                "Grading system error - please contact support"
            ],
            specific_feedback=(
                "There was an error in the grading system. "
                "Please try again or contact support."
            ),
            suggestions=[
                "Retry grading",
                "Check answer format",
                "Contact technical support"
            ],
            max_marks=max_marks
        )


def main():
    """Example usage of the AnswerGradingAgent"""

    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("❌ Error: OPENAI_API_KEY not found in config.env")
        print("Please check your config.env file")
        return

    agent = AnswerGradingAgent(api_key)

    question = (
        "Explain the concept of market segmentation and its "
        "importance in business strategy."
    )

    model_answer = """
    Market segmentation is the process of dividing a broad consumer
    or business market into sub-groups of consumers based on shared
    characteristics. This concept is crucial for business strategy
    for several reasons:

    1. Targeted Marketing: It allows businesses to focus their
       marketing efforts on specific customer groups, leading to more
       effective campaigns and higher conversion rates.

    2. Product Development: Understanding different segments helps
       in developing products that meet the specific needs and
       preferences of target customers.

    3. Competitive Advantage: By serving specific segments well,
       businesses can differentiate themselves from competitors and
       build customer loyalty.

    4. Resource Allocation: It enables efficient allocation of
       marketing and development resources to the most profitable
       customer segments.

    5. Customer Satisfaction: Tailored products and services lead
       to higher customer satisfaction and retention rates.

    Examples of segmentation criteria include demographic factors
    (age, income), geographic location, psychographic characteristics
    (lifestyle, values), and behavioral patterns (usage rate,
    brand loyalty).
    """

    student_answer = """
    Market segmentation is when you divide customers into groups.
    It's important because it helps businesses sell products better.
    You can target different people with different marketing. It also
    helps make products that people want. Companies can compete better
    this way.
    """

    print("🤖 Starting answer grading...")
    print(f"Question: {question}")
    print(f"Student Answer: {student_answer}")
    print("\n" + "=" * 50 + "\n")

    max_marks_test = 6
    result = agent.grade_answer(
        question,
        model_answer,
        student_answer,
        user_id=None,
        max_marks=max_marks_test
    )

    print("📊 GRADING RESULTS")
    print("=" * 50)
    print(f"Overall Score: {result.overall_score}/{max_marks_test}")
    print(f"Percentage: {result.percentage}%")
    print(f"Grade: {result.grade}")

    print("\n✅ STRENGTHS:")
    for strength in result.strengths:
        print(f"  • {strength}")

    print("\n🔧 AREAS FOR IMPROVEMENT:")
    for area in result.areas_for_improvement:
        print(f"  • {area}")

    print("\n💡 SPECIFIC FEEDBACK:")
    print(f"  {result.specific_feedback}")

    print("\n🚀 SUGGESTIONS:")
    for suggestion in result.suggestions:
        print(f"  • {suggestion}")


if __name__ == "__main__":
    main()
