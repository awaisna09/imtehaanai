#!/usr/bin/env python3
"""
Mastery Agent - Handles student reasoning classification and mastery updates
"""

from typing import Any, Dict, List, Optional
from openai import OpenAI
import logging
import hashlib
from datetime import datetime

# Import cache
try:
    from cache import cache_get, cache_set
except ImportError:
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

# Import Supabase operations helper for concurrency limiting
from services.supabase_ops import sb_execute

logger = logging.getLogger(__name__)


class MasteryAgent:
    """
    Agent responsible for mastery tracking:
    - Classifying student reasoning quality
    - Converting labels to mastery deltas
    - Applying mastery updates to Supabase
    """

    def __init__(
        self,
        api_key: str = None,
        supabase_client: Optional[Any] = None
    ):
        """
        Initialize Mastery Agent

        Args:
            api_key: OpenAI API key for classification
            supabase_client: Supabase client instance
        """
        self.api_key = api_key
        self.supabase = supabase_client

    def classify_reasoning(
        self,
        message_text: str,
        subject_name: Optional[str] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> str:
        """
        Classify a student's message as 'good', 'neutral', or 'confused'
        based on reasoning quality.
        Uses direct OpenAI API with gpt-4o-mini for low-cost classification.
        Uses centralized caching with configurable TTL.
        
        Args:
            message_text: Student message to classify
            subject_name: Optional subject name (e.g., "Business Studies", "Economics")
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for instrumentation
        
        Returns:
            str: 'good', 'neutral', or 'confused'
        """
        if not self.api_key:
            return "neutral"

        # Use centralized caching if available
        if DETERMINISTIC_CACHE_AVAILABLE:
            @cached_operation(
                CacheOperation.REASONING_CLASSIFICATION,
                ttl=CacheTTL.REASONING_CLASSIFICATION,
                job_id=job_id,
                trace_id=trace_id
            )
            def _classify():
                return self._classify_reasoning_impl(
                    message_text, subject_name=subject_name, 
                    job_id=job_id, trace_id=trace_id
                )
            
            return _classify()
        
        # Fallback to legacy caching (include subject in cache key for accuracy)
        subject_key = subject_name or "default"
        message_hash = hashlib.md5(message_text.encode()).hexdigest()[:16]
        cache_key = f"reasoning_classify:{subject_key}:{message_hash}"
        cached = cache_get(cache_key)
        if cached is not None:
            return cached
        
        result = self._classify_reasoning_impl(
            message_text, subject_name=subject_name, 
            job_id=job_id, trace_id=trace_id
        )
        cache_set(cache_key, result, ttl=300)
        return result
    
    def _classify_reasoning_impl(
        self,
        message_text: str,
        subject_name: Optional[str] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> str:
        """Internal implementation of reasoning classification"""
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

        try:
            import threading
            client = OpenAI(api_key=self.api_key, timeout=15.0)

            # Add timeout protection for OpenAI API call (15 seconds)
            result_container = {
                "value": None, "error": None, "completed": False
            }

            def invoke_classification():
                try:
                    # PHASE 1: Prompt Construction
                    with time_prompt_construction(
                        stage_name="mastery_agent_reasoning_prompt_construction",
                        job_id=job_id,
                        trace_id=trace_id
                    ):
                        # Normalize subject name
                        subject_display = subject_name or "Business Studies"
                        if subject_name:
                            subject_lower = subject_name.lower()
                            if "economics" in subject_lower:
                                subject_display = "Economics"
                            elif "geography" in subject_lower or "pak studies geography" in subject_lower:
                                subject_display = "Geography"
                            elif "history" in subject_lower or "pak studies history" in subject_lower:
                                subject_display = "History"
                            elif "islamiyat" in subject_lower:
                                subject_display = "Islamiyat"
                            elif "business" in subject_lower:
                                subject_display = "Business Studies"
                        
                        # Enhanced prompt with subject-aware definitions
                        system_prompt = (
                            "Classify the student's message into ONE category: "
                            "good, neutral, or confused.\n\n"
                            "Use these strict definitions:\n\n"
                            '1. "good"\n'
                            f"   The student demonstrates clear understanding of "
                            f"the {subject_display} concept, applies correct "
                            f"reasoning, uses accurate terminology, or asks an "
                            f"insightful higher-order question. "
                            "Signs of \"good\":\n"
                            "   • Correct definitions, explanations, or "
                            "applications\n"
                            f"   • Logical reasoning using {subject_display.lower()} concepts\n"
                            "   • Making comparisons or connections between concepts\n"
                            "   • Asking advanced, analytical, or evaluative "
                            "questions\n"
                            "   • Building on prior concepts accurately\n\n"
                            '2. "neutral"\n'
                            "   The student asks a standard or basic question, "
                            "makes a simple factual statement, or seeks "
                            "clarification without showing misunderstanding or "
                            "deeper insight. Signs of \"neutral\":\n"
                            "   • Simple definition requests\n"
                            "   • Basic clarifying questions\n"
                            "   • Standard textbook-level inquiries\n"
                            "   • No obvious reasoning or misunderstanding\n\n"
                            '3. "confused"\n'
                            "   The student shows misunderstanding, incorrect "
                            "reasoning, incorrect definitions, contradictions, or "
                            "fundamental misconceptions. Signs of \"confused\":\n"
                            "   • Wrong definitions or concepts\n"
                            "   • Incorrect relationships between concepts\n"
                            "   • Illogical reasoning or contradictions\n"
                            f"   • Confusing unrelated {subject_display.lower()} concepts\n\n"
                            "IMPORTANT:\n"
                            "Output ONLY one word:\n"
                            "good\n"
                            "neutral\n"
                            "confused"
                        )
                        user_message = message_text[:200]
                        prompt_size = len(system_prompt) + len(user_message)

                    # PHASE 2: API Call
                    with time_ai_call(
                        stage_name="mastery_agent_reasoning_api_call",
                        job_id=job_id,
                        trace_id=trace_id,
                        model="gpt-4o-mini",
                        prompt_tokens=prompt_size // 4  # Rough estimate
                    ):
                        resp = client.chat.completions.create(
                            model="gpt-4o-mini",
                            messages=[
                                {
                                    "role": "system",
                                    "content": system_prompt
                                },
                                {"role": "user", "content": user_message}
                            ],
                            max_tokens=10,  # Increased to allow for reasoning
                            temperature=0
                        )
                    result_container["value"] = resp
                    result_container["completed"] = True
                except Exception as e:
                    result_container["error"] = e
                    result_container["completed"] = True

            classify_thread = threading.Thread(
                target=invoke_classification, daemon=True
            )
            classify_thread.start()
            classify_thread.join(timeout=15)

            if not result_container["completed"]:
                logger.warning(
                    "Reasoning classification timed out, using 'neutral'"
                )
                return "neutral"

            if result_container["error"]:
                logger.warning(
                    f"Reasoning classification error: "
                    f"{result_container['error']}, using 'neutral'"
                )
                return "neutral"

            # PHASE 3: Response Parsing and Validation
            with time_response_parsing(
                stage_name="mastery_agent_reasoning_response_parsing",
                job_id=job_id,
                trace_id=trace_id,
                response_size=len(resp.choices[0].message.content) if resp.choices else None
            ):
                resp = result_container["value"]
                label = resp.choices[0].message.content.strip().lower()
                if label not in ["good", "neutral", "confused"]:
                    label = "neutral"

            return label

        except Exception as e:
            logger.warning(f"Reasoning classification failed: {e}")
            return "neutral"

    def label_to_delta(self, label: str) -> int:
        """
        Convert reasoning label to mastery delta.

        Args:
            label: 'good', 'neutral', or 'confused'

        Returns:
            int: Mastery delta (+2, 0, or -1)
        """
        if label == "good":
            return 2
        elif label == "confused":
            return -1
        else:
            return 0

    def apply_updates(
        self,
        user_id: Optional[str],
        updates: List[Dict],
        subject_id: Optional[int] = None,
        subject_name: Optional[str] = None
    ) -> None:
        """
        Apply mastery score updates for multiple concepts.

        updates = [
            { "concept_id": str, "delta": int, "reason": str }
        ]

        Workflow:
        1. Fetch existing mastery rows for these concept IDs.
        2. If no row exists → assume baseline mastery = 50.
        3. Apply delta and clamp between 0–100.
        4. Write updated mastery to Supabase with subject_name.
        5. For negative updates, create/update weakness entries.
        6. Update trends lightly (increase or decrease trend_score).
        
        Args:
            user_id: User ID to update mastery for
            updates: List of update dicts
            subject_id: Optional subject ID (for deriving subject_name)
            subject_name: Optional subject name (e.g., "Business Studies", "Economics")
        """
        if not self.supabase or not user_id:
            return

        # Get subject name if not provided
        if not subject_name and subject_id:
            subject_map = {
                101: "Business Studies",
                102: "Islamiyat",
                113: "Pak Studies Geography",
                114: "Pak Studies History",
                119: "Economics",
                103: "Mathematics",
                104: "Physics",
                105: "Chemistry"
            }
            subject_name = subject_map.get(subject_id, "Business Studies")

        # Map concept_id → delta, reason
        concept_ids = [u["concept_id"] for u in updates]

        # Fetch existing mastery rows with timeout protection
        try:
            # Import timeout wrapper
            try:
                from langgraph_tutor import safe_supabase_query
            except ImportError:
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

            def fetch_func():
                return sb_execute(
                    self.supabase.table("student_mastery")
                    .select("*")
                    .eq("user_id", user_id)
                    .in_("concept_id", concept_ids)
                )

            res = safe_supabase_query(
                fetch_func, timeout=5, default_return={"data": []}
            )
            if res is None:
                res = {"data": []}
            existing = {row["concept_id"]: row for row in (res.data or [])}
        except Exception:
            existing = {}

        rows_to_upsert = []
        weakness_rows = []
        trend_rows = []

        for update in updates:
            cid = update["concept_id"]
            delta = update["delta"]
            reason = update.get("reason", "tutor_chat")

            current_mastery = 50  # baseline
            if cid in existing:
                current_mastery = existing[cid].get("mastery_score", 50)

            new_mastery = max(0, min(100, current_mastery + delta))

            # Prepare mastery row for upsert
            # Always include updated_at to ensure timestamp is refreshed
            # on every message, even when delta is 0
            mastery_row = {
                "user_id": user_id,
                "concept_id": cid,
                "mastery_score": new_mastery,
                "updated_at": datetime.now().isoformat()
            }
            
            # Add subject if available (column name is 'subject', not 'subject_name')
            if subject_name:
                mastery_row["subject"] = subject_name
            
            rows_to_upsert.append(mastery_row)

            # Weakness logic (negative signals)
            if delta < 0:
                weakness_rows.append({
                    "user_id": user_id,
                    "concept_id": cid,
                    "severity": "high" if delta <= -5 else "medium",
                    "reason": reason
                })

            # Trend logic (tiny lightweight update)
            # Note: student_trends has unique constraint on (user_id, concept_id)
            # So we need to upsert, not insert
            # OPTIMIZED: Only create trend row if delta is non-zero (skip unnecessary updates)
            if delta != 0:
                trend_rows.append({
                    "user_id": user_id,
                    "concept_id": cid,
                    "trend_score": delta  # simple additive trend
                })

        # CRITICAL FIX: Deduplicate rows_to_upsert to prevent duplicate entries
        # Keep only the last occurrence of each (user_id, concept_id) pair
        # This prevents multiple entries for the same concept in one request
        original_count = len(rows_to_upsert)
        seen_keys = {}
        for row in rows_to_upsert:
            key = (row["user_id"], str(row["concept_id"]))  # Ensure concept_id is string for consistency
            seen_keys[key] = row  # Overwrite if duplicate, keep last one
        
        # Convert back to list
        rows_to_upsert = list(seen_keys.values())
        removed_count = original_count - len(rows_to_upsert)
        
        if removed_count > 0:
            logger.warning(
                f"[WARNING] Mastery: Removed {removed_count} duplicate(s) from "
                f"{original_count} rows (keeping {len(rows_to_upsert)} unique rows)"
            )
        
        # Write mastery rows with explicit update/insert logic
        # PARALLELIZED: Each row is processed independently in parallel
        # CRITICAL: Always update to ensure:
        # 1. New rows are created if concept_id doesn't exist
        # 2. Existing rows are updated if concept_id already exists
        # 3. updated_at timestamp is refreshed on EVERY message
        # 4. No duplicates are created (deduplicated above)
        try:
            # Import batch parallelization utility
            try:
                from services.batch_parallelization import (
                    run_batch_parallel_sync
                )
                BATCH_PARALLEL_AVAILABLE = True
            except ImportError:
                BATCH_PARALLEL_AVAILABLE = False
                logger.warning(
                    "[WARNING] Batch parallelization not available, "
                    "using sequential processing"
                )
            
            def upsert_single_mastery_row(row: Dict) -> Optional[Any]:
                """
                Upsert a single mastery row using native UPSERT.
                OPTIMIZED: Uses single UPSERT query instead of check + insert/update.
                This reduces from 2 queries to 1 query per row (50% faster).
                """
                try:
                    # Ensure concept_id is consistent type (string)
                    concept_id_str = str(row["concept_id"])
                    
                    # OPTIMIZED: Use native UPSERT (INSERT ... ON CONFLICT UPDATE)
                    # This combines check + insert/update into a single query
                    # Reduces from 2 queries (check + insert/update) to 1 query
                    insert_row = row.copy()
                    insert_row["concept_id"] = concept_id_str
                    
                    # Use upsert with conflict resolution on (user_id, concept_id)
                    # This automatically handles both insert and update cases
                    result = sb_execute(
                        self.supabase.table("student_mastery")
                        .upsert(
                            insert_row,
                            on_conflict="user_id,concept_id"  # Conflict resolution
                        )
                    )
                    
                    logger.debug(
                        f"[DEBUG] Mastery: Upserted row for "
                        f"user_id={row['user_id']}, concept_id={concept_id_str}"
                    )
                    return result
                except Exception as e:
                    logger.warning(
                        f"[WARNING] Failed to upsert mastery for "
                        f"concept {row.get('concept_id')}: {e}"
                    )
                    import traceback
                    logger.debug(f"[DEBUG] Traceback: {traceback.format_exc()}")
                    raise  # Re-raise to be handled by batch parallelization
            
            # Process rows in parallel if available
            if BATCH_PARALLEL_AVAILABLE and len(rows_to_upsert) > 1:
                # Use parallel batch processing
                results = run_batch_parallel_sync(
                    items=rows_to_upsert,
                    process_func=upsert_single_mastery_row,
                    job_type="mastery_update",
                    job_id=None,  # Could be passed from caller
                    trace_id=None,  # Could be passed from caller
                    base_limit=None,  # Uses MAX_DB_CONNECTIONS
                    error_handler=None  # Errors are logged and re-raised
                )
                
                # Count successful updates
                success_count = sum(
                    1 for _, result, exc in results
                    if exc is None and result is not None
                )
                
                if success_count > 0:
                    logger.info(
                        f"[SUCCESS] Mastery updated (parallel): "
                        f"{success_count}/{len(rows_to_upsert)} concepts "
                        f"for user {user_id}"
                    )
                else:
                    logger.warning(
                        f"[WARNING] Mastery update (parallel) returned no "
                        f"successful results for {len(rows_to_upsert)} concepts"
                    )
            else:
                # Fallback to sequential processing
                def upsert_mastery_func():
                    results = []
                    for row in rows_to_upsert:
                        try:
                            result = upsert_single_mastery_row(row)
                            if result:
                                results.append(result)
                        except Exception:
                            # Already logged in upsert_single_mastery_row
                            continue
                    return results
                
                result = safe_supabase_query(
                    upsert_mastery_func, timeout=10, default_return=None
                )
                if result:
                    logger.info(
                        f"[SUCCESS] Mastery updated (sequential): "
                        f"{len(rows_to_upsert)} concepts for user {user_id}"
                    )
                else:
                    logger.warning(
                        f"[WARNING] Mastery update (sequential) returned no "
                        f"result for {len(rows_to_upsert)} concepts"
                    )
        except Exception as e:
            logger.error(
                f"[ERROR] Failed to upsert mastery entries: {e}",
                exc_info=True
            )
            # Don't fail silently - log the error

        # Insert weakness rows with timeout protection
        if len(weakness_rows) > 0:
            try:
                def insert_weakness_func():
                    return sb_execute(
                        self.supabase.table("student_weaknesses")
                        .insert(weakness_rows)
                    )
                safe_supabase_query(
                    insert_weakness_func, timeout=5, default_return=None
                )
            except Exception:
                pass

        # Update trends with timeout protection
        # OPTIMIZED: Only update if there are trend rows (skip if all deltas were 0)
        if len(trend_rows) > 0:
            try:
                def upsert_trends_func():
                    return sb_execute(
                        self.supabase.table("student_trends")
                        .upsert(trend_rows, on_conflict="user_id,concept_id")
                    )
                safe_supabase_query(
                    upsert_trends_func, timeout=5, default_return=None
                )
            except Exception:
                pass

        # Update mastery_states.mastery_concept with the same mastery value from student_mastery
        # Store the latest updated concept's mastery (typically one concept per AI Tutor update)
        # Include subject name for proper tracking
        if len(rows_to_upsert) > 0:
            try:
                # Use the latest updated concept's mastery value (same as stored in student_mastery)
                # AI Tutor typically updates one concept at a time, so use the last one in the list
                latest_mastery_row = rows_to_upsert[-1]
                concept_mastery_value = latest_mastery_row["mastery_score"]

                def upsert_mastery_states_func():
                    # OPTIMIZED: Use native UPSERT instead of check + insert/update
                    # This reduces from 2 queries to 1 query (50% faster)
                    # Skip if user_id is not a valid UUID (causes errors)
                    try:
                        import uuid
                        # Validate UUID format
                        uuid.UUID(user_id)
                    except (ValueError, TypeError):
                        # Not a valid UUID, skip mastery_states update
                        logger.debug(
                            f"[DEBUG] Skipping mastery_states update - "
                            f"user_id '{user_id}' is not a valid UUID"
                        )
                        return None
                    
                    # Prepare upsert data with subject name
                    upsert_data = {
                        "user_id": user_id,
                        "mastery_concept": concept_mastery_value,  # Same mastery as stored in student_mastery
                        "mastery_micro": 0,
                        "mastery_macro": 0,
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    # Add subject name if available (for new entries and updates)
                    if subject_name:
                        upsert_data["subject"] = subject_name
                    
                    logger.info(
                        f"[MASTERY_STATES] Attempting upsert: user_id={user_id}, "
                        f"subject={subject_name or 'None'}, mastery_concept={concept_mastery_value}"
                    )
                    
                    # Try upsert with conflict resolution
                    # First try with (user_id, subject) if subject is provided
                    # Fallback to user_id only if that fails
                    try:
                        if subject_name:
                            # Check if row exists for this user and subject
                            existing_check = sb_execute(
                                self.supabase.table("mastery_states")
                                .select("user_id, subject, mastery_concept")
                                .eq("user_id", user_id)
                                .eq("subject", subject_name)
                                .limit(1)
                            )
                            
                            if existing_check.data and len(existing_check.data) > 0:
                                # Row exists - use UPDATE
                                logger.info(
                                    f"[MASTERY_STATES] Row exists, updating: user_id={user_id}, subject={subject_name}"
                                )
                                result = sb_execute(
                                    self.supabase.table("mastery_states")
                                    .update({
                                        "mastery_concept": concept_mastery_value,
                                        "updated_at": datetime.now().isoformat()
                                    })
                                    .eq("user_id", user_id)
                                    .eq("subject", subject_name)
                                )
                            else:
                                # Row doesn't exist - try INSERT
                                logger.info(
                                    f"[MASTERY_STATES] Row doesn't exist, inserting: user_id={user_id}, subject={subject_name}"
                                )
                                # Set default values for new row
                                insert_data = upsert_data.copy()
                                if "mastery_micro" not in insert_data:
                                    insert_data["mastery_micro"] = 0
                                if "mastery_macro" not in insert_data:
                                    insert_data["mastery_macro"] = 0
                                result = sb_execute(
                                    self.supabase.table("mastery_states")
                                    .insert(insert_data)
                                )
                            
                            return result
                        else:
                            # No subject - use user_id only constraint
                            return sb_execute(
                                self.supabase.table("mastery_states")
                                .upsert(upsert_data, on_conflict="user_id")
                            )
                    except Exception as upsert_error:
                        # If (user_id, subject) approach fails, try user_id only
                        error_str = str(upsert_error).lower()
                        logger.warning(
                            f"[MASTERY_STATES] Upsert failed, trying fallback: {upsert_error}"
                        )
                        
                        if subject_name and any(keyword in error_str for keyword in ["on_conflict", "conflict", "unique", "constraint", "column", "duplicate"]):
                            # Try update/insert with user_id only
                            try:
                                existing_check = sb_execute(
                                    self.supabase.table("mastery_states")
                                    .select("user_id")
                                    .eq("user_id", user_id)
                                    .limit(1)
                                )
                                
                                if existing_check.data and len(existing_check.data) > 0:
                                    # Update existing row (without subject filter)
                                    logger.info(
                                        f"[MASTERY_STATES] Fallback: Updating by user_id only: user_id={user_id}"
                                    )
                                    update_data = {
                                        "mastery_concept": concept_mastery_value,
                                        "updated_at": datetime.now().isoformat()
                                    }
                                    if subject_name:
                                        update_data["subject"] = subject_name
                                    return sb_execute(
                                        self.supabase.table("mastery_states")
                                        .update(update_data)
                                        .eq("user_id", user_id)
                                    )
                                else:
                                    # Insert new row
                                    logger.info(
                                        f"[MASTERY_STATES] Fallback: Inserting new row: user_id={user_id}, subject={subject_name}"
                                    )
                                    return sb_execute(
                                        self.supabase.table("mastery_states")
                                        .insert(upsert_data)
                                    )
                            except Exception as fallback_error:
                                logger.error(
                                    f"[MASTERY_STATES] Fallback also failed: {fallback_error}"
                                )
                                raise fallback_error
                        else:
                            raise  # Re-raise if it's a different error

                # Use safe_supabase_query with error handling
                # Don't let mastery_states update failure break the entire mastery update
                try:
                    result = safe_supabase_query(
                        upsert_mastery_states_func,
                        timeout=5,
                        default_return=None
                    )
                    
                    # Log the update for debugging
                    if result is not None:
                        logger.info(
                            f"[SUCCESS] Updated mastery_states.mastery_concept: "
                            f"user_id={user_id}, mastery_concept={concept_mastery_value}, "
                            f"subject={subject_name or 'N/A'}, concept_id={latest_mastery_row.get('concept_id', 'N/A')}"
                        )
                    else:
                        logger.warning(
                            f"[WARNING] mastery_states update returned None - "
                            f"user_id={user_id}, subject={subject_name or 'N/A'}, "
                            f"mastery_concept={concept_mastery_value}. "
                            f"This might indicate the update was skipped or failed silently."
                        )
                except Exception as mastery_states_error:
                    # Log error but don't fail the entire mastery update
                    logger.warning(
                        f"[WARNING] Failed to update mastery_states.mastery_concept (non-critical): {mastery_states_error}",
                        exc_info=True
                    )
            except Exception as e:
                # Outer exception handler - log but don't fail
                logger.error(
                    f"[ERROR] Failed to update mastery_states.mastery_concept: {e}",
                    exc_info=True
                )
                # Don't re-raise - mastery_states update is non-critical
