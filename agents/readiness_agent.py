#!/usr/bin/env python3
"""
Readiness Agent - Handles readiness assessment and learning path computation
"""

from typing import Any, Dict, List, Optional
import logging
import hashlib
import random

# Import cache
try:
    from cache import cache_get, cache_set, _hash_string
except ImportError:
    # Fallback if cache not available
    def cache_get(key): return None
    def cache_set(key, value, ttl=3600): return False
    def _hash_string(text): return hashlib.md5(text.encode()).hexdigest()[:12]

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

logger = logging.getLogger(__name__)


class ReadinessAgent:
    """
    Agent responsible for readiness assessment:
    - Classifying readiness levels from mastery scores
    - Computing readiness signals
    - Determining next learning steps
    """

    def __init__(
        self,
        supabase_client: Optional[Any] = None,
        concept_agent: Optional[Any] = None
    ):
        """
        Initialize Readiness Agent

        Args:
            supabase_client: Supabase client instance
            concept_agent: ConceptAgent instance for concept graph queries
        """
        self.supabase = supabase_client
        self.concept_agent = concept_agent

    def classify_readiness(self, mastery_score: int) -> str:
        """
        Given a mastery score (0–100), classify readiness level:
        - ready
        - almost_ready
        - needs_reinforcement
        - review_prerequisites
        """
        if mastery_score >= 70:
            return "ready"
        elif mastery_score >= 50:
            return "almost_ready"
        elif mastery_score >= 30:
            return "needs_reinforcement"
        else:
            return "review_prerequisites"

    def compute_readiness(
        self,
        user_id: Optional[str],
        concept_ids: List[str],
        mastery_updates: Optional[List[Dict]] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> Dict:
        """
        Compute the user's readiness level for the detected concepts.
        Uses centralized caching with configurable TTL.
        
        NOTE: If mastery_updates are provided, cache key includes them to ensure
        freshness. Cache is invalidated when mastery updates are applied.

        Args:
            user_id: User ID to compute readiness for
            concept_ids: List of concept IDs
            mastery_updates: Optional mastery updates from current session
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for instrumentation

        Returns a dict:
        {
            "overall_readiness": str,
            "concept_readiness": [
                { "concept_id": str, "mastery": int, "readiness": str }
            ],
            "average_mastery": float,
            "min_mastery": int
        }
        """
        # If missing dependencies → neutral/unknown output
        if not self.supabase or not user_id or len(concept_ids) == 0:
            return {
                "overall_readiness": "unknown",
                "concept_readiness": [],
                "average_mastery": None,
                "min_mastery": None
            }

        # Use centralized caching if available
        if DETERMINISTIC_CACHE_AVAILABLE:
            @cached_operation(
                CacheOperation.READINESS_ASSESSMENT,
                ttl=CacheTTL.READINESS_ASSESSMENT,
                job_id=job_id,
                trace_id=trace_id
            )
            def _compute_readiness():
                return self._compute_readiness_impl(
                    user_id, concept_ids, mastery_updates
                )
            
            return _compute_readiness()
        
        # Fallback to legacy caching (if centralized cache not available)
        # Use increased TTL: 15 minutes (900s) instead of 60s
        concept_ids_sorted = sorted(concept_ids)
        concept_ids_hash = _hash_string(":".join(concept_ids_sorted))
        cache_key = f"readiness:{user_id}:{concept_ids_hash}"
        cached = cache_get(cache_key)
        if cached is not None:
            # Log cache hit at INFO level for production monitoring
            logger.info(
                f"[CACHE HIT] readiness_assessment - "
                f"key: {cache_key[:50]}... "
                f"(user_id: {user_id}, concept_count: {len(concept_ids)})"
            )
            return cached
        
        # Log cache miss at INFO level for production monitoring
        logger.info(
            f"[CACHE MISS] readiness_assessment - "
            f"key: {cache_key[:50]}... "
            f"(user_id: {user_id}, concept_count: {len(concept_ids)})"
        )
        
        result = self._compute_readiness_impl(user_id, concept_ids, mastery_updates)
        # Increased TTL: 15 minutes (900s) - deterministic over short time windows,
        # invalidated on mastery updates
        cache_set(cache_key, result, ttl=900)
        return result
    
    def _compute_readiness_impl(
        self,
        user_id: Optional[str],
        concept_ids: List[str],
        mastery_updates: Optional[List[Dict]] = None
    ) -> Dict:
        """Internal implementation of readiness computation"""
        # Fetch mastery rows for these concept IDs
        # Convert concept_ids to integers for database query
        # (database stores concept_id as integer)
        try:
            concept_ids_int = [
                int(cid) for cid in concept_ids
                if cid and str(cid).strip() and str(cid) != "None"
            ]
            if not concept_ids_int:
                logger.warning(
                    f"[WARNING] No valid concept_ids to query: "
                    f"{concept_ids}"
                )
                mastery_rows = []
            else:
                # Add timeout protection for Supabase query
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

                def query_func():
                    return (
                        self.supabase.table("student_mastery")
                        .select("concept_id, mastery_score")
                        .eq("user_id", user_id)
                        .in_("concept_id", concept_ids_int)
                        .execute()
                    )

                res = safe_supabase_query(
                    query_func, timeout=5, default_return={"data": []}
                )
                if res is None:
                    res = {"data": []}
                mastery_rows = res.data or []
                logger.info(
                    f"[DEBUG] Fetched {len(mastery_rows)} mastery rows "
                    f"for {len(concept_ids_int)} concept_ids"
                )
        except Exception as e:
            logger.error(
                f"[ERROR] Failed to fetch mastery rows: {e}"
            )
            import traceback
            logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")
            mastery_rows = []

        # Map concept_id → mastery score
        # Convert database concept_ids (int) to strings for matching
        mastery_map = {
            str(row["concept_id"]): row["mastery_score"]
            for row in mastery_rows
        }

        # Apply mastery updates from current session if provided
        # This ensures readiness reflects the latest mastery changes
        if mastery_updates and len(mastery_updates) > 0:
            # Create a map of concept_id -> delta from updates
            updates_map = {
                str(update.get("concept_id")): update.get("delta", 0)
                for update in mastery_updates
                if update.get("concept_id") is not None
            }
            # Apply deltas to mastery scores
            for cid_str, delta in updates_map.items():
                current_mastery = mastery_map.get(cid_str, 50)
                # Apply delta and clamp between 0-100
                new_mastery = max(0, min(100, current_mastery + delta))
                mastery_map[cid_str] = new_mastery
                logger.info(
                    f"[DEBUG] Applied mastery update: concept {cid_str} "
                    f"{current_mastery} -> {new_mastery} (delta: {delta:+d})"
                )

        mastery_list = []
        for cid in concept_ids:
            # Use baseline 50 if no mastery data exists (new student)
            # This ensures we always return readiness, even for new students
            mastery_val = mastery_map.get(str(cid), 50)  # default baseline
            readiness_val = self.classify_readiness(mastery_val)
            mastery_list.append({
                "concept_id": str(cid),  # Ensure concept_id is string
                "mastery": mastery_val,
                "readiness": readiness_val
            })

        # Compute aggregate stats (always compute, even with baseline values)
        if len(mastery_list) > 0:
            avg = sum(m["mastery"] for m in mastery_list) / len(mastery_list)
            min_val = min(m["mastery"] for m in mastery_list)
        else:
            # This should never happen if concept_ids has items, but handle it
            avg = None
            min_val = None

        # Determine overall readiness: worst readiness "wins"
        readiness_order = [
            "review_prerequisites",
            "needs_reinforcement",
            "almost_ready",
            "ready"
        ]

        # Always compute overall readiness if we have concepts
        if len(mastery_list) > 0:
            overall = sorted(
                [m["readiness"] for m in mastery_list],
                key=lambda x: readiness_order.index(x)
            )[0]
        else:
            # Only return unknown if truly no concepts
            overall = "unknown"

        result = {
            "overall_readiness": overall,
            "concept_readiness": mastery_list,
            "average_mastery": avg,
            "min_mastery": min_val
        }

        return result

    def compute_next_step(
        self,
        readiness_result: Dict,
        concept_ids: List[str]
    ) -> Dict:
        """
        Given readiness, mastery, and concept graph,
        determine the student's next recommended step.

        Returns:
        {
            "decision": str,   # advance / reinforce / review_prerequisite /
                                # learn_next_concept / unknown
            "recommended_concept": Optional[str],
            "details": str
        }
        """
        # CRITICAL: If we have concepts, ALWAYS recommend one
        # This is the PRIMARY behavior - do this FIRST before any other checks
        # Shuffle the list first to ensure true randomness, then select
        if len(concept_ids) > 0:
            # Use timestamp + process time for maximum randomness
            import time
            import os
            unique_seed = (
                int(time.time() * 1000000) % 1000000 +
                os.getpid() % 1000 +
                int(time.perf_counter() * 1000000) % 1000
            ) % 1000000
            random.seed(unique_seed)
            # Shuffle a copy to ensure random order
            concept_ids_shuffled = concept_ids.copy()
            random.shuffle(concept_ids_shuffled)
            # Double shuffle for extra randomness
            random.shuffle(concept_ids_shuffled)
            recommended = random.choice(concept_ids_shuffled)
            # Reset random seed
            random.seed()
            # If readiness is missing/invalid/unknown, recommend concept
            # immediately
            if (not readiness_result or
                    not isinstance(readiness_result, dict) or
                    len(readiness_result) == 0):
                logger.info(
                    f"[DEBUG] No readiness result - "
                    f"recommending concept {recommended}"
                )
                return {
                    "decision": "learn_next_concept",
                    "recommended_concept": str(recommended),
                    "details": (
                        "Continue exploring concepts in this topic. "
                        "Try asking questions about the recommended concept "
                        "to deepen your understanding."
                    )
                }

        # If no concepts found, suggest exploring the topic
        if len(concept_ids) == 0:
            return {
                "decision": "explore_topic",
                "recommended_concept": None,
                "details": (
                    "No specific concepts detected. Continue exploring "
                    "the current topic and ask more questions to identify "
                    "key concepts."
                )
            }

        # CRITICAL: ALWAYS recommend a concept when concepts are available
        # This is the PRIMARY behavior - concepts should ALWAYS drive
        # learning path
        # Shuffle the list first to ensure true randomness, then select
        if len(concept_ids) > 0:
            # Use timestamp + process time for maximum randomness
            import time
            import os
            unique_seed = (
                int(time.time() * 1000000) % 1000000 +
                os.getpid() % 1000 +
                int(time.perf_counter() * 1000000) % 1000
            ) % 1000000
            random.seed(unique_seed)
            # Shuffle a copy to ensure random order
            concept_ids_shuffled = concept_ids.copy()
            random.shuffle(concept_ids_shuffled)
            # Double shuffle for extra randomness
            random.shuffle(concept_ids_shuffled)
            recommended = random.choice(concept_ids_shuffled)
            # Reset random seed
            random.seed()

        # Extract overall_readiness from readiness_result
        # Handle both raw readiness (from pipeline) and normalized (from API)
        # The raw readiness has "overall_readiness" as a string
        # ("almost_ready", "unknown", etc.)
        # The normalized readiness has "overall_readiness" as a float
        # (0.0, 0.75, etc.)
        # The frontend might show "overall" key
        overall_readiness = None
        has_readiness = (
            readiness_result and
            isinstance(readiness_result, dict) and
            len(readiness_result) > 0
        )

        if has_readiness:
            # Check for "overall" key (frontend format or normalized format)
            if "overall" in readiness_result:
                overall_readiness = readiness_result.get("overall")
            # Check for "overall_readiness" key (raw format from pipeline)
            elif "overall_readiness" in readiness_result:
                overall_readiness = readiness_result.get(
                    "overall_readiness"
                )

        # Log for debugging
        readiness_type = (
            type(overall_readiness).__name__
            if overall_readiness is not None
            else 'None'
        )
        readiness_keys = (
            list(readiness_result.keys())
            if isinstance(readiness_result, dict)
            else 'N/A'
        )
        logger.info(
            f"[DEBUG] compute_next_step: concepts={len(concept_ids)}, "
            f"has_readiness={has_readiness}, "
            f"overall_readiness={overall_readiness} "
            f"(type: {readiness_type}), "
            f"recommended={recommended}, "
            f"readiness_result={readiness_result}, "
            f"readiness_result keys={readiness_keys}"
        )

        # Check if unknown - handle both string and numeric forms
        # Treat 0.0, 0, None, "unknown", empty string, or missing as unknown
        # Also check if readiness_result is empty dict or has no readiness keys
        is_unknown = (
            not has_readiness or
            overall_readiness is None or
            overall_readiness == "unknown" or
            overall_readiness == 0 or
            overall_readiness == 0.0 or
            overall_readiness == "" or
            (isinstance(overall_readiness, (int, float)) and
             overall_readiness <= 0.0) or
            (isinstance(readiness_result, dict) and
             "overall" not in readiness_result and
             "overall_readiness" not in readiness_result)
        )

        # CRITICAL: If we have concepts, ALWAYS recommend one if readiness
        # is unknown
        # This ensures concepts are ALWAYS used when readiness is unclear
        if is_unknown:
            unknown_type = (
                type(overall_readiness).__name__
                if overall_readiness is not None
                else 'None'
            )
            logger.info(
                f"[DEBUG] Readiness unknown (value={overall_readiness}, "
                f"type={unknown_type}) - "
                f"returning learn_next_concept with "
                f"recommended={recommended}"
            )
            return {
                "decision": "learn_next_concept",
                "recommended_concept": str(recommended),
                "details": (
                    "Continue exploring concepts in this topic. "
                    "Try asking questions about the recommended concept "
                    "to deepen your understanding."
                )
            }

        # ADDITIONAL SAFETY: If we somehow get here with concepts but no
        # valid readiness, still recommend a concept (this should never
        # happen, but safety first)
        if (len(concept_ids) > 0 and
                (not has_readiness or overall_readiness is None)):
            # Use timestamp-based seed for true randomness
            import time
            random.seed(int(time.time() * 1000000) % 1000000)
            # Shuffle and select a RANDOM concept
            concept_ids_shuffled = concept_ids.copy()
            random.shuffle(concept_ids_shuffled)
            recommended = random.choice(concept_ids_shuffled)
            # Reset random seed
            random.seed()
            logger.warning(
                f"[WARNING] Safety check: concepts exist but readiness "
                f"invalid - recommending {recommended}"
            )
            return {
                "decision": "learn_next_concept",
                "recommended_concept": str(recommended),
                "details": (
                    "Continue exploring concepts in this topic. "
                    "Try asking questions about the recommended concept "
                    "to deepen your understanding."
                )
            }

        # If we get here, readiness is known - process it
        if not readiness_result:
            # Fallback if no readiness result - still recommend concept
            # Shuffle and select a RANDOM concept from the list
            if len(concept_ids) > 0:
                # Use timestamp-based seed for true randomness
                import time
                random.seed(int(time.time() * 1000000) % 1000000)
                # Shuffle a copy to ensure random order
                concept_ids_shuffled = concept_ids.copy()
                random.shuffle(concept_ids_shuffled)
                recommended = random.choice(concept_ids_shuffled)
                # Reset random seed
                random.seed()
                return {
                    "decision": "learn_next_concept",
                    "recommended_concept": str(recommended),
                    "details": (
                        "Continue exploring concepts in this topic. "
                        "Try asking questions about the recommended concept "
                        "to deepen your understanding."
                    )
                }
            return {
                "decision": "unknown",
                "recommended_concept": None,
                "details": "Unable to determine learning path."
            }

        # Get overall_readiness - handle both "overall" and
        # "overall_readiness" keys
        overall = None
        if "overall" in readiness_result:
            overall = readiness_result.get("overall")
        elif "overall_readiness" in readiness_result:
            overall = readiness_result.get("overall_readiness")

        # Log for debugging
        logger.info(
            f"[DEBUG] compute_next_step (known readiness): overall={overall}, "
            f"type={type(overall).__name__}, concepts={len(concept_ids)}"
        )

        # If overall is None or 0/0.0, treat as unknown and recommend concept
        # Shuffle and select a RANDOM concept from the list
        if overall is None or overall == 0 or overall == 0.0:
            if len(concept_ids) > 0:
                # Shuffle a copy to ensure random order
                concept_ids_shuffled = concept_ids.copy()
                random.shuffle(concept_ids_shuffled)
                recommended = random.choice(concept_ids_shuffled)
                logger.info(
                    f"[DEBUG] Overall is None/0 - "
                    f"recommending concept {recommended}"
                )
                return {
                    "decision": "learn_next_concept",
                    "recommended_concept": str(recommended),
                    "details": (
                        "Continue exploring concepts in this topic. "
                        "Try asking questions about the recommended concept "
                        "to deepen your understanding."
                    )
                }

        # Convert overall to string if it's numeric
        if isinstance(overall, (int, float)):
            # Map numeric values to readiness strings
            if overall <= 0.25:
                overall = "review_prerequisites"
            elif overall <= 0.5:
                overall = "needs_reinforcement"
            elif overall <= 0.75:
                overall = "almost_ready"
            else:
                overall = "ready"

        # 1. Check for very low mastery: recommend prerequisite review
        if overall == "review_prerequisites":
            if not self.concept_agent:
                # Use timestamp + process time for maximum randomness
                import time
                import os
                unique_seed = (
                    int(time.time() * 1000000) % 1000000 +
                    os.getpid() % 1000 +
                    int(time.perf_counter() * 1000000) % 1000
                ) % 1000000
                random.seed(unique_seed)
                # Shuffle and select a RANDOM concept from the list
                concept_ids_shuffled = concept_ids.copy()
                random.shuffle(concept_ids_shuffled)
                random.shuffle(concept_ids_shuffled)
                recommended = random.choice(concept_ids_shuffled)
                random.seed()
                return {
                    "decision": "reinforce",
                    "recommended_concept": recommended,
                    "details": (
                        "Insufficient mastery; reinforce current concept."
                    )
                }

            concept_graph = (
                self.concept_agent.get_prerequisites_and_next_concepts(
                    concept_ids
                )
            )
            prereqs = concept_graph.get("prerequisites", [])

            if len(prereqs) > 0:
                return {
                    "decision": "review_prerequisite",
                    "recommended_concept": prereqs[0].get("concept_id"),
                    "details": (
                        "Mastery too low; review prerequisite concept first."
                    )
                }
            else:
                # Use timestamp-based seed for true randomness
                import time
                # Use timestamp + process time for maximum randomness
                import time
                import os
                unique_seed = (
                    int(time.time() * 1000000) % 1000000 +
                    os.getpid() % 1000 +
                    int(time.perf_counter() * 1000000) % 1000
                ) % 1000000
                random.seed(unique_seed)
                # Shuffle and select a RANDOM concept from the list
                concept_ids_shuffled = concept_ids.copy()
                random.shuffle(concept_ids_shuffled)
                random.shuffle(concept_ids_shuffled)
                recommended = random.choice(concept_ids_shuffled)
                random.seed()
                return {
                    "decision": "reinforce",
                    "recommended_concept": recommended,
                    "details": (
                        "Insufficient mastery; reinforce current concept."
                    )
                }

        # 2. Medium-low mastery → reinforce current concept
        # Use timestamp + process time for maximum randomness
        if overall == "needs_reinforcement":
            import time
            import os
            unique_seed = (
                int(time.time() * 1000000) % 1000000 +
                os.getpid() % 1000 +
                int(time.perf_counter() * 1000000) % 1000
            ) % 1000000
            random.seed(unique_seed)
            # Shuffle and select a RANDOM concept from the list
            concept_ids_shuffled = concept_ids.copy()
            random.shuffle(concept_ids_shuffled)
            random.shuffle(concept_ids_shuffled)
            recommended = random.choice(concept_ids_shuffled)
            random.seed()
            return {
                "decision": "reinforce",
                "recommended_concept": recommended,
                "details": (
                    "Student needs reinforcement before progressing."
                )
            }

        # 3. Medium mastery → almost ready → maybe prepare next concept
        if overall == "almost_ready":
            if not self.concept_agent:
                # Use timestamp-based seed for true randomness
                import time
                # Use timestamp + process time for maximum randomness
                import time
                import os
                unique_seed = (
                    int(time.time() * 1000000) % 1000000 +
                    os.getpid() % 1000 +
                    int(time.perf_counter() * 1000000) % 1000
                ) % 1000000
                random.seed(unique_seed)
                # Shuffle and select a RANDOM concept from the list
                concept_ids_shuffled = concept_ids.copy()
                random.shuffle(concept_ids_shuffled)
                random.shuffle(concept_ids_shuffled)
                recommended = random.choice(concept_ids_shuffled)
                random.seed()
                return {
                    "decision": "reinforce",
                    "recommended_concept": recommended,
                    "details": (
                        "No next concept found; strengthen understanding."
                    )
                }

            concept_graph = (
                self.concept_agent.get_prerequisites_and_next_concepts(
                    concept_ids
                )
            )
            next_concepts = concept_graph.get("next_concepts", [])

            if len(next_concepts) > 0:
                return {
                    "decision": "learn_next_concept",
                    "recommended_concept": next_concepts[0].get("concept_id"),
                    "details": (
                        "Student is almost ready; consider preparing next "
                        "concept."
                    )
                }
            else:
                # Use timestamp-based seed for true randomness
                import time
                # Use timestamp + process time for maximum randomness
                import time
                import os
                unique_seed = (
                    int(time.time() * 1000000) % 1000000 +
                    os.getpid() % 1000 +
                    int(time.perf_counter() * 1000000) % 1000
                ) % 1000000
                random.seed(unique_seed)
                # Shuffle and select a RANDOM concept from the list
                concept_ids_shuffled = concept_ids.copy()
                random.shuffle(concept_ids_shuffled)
                random.shuffle(concept_ids_shuffled)
                recommended = random.choice(concept_ids_shuffled)
                random.seed()
                return {
                    "decision": "reinforce",
                    "recommended_concept": recommended,
                    "details": (
                        "No next concept found; strengthen understanding."
                    )
                }

        # 4. High mastery → advance
        if overall == "ready":
            if not self.concept_agent:
                return {
                    "decision": "advance",
                    "recommended_concept": None,
                    "details": (
                        "No next concept found, but mastery indicates "
                        "readiness to move ahead."
                    )
                }

            concept_graph = (
                self.concept_agent.get_prerequisites_and_next_concepts(
                    concept_ids
                )
            )
            next_concepts = concept_graph.get("next_concepts", [])

            if len(next_concepts) > 0:
                return {
                    "decision": "advance",
                    "recommended_concept": next_concepts[0].get("concept_id"),
                    "details": (
                        "Student has high mastery and is ready to advance."
                    )
                }
            else:
                return {
                    "decision": "advance",
                    "recommended_concept": None,
                    "details": (
                        "No next concept found, but mastery indicates "
                        "readiness to move ahead."
                    )
                }

        # Fallback: If we have concepts but couldn't determine readiness,
        # recommend one of them (this should never be reached if concepts
        # exist)
        # Shuffle and select a RANDOM concept from the list
        if len(concept_ids) > 0:
            # Shuffle a copy to ensure random order
            concept_ids_shuffled = concept_ids.copy()
            random.shuffle(concept_ids_shuffled)
            recommended = random.choice(concept_ids_shuffled)
            logger.warning(
                f"[WARNING] Reached fallback with concepts available - "
                f"recommending {recommended}"
            )
            return {
                "decision": "learn_next_concept",
                "recommended_concept": str(recommended),
                "details": (
                    "Continue exploring concepts in this topic. "
                    "Try asking questions about the recommended concept "
                    "to deepen your understanding."
                )
            }

        # Final fallback - only if no concepts
        logger.warning(
            "[WARNING] No concepts available - returning unknown decision"
        )
        return {
            "decision": "unknown",
            "recommended_concept": None,
            "details": "Unable to determine learning path."
        }
