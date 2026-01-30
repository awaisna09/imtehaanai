#!/usr/bin/env python3
"""
Learning Path Service - Handles learning path recommendations
Implements decision tree based on readiness levels and concept rotation
"""

import logging
from typing import Dict, List, Optional, Any
import hashlib

logger = logging.getLogger(__name__)


class LearningPathService:
    """
    Service for computing learning path recommendations based on:
    - Readiness levels (from readiness service)
    - Concept rotation (track shown concepts, recommend unseen)
    - Decision tree logic
    """
    
    def __init__(self, supabase_client):
        """
        Initialize LearningPathService.
        
        Args:
            supabase_client: Supabase client instance
        """
        self.supabase = supabase_client
        self.logger = logging.getLogger(__name__)
    
    def compute_learning_path(
        self,
        user_id: str,
        topic: str,
        subject_id: Optional[int] = None,
        readiness: Optional[Dict] = None,
        concept_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Compute learning path recommendation following the decision tree:
        
        1. If no concepts → "explore_topic"
        2. If readiness unknown → "learn_next_concept" (recommend unseen concept)
        3. If readiness = "review_prerequisites":
           - Check for prerequisite concepts
           - If found → "review_prerequisite" (recommend first prerequisite)
           - If not found → "reinforce" (recommend current concept)
        4. If readiness = "needs_reinforcement":
           - → "reinforce" (recommend current concept)
        5. If readiness = "almost_ready":
           - Check for next concepts
           - If found → "learn_next_concept" (recommend first next concept)
           - If not found → "reinforce" (recommend current concept)
        6. If readiness = "ready":
           - Check for next concepts
           - If found → "advance" (recommend first next concept)
           - If not found → "advance" (no specific concept)
        
        Uses concept rotation system to track shown concepts and recommend unseen ones.
        
        Args:
            user_id: User identifier
            topic: Topic identifier (string)
            subject_id: Optional subject ID
            readiness: Optional readiness result from readiness service
            concept_ids: Optional list of concept IDs (if not provided, will fetch from topic)
            
        Returns:
            Dict with:
            {
                "decision": str,  # explore_topic / learn_next_concept / review_prerequisite / 
                                  # reinforce / advance / unknown
                "recommended_concept": Optional[str],  # Concept ID
                "recommended_concept_name": Optional[str],  # Concept name
                "details": str
            }
        """
        try:
            # Step 1: Fetch all concepts for topic (if not provided)
            if concept_ids is None:
                concept_ids = self._fetch_concept_ids_for_topic(topic, subject_id)
            
            # Step 2: If no concepts → "explore_topic"
            if not concept_ids or len(concept_ids) == 0:
                return {
                    "decision": "explore_topic",
                    "recommended_concept": None,
                    "recommended_concept_name": None,
                    "details": "No concepts found for this topic. Continue exploring the topic and ask questions to identify key concepts."
                }
            
            # Step 3: Get readiness level
            overall_readiness = None
            if readiness:
                overall_readiness = readiness.get("overall_readiness") or readiness.get("overall")
            
            # Step 4: If readiness unknown → "learn_next_concept" (recommend unseen concept)
            if not overall_readiness or overall_readiness == "unknown":
                recommended = self._get_unseen_concept(user_id, topic, concept_ids)
                if recommended:
                    return {
                        "decision": "learn_next_concept",
                        "recommended_concept": str(recommended["concept_id"]),
                        "recommended_concept_name": recommended.get("name"),
                        "details": f"Continue exploring concepts in this topic. Try asking questions about '{recommended.get('name', 'the recommended concept')}' to deepen your understanding."
                    }
                else:
                    # All concepts shown, reset and recommend first
                    self._reset_shown_concepts(user_id, topic)
                    recommended = self._get_unseen_concept(user_id, topic, concept_ids)
                    if recommended:
                        return {
                            "decision": "learn_next_concept",
                            "recommended_concept": str(recommended["concept_id"]),
                            "recommended_concept_name": recommended.get("name"),
                            "details": f"Continue exploring concepts in this topic. Try asking questions about '{recommended.get('name', 'the recommended concept')}' to deepen your understanding."
                        }
            
            # Step 5: Decision tree based on readiness
            if overall_readiness == "review_prerequisites":
                # Check for prerequisite concepts
                prerequisite = self._get_prerequisite_concept(concept_ids, topic, subject_id)
                if prerequisite:
                    return {
                        "decision": "review_prerequisite",
                        "recommended_concept": str(prerequisite["concept_id"]),
                        "recommended_concept_name": prerequisite.get("name"),
                        "details": f"Review the prerequisite concept '{prerequisite.get('name', 'recommended concept')}' to build a stronger foundation before continuing."
                    }
                else:
                    # No prerequisites found → reinforce current concept
                    recommended = self._get_unseen_concept(user_id, topic, concept_ids) or self._get_first_concept(concept_ids)
                    return {
                        "decision": "reinforce",
                        "recommended_concept": str(recommended["concept_id"]) if recommended else None,
                        "recommended_concept_name": recommended.get("name") if recommended else None,
                        "details": "Focus on reinforcing your understanding of the current concepts before moving forward."
                    }
            
            elif overall_readiness == "needs_reinforcement":
                # → "reinforce" (recommend current concept)
                recommended = self._get_unseen_concept(user_id, topic, concept_ids) or self._get_first_concept(concept_ids)
                return {
                    "decision": "reinforce",
                    "recommended_concept": str(recommended["concept_id"]) if recommended else None,
                    "recommended_concept_name": recommended.get("name") if recommended else None,
                    "details": "Focus on reinforcing your understanding of the current concepts before moving forward."
                }
            
            elif overall_readiness == "almost_ready":
                # Check for next concepts
                next_concept = self._get_next_concept(concept_ids, topic, subject_id)
                if next_concept:
                    return {
                        "decision": "learn_next_concept",
                        "recommended_concept": str(next_concept["concept_id"]),
                        "recommended_concept_name": next_concept.get("name"),
                        "details": f"You're almost ready! Try learning about '{next_concept.get('name', 'the next concept')}' to advance your understanding."
                    }
                else:
                    # No next concepts found → reinforce
                    recommended = self._get_unseen_concept(user_id, topic, concept_ids) or self._get_first_concept(concept_ids)
                    return {
                        "decision": "reinforce",
                        "recommended_concept": str(recommended["concept_id"]) if recommended else None,
                        "recommended_concept_name": recommended.get("name") if recommended else None,
                        "details": "Focus on reinforcing your understanding of the current concepts."
                    }
            
            elif overall_readiness == "ready":
                # Check for next concepts
                next_concept = self._get_next_concept(concept_ids, topic, subject_id)
                if next_concept:
                    return {
                        "decision": "advance",
                        "recommended_concept": str(next_concept["concept_id"]),
                        "recommended_concept_name": next_concept.get("name"),
                        "details": f"You're ready to advance! Learn about '{next_concept.get('name', 'the next concept')}' to continue your progress."
                    }
                else:
                    # No next concepts → advance (no specific concept)
                    return {
                        "decision": "advance",
                        "recommended_concept": None,
                        "recommended_concept_name": None,
                        "details": "You're ready to advance! Continue exploring new concepts in this topic or move to the next topic."
                    }
            
            # Fallback: unknown readiness
            recommended = self._get_unseen_concept(user_id, topic, concept_ids)
            if recommended:
                return {
                    "decision": "learn_next_concept",
                    "recommended_concept": str(recommended["concept_id"]),
                    "recommended_concept_name": recommended.get("name"),
                    "details": f"Continue exploring concepts in this topic. Try asking questions about '{recommended.get('name', 'the recommended concept')}' to deepen your understanding."
                }
            
            return {
                "decision": "unknown",
                "recommended_concept": None,
                "recommended_concept_name": None,
                "details": "Unable to determine learning path."
            }
            
        except Exception as e:
            self.logger.error(f"[LEARNING PATH] Error computing learning path: {e}")
            import traceback
            self.logger.error(f"[LEARNING PATH] Traceback: {traceback.format_exc()}")
            return {
                "decision": "unknown",
                "recommended_concept": None,
                "recommended_concept_name": None,
                "details": f"Error computing learning path: {str(e)}"
            }
    
    def _fetch_concept_ids_for_topic(self, topic_id: str, subject_id: Optional[int]) -> List[int]:
        """Fetch concept IDs for a topic"""
        try:
            from agents.concept_agent import ConceptAgent
            import os
            api_key = os.getenv("OPENAI_API_KEY")
            concept_agent = ConceptAgent(api_key=api_key, supabase_client=self.supabase)
            concepts = concept_agent.fetch_concepts_by_topic(
                topic_id=topic_id,
                limit=10,
                random_order=False,  # Get in consistent order
                subject_id=subject_id
            )
            return [int(c.get("concept_id")) for c in concepts if c.get("concept_id")]
        except Exception as e:
            self.logger.error(f"[LEARNING PATH] Error fetching concepts: {e}")
            return []
    
    def _get_unseen_concept(self, user_id: str, topic: str, concept_ids: List[int]) -> Optional[Dict]:
        """
        Get first unseen concept using concept rotation system.
        Tracks shown concepts: shown_concepts:{user_id}:{topic_id}
        """
        try:
            # Get shown concepts from cache or database
            cache_key = f"shown_concepts:{user_id}:{topic}"
            shown_concept_ids = self._get_shown_concepts(user_id, topic)
            
            # Find unseen concepts
            unseen_ids = [cid for cid in concept_ids if cid not in shown_concept_ids]
            
            if unseen_ids:
                # Get concept details for first unseen
                recommended_id = unseen_ids[0]
                concept_details = self._get_concept_details(recommended_id)
                
                # Mark as shown
                self._mark_concept_shown(user_id, topic, recommended_id)
                
                return concept_details
            
            # All concepts shown
            return None
            
        except Exception as e:
            self.logger.error(f"[LEARNING PATH] Error getting unseen concept: {e}")
            return None
    
    def _get_shown_concepts(self, user_id: str, topic: str) -> List[int]:
        """Get list of shown concept IDs for user/topic"""
        try:
            # Try cache first
            try:
                from cache import cache_get
                cache_key = f"shown_concepts:{user_id}:{topic}"
                cached = cache_get(cache_key)
                if cached:
                    return cached
            except ImportError:
                pass
            
            # Try database (if we have a table for this)
            # For now, return empty list (no concepts shown yet)
            return []
            
        except Exception as e:
            self.logger.warning(f"[LEARNING PATH] Error getting shown concepts: {e}")
            return []
    
    def _mark_concept_shown(self, user_id: str, topic: str, concept_id: int):
        """Mark a concept as shown for user/topic"""
        try:
            shown = self._get_shown_concepts(user_id, topic)
            if concept_id not in shown:
                shown.append(concept_id)
                
                # Update cache
                try:
                    from cache import cache_set
                    cache_key = f"shown_concepts:{user_id}:{topic}"
                    cache_set(cache_key, shown, ttl=86400)  # 24 hours
                except ImportError:
                    pass
        except Exception as e:
            self.logger.warning(f"[LEARNING PATH] Error marking concept shown: {e}")
    
    def _reset_shown_concepts(self, user_id: str, topic: str):
        """Reset shown concepts when all concepts have been shown"""
        try:
            try:
                from cache import cache_set
                cache_key = f"shown_concepts:{user_id}:{topic}"
                cache_set(cache_key, [], ttl=86400)
            except ImportError:
                pass
        except Exception as e:
            self.logger.warning(f"[LEARNING PATH] Error resetting shown concepts: {e}")
    
    def _get_concept_details(self, concept_id: int) -> Optional[Dict]:
        """Get concept details by ID"""
        try:
            if not self.supabase:
                return None
            
            # Determine table name based on concept_id (would need subject_id, but use main table for now)
            result = self.supabase.table("concepts").select("concept_id, concept, explanation").eq("concept_id", concept_id).limit(1).execute()
            
            if result.data and len(result.data) > 0:
                row = result.data[0]
                return {
                    "concept_id": row.get("concept_id"),
                    "name": row.get("concept"),
                    "description": row.get("explanation")
                }
            return None
            
        except Exception as e:
            self.logger.warning(f"[LEARNING PATH] Error getting concept details: {e}")
            return None
    
    def _get_first_concept(self, concept_ids: List[int]) -> Optional[Dict]:
        """Get first concept from list"""
        if concept_ids:
            return self._get_concept_details(concept_ids[0])
        return None
    
    def _get_prerequisite_concept(self, concept_ids: List[int], topic: str, subject_id: Optional[int]) -> Optional[Dict]:
        """Get prerequisite concept (simplified - would need concept graph)"""
        # For now, return None (no prerequisite logic implemented)
        # This would require a concept graph with prerequisite relationships
        return None
    
    def _get_next_concept(self, concept_ids: List[int], topic: str, subject_id: Optional[int]) -> Optional[Dict]:
        """Get next concept (simplified - would need concept graph)"""
        # For now, return first unseen concept
        # This would require a concept graph with next concept relationships
        return None
