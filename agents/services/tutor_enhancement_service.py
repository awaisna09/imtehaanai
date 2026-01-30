"""
Tutor Enhancement Service

This module handles mastery, readiness, and learning path computation
for tutor conversations. It is used exclusively by workers processing
tutor_enhance jobs, not by the synchronous API path.

The service reads conversation and message data independently and computes
enhancements without requiring the original tutor response generation context.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class TutorEnhancementService:
    """
    Service for computing tutor conversation enhancements:
    - Mastery updates based on conversation quality
    - Readiness assessment for concepts
    - Learning path recommendations
    
    This service is designed to run independently in workers,
    reading data from the database rather than requiring
    the original tutor response context.
    """
    
    def __init__(self, supabase_client):
        """
        Initialize the enhancement service.
        
        Args:
            supabase_client: Supabase client instance
        """
        self.supabase = supabase_client
        
        # Import services lazily to avoid circular dependencies
        self._mastery_service = None
        self._readiness_service = None
        self._learning_path_service = None
        self._concept_service = None
    
    def _get_mastery_service(self):
        """Lazy load mastery service"""
        if self._mastery_service is None:
            from agents.services.mastery_service import MasteryService
            from agents.mastery_agent import MasteryAgent
            import os
            api_key = os.getenv("OPENAI_API_KEY")
            mastery_agent = MasteryAgent(api_key=api_key, supabase_client=self.supabase)
            self._mastery_service = MasteryService(mastery_agent)
        return self._mastery_service
    
    def _get_readiness_service(self):
        """Lazy load readiness service"""
        if self._readiness_service is None:
            from agents.services.readiness_service import ReadinessService
            from agents.readiness_agent import ReadinessAgent
            # Get concept_agent for readiness agent
            concept_agent = None
            if self._concept_service:
                concept_agent = self._concept_service.concept_agent
            else:
                # Initialize concept agent if not already done
                from agents.concept_agent import ConceptAgent
                import os
                api_key = os.getenv("OPENAI_API_KEY")
                concept_agent = ConceptAgent(api_key=api_key, supabase_client=self.supabase)
            readiness_agent = ReadinessAgent(supabase_client=self.supabase, concept_agent=concept_agent)
            self._readiness_service = ReadinessService(readiness_agent)
        return self._readiness_service
    
    def _get_learning_path_service(self):
        """Lazy load learning path service"""
        if self._learning_path_service is None:
            from agents.services.learning_path_service import LearningPathService
            self._learning_path_service = LearningPathService(self.supabase)
        return self._learning_path_service
    
    def _get_concept_service(self):
        """Lazy load concept service"""
        if self._concept_service is None:
            from agents.services.concept_service import ConceptService
            from agents.concept_agent import ConceptAgent
            import os
            # Initialize ConceptAgent first, then wrap it in ConceptService
            api_key = os.getenv("OPENAI_API_KEY")
            concept_agent = ConceptAgent(api_key=api_key, supabase_client=self.supabase)
            self._concept_service = ConceptService(concept_agent=concept_agent)
        return self._concept_service
    
    def read_conversation_data(
        self,
        conversation_id: str,
        assistant_message_id: Optional[str] = None,
        user_message_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Read conversation and message data from the database.
        
        This method fetches:
        - Assistant message (concept_ids, reasoning_label if available)
        - User message (for context)
        - Recent conversation history
        
        Args:
            conversation_id: Conversation identifier
            assistant_message_id: Optional assistant message ID
            user_message_id: Optional user message ID
            
        Returns:
            Dict containing:
                - concept_ids: List of concept IDs from assistant message
                - reasoning_label: Reasoning label if available
                - topic_id: Topic ID from conversation
                - user_id: User ID from conversation
                - subject_id: Subject ID if available
        """
        result = {
            "concept_ids": [],
            "reasoning_label": "neutral",
            "topic_id": None,
            "user_id": None,
            "subject_id": None
        }
        
        try:
            # Fetch assistant message if message_id provided
            if assistant_message_id:
                try:
                    message_result = self.supabase.table("tutor_messages").select(
                        "concept_ids, message_text, topic, user_id, subject_id"
                    ).eq("message_id", assistant_message_id).execute()
                    
                    if message_result.data and len(message_result.data) > 0:
                        message_data = message_result.data[0]
                        concept_ids_raw = message_data.get("concept_ids", [])
                        result["concept_ids"] = [
                            int(cid) for cid in concept_ids_raw 
                            if cid and str(cid).strip()
                        ]
                        result["topic_id"] = message_data.get("topic")
                        result["user_id"] = message_data.get("user_id")
                        result["subject_id"] = message_data.get("subject_id")
                        
                        # Try to infer reasoning_label from message content
                        # For now, default to neutral (could be enhanced with lightweight classifier)
                        result["reasoning_label"] = "neutral"
                        
                        logger.debug(
                            f"Read assistant message: "
                            f"concept_ids={len(result['concept_ids'])}, "
                            f"topic_id={result['topic_id']}"
                        )
                except Exception as e:
                    logger.warning(f"Could not fetch assistant message: {e}")
            
            # If no concept_ids from assistant message, try to get from conversation
            if not result["concept_ids"] and conversation_id:
                try:
                    # Fetch most recent assistant message from conversation
                    recent_result = self.supabase.table("tutor_messages").select(
                        "concept_ids, topic, user_id, subject_id"
                    ).eq("conversation_id", conversation_id).eq(
                        "role", "assistant"
                    ).order("created_at", desc=True).limit(1).execute()
                    
                    if recent_result.data and len(recent_result.data) > 0:
                        message_data = recent_result.data[0]
                        concept_ids_raw = message_data.get("concept_ids", [])
                        result["concept_ids"] = [
                            int(cid) for cid in concept_ids_raw 
                            if cid and str(cid).strip()
                        ]
                        if not result["topic_id"]:
                            result["topic_id"] = message_data.get("topic")
                        if not result["user_id"]:
                            result["user_id"] = message_data.get("user_id")
                        if not result["subject_id"]:
                            result["subject_id"] = message_data.get("subject_id")
                except Exception as e:
                    logger.warning(f"Could not fetch recent assistant message: {e}")
            
        except Exception as e:
            logger.error(f"Error reading conversation data: {e}")
        
        return result
    
    def compute_enhancements(
        self,
        user_id: str,
        topic_id: str,
        subject_id: Optional[int],
        concept_ids: List[int],
        reasoning_label: str = "neutral",
        conversation_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Compute all tutor enhancements: mastery, readiness, and learning path.
        
        This is the main entry point for enhancement computation.
        It runs all three computations independently and returns results.
        
        Args:
            user_id: User identifier
            topic_id: Topic identifier
            subject_id: Subject identifier
            concept_ids: List of concept IDs to process
            reasoning_label: Reasoning label for mastery computation
            conversation_id: Optional conversation ID for context
            
        Returns:
            Dict containing:
                - mastery_updates: List of mastery updates applied
                - readiness: Readiness assessment result
                - learning_path: Learning path recommendation
                - concept_ids: Processed concept IDs
                - reasoning_label: Reasoning label used
        """
        result = {
            "mastery_updates": [],
            "readiness": None,
            "learning_path": None,
            "concept_ids": concept_ids,
            "reasoning_label": reasoning_label
        }
        
        # concept_ids should already be populated from the concepts table
        # This is just a safety check and logging
        if not concept_ids:
            logger.warning(f"[ENHANCEMENT] No concept_ids provided - mastery and readiness will be skipped, correlation_id: {correlation_id}")
        
        # 1. Update mastery
        if concept_ids:
            try:
                mastery_service = self._get_mastery_service()
                delta = mastery_service.label_to_delta(reasoning_label)
                updates = [{
                    "concept_id": cid,
                    "delta": delta,
                    "reason": f"tutor_chat_{reasoning_label}"
                } for cid in concept_ids]
                
                if updates:
                    mastery_updates = mastery_service.apply_mastery_updates(
                        user_id=user_id,
                        updates=updates,
                        subject_id=subject_id
                    )
                    result["mastery_updates"] = mastery_updates
                    logger.info(f"[ENHANCEMENT] Mastery updates applied: {len(mastery_updates)} updates, correlation_id: {correlation_id}")
            except Exception as e:
                logger.error(f"[ENHANCEMENT] Mastery update failed (non-fatal): {e}, correlation_id: {correlation_id}")
                # Continue with other computations
        
        # 2. Compute readiness
        readiness = None
        if concept_ids:
            try:
                readiness_service = self._get_readiness_service()
                readiness = readiness_service.compute_readiness_signal(
                    user_id=user_id,
                    concept_ids=[str(cid) for cid in concept_ids],  # Convert to strings
                    mastery_updates=result.get("mastery_updates", []),
                    subject_id=subject_id
                )
                result["readiness"] = readiness
                logger.info(f"[ENHANCEMENT] Readiness computed, correlation_id: {correlation_id}")
            except Exception as e:
                logger.warning(f"[ENHANCEMENT] Readiness computation failed (non-fatal): {e}, correlation_id: {correlation_id}")
        
        # 3. Compute learning path (uses readiness result if available)
        try:
            learning_path_service = self._get_learning_path_service()
            learning_path = learning_path_service.compute_learning_path(
                user_id=user_id,
                topic=topic_id,
                subject_id=subject_id,
                readiness=readiness,  # Pass readiness result
                concept_ids=concept_ids  # Pass concept IDs
            )
            result["learning_path"] = learning_path
            logger.info(f"[ENHANCEMENT] Learning path computed, correlation_id: {correlation_id}")
        except Exception as e:
            logger.warning(f"[ENHANCEMENT] Learning path computation failed (non-fatal): {e}, correlation_id: {correlation_id}")
        
        return result
    
    def process_enhancement_job(
        self,
        user_id: str,
        conversation_id: str,
        topic_id: str,
        subject_id: Optional[int],
        assistant_message_id: Optional[str] = None,
        user_message_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process a complete enhancement job.
        
        This method:
        1. Reads conversation and message data from DB
        2. Computes all enhancements (mastery, readiness, learning_path)
        3. Stores results (if needed)
        4. Returns enhancement results
        
        Args:
            user_id: User identifier
            conversation_id: Conversation identifier
            topic_id: Topic identifier
            subject_id: Subject identifier
            assistant_message_id: Optional assistant message ID
            user_message_id: Optional user message ID
            correlation_id: Optional correlation ID for tracing
            
        Returns:
            Dict containing all enhancement results
        """
        # Always fetch concepts directly from concepts table using topic_id and subject_id
        # This is the source of truth - no need to read from messages/conversations
        logger.info(f"[ENHANCEMENT] Fetching concepts for topic_id={topic_id}, subject_id={subject_id}, correlation_id={correlation_id}")
        
        concept_ids = []
        try:
            concept_service = self._get_concept_service()
            logger.info(f"[ENHANCEMENT] ConceptService initialized: {concept_service is not None}, correlation_id: {correlation_id}")
            
            # Use fetch_concepts_by_topic which handles subject-specific tables
            # Ensure topic_id is a string (ConceptAgent expects string)
            topic_id_str = str(topic_id) if topic_id else None
            logger.info(f"[ENHANCEMENT] Calling fetch_concepts_by_topic with topic_id={topic_id_str} (type: {type(topic_id_str)}), subject_id={subject_id}, limit=10, correlation_id: {correlation_id}")
            concept_rows = concept_service.fetch_concepts_by_topic(
                topic_id=topic_id_str, 
                limit=10,
                subject_id=subject_id
            )
            logger.info(f"[ENHANCEMENT] fetch_concepts_by_topic returned: type={type(concept_rows)}, length={len(concept_rows) if concept_rows else 0}, correlation_id: {correlation_id}")
            
            if concept_rows:
                logger.info(f"[ENHANCEMENT] First concept row sample: {concept_rows[0] if len(concept_rows) > 0 else 'None'}, correlation_id: {correlation_id}")
            
            # Extract concept_ids from concept_rows
            # Handle both dict format (from ConceptService) and direct format
            concept_ids = []
            if concept_rows:
                for row in concept_rows:
                    concept_id = row.get("concept_id")
                    if concept_id is not None:
                        try:
                            concept_ids.append(int(concept_id))
                        except (ValueError, TypeError) as e:
                            logger.warning(f"[ENHANCEMENT] Skipping invalid concept_id: {concept_id}, error: {e}, correlation_id: {correlation_id}")
            
            logger.info(f"[ENHANCEMENT] Fetched {len(concept_ids)} concepts from concepts table for topic {topic_id}, subject_id={subject_id}, correlation_id: {correlation_id}")
            if len(concept_ids) == 0:
                logger.warning(f"[ENHANCEMENT] NO CONCEPTS FOUND - This will cause empty mastery/readiness. Topic: {topic_id}, Subject: {subject_id}, correlation_id: {correlation_id}")
                logger.warning(f"[ENHANCEMENT] Concept rows returned: {len(concept_rows) if concept_rows else 0}, Raw rows: {concept_rows[:3] if concept_rows else 'None'}")
                logger.warning(f"[ENHANCEMENT] Concept rows type: {type(concept_rows)}, Is list: {isinstance(concept_rows, list)}, correlation_id: {correlation_id}")
                # Try direct query as fallback (bypass ConceptService to avoid caching issues)
                try:
                    from agents.concept_agent import ConceptAgent
                    import os
                    api_key = os.getenv("OPENAI_API_KEY")
                    direct_agent = ConceptAgent(api_key=api_key, supabase_client=self.supabase)
                    direct_concepts = direct_agent.fetch_concepts_by_topic(
                        topic_id_str, 
                        limit=10, 
                        random_order=False,  # Consistent order
                        subject_id=subject_id
                    )
                    logger.info(f"[ENHANCEMENT] Direct agent query returned {len(direct_concepts) if direct_concepts else 0} concepts")
                    if direct_concepts:
                        concept_ids = [int(c.get("concept_id")) for c in direct_concepts if c.get("concept_id")]
                        logger.info(f"[ENHANCEMENT] Using direct query result: {len(concept_ids)} concept_ids")
                        # Update concept_rows for consistency
                        concept_rows = direct_concepts
                except Exception as direct_e:
                    logger.error(f"[ENHANCEMENT] Direct query fallback also failed: {direct_e}, correlation_id: {correlation_id}")
                    import traceback
                    logger.error(f"[ENHANCEMENT] Direct query traceback: {traceback.format_exc()}")
        except Exception as e:
            logger.error(f"[ENHANCEMENT] Failed to fetch concepts for topic {topic_id}: {e}, correlation_id: {correlation_id}")
            import traceback
            logger.error(f"[ENHANCEMENT] Traceback: {traceback.format_exc()}")
        
        # Get reasoning_label from conversation data if available, otherwise default to "neutral"
        reasoning_label = "neutral"
        try:
            conversation_data = self.read_conversation_data(
                conversation_id=conversation_id,
                assistant_message_id=assistant_message_id,
                user_message_id=user_message_id
            )
            reasoning_label = conversation_data.get("reasoning_label", "neutral")
            logger.info(f"[ENHANCEMENT] Reasoning label from conversation: {reasoning_label}, correlation_id: {correlation_id}")
        except Exception as e:
            logger.warning(f"[ENHANCEMENT] Could not read conversation data for reasoning_label (using default): {e}, correlation_id: {correlation_id}")
        
        logger.info(f"[ENHANCEMENT] Final parameters: user_id={user_id}, topic_id={topic_id}, subject_id={subject_id}, concept_ids_count={len(concept_ids)}, reasoning_label={reasoning_label}, correlation_id={correlation_id}")
        
        # Compute enhancements
        enhancements = self.compute_enhancements(
            user_id=user_id,
            topic_id=topic_id,
            subject_id=subject_id,
            concept_ids=concept_ids,
            reasoning_label=reasoning_label,
            conversation_id=conversation_id,
            correlation_id=correlation_id
        )
        
        # Add analytics metadata
        enhancements["analytics"] = {
            "concepts_covered": len(concept_ids),
            "timestamp": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id
        }
        
        # Store results in database (optional - could store in a tutor_enhancements table)
        # For now, results are stored in job result and can be queried via job_id
        # Mastery updates are already persisted to student_mastery table via MasteryService
        
        # Optionally store enhancement metadata in a dedicated table
        # This is optional since results are also stored in job result
        try:
            # Store enhancement summary in tutor_messages or a dedicated table if needed
            # For now, we rely on job result storage which is sufficient
            pass
        except Exception as e:
            logger.warning(f"Could not store enhancement metadata in DB (non-fatal): {e}")
            # Non-critical - job result storage is sufficient
        
        return enhancements
