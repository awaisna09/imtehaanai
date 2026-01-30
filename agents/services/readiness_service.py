#!/usr/bin/env python3
"""
Readiness Service - Handles student readiness assessment
Wraps ReadinessAgent internally.
"""

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ReadinessService:
    """
    Computes readiness scores and next learning step decisions.
    Wraps ReadinessAgent internally.
    """

    def __init__(self, readiness_agent):
        """
        Initialize ReadinessService.

        Args:
            readiness_agent: ReadinessAgent instance to wrap
        """
        self.readiness_agent = readiness_agent
        self.logger = logging.getLogger(__name__)

    def classify_readiness(self, mastery_score: int) -> str:
        """
        Given a mastery score (0–100), classify readiness level.
        Delegates to ReadinessAgent.

        Args:
            mastery_score: Mastery score from 0 to 100

        Returns:
            str: 'ready', 'almost_ready', 'needs_reinforcement',
                 or 'review_prerequisites'
        """
        if not self.readiness_agent:
            return "unknown"
        return self.readiness_agent.classify_readiness(mastery_score)

    def compute_readiness_signal(
        self,
        user_id: Optional[str],
        concept_ids: List[str],
        mastery_updates: Optional[List[Dict]] = None,
        subject: Optional[str] = None,
        subject_id: Optional[int] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> Dict:
        """
        Compute the user's readiness level for the detected concepts.
        Delegates to ReadinessAgent.compute_readiness().

        Args:
            user_id: User ID to compute readiness for
            concept_ids: List of concept IDs to check readiness for
            mastery_updates: Optional list of mastery updates from current
                           session to apply before calculating readiness
            subject: Optional subject name (e.g., "Business Studies",
                    "Economics") for context/filtering if needed in future
            subject_id: Optional subject ID (e.g., 101, 119) for
                       context/filtering if needed in future
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for instrumentation

        Returns:
            Dict with:
            {
                "overall_readiness": str,
                "concept_readiness": [
                    { "concept_id": str, "mastery": int, "readiness": str }
                ],
                "average_mastery": float,
                "min_mastery": int
            }
        """
        if not self.readiness_agent:
            return {
                "overall_readiness": "unknown",
                "concept_readiness": [],
                "average_mastery": None,
                "min_mastery": None
            }
        # Note: ReadinessAgent.compute_readiness() may not currently
        # accept subject params, but we're adding them here for future
        # extensibility
        return self.readiness_agent.compute_readiness(
            user_id, concept_ids, mastery_updates=mastery_updates,
            job_id=job_id, trace_id=trace_id
        )

    def compute_next_learning_step(
        self,
        readiness_result: Dict,
        concept_ids: List[str]
    ) -> Dict:
        """
        Given readiness, mastery, and concept graph,
        determine the student's next recommended step.
        Delegates to ReadinessAgent.compute_next_step().

        Args:
            readiness_result: Result from compute_readiness_signal()
            concept_ids: List of concept IDs

        Returns:
            Dict with:
            {
                "decision": str,  # advance / reinforce / review_prerequisite /
                                  # learn_next_concept / unknown
                "recommended_concept": Optional[str],
                "details": str
            }
        """
        if not self.readiness_agent:
            return {
                "decision": "unknown",
                "recommended_concept": None,
                "details": "Not enough information to determine next step."
            }
        return self.readiness_agent.compute_next_step(
            readiness_result, concept_ids
        )
