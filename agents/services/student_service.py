#!/usr/bin/env python3
"""
Student Service - Handles student profile retrieval
"""

import logging
import os
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class StudentService:
    """
    Handles student profile retrieval from Supabase.
    """

    def __init__(self, supabase_client, cache_get=None, cache_set=None):
        """
        Initialize StudentService.

        Args:
            supabase_client: Supabase client instance
            cache_get: Optional cache get function
            cache_set: Optional cache set function
        """
        self.supabase = supabase_client
        self.cache_get = cache_get
        self.cache_set = cache_set
        self.logger = logging.getLogger(__name__)

    def get_student_profile(self, user_id: Optional[str]) -> Dict:
        """
        Get student profile including learning style, speed, grade level,
        and subject strengths.

        Args:
            user_id: Student user ID

        Returns:
            Dict with:
            {
                "learning_style": str,
                "speed": str,
                "grade_level": str,
                "subject_strengths": List[str]
            }
            Returns defaults if user_id is None or profile not found.
        """
        if not user_id or not self.supabase:
            return self._get_default_profile()

        # Check cache first
        if self.cache_get:
            cache_key = f"student_profile:{user_id}"
            cached = self.cache_get(cache_key)
            if cached is not None:
                if os.getenv("DEBUG", "0") == "1":
                    self.logger.info(
                        f"Cache hit for student_profile:{user_id}"
                    )
                return cached

        try:
            # Query student_profiles table (or users table with profile fields)
            res = (
                self.supabase.table("student_profiles")
                .select(
                    "learning_style, speed, grade_level, subject_strengths"
                )
                .eq("user_id", user_id)
                .limit(1)
                .execute()
            )

            rows = res.data or []
            if rows and rows[0]:
                profile = {
                    "learning_style": rows[0].get(
                        "learning_style", "visual"
                    ),
                    "speed": rows[0].get("speed", "moderate"),
                    "grade_level": rows[0].get(
                        "grade_level", "intermediate"
                    ),
                    "subject_strengths": rows[0].get(
                        "subject_strengths", []
                    )
                }

                # Cache for 1 hour (3600 seconds)
                if self.cache_set:
                    cache_key = f"student_profile:{user_id}"
                    self.cache_set(cache_key, profile, ttl=3600)

                return profile

            # Fallback: try users table if student_profiles doesn't exist
            try:
                res = (
                    self.supabase.table("users")
                    .select("learning_style, speed, grade_level")
                    .eq("id", user_id)
                    .limit(1)
                    .execute()
                )

                rows = res.data or []
                if rows and rows[0]:
                    profile = {
                        "learning_style": rows[0].get(
                            "learning_style", "visual"
                        ),
                        "speed": rows[0].get("speed", "moderate"),
                        "grade_level": rows[0].get(
                            "grade_level", "intermediate"
                        ),
                        "subject_strengths": []
                    }

                    # Cache for 1 hour
                    if self.cache_set:
                        cache_key = f"student_profile:{user_id}"
                        self.cache_set(cache_key, profile, ttl=3600)

                    return profile
            except Exception:
                pass

            # No profile found, return defaults
            return self._get_default_profile()

        except Exception as e:
            self.logger.error(
                f"Error fetching student profile: {e}"
            )
            return self._get_default_profile()

    def _get_default_profile(self) -> Dict:
        """
        Return default student profile when none is found.

        Returns:
            Dict with default values
        """
        return {
            "learning_style": "visual",
            "speed": "moderate",
            "grade_level": "intermediate",
            "subject_strengths": []
        }
