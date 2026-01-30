"""
Redis-based Rate Limiting Service
Per-user rate limiting based on authenticated user identity
Different limits for different categories of AI work
"""

import os
import time
from typing import Dict, Optional, Tuple, List, Any
from enum import Enum
from datetime import timedelta
from dotenv import load_dotenv

from services.redis_connection import get_redis_client

load_dotenv('config.env')


class RateLimitCategory(str, Enum):
    """Rate limit categories for different AI work types"""
    TUTOR_CHAT = "tutor_chat"
    ANSWER_GRADING = "answer_grading"
    MOCK_EXAM_GRADING = "mock_exam_grading"
    CONCEPT_EXPLANATION = "concept_explanation"
    LESSON_CREATION = "lesson_creation"
    
    # Aggregate categories
    ALL_AI_WORK = "all_ai_work"


class RateLimitConfig:
    """Rate limit configuration for each category"""
    
    # Default limits (requests per window)
    DEFAULT_LIMITS = {
        RateLimitCategory.TUTOR_CHAT: {
            "requests": 100,  # 100 tutor chat requests
            "window": 3600,   # per hour
        },
        RateLimitCategory.ANSWER_GRADING: {
            "requests": 200,  # 200 grading requests
            "window": 3600,   # per hour
        },
        RateLimitCategory.MOCK_EXAM_GRADING: {
            "requests": 20,   # 20 exam gradings
            "window": 3600,   # per hour
        },
        RateLimitCategory.CONCEPT_EXPLANATION: {
            "requests": 500,  # 500 explanations
            "window": 3600,   # per hour
        },
        RateLimitCategory.LESSON_CREATION: {
            "requests": 50,   # 50 lesson creations
            "window": 3600,   # per hour
        },
        RateLimitCategory.ALL_AI_WORK: {
            "requests": 1000,  # 1000 total AI requests
            "window": 3600,    # per hour
        },
    }
    
    @classmethod
    def get_limit(cls, category: RateLimitCategory, user_tier: str = "standard") -> Dict[str, int]:
        """
        Get rate limit for category and user tier (PERMANENT ENFORCEMENT)
        Environment variables override defaults if provided
        Conservative tier multipliers to prevent abuse
        
        Args:
            category: Rate limit category
            user_tier: User tier (standard, premium, admin)
        
        Returns:
            Dict with 'requests' and 'window' keys
        """
        # Try to get from environment first (allows configuration without code changes)
        env_limit = cls.get_from_env(category)
        if env_limit.get("requests", 0) > 0:
            base_limit = env_limit
        else:
            base_limit = cls.DEFAULT_LIMITS.get(category, {"requests": 100, "window": 3600})
        
        # Apply tier multipliers (conservative multipliers to prevent abuse)
        tier_multipliers = {
            "standard": 1.0,   # Standard users: base limits
            "premium": 1.5,    # Premium users: 1.5x limits (conservative, prevents abuse)
            "admin": 3.0,      # Admins: 3x limits (conservative, prevents accidental overload)
        }
        
        multiplier = tier_multipliers.get(user_tier, 1.0)
        
        return {
            "requests": int(base_limit["requests"] * multiplier),
            "window": base_limit["window"]
        }
    
    @classmethod
    def get_from_env(cls, category: RateLimitCategory) -> Dict[str, int]:
        """
        Get rate limit from environment variables
        
        Format: RATE_LIMIT_<CATEGORY>_REQUESTS and RATE_LIMIT_<CATEGORY>_WINDOW
        Example: RATE_LIMIT_TUTOR_CHAT_REQUESTS=100, RATE_LIMIT_TUTOR_CHAT_WINDOW=3600
        """
        category_key = category.value.upper()
        requests = int(os.getenv(f"RATE_LIMIT_{category_key}_REQUESTS", 0))
        window = int(os.getenv(f"RATE_LIMIT_{category_key}_WINDOW", 0))
        
        if requests > 0 and window > 0:
            return {"requests": requests, "window": window}
        
        return cls.DEFAULT_LIMITS.get(category, {"requests": 100, "window": 3600})


class RateLimiter:
    """Redis-based rate limiter using sliding window log algorithm"""
    
    def __init__(self):
        self.redis = get_redis_client()
        self.key_prefix = "rate_limit:"
    
    def _get_key(self, user_id: str, category: RateLimitCategory) -> str:
        """Generate Redis key for user and category"""
        return f"{self.key_prefix}{category.value}:{user_id}"
    
    def _get_global_key(self, user_id: str) -> str:
        """Generate Redis key for global user limit"""
        return f"{self.key_prefix}global:{user_id}"
    
    def check_rate_limit(
        self,
        user_id: str,
        category: RateLimitCategory,
        user_tier: str = "standard",
        check_queue_back_pressure: bool = True
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if user has exceeded rate limit for category (PERMANENT ENFORCEMENT)
        Also checks queue back-pressure to prevent uncontrolled job growth
        
        Args:
            user_id: Authenticated user ID (REQUIRED - no anonymous users allowed)
            category: Rate limit category
            user_tier: User tier (standard, premium, admin)
            check_queue_back_pressure: If True, also check queue depth (default: True)
        
        Returns:
            Tuple of (allowed: bool, info: dict)
            info contains:
                - allowed: bool
                - remaining: int (remaining requests)
                - reset_at: float (timestamp when limit resets)
                - limit: int (total limit)
                - queue_back_pressure: bool (if queue is under back-pressure)
                - queue_depth: int (current queue depth)
        
        Raises:
            ValueError: If user_id is missing or anonymous (strict enforcement)
        """
        # PERMANENT ENFORCEMENT: Require authenticated user (no anonymous users)
        if not user_id or user_id == "anonymous":
            raise ValueError(
                "Rate limiting requires authenticated user identity. "
                "Anonymous users are not allowed for AI operations."
            )
        
        # Get rate limit config
        limit_config = RateLimitConfig.get_limit(category, user_tier)
        requests_limit = limit_config["requests"]
        window_seconds = limit_config["window"]
        
        # Get current timestamp
        now = time.time()
        window_start = now - window_seconds
        
        # Redis key for this user and category
        key = self._get_key(user_id, category)
        
        # PERMANENT ENFORCEMENT: Fail closed (reject on error) - no fail-open behavior
        try:
            # Remove entries outside the window (older than window_start)
            self.redis.zremrangebyscore(key, 0, window_start)
            
            # Count current requests in window
            current_count = self.redis.zcard(key)
            
            # Check if limit exceeded
            rate_limit_allowed = current_count < requests_limit
            
            # Check queue back-pressure if enabled (prevents uncontrolled job growth)
            queue_back_pressure = False
            queue_depth = 0
            queue_name = None
            
            if check_queue_back_pressure:
                try:
                    from services.job_queue import (
                        job_queue, QUEUE_TUTOR, QUEUE_GRADING, QUEUE_MOCK_EXAM,
                        QUEUE_HELPING, QUEUE_LESSON
                    )
                    from services.job_queue import MAX_QUEUE_SIZE
                    import os
                    
                    # Map category to queue name
                    category_to_queue = {
                        RateLimitCategory.TUTOR_CHAT: QUEUE_TUTOR,
                        RateLimitCategory.ANSWER_GRADING: QUEUE_GRADING,
                        RateLimitCategory.MOCK_EXAM_GRADING: QUEUE_MOCK_EXAM,
                        RateLimitCategory.CONCEPT_EXPLANATION: QUEUE_HELPING,
                        RateLimitCategory.LESSON_CREATION: QUEUE_LESSON,
                    }
                    
                    queue_name = category_to_queue.get(category)
                    if queue_name:
                        queue_depth = job_queue.get_queue_length(queue_name)
                        back_pressure_threshold = MAX_QUEUE_SIZE * float(
                            os.getenv("QUEUE_BACK_PRESSURE_THRESHOLD", "0.8")
                        )
                        queue_back_pressure = queue_depth >= back_pressure_threshold
                        
                        # If queue is under back-pressure, apply stricter rate limiting
                        # Reduce effective rate limit by 50% when queue is under pressure
                        if queue_back_pressure:
                            effective_limit = int(requests_limit * 0.5)
                            rate_limit_allowed = current_count < effective_limit
                except Exception as e:
                    # If queue check fails, still enforce rate limit but log error
                    print(f"⚠️ Queue back-pressure check error: {e}")
                    # Continue with rate limit check only
            
            # Final decision: must pass both rate limit AND queue back-pressure check
            # When queue is under back-pressure, apply stricter rate limiting (50% reduction)
            # The effective_limit and rate_limit_allowed are already calculated above if back-pressure is active
            allowed = rate_limit_allowed
            
            if allowed:
                # Add current request to log
                self.redis.zadd(key, {str(now): now})
                # Set expiration on key (window + 1 second buffer)
                self.redis.expire(key, window_seconds + 1)
            
            # Calculate remaining requests
            if queue_back_pressure:
                # When under back-pressure, remaining is based on reduced limit
                effective_limit = int(requests_limit * 0.5)
                remaining = max(0, effective_limit - current_count - (1 if allowed else 0))
            else:
                remaining = max(0, requests_limit - current_count - (1 if allowed else 0))
            
            # Calculate reset time (oldest entry + window)
            oldest_entry = self.redis.zrange(key, 0, 0, withscores=True)
            if oldest_entry:
                reset_at = oldest_entry[0][1] + window_seconds
            else:
                reset_at = now + window_seconds
            
            return allowed, {
                "allowed": allowed,
                "remaining": remaining,
                "reset_at": reset_at,
                "limit": requests_limit,
                "current": current_count + (1 if allowed else 0),
                "queue_back_pressure": queue_back_pressure,
                "queue_depth": queue_depth,
                "queue_name": queue_name
            }
            
        except Exception as e:
            # PERMANENT ENFORCEMENT: Fail closed - reject on error (no fail-open)
            print(f"❌ Rate limit check error (FAIL CLOSED): {e}")
            raise RuntimeError(
                f"Rate limiting service unavailable. Request rejected for safety. "
                f"Error: {str(e)}"
            )
    
    def check_global_rate_limit(
        self,
        user_id: str,
        user_tier: str = "standard",
        check_queue_back_pressure: bool = True
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check global rate limit across all AI work categories (PERMANENT ENFORCEMENT)
        
        Args:
            user_id: Authenticated user ID (REQUIRED)
            user_tier: User tier (standard, premium, admin)
            check_queue_back_pressure: If True, also check queue depth (default: True)
        
        Returns:
            Tuple of (allowed: bool, info: dict)
        """
        return self.check_rate_limit(
            user_id,
            RateLimitCategory.ALL_AI_WORK,
            user_tier,
            check_queue_back_pressure
        )
    
    def check_multiple_limits(
        self,
        user_id: str,
        categories: List[RateLimitCategory],
        user_tier: str = "standard",
        check_queue_back_pressure: bool = True
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check rate limits for multiple categories (PERMANENT ENFORCEMENT)
        
        Args:
            user_id: Authenticated user ID (REQUIRED - no anonymous users)
            categories: List of categories to check
            user_tier: User tier (standard, premium, admin)
            check_queue_back_pressure: If True, also check queue depth (default: True)
        
        Returns:
            Tuple of (allowed: bool, info: dict)
            info contains results for each category and global limit
        
        Raises:
            ValueError: If user_id is missing or anonymous
        """
        # PERMANENT ENFORCEMENT: Require authenticated user
        if not user_id or user_id == "anonymous":
            raise ValueError(
                "Rate limiting requires authenticated user identity. "
                "Anonymous users are not allowed for AI operations."
            )
        
        results = {}
        all_allowed = True
        
        # Check each category
        for category in categories:
            allowed, info = self.check_rate_limit(
                user_id, category, user_tier, check_queue_back_pressure
            )
            results[category.value] = info
            if not allowed:
                all_allowed = False
        
        # Check global limit
        global_allowed, global_info = self.check_global_rate_limit(
            user_id, user_tier, check_queue_back_pressure
        )
        results["global"] = global_info
        if not global_allowed:
            all_allowed = False
        
        return all_allowed, {
            "allowed": all_allowed,
            "categories": results
        }
    
    def get_rate_limit_status(
        self,
        user_id: str,
        category: RateLimitCategory,
        user_tier: str = "standard"
    ) -> Dict[str, Any]:
        """
        Get current rate limit status without incrementing counter (PERMANENT ENFORCEMENT)
        
        Args:
            user_id: Authenticated user ID (REQUIRED - no anonymous users)
            category: Rate limit category
            user_tier: User tier (standard, premium, admin)
        
        Returns:
            Dict with rate limit status
        
        Raises:
            ValueError: If user_id is missing or anonymous
        """
        # PERMANENT ENFORCEMENT: Require authenticated user
        if not user_id or user_id == "anonymous":
            raise ValueError(
                "Rate limiting requires authenticated user identity. "
                "Anonymous users are not allowed for AI operations."
            )
        
        limit_config = RateLimitConfig.get_limit(category, user_tier)
        requests_limit = limit_config["requests"]
        window_seconds = limit_config["window"]
        
        now = time.time()
        window_start = now - window_seconds
        key = self._get_key(user_id, category)
        
        try:
            # Remove old entries
            self.redis.zremrangebyscore(key, 0, window_start)
            
            # Count current requests
            current_count = self.redis.zcard(key)
            
            # Get oldest entry for reset time
            oldest_entry = self.redis.zrange(key, 0, 0, withscores=True)
            if oldest_entry:
                reset_at = oldest_entry[0][1] + window_seconds
            else:
                reset_at = now + window_seconds
            
            return {
                "limit": requests_limit,
                "remaining": max(0, requests_limit - current_count),
                "current": current_count,
                "reset_at": reset_at,
                "window_seconds": window_seconds
            }
        except Exception as e:
            print(f"⚠️ Rate limit status error: {e}")
            return {
                "limit": requests_limit,
                "remaining": requests_limit,
                "reset_at": now + window_seconds,
                "error": str(e)
            }
    
    def reset_rate_limit(self, user_id: str, category: Optional[RateLimitCategory] = None):
        """
        Reset rate limit for user (admin function)
        
        Args:
            user_id: User ID
            category: Optional category to reset, None resets all
        """
        try:
            if category:
                key = self._get_key(user_id, category)
                self.redis.delete(key)
            else:
                # Reset all categories
                pattern = f"{self.key_prefix}*:{user_id}"
                cursor = 0
                while True:
                    cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                    if keys:
                        self.redis.delete(*keys)
                    if cursor == 0:
                        break
        except Exception as e:
            print(f"⚠️ Rate limit reset error: {e}")


# Global rate limiter instance
rate_limiter = RateLimiter()
