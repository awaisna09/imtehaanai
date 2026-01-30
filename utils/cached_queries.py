"""
Cached Database Queries
Read-through caching for static/semi-static reference data and read-heavy queries
"""

import os
import sys
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

# Load environment variables first (optional - only if file exists)
# In production (Railway), environment variables are set directly
if os.path.exists('config.env'):
    load_dotenv('config.env')

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.read_cache import read_cache, CacheTTL
from supabase import create_client

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

supabase_client = None
if supabase_url and supabase_key:
    try:
        supabase_client = create_client(supabase_url, supabase_key)
    except Exception as e:
        print(f"[WARNING] Could not initialize Supabase client: {e}")


def get_subject_by_id(subject_id: int) -> Optional[Dict[str, Any]]:
    """
    Get subject by ID (cached)
    Static reference data - 24 hour cache
    """
    if not supabase_client:
        return None
    
    def fetch_subject():
        if not supabase_client:
            return None
        result = supabase_client.table("subjects").select("*").eq("subject_id", subject_id).single().execute()
        return result.data if result else None
    
    return read_cache.get_or_fetch(
        namespace="subjects",
        identifier=str(subject_id),
        fetch_func=fetch_subject,
        ttl=CacheTTL.STATIC_REFERENCE
    )


def get_all_subjects() -> List[Dict[str, Any]]:
    """
    Get all subjects (cached)
    Static reference data - 24 hour cache
    """
    if not supabase_client:
        return []
    
    def fetch_all_subjects():
        if not supabase_client:
            return []
        result = supabase_client.table("subjects").select("*").execute()
        return result.data if result else []
    
    return read_cache.get_or_fetch(
        namespace="subjects",
        identifier="all",
        fetch_func=fetch_all_subjects,
        ttl=CacheTTL.STATIC_REFERENCE
    )


def get_topic_by_id(topic_id: int, table_name: str = "topics") -> Optional[Dict[str, Any]]:
    """
    Get topic by ID (cached)
    Semi-static reference data - 1 hour cache
    """
    if not supabase_client:
        return None
    
    def fetch_topic():
        if not supabase_client:
            return None
        result = supabase_client.table(table_name).select("*").eq("topic_id", topic_id).single().execute()
        return result.data if result else None
    
    return read_cache.get_or_fetch(
        namespace="topics",
        identifier=f"{table_name}:{topic_id}",
        fetch_func=fetch_topic,
        ttl=CacheTTL.SEMI_STATIC
    )


def get_subject_id_from_topic(topic_id: int) -> Optional[int]:
    """
    Get subject_id from topic_id (cached)
    Semi-static reference data - 1 hour cache
    Checks multiple topic tables
    """
    if not supabase_client:
        return None
    
    # Try cached lookup first
    cached = read_cache.get("topic_subject_map", str(topic_id))
    if cached is not None:
        return cached
    
    # Check subject-specific tables (in order of topic_id ranges)
    topic_tables = [
        ("topics_isl", 102),      # Islamiyat (100-199)
        ("topics_history", 114),  # Pak Studies History (200-302)
        ("topics_geography", 113), # Pak Studies Geography (305-400)
        ("topics_economics", 119), # Economics (500-699)
    ]
    
    subject_id = None
    
    for table_name, default_subject_id in topic_tables:
        try:
            # Use closure-safe approach for lambda
            def make_fetch_func(tbl_name, topic):
                def fetch():
                    if not supabase_client:
                        return None
                    result = supabase_client.table(tbl_name).select("subject_id").eq("topic_id", topic).limit(1).execute()
                    return result.data if result else None
                return fetch
            
            topic_data = read_cache.get_or_fetch(
                namespace="topics",
                identifier=f"{table_name}:{topic_id}",
                fetch_func=make_fetch_func(table_name, topic_id),
                ttl=CacheTTL.SEMI_STATIC
            )
            
            if topic_data and isinstance(topic_data, list) and len(topic_data) > 0:
                subject_id = topic_data[0].get("subject_id", default_subject_id)
                break
        except Exception:
            continue
    
    # Fallback to main topics table
    if not subject_id:
        try:
            def fetch_main_topics():
                if not supabase_client:
                    return None
                result = supabase_client.table("topics").select("subject_id").eq("topic_id", topic_id).limit(1).execute()
                return result.data if result else None
            
            topic_data = read_cache.get_or_fetch(
                namespace="topics",
                identifier=f"topics:{topic_id}",
                fetch_func=fetch_main_topics,
                ttl=CacheTTL.SEMI_STATIC
            )
            
            if topic_data and isinstance(topic_data, list) and len(topic_data) > 0:
                subject_id = topic_data[0].get("subject_id", 101)  # Default to Business Studies
        except Exception:
            pass
    
    # Default fallback
    if not subject_id:
        subject_id = 101  # Business Studies
    
    # Cache the mapping for faster future lookups
    read_cache.set("topic_subject_map", str(topic_id), subject_id, ttl=CacheTTL.SEMI_STATIC)
    
    return subject_id


def get_concepts_by_topic(topic_id: int) -> List[Dict[str, Any]]:
    """
    Get concepts for a topic (cached)
    Semi-static reference data - 1 hour cache
    """
    if not supabase_client:
        return []
    
    def fetch_concepts():
        if not supabase_client:
            return []
        result = supabase_client.table("concepts").select("*").eq("topic_id", topic_id).execute()
        return result.data if result else []
    
    return read_cache.get_or_fetch(
        namespace="concepts",
        identifier=f"topic:{topic_id}",
        fetch_func=fetch_concepts,
        ttl=CacheTTL.SEMI_STATIC
    )


def get_question_by_id(question_id: str, table_name: str = "questions") -> Optional[Dict[str, Any]]:
    """
    Get question by ID (cached)
    Semi-static reference data - 1 hour cache
    """
    if not supabase_client:
        return None
    
    def fetch_question():
        if not supabase_client:
            return None
        result = supabase_client.table(table_name).select("*").eq("question_id", question_id).single().execute()
        return result.data if result else None
    
    return read_cache.get_or_fetch(
        namespace="questions",
        identifier=f"{table_name}:{question_id}",
        fetch_func=fetch_question,
        ttl=CacheTTL.SEMI_STATIC
    )


def get_user_profile(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user profile (cached)
    User-specific data - 30 minute cache
    """
    if not supabase_client:
        return None
    
    def fetch_user_profile():
        if not supabase_client:
            return None
        # Users table uses 'id' as primary key, not 'user_id'
        result = supabase_client.table("users").select("*").eq("id", user_id).single().execute()
        return result.data if result else None
    
    return read_cache.get_or_fetch(
        namespace="users",
        identifier=user_id,
        fetch_func=fetch_user_profile,
        ttl=CacheTTL.USER_DATA
    )


def get_user_tier(user_id: str) -> str:
    """
    Get user tier for rate limiting (cached)
    User-specific data - 30 minute cache
    """
    profile = get_user_profile(user_id)
    if profile:
        return profile.get("tier", "standard")
    return "standard"
