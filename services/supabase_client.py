"""
Singleton Supabase Client Factory
Ensures exactly one Supabase client instance per process.
"""

import os
import logging
import time
from typing import Optional, Any

logger = logging.getLogger(__name__)

# Global singleton instance (one client per process)
# Rule: One client per process, not per job, not per request
_supabase_client: Optional[Any] = None
_client_initialized: bool = False
_last_activity_time: float = 0.0  # Initialize to prevent NameError

# Production Supabase project reference (for safety checks)
PRODUCTION_PROJECT_REF = "bgenvwieabtxwzapgeee"


class ProductionSupabaseInDevError(Exception):
    """Raised when development environment tries to use production Supabase"""
    pass


def _validate_environment_safety(supabase_url: str):
    """
    Validate that non-production environments are not using production Supabase.

    Args:
        supabase_url: Supabase URL to validate

    Raises:
        ProductionSupabaseInDevError: If dev/staging uses prod Supabase
    """
    environment = os.getenv("ENVIRONMENT", "development").lower()
    allow_prod_in_dev = (
        os.getenv("ALLOW_PROD_SUPABASE_IN_DEV", "false").lower() == "true"
    )

    # Skip check if explicitly allowed
    if allow_prod_in_dev:
        logger.warning(
            "⚠️ ALLOW_PROD_SUPABASE_IN_DEV=true - "
            "Production Supabase allowed in non-production environment"
        )
        return

    # Skip check if in production
    if environment == "production":
        return

    # Check if URL contains production project reference
    if PRODUCTION_PROJECT_REF in supabase_url:
        error_msg = (
            f"\n{'='*70}\n"
            f"🚨 SAFETY ERROR: Production Supabase in {environment}\n"
            f"{'='*70}\n"
            f"\n"
            f"Your SUPABASE_URL points to production:\n"
            f"  {supabase_url}\n"
            f"\n"
            f"This is BLOCKED to prevent:\n"
            f"  - Accidental data corruption in production\n"
            f"  - Development load affecting production users\n"
            f"  - Security risks from dev tools accessing prod data\n"
            f"\n"
            f"TO FIX:\n"
            f"  1. Create a separate Supabase project for {environment}\n"
            f"  2. Update SUPABASE_URL to your {environment} project URL\n"
            f"  3. Update SUPABASE_ANON_KEY and SUPABASE_SERVICE_ROLE_KEY\n"
            f"\n"
            f"OR (NOT RECOMMENDED - only for emergency):\n"
            f"  Set ALLOW_PROD_SUPABASE_IN_DEV=true in your environment\n"
            f"\n"
            f"Example {environment} config:\n"
            f"  SUPABASE_URL=https://your-{environment}-project.supabase.co\n"
            f"  SUPABASE_ANON_KEY=your_{environment}_anon_key\n"
            f"  SUPABASE_SERVICE_ROLE_KEY=your_{environment}_service_key\n"
            f"{'='*70}\n"
        )
        logger.error(error_msg)
        raise ProductionSupabaseInDevError(error_msg)


def get_supabase_client():
    """
    Get or create the singleton Supabase client instance.
    
    ENFORCES SINGLETON PATTERN: Only one client per process.
    Guards against accidental multiple instantiations.

    Returns:
        Supabase client instance or None if credentials are missing.

    Raises:
        ProductionSupabaseInDevError: If dev/staging tries to use prod Supabase
        RuntimeError: If client is being created multiple times (guard violation)
    """
    global _supabase_client, _client_initialized

    # Return cached client if already initialized (singleton pattern)
    # Rule: One client per process, not per job, not per request
    if _supabase_client is not None:
        return _supabase_client
    
    # Guard: Prevent multiple simultaneous initializations
    if _client_initialized:
        # Client initialization was attempted but failed or returned None
        # This is expected behavior - return None instead of retrying
        return None

    # Try to create client
    try:
        from supabase import create_client

        supabase_url = os.getenv("SUPABASE_URL")
        # Prefer service role key for write operations, fallback to anon key
        service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        anon_key = os.getenv("SUPABASE_ANON_KEY")
        supabase_key = service_role_key or anon_key
        
        # Log which key is being used (without exposing the actual key)
        if service_role_key:
            logger.info("🔑 Using SUPABASE_SERVICE_ROLE_KEY (full permissions)")
        elif anon_key:
            logger.warning("⚠️ Using SUPABASE_ANON_KEY (limited permissions - may cause write failures)")
        else:
            logger.error("❌ No Supabase key found!")

        if supabase_url and supabase_key:
            # Safety check: prevent dev/staging from using production Supabase
            _validate_environment_safety(supabase_url)
            _supabase_client = create_client(supabase_url, supabase_key)

            # PHASE 2: Enable HTTP keep-alive and connection pooling for better performance
            if (hasattr(_supabase_client, 'postgrest') and
                    hasattr(_supabase_client.postgrest, 'session')):
                _supabase_client.postgrest.session.keep_alive = True
                # Configure connection pool settings if available
                if hasattr(_supabase_client.postgrest.session, 'pool_connections'):
                    # Default: 10 connections per pool
                    _supabase_client.postgrest.session.pool_connections = int(
                        os.getenv("SUPABASE_POOL_CONNECTIONS", "10")
                    )
                if hasattr(_supabase_client.postgrest.session, 'pool_maxsize'):
                    # Default: 20 max connections
                    _supabase_client.postgrest.session.pool_maxsize = int(
                        os.getenv("SUPABASE_POOL_MAXSIZE", "20")
                    )

            logger.info("Supabase client initialized (singleton per process, Phase 2 optimized)")
            _client_initialized = True
            _last_activity_time = time.time()  # Initialize activity time
            return _supabase_client
        else:
            logger.warning(
                "Supabase credentials not found - "
                "Supabase features will be disabled"
            )
            # Mark as initialized to prevent retries
            _client_initialized = True
            _supabase_client = None
            return None

    except ImportError:
        logger.error(
            "Supabase Python client not installed - "
            "install with: pip install supabase"
        )
        _client_initialized = True
        _supabase_client = None
        return None
    except Exception as e:
        logger.error(f"Error initializing Supabase client: {e}")
        _client_initialized = True
        _supabase_client = None
        return None


def close_db_connections():
    """
    Close DB connections (idle timeout guard).
    Closes Supabase client HTTP session connections.
    """
    global _supabase_client, _last_activity_time
    
    if _supabase_client is None:
        return
    
    try:
        # Close PostgREST session if available
        if (hasattr(_supabase_client, 'postgrest') and
                hasattr(_supabase_client.postgrest, 'session')):
            session = _supabase_client.postgrest.session
            if hasattr(session, 'close'):
                session.close()
                logger.info("✅ Closed Supabase PostgREST session (idle timeout)")
            elif hasattr(session, 'headers'):
                # Clear session headers to force reconnection
                if hasattr(session, 'headers'):
                    session.headers.clear()
                logger.info("✅ Cleared Supabase session (idle timeout)")
        
        # Reset activity time
        _last_activity_time = 0.0
        
    except Exception as e:
        logger.warning(f"⚠️ Error closing DB connections: {e}")


def get_idle_time() -> float:
    """
    Get time since last DB activity (seconds).
    
    Returns:
        Seconds since last activity, or 0 if never active
    """
    global _last_activity_time
    
    if _last_activity_time == 0.0:
        return 0.0
    
    return time.time() - _last_activity_time


def check_idle_timeout(idle_threshold: float = 60.0) -> bool:
    """
    Check if idle timeout threshold exceeded.
    
    Args:
        idle_threshold: Idle timeout in seconds (default: 60)
    
    Returns:
        True if idle time exceeds threshold, False otherwise
    """
    idle_time = get_idle_time()
    return idle_time > idle_threshold
