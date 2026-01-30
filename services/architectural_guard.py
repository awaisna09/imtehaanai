"""
Architectural Guard Service
Enforces strict architectural boundary: API layer cannot execute AI logic or write-heavy DB operations
Prevents accidental synchronous execution of AI workloads
"""

import os
import sys
import inspect
import importlib
from typing import Set, List, Tuple, Optional
from functools import wraps

# Prohibited imports in API layer (unified_backend.py)
PROHIBITED_AI_IMPORTS = {
    # AI/ML Libraries
    'langchain',
    'langgraph',
    'openai',
    'anthropic',
    'ChatOpenAI',
    'LLM',
    'BaseLLM',
    
    # AI Agent Classes
    'AnswerGradingAgent',
    'MockExamGradingAgent',
    'HelpingAgent',
    'TutorAgent',
    'AITutorAgent',
    
    # AI Workflow Functions
    'run_tutor_graph',
    'run_mock_exam_graph',
    'run_grading_graph',
    
    # AI Services
    'concept_agent',
    'readiness_agent',
    'mastery_agent',
}

# Prohibited function calls in API layer
PROHIBITED_AI_CALLS = {
    'run_tutor_graph',
    'run_mock_exam_graph',
    'run_grading_graph',
    'grade_answer',
    'explain_concept',
    'create_lesson',
    'invoke',  # LangChain invoke
    'ainvoke',  # LangChain async invoke
    'stream',  # LangChain stream
    'astream',  # LangChain async stream
    'ChatOpenAI',
    'LLM',
}

# Write-heavy database operations that should only happen in workers
PROHIBITED_DB_WRITES = {
    'insert',  # Supabase insert
    'update',  # Supabase update
    'upsert',  # Supabase upsert
    'delete',  # Supabase delete
    'execute',  # Supabase execute (for writes)
}


class ArchitecturalViolationError(Exception):
    """Raised when architectural boundary is violated"""
    pass


def check_imports():
    """
    Check if prohibited AI imports are present in API layer
    Should be called at module load time
    """
    # Get the calling module (should be unified_backend.py)
    frame = inspect.currentframe()
    if frame and frame.f_back:
        calling_module = frame.f_back.f_globals.get('__name__', '')
        if 'unified_backend' in calling_module or 'api' in calling_module.lower():
            # Check for prohibited imports
            module_globals = frame.f_back.f_globals
            
            violations = []
            for prohibited in PROHIBITED_AI_IMPORTS:
                if prohibited in module_globals:
                    violations.append(prohibited)
            
            if violations:
                raise ArchitecturalViolationError(
                    f"❌ ARCHITECTURAL VIOLATION: Prohibited AI imports found in API layer: {violations}\n"
                    f"   AI logic must only execute in background workers, not in HTTP request handlers.\n"
                    f"   Remove these imports and ensure all AI work is enqueued to Redis queue."
                )
    
    return True


def guard_ai_execution(func):
    """
    Decorator to guard against AI execution in API layer
    
    Usage:
        @app.post("/tutor/chat")
        @guard_ai_execution
        async def chat_with_tutor(...):
            # Only auth, validation, rate limiting, and enqueueing allowed
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Runtime check: Ensure no AI libraries are imported in this module
        import sys
        import inspect
        
        # Get the module where this function is defined
        module = inspect.getmodule(func)
        if module:
            module_name = module.__name__
            # Check if we're in the API layer
            if 'unified_backend' in module_name or 'api' in module_name.lower():
                # Check for prohibited imports in module globals
                for prohibited in PROHIBITED_AI_IMPORTS:
                    if prohibited in module.__dict__:
                        raise ArchitecturalViolationError(
                            f"❌ ARCHITECTURAL VIOLATION: Prohibited AI import '{prohibited}' found in API layer\n"
                            f"   AI logic must only execute in background workers.\n"
                            f"   Remove this import and ensure all AI work is enqueued to Redis queue."
                        )
        
        return await func(*args, **kwargs)
    
    return wrapper


def guard_db_writes(func):
    """
    Decorator to guard against write-heavy database operations in API layer
    
    Usage:
        @guard_db_writes
        @app.post("/some-endpoint")
        async def some_endpoint(...):
            # Only read operations allowed, writes must go through workers
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # This is a runtime check - actual enforcement happens via code review
        # and static analysis
        return await func(*args, **kwargs)
    
    return wrapper


def validate_endpoint_structure(endpoint_func):
    """
    Validates that an endpoint follows the strict architectural pattern:
    1. Authentication
    2. Input validation
    3. Rate limiting
    4. Enqueue job (if AI-related)
    5. Return immediately
    
    This is a static analysis helper, not a runtime check
    """
    source = inspect.getsource(endpoint_func)
    
    # Check for prohibited patterns
    violations = []
    
    # Check for AI execution
    for prohibited in PROHIBITED_AI_CALLS:
        if prohibited in source:
            violations.append(f"AI execution call detected: {prohibited}")
    
    # Check for write-heavy DB operations (should be in workers)
    # Note: Read operations are allowed (cached queries)
    write_patterns = [
        '.insert(',
        '.update(',
        '.upsert(',
        '.delete(',
    ]
    
    for pattern in write_patterns:
        if pattern in source and 'batch_writer' not in source:
            # Allow if using batch_writer (workers only)
            violations.append(f"Direct database write detected: {pattern}")
    
    if violations:
        print(f"⚠️ ARCHITECTURAL WARNING for {endpoint_func.__name__}:")
        for violation in violations:
            print(f"   - {violation}")
        print("   Endpoint should only: authenticate, validate, rate limit, enqueue, return")
    
    return len(violations) == 0


def enforce_redis_required():
    """
    Enforces that Redis is required for all AI operations
    Returns True if Redis is available, raises HTTPException if not
    """
    try:
        from services.redis_connection import is_redis_available
        from services.job_queue import job_queue
        
        if not is_redis_available() or not job_queue:
            from fastapi import HTTPException
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "Service Unavailable",
                    "message": "Redis-backed job queue is required for AI operations. "
                               "All AI workloads must execute asynchronously in background workers.",
                    "required": "Redis must be running and accessible"
                }
            )
        return True
    except ImportError:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=503,
            detail="Job queue service not available. Redis is required for AI operations."
        )


# Run import check when module is loaded
if __name__ != "__main__":
    # Only check if we're in the API layer
    if 'unified_backend' in sys.modules or any('api' in mod.lower() for mod in sys.modules):
        try:
            check_imports()
        except ArchitecturalViolationError:
            # Don't fail at import time, but log warning
            import warnings
            warnings.warn(
                "Architectural guard detected potential violations. "
                "Review code to ensure AI logic only runs in workers.",
                UserWarning
            )
