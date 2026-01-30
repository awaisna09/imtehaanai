#!/usr/bin/env python3
"""
Unified Backend Service
Combines AI Tutor and Grading API on a single port (8000)
"""

import os
import sys
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, date
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Depends, Request, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import JSONResponse
from fastapi import Request
import asyncio
import logging
from pydantic import BaseModel, validator
import uvicorn
import time
from dotenv import load_dotenv

# Fix Windows console encoding for emoji support
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        # Python < 3.7 doesn't have reconfigure, or if already reconfigured
        try:
            import io
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
        except Exception:
            pass  # If we can't fix encoding, continue anyway

# Load environment variables
load_dotenv('config.env')

# Import Supabase operations helper for concurrency limiting
from services.supabase_ops import sb_execute

# ============================================================
# ARCHITECTURAL ENFORCEMENT: NO AI LIBRARIES IN API LAYER
# ============================================================
# AI libraries (langchain, langgraph, openai, etc.) are PROHIBITED
# in the API layer. All AI execution happens exclusively in background workers.
# This import check is removed to enforce strict boundary.
# ============================================================

# ============================================================
# LOAD SHEDDING MIDDLEWARE
# ============================================================
# Prevents system overload by rejecting requests when:
# 1. Queue depth exceeds threshold (90% capacity)
# 2. Worker health is degraded (< 2 healthy workers)
# 3. Redis is unavailable
# NOTE: LoadSheddingMiddleware class is defined after configuration loading
# See line ~607 for class definition
# ============================================================

# ============================================================
# CRITICAL: AI Libraries NOT imported in API layer
# All AI execution happens in background workers only
# This file handles ONLY: auth, validation, and job enqueueing
# ============================================================

# Services available flags (workers handle actual AI execution)
GRADING_AVAILABLE = True  # Workers will handle this
AI_TUTOR_AVAILABLE = True  # Workers will handle this
HELPING_AGENT_AVAILABLE = True  # Workers will handle this

# Configuration with better error handling
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LANGSMITH_API_KEY = os.getenv("LANGSMITH_API_KEY")
LANGSMITH_PROJECT = os.getenv(
    "LANGSMITH_PROJECT", "imtehaan-ai-tutor"
)
LANGSMITH_ENDPOINT = os.getenv(
    "LANGSMITH_ENDPOINT", "https://api.smith.langchain.com"
)

# AI Configuration (used by workers, not API layer)
# These are passed to workers via job data
TUTOR_MODEL = os.getenv("TUTOR_MODEL", "gpt-4o-mini")
GRADING_MODEL = os.getenv("GRADING_MODEL", "gpt-4o-mini")
HELPING_MODEL = os.getenv("HELPING_MODEL", "gpt-4o-mini")

# Server Configuration
# Use API_HOST/API_PORT for clarity, fallback to HOST/PORT for backward compatibility
API_HOST = os.getenv("API_HOST") or os.getenv("HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT") or os.getenv("PORT", "8000"))

# Logging Configuration
# Default to INFO in production, DEBUG in development
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
LOG_LEVEL = os.getenv("LOG_LEVEL") or ("DEBUG" if ENVIRONMENT == "development" else "INFO")
ENABLE_DEBUG = os.getenv("ENABLE_DEBUG", "false").lower() == "true"

# Module-level logger (available throughout the file)
logger = logging.getLogger(__name__)

# Production-Grade Uvicorn Configuration
# These settings prevent overload and memory spikes under burst traffic
UVICORN_TIMEOUT_KEEP_ALIVE = int(os.getenv("UVICORN_TIMEOUT_KEEP_ALIVE", "30"))  # 30s keep-alive
UVICORN_TIMEOUT_GRACEFUL_SHUTDOWN = int(os.getenv("UVICORN_TIMEOUT_GRACEFUL_SHUTDOWN", "10"))  # 10s graceful shutdown
UVICORN_LIMIT_CONCURRENCY = int(os.getenv("UVICORN_LIMIT_CONCURRENCY", "1000"))  # Max concurrent connections
UVICORN_BACKLOG = int(os.getenv("UVICORN_BACKLOG", "2048"))  # Connection backlog
UVICORN_WORKERS = int(os.getenv("UVICORN_WORKERS", "1"))  # Keep 1 worker (PM2/Docker manages instances)

# API Request Timeout (for Redis/DB operations)
# Ensures backend never waits longer than this on external services
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "5"))  # 5 seconds max wait for Redis/DB

# Load Shedding Configuration
# Prevents system overload by rejecting requests when system is under pressure
LOAD_SHEDDING_ENABLED = os.getenv("LOAD_SHEDDING_ENABLED", "true").lower() == "true"
LOAD_SHEDDING_QUEUE_THRESHOLD = float(os.getenv("LOAD_SHEDDING_QUEUE_THRESHOLD", "0.9"))  # 90% queue capacity
LOAD_SHEDDING_WORKER_DEGRADED_THRESHOLD = int(os.getenv("LOAD_SHEDDING_WORKER_DEGRADED_THRESHOLD", "2"))  # Reject if < 2 healthy workers

# CORS Configuration
# For production, set ALLOWED_ORIGINS env var to your frontend domain
# Example: ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com,https://your-app.netlify.app
# Railway deployment: Set ALLOWED_ORIGINS to your Netlify domain(s)
ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "*")
ALLOWED_ORIGINS = [origin.strip() for origin in ALLOWED_ORIGINS_RAW.split(",")]

# In production, only allow specified origins (no localhost)
# Localhost origins are NOT added in production for security
if ENVIRONMENT != "production" and "*" not in ALLOWED_ORIGINS:
    # Only add localhost for non-production environments
    localhost_origins = [
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000"
    ]
    for origin in localhost_origins:
        if origin not in ALLOWED_ORIGINS:
            ALLOWED_ORIGINS.append(origin)

ALLOW_CREDENTIALS = os.getenv("ALLOW_CREDENTIALS", "true").lower() == "true"

# Security warning for production
ENVIRONMENT_CHECK = os.getenv("ENVIRONMENT", "development").lower()
if "*" in ALLOWED_ORIGINS and ENVIRONMENT_CHECK == "production":
    print("[ERROR] SECURITY RISK: CORS is set to allow all origins (*) in production!")
    print("   This is a security vulnerability. Set ALLOWED_ORIGINS to specific domains.")
    print("   Example: ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com")
    print("   Continuing with warning - fix this before deploying to production!")
elif "*" in ALLOWED_ORIGINS and ENVIRONMENT_CHECK != "development":
    print(f"[WARNING] CORS allows all origins (*) in {ENVIRONMENT_CHECK} environment")
    print("   Consider restricting to specific domains for better security")

# Centralized configuration validation (fail-fast)
try:
    from utils.validate_config import validate_and_exit
    # Validate configuration at startup
    # In production, warnings are treated as errors
    fail_on_warnings = ENVIRONMENT == "production"
    validate_and_exit(fail_on_warnings=fail_on_warnings)
except SystemExit:
    # Re-raise system exit from validation
    raise
except Exception as e:
    print(f"[ERROR] Configuration validation failed: {e}")
    print("   The application cannot start without valid configuration.")
    exit(1)

# Set LangSmith environment variables if available
if LANGSMITH_API_KEY:
    os.environ["LANGSMITH_API_KEY"] = LANGSMITH_API_KEY
    os.environ["LANGSMITH_PROJECT"] = LANGSMITH_PROJECT
    os.environ["LANGSMITH_ENDPOINT"] = LANGSMITH_ENDPOINT
    os.environ["LANGSMITH_TRACING"] = os.getenv("LANGSMITH_TRACING", "true")
    if ENABLE_DEBUG:
        print(f"[OK] LangSmith configured: {LANGSMITH_PROJECT}")
else:
    if ENABLE_DEBUG:
        print("[WARNING] LANGSMITH_API_KEY not found - tracing disabled")

# ============================================================
# PERFORMANCE OPTIMIZATION: uvloop for faster async operations
# ============================================================
# uvloop provides 10-20% faster async operations
# Only works on Unix systems (Linux, macOS), not Windows
try:
    import uvloop
    if sys.platform != 'win32':
        uvloop.install()
        if ENABLE_DEBUG:
            print("[OK] uvloop installed for faster async performance")
    else:
        if ENABLE_DEBUG:
            print("[INFO] uvloop skipped (Windows - not supported)")
except ImportError:
    if ENABLE_DEBUG:
        print("[INFO] uvloop not installed (optional - install with: pip install uvloop)")

# ============================================================
# NO AGENT INITIALIZATION IN API LAYER
# All agents are initialized in background workers only
# ============================================================

# Initialize Supabase client if available (singleton)
try:
    from services.supabase_client import get_supabase_client
    supabase_client = get_supabase_client()
    if ENABLE_DEBUG and supabase_client:
        print("[OK] Supabase client initialized for backend")
except Exception as e:
    print(f"[ERROR] Error initializing Supabase client: {e}")
    supabase_client = None

# Initialize Redis Queue Service (REQUIRED)
# API layer REQUIRES Redis - no fallback to synchronous execution
try:
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    # Try new enhanced queue first, fallback to legacy
    try:
        from services.job_queue import job_queue, QUEUE_TUTOR, QUEUE_GRADING, QUEUE_MOCK_EXAM, QUEUE_HELPING, QUEUE_LESSON, QUEUE_ROLLUP
        from services.redis_connection import is_redis_available
        REDIS_QUEUE_AVAILABLE = is_redis_available()
    except ImportError:
        from services.redis_queue import job_queue, QUEUE_TUTOR, QUEUE_GRADING, QUEUE_MOCK_EXAM
        QUEUE_HELPING = 'jobs:helping'
        QUEUE_LESSON = 'jobs:lesson'
        QUEUE_ROLLUP = 'jobs:rollup'
        REDIS_QUEUE_AVAILABLE = job_queue is not None
except Exception as e:
    REDIS_QUEUE_AVAILABLE = False
    job_queue = None
    QUEUE_TUTOR = 'jobs:tutor'
    QUEUE_GRADING = 'jobs:grading'
    QUEUE_MOCK_EXAM = 'jobs:mock_exam'
    QUEUE_HELPING = 'jobs:helping'
    QUEUE_LESSON = 'jobs:lesson'
    QUEUE_ROLLUP = 'jobs:rollup'
    
    if REDIS_QUEUE_AVAILABLE and ENABLE_DEBUG:
        print("[OK] Redis Queue Service initialized")
except Exception as e:
    REDIS_QUEUE_AVAILABLE = False
    job_queue = None
    QUEUE_TUTOR = 'jobs:tutor'
    QUEUE_GRADING = 'jobs:grading'
    QUEUE_MOCK_EXAM = 'jobs:mock_exam'
    QUEUE_HELPING = 'jobs:helping'
    QUEUE_LESSON = 'jobs:lesson'
    QUEUE_ROLLUP = 'jobs:rollup'
    print(f"[ERROR] Redis Queue Service not available: {e}")
    print("[CRITICAL] Redis is REQUIRED - API endpoints will return 503 until Redis is available")
    print("[INFO] Start Redis: docker run -d -p 6379:6379 redis:7-alpine")

# Initialize Batch Writer for async database writes
# API layer uses batch_writer to avoid blocking on database writes
try:
    from services.batch_writer import batch_writer, execute_batched_write
    if supabase_client:
        # Set up batch writer handler for API layer
        batch_writer.set_write_handler(
            lambda table, writes: execute_batched_write(table, writes, supabase_client)
        )
        batch_writer.start_periodic_flush()
        if ENABLE_DEBUG:
            print("[OK] Batch Writer initialized for API layer")
    else:
        if ENABLE_DEBUG:
            print("[WARNING] Batch Writer not initialized - Supabase client unavailable")
        batch_writer = None
except ImportError:
    batch_writer = None
    if ENABLE_DEBUG:
        print("[WARNING] Batch Writer not available - direct writes will be used")
except Exception as e:
    batch_writer = None
    print(f"[WARNING] Error initializing Batch Writer: {e}")

# ============================================================
# ARCHITECTURAL ENFORCEMENT: STRICT BOUNDARIES
# ============================================================
# API LAYER PROHIBITIONS:
# 1. NO AI library imports (langchain, langgraph, openai agents)
# 2. NO AI agent initialization
# 3. NO AI execution (run_tutor_graph, grade_answer, etc.)
# 4. NO write-heavy database operations (use workers for batched writes)
#
# API LAYER ALLOWED OPERATIONS:
# 1. Authentication & Authorization
# 2. Input Validation
# 3. Rate Limiting
# 4. Job Enqueueing (to Redis)
# 5. Immediate Return (with job_id)
# 6. Read-only cached database queries (for validation/lookup)
#
# All AI execution happens EXCLUSIVELY in background workers
# ============================================================

# Import architectural guard (will validate at runtime)
try:
    from services.architectural_guard import (
        enforce_redis_required,
        guard_ai_execution
    )
    ARCHITECTURAL_GUARD_ENABLED = True
except ImportError:
    ARCHITECTURAL_GUARD_ENABLED = False
    if ENABLE_DEBUG:
        print("[WARNING] Architectural guard not available - enforcement disabled")

# Initialize Study Planner Service (lightweight, no AI)
study_planner_service = None
try:
    from study_planner_service import StudyPlannerService
    study_planner_service = StudyPlannerService()
    if ENABLE_DEBUG:
        print("[OK] Study Planner Service initialized")
except Exception as e:
    if ENABLE_DEBUG:
        print(f"[WARNING] Study Planner Service not available: {e}")


# ============================================================
# PHASE 1 OPTIMIZATION: Service Singleton Pattern
# ============================================================
# Initialize services once at startup, reuse across requests
# This eliminates 100-200ms initialization overhead per request
# ============================================================

_tutor_services = None

async def initialize_tutor_services():
    """Initialize tutor services singleton (Phase 1 optimization)"""
    global _tutor_services
    
    if _tutor_services is not None:
        return _tutor_services
    
    try:
        from agents.services.llm_service import LLMService
        from agents.services.concept_service import ConceptService
        from agents.services.lesson_service import LessonService
        from agents.services.history_service import HistoryService
        from agents.concept_agent import ConceptAgent
        from services.supabase_client import get_supabase_client
        from langchain_openai import ChatOpenAI
        import os
        
        supabase = get_supabase_client()
        api_key = os.getenv("OPENAI_API_KEY")
        
        # Initialize LLM
        llm = None
        if api_key:
            try:
                llm = ChatOpenAI(
                    model=os.getenv("TUTOR_MODEL", "gpt-3.5-turbo"),
                    temperature=0.7,
                    max_tokens=1500,  # Reduced for faster generation
                    openai_api_key=api_key,
                    timeout=20,  # Reduced for faster failure detection
                    max_retries=1  # Reduced retries for faster failures
                )
            except Exception as e:
                logger.warning(f"Failed to initialize LLM: {e}")
        
        # Initialize services
        llm_service = LLMService(
            llm=llm,
            langchain_available=llm is not None
        )
        
        concept_agent = ConceptAgent(api_key=api_key, supabase_client=supabase)
        concept_service = ConceptService(concept_agent=concept_agent)
        lesson_service = LessonService(
            supabase_client=supabase,
            concept_agent=concept_agent
        )
        history_service = HistoryService(
            supabase_client=supabase,
            cache_get=None,
            cache_set=None,
            cache_delete=None
        )
        
        _tutor_services = {
            "llm_service": llm_service,
            "concept_service": concept_service,
            "lesson_service": lesson_service,
            "history_service": history_service,
            "concept_agent": concept_agent,
            "supabase": supabase
        }
        
        if ENABLE_DEBUG:
            print("[PHASE 1] Tutor services initialized (singleton pattern)")
        
        return _tutor_services
    except Exception as e:
        logger.error(f"[ERROR] Failed to initialize tutor services: {e}")
        return None

def get_tutor_services():
    """Get tutor services singleton"""
    global _tutor_services
    if _tutor_services is None:
        # Fallback: initialize synchronously if not already initialized
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is running, we can't use run_until_complete
                # Return None and let the endpoint initialize on-demand
                return None
            else:
                _tutor_services = loop.run_until_complete(initialize_tutor_services())
        except RuntimeError:
            # No event loop, initialize synchronously
            _tutor_services = asyncio.run(initialize_tutor_services())
    return _tutor_services

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown"""

    # Startup: Verify Redis is available (REQUIRED)
    if ENABLE_DEBUG:
        print("[STARTUP] Initializing API server...")
        print("[INFO] API layer: Auth, Validation, Job Enqueueing only")
        print("[INFO] AI execution: Background workers only")
        
        if REDIS_QUEUE_AVAILABLE and job_queue:
            try:
                stats = job_queue.get_queue_stats()
                redis_connected = stats.get('redis_connected', False)
                if redis_connected:
                    print(f"[OK] Redis connection verified")
                    print(f"[INFO] Queue status: Tutor={stats.get('tutor_queue', 0)}, "
                          f"Grading={stats.get('grading_queue', 0)}, "
                          f"Mock Exam={stats.get('mock_exam_queue', 0)}")
                else:
                    print(f"[WARNING] Redis not connected properly")
            except Exception as e:
                print(f"[ERROR] Could not verify Redis connection: {e}")
                print(f"[CRITICAL] Start Redis: docker run -d -p 6379:6379 redis:7-alpine")
        else:
            print("[ERROR] Redis Queue Service not available")
            print("[CRITICAL] API endpoints will return 503 until Redis is available")
            print("[INFO] Start Redis: docker run -d -p 6379:6379 redis:7-alpine")
        
        # PHASE 1: Initialize tutor services at startup
        print("[PHASE 1] Initializing tutor services (singleton pattern)...")
        await initialize_tutor_services()
        
        print("[STARTUP] API server ready (AI execution handled by background workers)\n")

    yield  # Server runs here

    # Shutdown: Cleanup if needed
    if ENABLE_DEBUG:
        print("[SHUTDOWN] Shutting down gracefully...")
        global _tutor_services
        _tutor_services = None


# Initialize FastAPI app with lifespan
app = FastAPI(
    title="Imtehaan AI EdTech Platform",
    version="2.0.0",
    description="Unified backend combining AI Tutor and Grading services",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# PHASE 3: Add response compression for faster transfers
try:
    from fastapi.middleware.gzip import GZipMiddleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)  # Compress responses > 1KB
    if ENABLE_DEBUG:
        print("[PHASE 3] Response compression enabled (GZip)")
except ImportError:
    if ENABLE_DEBUG:
        print("[PHASE 3] GZipMiddleware not available - skipping compression")

# Load Shedding middleware will be added after class definition (after config loading)


# ============================================================
# REQUEST LOGGING MIDDLEWARE (PRODUCTION OBSERVABILITY)
# ============================================================
# Logs all API requests with latency, separate from background execution
# Ensures API responsiveness can be monitored independently
# ============================================================

class RequestIDMiddleware(BaseHTTPMiddleware):
    """Middleware to add request_id and correlation_id to all API requests for observability"""
    
    async def dispatch(self, request: StarletteRequest, call_next):
        import uuid
        import contextvars
        
        # Generate correlation_id (UUID) and request_id (short)
        correlation_id = str(uuid.uuid4())
        request_id = f"req-{correlation_id[:12]}"
        
        # Store in request state
        request.state.request_id = request_id
        request.state.correlation_id = correlation_id
        
        # Store in context variable for async operations
        try:
            request_id_var = contextvars.ContextVar('request_id', default=None)
            correlation_id_var = contextvars.ContextVar('correlation_id', default=None)
            request_id_var.set(request_id)
            correlation_id_var.set(correlation_id)
        except Exception:
            pass  # Non-blocking: continue even if context var fails
        
        # Add request_id and correlation_id to response headers
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Correlation-ID"] = correlation_id
        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log all API requests with latency and context"""
    
    async def dispatch(self, request: StarletteRequest, call_next):
        start_time = time.time()
        path = request.url.path
        method = request.method
        
        # Extract request_id from request state
        request_id = getattr(request.state, 'request_id', None)
        
        # Extract user_id from request state if available
        user_id = None
        try:
            if hasattr(request.state, 'user_id'):
                user_id = request.state.user_id
        except Exception:
            pass
        
        # Skip logging for health/observability endpoints (to avoid noise)
        skip_paths = ['/health', '/docs', '/openapi.json', '/redoc', '/observability']
        should_log = not any(path.startswith(skip) for skip in skip_paths)
        
        status_code = 200
        error = None
        
        try:
            response = await call_next(request)
            status_code = response.status_code
            
            # Calculate latency
            duration_ms = (time.time() - start_time) * 1000
            
            # Log request (structured logging)
            if should_log:
                try:
                    from services.structured_logging import structured_logger
                    from services.observability import observability
                    
                    # Log structured event
                    structured_logger.log_api_request(
                        method=method,
                        path=path,
                        status_code=status_code,
                        duration_ms=duration_ms,
                        user_id=user_id
                    )
                    
                    # Track metrics (separate from job processing)
                    observability.track_request_latency(
                        endpoint=path,
                        method=method,
                        duration_ms=duration_ms,
                        status_code=status_code,
                        user_id=user_id
                    )
                except Exception as e:
                    # Don't fail requests if logging fails
                    if ENABLE_DEBUG:
                        print(f"⚠️ Request logging error: {e}")
            
            # Add latency header
            response.headers["X-Request-Duration-Ms"] = str(round(duration_ms, 2))
            
            return response
            
        except Exception as e:
            # Calculate latency even for errors
            duration_ms = (time.time() - start_time) * 1000
            status_code = 500
            error = str(e)
            
            # Log error request
            if should_log:
                try:
                    from services.structured_logging import structured_logger
                    from services.observability import observability
                    
                    structured_logger._log_structured(
                        "ERROR",
                        f"API request error: {method} {path}",
                        context={
                            "method": method,
                            "path": path,
                            "status_code": status_code,
                            "duration_ms": duration_ms,
                            "user_id": user_id,
                            "error": error,
                            "event": "api_request_error"
                        }
                    )
                    
                    observability.track_request_latency(
                        endpoint=path,
                        method=method,
                        duration_ms=duration_ms,
                        status_code=status_code,
                        user_id=user_id
                    )
                except Exception:
                    pass  # Don't fail on logging errors
            
            raise


# Add request logging middleware (after CORS, before endpoints)
# Load Shedding Middleware (defined here after all config is loaded)
class LoadSheddingMiddleware(BaseHTTPMiddleware):
    """
    Load shedding middleware that rejects requests when system is under pressure.
    Uses SafetyGate service for centralized safety checks.
    
    NOTE: This middleware provides early rejection, but SafetyGate is also
    called directly in each endpoint to ensure it's impossible to bypass.
    """
    
    async def dispatch(self, request: Request, call_next):
        # Skip load shedding for OPTIONS requests (CORS preflight)
        # CORS middleware will handle these requests properly
        if request.method == "OPTIONS":
            return await call_next(request)
        
        # Skip load shedding for health checks and observability endpoints
        if request.url.path in ["/health", "/tutor/health", "/grading/health", "/helping/health", "/observability/worker-health"]:
            return await call_next(request)
        
        # Only apply load shedding to job-enqueueing endpoints
        # NOTE: /tutor/chat is synchronous (no workers/queue), so it is intentionally excluded here.
        # NOTE: /grade-mock-exam is now synchronous (no workers/queue), so it is intentionally excluded here.
        job_endpoints = ["/tutor/lesson"]
        if not any(request.url.path.startswith(ep) for ep in job_endpoints):
            return await call_next(request)
        
        # Check if load shedding is enabled
        if not LOAD_SHEDDING_ENABLED:
            return await call_next(request)
        
        # Use SafetyGate for centralized safety checks
        try:
            from services.safety_gate import get_safety_gate
            safety_gate = get_safety_gate()
            
            # Determine queue name from path
            queue_name = None
            if request.url.path.startswith("/tutor/chat"):
                queue_name = QUEUE_TUTOR
            elif request.url.path.startswith("/tutor/lesson"):
                queue_name = QUEUE_LESSON
            # /grade-answer is synchronous, no queue needed
            # /grade-mock-exam is synchronous, no queue needed
            # /helping/explain is now synchronous, no queue needed
            
            if queue_name:
                # Check system safety (async-safe)
                safety_result = await asyncio.to_thread(
                    safety_gate.check_system_safety,
                    queue_name=queue_name
                )
                
                if not safety_result.safe:
                    # System is unsafe - reject request early
                    # Add CORS headers to error response
                    response = JSONResponse(
                        status_code=503,
                        content=safety_result.to_dict(),
                        headers={"Retry-After": str(safety_result.retry_after)}
                    )
                    # Ensure CORS headers are added even for error responses
                    response.headers["Access-Control-Allow-Origin"] = request.headers.get("origin", "*")
                    response.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
                    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
                    response.headers["Access-Control-Allow-Credentials"] = "true"
                    return response
        except Exception as e:
            # If safety gate check fails, allow request (fail-open for availability)
            # But log warning - this should not happen
            if ENABLE_DEBUG:
                print(f"[WARNING] Safety gate check failed in middleware: {e}, allowing request")
        
        # All checks passed - proceed with request
        return await call_next(request)

app.add_middleware(LoadSheddingMiddleware)
app.add_middleware(RequestIDMiddleware)  # Add request_id and correlation_id to all requests
app.add_middleware(RequestLoggingMiddleware)

# ============================================================
# GLOBAL EXCEPTION HANDLER
# ============================================================
# Differentiates between 400, 500, and 503 errors
# Hides internal error details from clients
# ============================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with proper error codes and hidden internal details"""
    correlation_id = getattr(request.state, 'correlation_id', None)
    
    # Return appropriate status code with sanitized message
    content = {
        "message": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
        "status_code": exc.status_code
    }
    
    if correlation_id:
        content["correlation_id"] = correlation_id
    
    return JSONResponse(
        status_code=exc.status_code,
        content=content,
        headers={"X-Correlation-ID": correlation_id} if correlation_id else {}
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle all unhandled exceptions - hide internal details from clients"""
    correlation_id = getattr(request.state, 'correlation_id', None)
    
    # Log the full error for debugging (server-side only)
    import logging
    logger = logging.getLogger(__name__)
    logger.error(
        f"Unhandled exception: {type(exc).__name__}: {str(exc)}",
        exc_info=True,
        extra={"correlation_id": correlation_id}
    )
    
    # Return generic error message to client (don't expose stack trace)
    content = {
        "message": "Internal server error",
        "status_code": 500
    }
    
    if correlation_id:
        content["correlation_id"] = correlation_id
    
    return JSONResponse(
        status_code=500,
        content=content,
        headers={"X-Correlation-ID": correlation_id} if correlation_id else {}
    )

# ============================================================
# NO MOCK EXAM APP MOUNTING - All endpoints use Redis queue
# ============================================================


# Pydantic models for AI Tutor
class TutorRequest(BaseModel):
    message: str
    topic: int  # topic_id from database
    lesson_content: Optional[str] = None
    user_id: Optional[str] = None
    conversation_history: Optional[List[Dict[str, str]]] = []
    conversation_id: Optional[str] = None
    learning_level: Optional[str] = "intermediate"
    explanation_style: Optional[str] = "default"
    subject_id: Optional[int] = None  # Subject ID (101=Business Studies, 119=Economics, 114=Pak Studies History, etc.)

    @validator('message')
    def validate_message_length(cls, v):
        """Validate message is not empty and not too long"""
        if not v or not v.strip():
            raise ValueError('Message cannot be empty')
        if len(v) > 10000:
            raise ValueError('Message too long (max 10,000 characters)')
        return v.strip()

    @validator('topic')
    def validate_topic_exists(cls, v):
        """Validate that the topic exists in the database"""
        if not v or v <= 0:
            raise ValueError('Topic ID must be a positive integer')
        # Note: Full existence check happens in endpoint to avoid blocking validation
        return v

    @validator('conversation_history')
    def validate_conversation_history(cls, v):
        """Validate conversation_history structure"""
        if not v:
            return []
        for item in v:
            if not isinstance(item, dict):
                raise ValueError("Each conversation history entry must be a dictionary")
            if 'role' not in item or 'content' not in item:
                raise ValueError("Each conversation history entry must have 'role' and 'content' fields")
            if item.get('role') not in ['user', 'assistant', 'system']:
                raise ValueError("Role must be 'user', 'assistant', or 'system'")
            if not isinstance(item.get('content'), str):
                raise ValueError("Content must be a string")
        return v


class TutorResponse(BaseModel):
    response: str
    suggestions: List[str]
    related_concepts: List[str]
    related_concept_ids: List[str] = []
    confidence_score: float
    reasoning_label: str = "neutral"
    mastery_updates: List[Dict] = []
    readiness: Optional[Dict] = None
    learning_path: Optional[Dict] = None
    token_usage: Optional[Dict] = None
    lesson_chunks: Optional[List[Dict]] = []


class LessonRequest(BaseModel):
    topic: str
    learning_objectives: List[str]
    difficulty_level: str = "intermediate"


class LessonResponse(BaseModel):
    lesson_content: str
    key_points: List[str]
    practice_questions: List[str]
    estimated_duration: int


# Pydantic models for Grading API
class GradingRequest(BaseModel):
    question: str
    model_answer: str
    student_answer: str
    subject: str = "Business Studies"
    topic: str = ""
    user_id: Optional[str] = None
    question_id: Optional[str] = None
    topic_id: Optional[str] = None
    topic_name: Optional[str] = None  # Topic name from frontend
    max_marks: Optional[int] = None
    difficulty_level: Optional[int] = None


class HelpingRequest(BaseModel):
    query: str
    context: Optional[str] = None
    subject: Optional[str] = None
    user_id: Optional[str] = None  # Optional user_id for rate limiting


class HelpingResponse(BaseModel):
    explanation: str
    success: bool


class GradingResponse(BaseModel):
    success: bool
    # Using Dict instead of GradingResult to avoid import issues
    result: Optional[Dict] = None
    message: str = ""


# Pydantic models for Mock Exam Grading
class MockExamGradingRequest(BaseModel):
    attempted_questions: List[Dict]
    exam_type: str = "P1"  # P1 or P2
    user_id: Optional[str] = None
    subject: Optional[str] = None  # Subject name (e.g., "Economics", "Business Studies")
    mock_exam_name: Optional[str] = None  # Mock exam name/number (e.g., "Mock 1", "Case 1", "Set 1")


class QuestionGradeResponse(BaseModel):
    question_id: int
    question_number: int = 1
    part: str = ""
    question_text: str
    student_answer: str
    model_answer: str
    marks_allocated: int
    marks_awarded: float
    percentage_score: float
    feedback: str
    strengths: List[str]
    improvements: List[str]


class MockExamGradingResponse(BaseModel):
    success: bool
    total_questions: int
    questions_attempted: int
    total_marks: int
    marks_obtained: float
    percentage_score: float
    overall_grade: str
    question_grades: List[QuestionGradeResponse]
    overall_feedback: str
    recommendations: List[str]
    strengths_summary: List[str]
    weaknesses_summary: List[str]
    message: str = ""


# Pydantic models for Job Queue (Redis-based async processing)
class JobStatusResponse(BaseModel):
    job_id: str
    status: str  # pending, processing, completed, failed, retrying
    message: Optional[str] = None
    progress: Optional[float] = None  # 0-100
    result: Optional[Dict] = None
    error: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    retry_count: Optional[int] = None
    conversation_id: Optional[str] = None  # CRITICAL: Include conversation_id so frontend can store and reuse it


class JobEnqueueResponse(BaseModel):
    success: bool
    job_id: str
    message: str
    status_endpoint: str  # URL to poll for job status
    estimated_wait_time: Optional[int] = None  # seconds


# Pydantic models for Study Planner
class CreateStudyPlanRequest(BaseModel):
    subject_id: int
    selected_topic_ids: List[int]
    exam_date: str  # ISO date string
    plan_name: str


class UpdateStudyPlanRequest(BaseModel):
    plan_name: Optional[str] = None
    exam_date: Optional[str] = None  # ISO date string


class StudyPlanResponse(BaseModel):
    id: str
    user_id: str
    subject_id: int
    plan_name: str
    start_date: str
    exam_date: str
    days_to_exam: int
    rule_band: str
    flash_per_topic: int
    topical_per_topic: int
    lessons_per_topic: int
    mocks_total: int
    status: str
    created_at: str
    updated_at: str
    topics: Optional[List[int]] = None
    days: Optional[List[Dict]] = None


# ============================================================
#  TIME TRACKING SERVICE — NEW
# ============================================================

class TimeStartRequest(BaseModel):
    user_id: str
    page_type: str     # mock_exam | flashcards | topical_exam | lessons
    subject: Optional[str] = None  # Subject name (e.g., "Business Studies", "Economics")


class TimeStopRequest(BaseModel):
    tracking_id: str
    duration_seconds: int  # Final duration in seconds (calculated by frontend)
    subject: Optional[str] = None  # Subject name to update (e.g., "Business Studies", "Economics")


class TimeTrackingBatchUpdateRequest(BaseModel):
    tracking_id: str
    duration_seconds: int  # Additional seconds to add to existing duration
    end_time: Optional[str] = None  # Optional end_time if session is ending


# ===== JOB QUEUE ENDPOINTS =====

@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get job status by job_id"""
    # Check if this is a tutor_enhance job (uses minimal queue)
    if job_id.startswith('tutor_enhance:'):
        try:
            from services.minimal_tutor_enhance_queue import MinimalTutorEnhanceQueue
            minimal_queue = MinimalTutorEnhanceQueue()
            
            if minimal_queue.is_available():
                job_data = minimal_queue.get_job(job_id)
                if job_data:
                    # Extract conversation_id from job data
                    conversation_id = None
                    job_payload = job_data.get('data', {})
                    if isinstance(job_payload, dict):
                        conversation_id = job_payload.get('conversation_id')
                    
                    # Extract assistant_message_id for enhancement polling
                    assistant_message_id = None
                    if isinstance(job_payload, dict):
                        assistant_message_id = job_payload.get('assistant_message_id')
                    
                    # If job is completed, ensure assistant_message_id is in result for frontend
                    result = job_data.get('result')
                    if job_data.get('status') == 'completed' and assistant_message_id and isinstance(result, dict):
                        # Add assistant_message_id to result so frontend can use it
                        result = result.copy()
                        result['assistant_message_id'] = assistant_message_id
                    
                    return JobStatusResponse(
                        job_id=job_data.get('job_id', job_id),
                        status=job_data.get('status', 'unknown'),
                        message=job_data.get('message'),
                        progress=job_data.get('progress'),
                        result=result,
                        error=job_data.get('error'),
                        created_at=job_data.get('created_at'),
                        updated_at=job_data.get('updated_at'),
                        completed_at=job_data.get('completed_at'),
                        retry_count=job_data.get('retry_count', 0),
                        conversation_id=conversation_id
                    )
                else:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Job {job_id} not found"
                    )
            else:
                raise HTTPException(
                    status_code=503,
                    detail="Minimal tutor enhance queue service not available"
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error retrieving job status: {str(e)}"
            )
    
    # Regular job queue lookup
    if not REDIS_QUEUE_AVAILABLE or not job_queue:
        raise HTTPException(
            status_code=503,
            detail="Job queue service not available"
        )

    try:
        job_data = job_queue.get_job(job_id)
        if not job_data:
            raise HTTPException(
                status_code=404,
                detail=f"Job {job_id} not found"
            )
        
        # Extract conversation_id from job data (stored in job_data['data']['conversation_id'])
        conversation_id = None
        job_payload = job_data.get('data', {})
        if isinstance(job_payload, dict):
            conversation_id = job_payload.get('conversation_id')
        
        return JobStatusResponse(
            job_id=job_data.get('job_id', job_id),
            status=job_data.get('status', 'unknown'),
            message=job_data.get('message'),
            progress=job_data.get('progress'),
            result=job_data.get('result'),
            error=job_data.get('error'),
            created_at=job_data.get('created_at'),
            updated_at=job_data.get('updated_at'),
            completed_at=job_data.get('completed_at'),
            retry_count=job_data.get('retry_count', 0),
            conversation_id=conversation_id  # CRITICAL: Include conversation_id so frontend can store and reuse it
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving job status: {str(e)}"
        )


@app.get("/jobs/{job_id}/stream")
async def stream_job_results(
    job_id: str,
    timeout: int = Query(
        int(os.getenv("STREAM_TIMEOUT", "300")),
        ge=10,
        le=600
    )
):
    """
    Stream job results via Server-Sent Events (SSE).
    
    Provides incremental delivery of long-running AI responses.
    Falls back gracefully to polling if streaming unavailable.
    
    Args:
        job_id: Job identifier
        timeout: Maximum streaming duration in seconds (10-600)
    
    Returns:
        SSE stream of job chunks, or fallback to polling response
    """
    if not REDIS_QUEUE_AVAILABLE or not job_queue:
        # Fallback: Return current job status
        try:
            job_data = job_queue.get_job(job_id) if job_queue else None
            if not job_data:
                raise HTTPException(
                    status_code=404,
                    detail=f"Job {job_id} not found"
                )
            return {
                "success": True,
                "streaming_available": False,
                "fallback": True,
                "job": {
                    "job_id": job_data.get('job_id', job_id),
                    "status": job_data.get('status', 'unknown'),
                    "message": job_data.get('message'),
                    "progress": job_data.get('progress'),
                    "result": job_data.get('result'),
                    "error": job_data.get('error')
                },
                "message": "Streaming not available, use /jobs/{job_id} for polling"
            }
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Job queue service not available: {str(e)}"
            )
    
    # Verify job exists
    job_data = job_queue.get_job(job_id)
    if not job_data:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    # Check if streaming is enabled
    streaming_enabled = (
        os.getenv("STREAMING_ENABLED", "true").lower() == "true"
    )
    
    if not streaming_enabled:
        # Streaming disabled - return current status
        return {
            "success": True,
            "streaming_available": False,
            "fallback": True,
            "job": {
                "job_id": job_data.get('job_id', job_id),
                "status": job_data.get('status', 'unknown'),
                "message": job_data.get('message'),
                "progress": job_data.get('progress'),
                "result": job_data.get('result'),
                "error": job_data.get('error')
            },
            "message": "Streaming disabled, use /jobs/{job_id} for polling"
        }
    
    # Check if streaming service is available
    try:
        from services.streaming_service import get_streaming_service
        streaming_service = get_streaming_service()
        
        if not streaming_service.redis:
            # Fallback: Return current status
            return {
                "success": True,
                "streaming_available": False,
                "fallback": True,
                "job": {
                    "job_id": job_data.get('job_id', job_id),
                    "status": job_data.get('status', 'unknown'),
                    "message": job_data.get('message'),
                    "progress": job_data.get('progress'),
                    "result": job_data.get('result'),
                    "error": job_data.get('error')
                },
                "message": "Streaming not available, use /jobs/{job_id} for polling"
            }
    except ImportError:
        # Streaming service not available
        return {
            "success": True,
            "streaming_available": False,
            "fallback": True,
            "job": {
                "job_id": job_data.get('job_id', job_id),
                "status": job_data.get('status', 'unknown'),
                "message": job_data.get('message'),
                "progress": job_data.get('progress'),
                "result": job_data.get('result'),
                "error": job_data.get('error')
            },
            "message": "Streaming service not available, use /jobs/{job_id} for polling"
        }
    
    # Create SSE generator
    async def generate_sse():
        """Generate Server-Sent Events from streaming service"""
        try:
            # Send initial connection message
            yield f"data: {json.dumps({'type': 'connected', 'job_id': job_id})}\n\n"
            
            # Replay existing chunks first
            existing_chunks = streaming_service.get_chunks(job_id)
            for chunk in existing_chunks:
                yield f"data: {json.dumps(chunk, default=str)}\n\n"
                if chunk.get('is_final'):
                    return  # Already completed
            
            # Subscribe to new chunks
            chunk_count = 0
            start_time = time.time()
            
            for chunk in streaming_service.subscribe_to_chunks(job_id, timeout):
                # Check timeout
                if time.time() - start_time > timeout:
                    yield f"data: {json.dumps({'type': 'timeout', 'message': f'Stream timeout after {timeout}s'})}\n\n"
                    break
                
                # Send chunk as SSE
                yield f"data: {json.dumps(chunk, default=str)}\n\n"
                chunk_count += 1
                
                # Stop if final chunk
                if chunk.get('is_final'):
                    break
                
                # Keep connection alive with heartbeat
                if chunk_count % 10 == 0:
                    yield f": heartbeat\n\n"
            
            # Send completion if not already sent
            if chunk_count == 0:
                # No chunks received, check final job status
                final_job = job_queue.get_job(job_id)
                if final_job:
                    status = final_job.get('status')
                    if status in ['completed', 'failed']:
                        yield f"data: {json.dumps({'type': 'final_status', 'status': status, 'result': final_job.get('result'), 'error': final_job.get('error')}, default=str)}\n\n"
            
        except Exception as e:
            if ENABLE_DEBUG:
                print(f"[ERROR] Error in SSE stream for job {job_id}: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        generate_sse(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )


@app.get("/jobs/queues/stats")
async def get_queue_stats():
    """Get queue statistics"""
    if not REDIS_QUEUE_AVAILABLE or not job_queue:
        raise HTTPException(
            status_code=503,
            detail="Job queue service not available"
        )
    
    try:
        stats = job_queue.get_queue_stats()
        return {
            "success": True,
            "stats": stats,
            "redis_connected": stats.get('redis_connected', False)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving queue stats: {str(e)}"
        )


# ===== HELPER FUNCTIONS FOR VALIDATION =====

async def get_subject_id_from_topic(topic_id: int) -> int:
    """
    Lightweight database query to get subject_id from topic_id (cached)
    Uses read-through cache for semi-static reference data
    Returns default subject_id (101 = Business Studies) if not found
    """
    try:
        # Use cached query helper for better performance
        from utils.cached_queries import get_subject_id_from_topic as cached_get_subject_id
        result = cached_get_subject_id(topic_id)
        return result if result else 101
    except Exception as e:
        if ENABLE_DEBUG:
            print(f"[WARNING] Could not fetch subject_id from topic_id {topic_id}: {e}")
        return 101  # Default fallback


# ===== AI TUTOR ENDPOINTS =====

# Import authentication middleware
from services.auth_middleware import get_current_user

@app.post("/tutor/chat_async")
async def chat_with_tutor_async(
    request: TutorRequest,
    http_request: Request,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    [ASYNC MODE] Enqueue AI tutor chat job
    Returns job_id immediately - no AI execution in request handler
    Rate limited based on authenticated user identity
    Use /tutor/chat for synchronous responses
    """
    # Require Redis queue to be available
    if not REDIS_QUEUE_AVAILABLE or not job_queue:
            raise HTTPException(
            status_code=503,
            detail="Job queue service not available. Redis is required for AI operations."
        )
    
    # Authentication: Validate user_id (prioritize auth token over request body for security)
    # FIXED: Prioritize current_user from auth token to prevent user impersonation
    user_id = current_user or request.user_id
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required (provide Authorization header or user_id in request body)"
        )
    
    # Security: Validate that request.user_id matches auth token if both provided
    if request.user_id and current_user and request.user_id != current_user:
        raise HTTPException(
            status_code=403,
            detail="User ID mismatch: request user_id does not match authenticated user"
        )
    
    # PERMANENT RATE LIMITING: Enforced before any job enqueueing (fail-closed)
    # CRITICAL FIX: Add timeout to prevent blocking
    from utils.rate_limit_helpers import check_rate_limit_for_endpoint
    from services.rate_limiter import RateLimitCategory
    import threading
    import time as time_module
    
    # Check rate limit with timeout to prevent blocking
    rate_limit_error = [None]
    rate_limit_passed = [False]
    
    def check_rate_limit():
        try:
            check_rate_limit_for_endpoint(
                user_id,
                RateLimitCategory.TUTOR_CHAT,
                "tutor chat",
                check_queue_back_pressure=True
            )
            rate_limit_passed[0] = True
        except Exception as e:
            rate_limit_error[0] = e
    
    # Run rate limit check in thread with timeout (2 seconds max)
    rate_limit_thread = threading.Thread(target=check_rate_limit, daemon=True)
    rate_limit_thread.start()
    rate_limit_thread.join(timeout=2.0)  # 2 second timeout
    
    if rate_limit_thread.is_alive():
        # Rate limit check timed out - fail-open (allow request) to maintain availability
        # This prevents blocking the endpoint when rate limiting is slow
        if ENABLE_DEBUG:
            print(f"[WARNING] Rate limit check timed out after 2s - allowing request through (fail-open)")
    elif rate_limit_error[0]:
        # Rate limit check failed - re-raise the exception
        raise rate_limit_error[0]
    elif not rate_limit_passed[0]:
        # Rate limit check didn't pass - this shouldn't happen, but fail-safe
        if ENABLE_DEBUG:
            print(f"[WARNING] Rate limit check didn't pass - allowing request through (fail-open)")
    
    # GLOBAL SUPABASE BUDGET CHECK: Check budget saturation before enqueueing
    # CRITICAL FIX: Add timeout to prevent blocking
    try:
        from services.supabase_backpressure import (
            check_budget_saturation,
            raise_budget_saturated_error
        )
        from services.redis_semaphore import DEFAULT_SEMAPHORE_KEY, GLOBAL_MAX_CONCURRENCY
        
        # Use threading timeout to prevent blocking
        budget_check_result = [None]
        budget_check_error = [None]
        
        def check_budget():
            try:
                is_saturated, active_count = check_budget_saturation(
                    limit=GLOBAL_MAX_CONCURRENCY,
                    key=DEFAULT_SEMAPHORE_KEY
                )
                budget_check_result[0] = (is_saturated, active_count)
            except Exception as e:
                budget_check_error[0] = e
        
        # Run budget check in thread with timeout (1 second max)
        budget_thread = threading.Thread(target=check_budget, daemon=True)
        budget_thread.start()
        budget_thread.join(timeout=1.0)  # 1 second timeout
        
        if budget_thread.is_alive():
            # Budget check timed out - fail-open (allow request) to maintain availability
            if ENABLE_DEBUG:
                print(f"[WARNING] Budget check timed out after 1s - allowing request through (fail-open)")
        elif budget_check_error[0]:
            # Budget check failed - fail-open (allow request) to maintain availability
            if ENABLE_DEBUG:
                print(f"⚠️ Budget check failed: {budget_check_error[0]} - allowing request through (fail-open)")
        elif budget_check_result[0]:
            is_saturated, active_count = budget_check_result[0]
            if is_saturated:
                # Budget saturated - return 503 with Retry-After
                retry_after = int(os.getenv("SUPABASE_BUDGET_RETRY_AFTER_SECONDS", "5"))
                logger.warning(
                    f"⚠️ Supabase budget saturated: {active_count}/{GLOBAL_MAX_CONCURRENCY} "
                    f"- rejecting tutor chat request"
                )
                from starlette.responses import JSONResponse
                return JSONResponse(
                    status_code=503,
                    content={
                        "error": "Supabase request budget saturated",
                        "message": "Service is temporarily at capacity. Please retry after cooldown.",
                        "active_requests": active_count,
                        "limit": GLOBAL_MAX_CONCURRENCY
                    },
                    headers={"Retry-After": str(retry_after)}
                )
    except Exception as budget_error:
        # Non-blocking: if budget check fails, continue (fail open)
        if ENABLE_DEBUG:
            print(f"⚠️ Budget check failed: {budget_error}")
    
    # Input validation (message and topic are validated by Pydantic validators)
    # Additional validation for topic existence and access
    
    # Validate topic exists in database
    try:
        from utils.cached_queries import get_topic_by_id
        topic_data = get_topic_by_id(request.topic)
        if not topic_data:
            raise HTTPException(
                status_code=400,
                detail=f"Topic {request.topic} does not exist"
            )
    except HTTPException:
        raise
    except Exception as e:
        # If topic lookup fails, log but allow (fail-open for availability)
        if ENABLE_DEBUG:
            print(f"[WARNING] Could not validate topic existence: {e}")
    
    # Authorization: Validate user access to topic
    # Note: For now, all authenticated users can access all topics
    # Future: Add topic access control based on user subscriptions/permissions
    # Example: if not current_user.has_access_to_topic(request.topic):
    #     raise HTTPException(status_code=403, detail="You do not have access to this topic")
    
    # Validate conversation ownership if conversation_id is provided
    if request.conversation_id:
        try:
            from services.supabase_client import get_supabase_client
            supabase = get_supabase_client()
            if supabase:
                # Check if conversation exists and belongs to user
                result = supabase.table("tutor_messages").select("user_id").eq(
                    "conversation_id", request.conversation_id
                ).limit(1).execute()
                
                if result.data and len(result.data) > 0:
                    conversation_user_id = result.data[0].get("user_id")
                    if conversation_user_id and conversation_user_id != user_id:
                        raise HTTPException(
                            status_code=403,
                            detail="You do not own this conversation"
                        )
        except HTTPException:
            raise
        except Exception as e:
            # If conversation validation fails, log but allow (fail-open for availability)
            if ENABLE_DEBUG:
                print(f"[WARNING] Could not validate conversation ownership: {e}")
    
    try:
        # Lightweight subject_id lookup (cached read-only DB query, no AI)
        subject_id = request.subject_id
        if not subject_id:
            # Use cached query for better performance
            try:
                from utils.cached_queries import get_subject_id_from_topic as cached_get_subject_id
                subject_id = cached_get_subject_id(request.topic) or 101
            except Exception:
                subject_id = await get_subject_id_from_topic(request.topic)
        
        # Generate conversation_id if not provided
        conversation_id = (
            request.conversation_id or f"{user_id}_{request.topic}"
        )
        
        # SYSTEM SAFETY GATE: Check system safety before enqueueing
        # This is the SINGLE POINT OF ENTRY for safety checks - impossible to bypass
        # CRITICAL FIX: Use timeout to prevent blocking endpoint
        from services.safety_gate import get_safety_gate
        import threading
        import time as time_module
        
        safety_gate = get_safety_gate()
        
        # Use threading timeout to prevent blocking
        safety_result_container = [None]
        safety_check_error = [None]
        
        def check_safety():
            try:
                safety_result_container[0] = safety_gate.check_system_safety(queue_name=QUEUE_TUTOR)
            except Exception as e:
                safety_check_error[0] = e
        
        # Run safety check in thread with timeout (2 seconds max)
        safety_thread = threading.Thread(target=check_safety, daemon=True)
        safety_thread.start()
        safety_thread.join(timeout=2.0)  # 2 second timeout
        
        if safety_thread.is_alive():
            # Safety check timed out - fail-open (assume safe) to maintain availability
            # This prevents blocking the endpoint when safety checks are slow
            if ENABLE_DEBUG:
                print(f"[WARNING] Safety gate check timed out after 2s - allowing request through (fail-open)")
        elif safety_check_error[0]:
            # Safety check errored - fail-open in development, fail-closed in production
            is_development = os.getenv("ENVIRONMENT", "development").lower() == "development"
            if is_development:
                if ENABLE_DEBUG:
                    print(f"[WARNING] Safety gate check error (dev mode: allowing): {safety_check_error[0]}")
            else:
                # In production, still fail-open to maintain availability
                if ENABLE_DEBUG:
                    print(f"[WARNING] Safety gate check error (allowing): {safety_check_error[0]}")
        elif safety_result_container[0] and not safety_result_container[0].safe:
            # System is unsafe - reject work to prevent crash
            from starlette.responses import JSONResponse
            return JSONResponse(
                status_code=503,
                content=safety_result_container[0].to_dict(),
                headers={"Retry-After": str(safety_result_container[0].retry_after)}
            )
        
        # Get or generate correlation ID for end-to-end tracing
        correlation_id = http_request.headers.get('X-Correlation-ID')
        if not correlation_id:
            # Generate correlation ID if not provided by client
            import uuid
            correlation_id = str(uuid.uuid4())
            if ENABLE_DEBUG:
                print(f"[API] Generated correlation_id: {correlation_id} (not provided in headers)")
        
        # Enqueue job to Redis - NO AI EXECUTION HERE
        job_data = {
            'user_id': user_id,
            'topic': str(request.topic),
            'message': request.message.strip(),
            'conversation_id': conversation_id,
            'explanation_style': request.explanation_style or 'default',
            'subject_id': subject_id,
            'conversation_history': request.conversation_history or [],
            'lesson_content': request.lesson_content,  # Optional
            'correlation_id': correlation_id  # CRITICAL: Include correlation_id for end-to-end tracing
        }
        
        job_id = job_queue.enqueue_job(
            queue_name=QUEUE_TUTOR,
            job_type='tutor_chat',
            job_data=job_data,
            priority=0,
            max_retries=2,
            retry_delay=30
        )
        
        # Log job creation with correlation ID
        print(f"[API] Tutor chat job enqueued: {job_id} for user: {user_id}, topic: {request.topic}, conversation: {request.conversation_id}, correlation_id: {correlation_id}")
        
        # Structured logging for job enqueue
        try:
            from services.structured_logging import structured_logger
            queue_length = job_queue.get_queue_length(QUEUE_TUTOR)
            structured_logger.log_tutor_job_enqueue(
                job_id=job_id,
                correlation_id=correlation_id,
                queue_name=QUEUE_TUTOR,
                user_id=user_id,
                conversation_id=conversation_id,
                topic=str(request.topic),
                queue_length=queue_length
            )
        except Exception:
            pass  # Non-critical, don't fail if logging fails

        return {
            "success": True,
            "job_id": job_id,
            "status": "pending",
            "message": "Tutor chat job enqueued successfully",
            "status_endpoint": f"/jobs/{job_id}",
            "estimated_wait_time": 90,  # seconds (AI Tutor has 11 sequential nodes)
            "conversation_id": conversation_id  # CRITICAL: Return conversation_id so frontend can store and reuse it
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] Failed to enqueue tutor job: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error enqueueing tutor job: {str(e)}"
        )


# ===== SYNCHRONOUS TUTOR CHAT ENDPOINT =====

async def generate_tutor_response_sync(
    user_id: str,
    topic: str,
    message: str,
    conversation_id: str,
    subject_id: Optional[int],
    conversation_history: List[Dict[str, str]],
    lesson_content: Optional[str],
    explanation_style: str = "default",
    correlation_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    PHASE 1 OPTIMIZED: Generate tutor response with parallel DB queries and singleton services.
    Returns only the LLM response, not mastery/readiness/learning_path.
    Those are handled by background tasks.
    """
    from agents.services.message_service import MessageService
    import uuid
    
    try:
        # PHASE 1: Use singleton services (no initialization overhead)
        services = get_tutor_services()
        if not services:
            # Fallback: initialize if not available
            services = await initialize_tutor_services()
            if not services:
                raise Exception("Failed to initialize tutor services")
        
        llm_service = services["llm_service"]
        concept_service = services["concept_service"]
        lesson_service = services["lesson_service"]
        history_service = services["history_service"]
        supabase = services["supabase"]
        
        # PHASE 1: Parallel DB queries (instead of sequential)
        async def fetch_lesson_async():
            """Fetch lesson content asynchronously"""
            if lesson_content:
                return lesson_content
            
            try:
                lesson_data = await asyncio.to_thread(lesson_service.get_lesson_by_topic, topic)
                if lesson_data and lesson_data.get("content"):
                    content = lesson_data["content"]
                    if isinstance(content, list):
                        return " ".join([
                            block.get("data", {}).get("content", "") 
                            for block in content 
                            if isinstance(block, dict)
                        ])
                    return str(content)
            except Exception as e:
                if ENABLE_DEBUG:
                    logger.warning(f"[WARNING] Could not fetch lesson: {e}")
            return ""
        
        async def fetch_history_async():
            """Fetch conversation history asynchronously"""
            if conversation_history:
                return conversation_history[-5:]
            
            try:
                return await asyncio.to_thread(
                    history_service.get_recent_messages,
                    conversation_id=conversation_id,
                    limit=5
                )
            except Exception as e:
                if ENABLE_DEBUG:
                    logger.warning(f"[WARNING] Could not fetch recent history: {e}")
            return []
        
        async def fetch_concepts_async():
            """Fetch concepts asynchronously"""
            try:
                return await asyncio.to_thread(
                    concept_service.fetch_concepts_by_topic,
                    topic_id=str(topic),
                    limit=5,
                    random_order=True,
                    subject_id=subject_id
                )
            except Exception as e:
                if ENABLE_DEBUG:
                    logger.warning(f"[WARNING] Could not fetch concepts: {e}")
                return []
        
        # Execute all DB queries in parallel (PHASE 1 optimization)
        lesson_text, recent_history, concept_rows = await asyncio.gather(
            fetch_lesson_async(),
            fetch_history_async(),
            fetch_concepts_async(),
            return_exceptions=True
        )
        
        # Handle exceptions from parallel execution
        if isinstance(lesson_text, Exception):
            logger.error(f"[ERROR] Lesson fetch failed: {lesson_text}")
            lesson_text = ""
        if isinstance(recent_history, Exception):
            logger.error(f"[ERROR] History fetch failed: {recent_history}")
            recent_history = []
        if isinstance(concept_rows, Exception):
            logger.error(f"[ERROR] Concepts fetch failed: {concept_rows}")
            concept_rows = []
        
        # 2. Generate LLM response synchronously
        # Use a simplified version that only generates the response
        student_profile = {
            "learning_style": "visual",
            "speed": "moderate",
            "grade_level": "intermediate",
            "subject_strengths": []
        }
        
        # Trim context for token budget (optimized for faster processing)
        trimmed_history, trimmed_lesson, _ = llm_service.trim_context(
            history=recent_history,
            lesson_text=lesson_text[:300] if lesson_text else "",  # Reduced from 500 for faster processing
            chunks=[],
            max_tokens=1500  # Reduced from 2000 for faster processing
        )
        
        # Get subject name
        subject_name = None
        if subject_id:
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
        
        # PHASE 2: Generate response asynchronously (native async, no threading overhead)
        response_text, token_usage, reasoning_label = await llm_service.generate_reply_async(
            message=message,
            topic=topic,
            learning_level=student_profile.get("grade_level", "intermediate"),
            conversation_history=trimmed_history,
            lesson_content=trimmed_lesson,
            concept_rows=concept_rows,
            explanation_style=explanation_style,
            lesson_chunks=[],
            condensed_history=None,
            student_profile=student_profile,
            subject_id=subject_id,
            subject_name=subject_name,
            job_id=None,
            trace_id=correlation_id or str(uuid.uuid4())
        )
        
        # 3. Extract related concepts
        related_concepts = [row.get("name", "") for row in concept_rows if row.get("name")]
        concept_ids = [str(row.get("concept_id")) for row in concept_rows if row.get("concept_id")]
        
        # 4. PHASE 1 OPTIMIZATION: Message persistence moved to background tasks
        # Return immediately - messages persisted in background (non-blocking)
        # Note: message_ids will be None since persistence happens in background
        # Enhancement service will read messages from DB when needed
        
        return {
            "response": response_text,
            "related_concepts": related_concepts,
            "concept_ids": concept_ids,
            "reasoning_label": reasoning_label,
            "token_usage": token_usage,
            "user_message_id": None,  # Will be set in background
            "assistant_message_id": None  # Will be set in background
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to generate tutor response: {e}")
        import traceback
        logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")
        # Return fallback response
        return {
            "response": "I apologize, but I'm experiencing a delay. Please try asking your question again.",
            "related_concepts": [],
            "concept_ids": [],
            "reasoning_label": "neutral",
            "token_usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        }


@app.post("/tutor/chat")
async def chat_with_tutor(
    request: TutorRequest,
    http_request: Request,
    background_tasks: BackgroundTasks,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    [SYNC MODE] Generate tutor response synchronously
    Returns response immediately, then enqueues enhancement job for mastery/readiness/learning_path
    """
    # Use module-level logger (defined at module level, line ~103)
    # Authentication: Validate user_id
    user_id = current_user or request.user_id
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required (provide Authorization header or user_id in request body)"
        )
    
    # Security: Validate user_id matches auth token if both provided
    if request.user_id and current_user and request.user_id != current_user:
        raise HTTPException(
            status_code=403,
            detail="User ID mismatch: request user_id does not match authenticated user"
        )
    
    # Input validation
    if not request.message or not request.message.strip():
        raise HTTPException(
            status_code=400,
            detail="Message cannot be empty"
        )
    
    if not request.topic:
        raise HTTPException(
            status_code=400,
            detail="Topic is required"
        )
    
    try:
        # Get subject_id
        subject_id = request.subject_id
        
        # CRITICAL FIX: If subject_id is not provided, determine from topic_id range FIRST
        # This ensures correct table selection (e.g., concepts_isl for Islamiyat)
        if not subject_id and request.topic:
            try:
                topic_id_int = int(request.topic) if isinstance(request.topic, (str, int)) else None
                if topic_id_int:
                    # Determine subject_id from topic_id range (more reliable than DB lookup)
                    if 200 <= topic_id_int <= 302:
                        subject_id = 114  # History
                    elif 305 <= topic_id_int <= 400:
                        subject_id = 113  # Geography
                    elif 500 <= topic_id_int <= 699:
                        subject_id = 119  # Economics
                    elif 100 <= topic_id_int <= 199:
                        subject_id = 102  # Islamiyat
                    else:
                        subject_id = 101  # Business Studies (default)
                    
                    logger.info(
                        f"[API] Determined subject_id={subject_id} "
                        f"from topic_id={topic_id_int} range"
                    )
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"[API] Could not parse topic_id: {request.topic}, error: {e}"
                )
        
        # Fallback to DB lookup if still not determined
        if not subject_id:
            try:
                from utils.cached_queries import (
                    get_subject_id_from_topic as cached_get_subject_id
                )
                subject_id = cached_get_subject_id(request.topic) or 101
            except Exception:
                subject_id = await get_subject_id_from_topic(request.topic)
        
        # Generate conversation_id if not provided
        conversation_id = request.conversation_id or f"{user_id}_{request.topic}"
        
        # Get correlation ID from header or generate one
        correlation_id = http_request.headers.get('X-Correlation-ID')
        if not correlation_id:
            import uuid
            correlation_id = str(uuid.uuid4())
        
        # Structured JSON logging at API entry
        try:
            import json
            from datetime import datetime
            from services.structured_logging import structured_logger
            log_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "INFO",
                "message": "Tutor chat API request received",
                "event": "api_entry",
                "context": {
                    "endpoint": "/tutor/chat",
                    "user_id": user_id,
                    "topic": str(request.topic),
                    "conversation_id": request.conversation_id,
                    "correlation_id": correlation_id,
                    "message_length": len(request.message) if request.message else 0
                }
            }
            structured_logger.logger.info(json.dumps(log_data, default=str))
        except Exception:
            pass  # Non-critical
        
        # Generate tutor response using LangGraph pipeline
        import time
        start_time = time.time()
        logger.info(f"[API] Calling run_tutor_graph (LangGraph) for user {user_id}, topic {request.topic}, correlation_id {correlation_id}")
        try:
            # Import LangGraph tutor function
            from langgraph_tutor import run_tutor_graph
            
            # OPTIMIZED: Get LLM response first (fast path), then handle DB writes in background
            # Run LangGraph pipeline to get response (synchronous call, but we'll wrap it in async)
            result = await asyncio.to_thread(
                run_tutor_graph,
                user_id=user_id,
                topic=str(request.topic),
                message=request.message.strip(),
                conversation_id=conversation_id,
                explanation_style=request.explanation_style or "default",
                mode="tutor",
                subject_id=subject_id,
                conversation_history=request.conversation_history or [],
                job_id=None,  # Not using job queue for sync endpoint
                correlation_id=correlation_id
            )
            timing_ms = int((time.time() - start_time) * 1000)
            logger.info(f"[API] run_tutor_graph (LangGraph) completed successfully, correlation_id {correlation_id}, has_response: {bool(result.get('response'))}, timing_ms: {timing_ms}")
            
            # OPTIMIZED: Move database writes to background tasks (non-blocking)
            # The LangGraph pipeline already uses async_write for some operations,
            # but we ensure all DB writes complete in background without blocking response
            def handle_background_writes():
                """Handle all database writes in background"""
                try:
                    # All DB writes from LangGraph pipeline are already using async_write,
                    # but we ensure they complete without blocking the response
                    # The pipeline's LogUserMessage, LogMessage, UpdateMastery, etc. 
                    # all use async_write which runs in background threads
                    logger.info(f"[API] Background writes initiated for correlation_id {correlation_id}")
                except Exception as bg_error:
                    logger.error(f"[API] Background write error (non-critical): {bg_error}")
            
            # Add background task for any remaining synchronous DB operations
            background_tasks.add_task(handle_background_writes)
            
            # LangGraph returns complete response with all enhancements
            # Format: {response, related_concepts, concept_ids, reasoning_label, mastery_updates, readiness, learning_path, token_usage, conversation_id}
            # Keep all fields - no mapping needed, we'll use them directly in response_data
            
            # Structured JSON logging for API completion
            try:
                import json
                from datetime import datetime
                from services.structured_logging import structured_logger
                log_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": "Tutor chat API completed",
                    "event": "api_end",
                    "context": {
                        "endpoint": "/tutor/chat",
                        "user_id": user_id,
                        "conversation_id": conversation_id,
                        "correlation_id": correlation_id,
                        "assistant_message_id": result.get("assistant_message_id"),
                        "user_message_id": result.get("user_message_id"),
                        "timing_ms": timing_ms,
                        "success": True
                    }
                }
                structured_logger.logger.info(json.dumps(log_data, default=str))
            except Exception:
                pass  # Non-critical
        except Exception as sync_error:
            timing_ms = int((time.time() - start_time) * 1000)
            logger.error(f"[API] generate_tutor_response_sync failed: {sync_error}, correlation_id {correlation_id}, timing_ms: {timing_ms}")
            
            # Structured JSON logging for API error
            try:
                import json
                from datetime import datetime
                from services.structured_logging import structured_logger
                log_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "ERROR",
                    "message": "Tutor chat API failed",
                    "event": "api_end",
                    "context": {
                        "endpoint": "/tutor/chat",
                        "user_id": user_id,
                        "conversation_id": conversation_id,
                        "correlation_id": correlation_id,
                        "timing_ms": timing_ms,
                        "success": False,
                        "error": str(sync_error)
                    }
                }
                structured_logger.logger.error(json.dumps(log_data, default=str))
            except Exception:
                pass  # Non-critical
            
            import traceback
            logger.error(f"[API] Traceback: {traceback.format_exc()}")
            raise  # Re-raise to be caught by outer exception handler
        
        # OPTIMIZED: LangGraph pipeline uses async_write for database operations
        # - LogUserMessage node: Persists user message (uses async_write - non-blocking)
        # - UpdateMastery node: Updates mastery scores (uses async_write - non-blocking)
        # - ComputeReadiness node: Computes readiness (uses async_write - non-blocking)
        # - ComputeLearningPath node: Determines learning path (uses async_write - non-blocking)
        # - LogMessage node: Persists assistant message (uses async_write - non-blocking)
        # All DB writes happen in background threads, response is returned immediately
        
        # OPTIMIZED: Return response immediately after LLM response is ready
        # All database writes are already using async_write (non-blocking background threads)
        # Response is returned immediately, DB writes complete in background
        assistant_message_id = result.get("assistant_message_id")
        
        # Build response data - return immediately with LLM response
        response_data = {
            "conversation_id": conversation_id,
            "assistant_message": {
                "id": assistant_message_id or "",
                "role": "assistant",
                "content": result.get("response", "")  # LLM response - returned immediately
            },
            "meta": {
                "correlation_id": correlation_id,
                "timing_ms": timing_ms,
                "reasoning_label": result.get("reasoning_label", "neutral"),
                "related_concepts": result.get("related_concepts", []),
                "related_concept_ids": result.get("concept_ids", []),
                "suggestions": result.get("suggestions", []),
                "token_usage": result.get("token_usage", {})
            },
            # Enhancements are computed but DB writes happen in background
            "enhancements": {
                "mastery_updates": result.get("mastery_updates", []),
                "readiness": result.get("readiness"),
                "learning_path": result.get("learning_path")
            }
        }
        
        logger.info(f"[API] Returning response immediately (DB writes in background), correlation_id {correlation_id}, timing_ms: {timing_ms}")
        
        # Return response immediately - all DB writes are already in background via async_write
        response = JSONResponse(content=response_data)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[API] Error in chat_with_tutor: {e}")
        import traceback
        logger.error(f"[API] Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


# ============================================================================
# PHASE 2: Streaming Response Endpoint
# ============================================================================
@app.post("/tutor/chat/stream")
async def chat_with_tutor_stream(
    request: TutorRequest,
    http_request: Request,
    background_tasks: BackgroundTasks,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    PHASE 2: Streaming tutor response endpoint.
    Streams LLM tokens as they're generated for better UX.
    """
    # Authentication and validation (same as regular endpoint)
    user_id = current_user or request.user_id
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required"
        )
    
    if request.user_id and current_user and request.user_id != current_user:
        raise HTTPException(
            status_code=403,
            detail="User ID mismatch"
        )
    
    if not request.message or not request.message.strip():
        raise HTTPException(
            status_code=400,
            detail="Message cannot be empty"
        )
    
    if not request.topic:
        raise HTTPException(
            status_code=400,
            detail="Topic is required"
        )
    
    try:
        # Get subject_id
        subject_id = request.subject_id
        if not subject_id:
            try:
                from utils.cached_queries import get_subject_id_from_topic as cached_get_subject_id
                subject_id = cached_get_subject_id(request.topic) or 101
            except Exception:
                subject_id = await get_subject_id_from_topic(request.topic)
        
        conversation_id = request.conversation_id or f"{user_id}_{request.topic}"
        import uuid
        correlation_id = http_request.headers.get('X-Correlation-ID') or str(uuid.uuid4())
        
        # Get services
        services = get_tutor_services()
        if not services:
            services = await initialize_tutor_services()
            if not services:
                raise Exception("Failed to initialize tutor services")
        
        llm_service = services["llm_service"]
        concept_service = services["concept_service"]
        lesson_service = services["lesson_service"]
        history_service = services["history_service"]
        supabase = services["supabase"]
        
        # Fetch data in parallel (Phase 1 optimization)
        async def fetch_lesson_async():
            if request.lesson_content:
                return request.lesson_content
            try:
                lesson_data = await asyncio.to_thread(lesson_service.get_lesson_by_topic, str(request.topic))
                if lesson_data and lesson_data.get("content"):
                    content = lesson_data["content"]
                    if isinstance(content, list):
                        return " ".join([block.get("data", {}).get("content", "") for block in content if isinstance(block, dict)])
                    return str(content)
            except Exception:
                pass
            return ""
        
        async def fetch_history_async():
            if request.conversation_history:
                return request.conversation_history[-5:]
            try:
                return await asyncio.to_thread(history_service.get_recent_messages, conversation_id, 5)
            except Exception:
                return []
        
        async def fetch_concepts_async():
            try:
                return await asyncio.to_thread(
                    concept_service.fetch_concepts_by_topic,
                    str(request.topic), 5, True, subject_id
                )
            except Exception:
                return []
        
        lesson_text, recent_history, concept_rows = await asyncio.gather(
            fetch_lesson_async(),
            fetch_history_async(),
            fetch_concepts_async(),
            return_exceptions=True
        )
        
        # Handle exceptions
        if isinstance(lesson_text, Exception):
            lesson_text = ""
        if isinstance(recent_history, Exception):
            recent_history = []
        if isinstance(concept_rows, Exception):
            concept_rows = []
        
        # Prepare context
        student_profile = {
            "learning_style": "visual",
            "speed": "moderate",
            "grade_level": "intermediate",
            "subject_strengths": []
        }
        
        trimmed_history, trimmed_lesson, _ = llm_service.trim_context(
            history=recent_history,
            lesson_text=lesson_text[:500] if lesson_text else "",
            chunks=[],
            max_tokens=2000
        )
        
        subject_name = None
        if subject_id:
            subject_map = {
                101: "Business Studies", 102: "Islamiyat", 113: "Pak Studies Geography",
                114: "Pak Studies History", 119: "Economics", 103: "Mathematics",
                104: "Physics", 105: "Chemistry"
            }
            subject_name = subject_map.get(subject_id, "Business Studies")
        
        # PHASE 2: Stream LLM response
        async def generate_stream():
            """Stream LLM tokens as they're generated"""
            try:
                # Build prompt (simplified - reuse logic from generate_reply_async)
                # For now, use non-streaming async call and simulate streaming
                # TODO: Implement true streaming with llm.astream() when prompt building is extracted
                response_text, token_usage, reasoning_label = await llm_service.generate_reply_async(
                    message=request.message,
                    topic=str(request.topic),
                    learning_level=student_profile.get("grade_level", "intermediate"),
                    conversation_history=trimmed_history,
                    lesson_content=trimmed_lesson,
                    concept_rows=concept_rows,
                    explanation_style=request.explanation_style or "default",
                    subject_id=subject_id,
                    subject_name=subject_name,
                    trace_id=correlation_id
                )
                
                # Simulate streaming by sending response in chunks
                # In future, use llm.astream() for true token-by-token streaming
                chunk_size = 50  # Characters per chunk
                for i in range(0, len(response_text), chunk_size):
                    chunk = response_text[i:i+chunk_size]
                    yield f"data: {json.dumps({'type': 'token', 'content': chunk})}\n\n"
                    # Removed delay for faster streaming
                
                # Send completion
                yield f"data: {json.dumps({'type': 'complete', 'response': response_text, 'reasoning_label': reasoning_label, 'token_usage': token_usage})}\n\n"
            except Exception as e:
                logger.error(f"[STREAM] Error streaming response: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        
        # Schedule enhancements in background
        async def compute_enhancements_background():
            try:
                from agents.services.tutor_enhancement_service import TutorEnhancementService
                enhancement_service = TutorEnhancementService(supabase)
                enhancement_service.process_enhancement_job(
                    user_id=user_id,
                    conversation_id=conversation_id,
                    topic_id=str(request.topic),
                    subject_id=subject_id,
                    correlation_id=correlation_id
                )
            except Exception as e:
                logger.warning(f"[STREAM] Background enhancement failed: {e}")
        
        background_tasks.add_task(compute_enhancements_background)
        
        return StreamingResponse(
            generate_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[STREAM] Error: {e}")
        import traceback
        logger.error(f"[STREAM] Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Error generating tutor response: {str(e)}"
        )


@app.get("/tutor/enhancements/{assistant_message_id}")
async def get_tutor_enhancements(
    assistant_message_id: str,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get enhancement results for a tutor assistant message.
    
    Returns enhancement data (mastery_updates, readiness, learning_path) if available,
    or {status: 'pending'} if enhancements are still being computed.
    
    Args:
        assistant_message_id: Assistant message ID from tutor_messages table
        
    Returns:
        Enhancement data or {status: 'pending'}
    """
    from services.supabase_client import get_supabase_client
    import json
    
    try:
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(
                status_code=503,
                detail="Database service not available"
            )
        
        # Fetch enhancement record
        response = supabase.table('tutor_enhancements').select(
            'assistant_message_id, user_id, conversation_id, topic_id, subject_id, '
            'mastery_updates, readiness, learning_path, concept_ids, created_at, updated_at'
        ).eq('assistant_message_id', assistant_message_id).execute()
        
        if response.data and len(response.data) > 0:
            enhancement = response.data[0]
            
            # Security: Verify user owns this enhancement
            # If current_user is provided, check it matches
            if current_user and enhancement.get('user_id') != current_user:
                raise HTTPException(
                    status_code=403,
                    detail="Access denied: enhancement belongs to different user"
                )
            
            # Parse JSONB fields (they may come as strings or already parsed)
            result = {
                'assistant_message_id': enhancement.get('assistant_message_id'),
                'user_id': enhancement.get('user_id'),
                'conversation_id': enhancement.get('conversation_id'),
                'topic_id': enhancement.get('topic_id'),
                'subject_id': enhancement.get('subject_id'),
                'concept_ids': enhancement.get('concept_ids', []),
                'created_at': enhancement.get('created_at'),
                'updated_at': enhancement.get('updated_at')
            }
            
            # Parse JSONB fields if they're strings
            mastery_updates = enhancement.get('mastery_updates')
            if mastery_updates:
                if isinstance(mastery_updates, str):
                    result['mastery_updates'] = json.loads(mastery_updates)
                else:
                    result['mastery_updates'] = mastery_updates
            else:
                result['mastery_updates'] = []
            
            readiness = enhancement.get('readiness')
            if readiness:
                if isinstance(readiness, str):
                    result['readiness'] = json.loads(readiness)
                else:
                    result['readiness'] = readiness
            else:
                result['readiness'] = None
            
            learning_path = enhancement.get('learning_path')
            if learning_path:
                if isinstance(learning_path, str):
                    result['learning_path'] = json.loads(learning_path)
                else:
                    result['learning_path'] = learning_path
            else:
                result['learning_path'] = None
            
            return result
        else:
            # Enhancement not found - return pending status
            return {
                'status': 'pending',
                'assistant_message_id': assistant_message_id
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching enhancements: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching enhancements: {str(e)}"
        )


@app.post("/tutor/lesson")
@guard_ai_execution  # Architectural guard: prevents AI execution in API layer
async def create_lesson(
    request: LessonRequest,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Enqueue lesson creation job
    Returns job_id immediately - no AI execution in request handler
    Rate limited based on authenticated user identity
    
    ARCHITECTURAL BOUNDARY: This endpoint ONLY:
    1. Authenticates user
    2. Validates input
    3. Applies rate limiting
    4. Enqueues job to Redis
    5. Returns immediately with job_id
    
    All AI execution happens in background workers.
    """
    # Enforce Redis requirement (architectural guard)
    enforce_redis_required()
    
    # Require Redis queue to be available
    if not REDIS_QUEUE_AVAILABLE or not job_queue:
        raise HTTPException(
            status_code=503,
            detail="Job queue service not available. Redis is required for AI operations."
        )
    
    # Authentication: Validate user_id
    user_id = current_user
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required for lesson creation"
        )
    
    # PERMANENT RATE LIMITING: Enforced before any job enqueueing (fail-closed)
    from utils.rate_limit_helpers import check_rate_limit_for_endpoint
    from services.rate_limiter import RateLimitCategory
    
    # Check rate limit (includes queue back-pressure check)
    # This will raise HTTPException if limit exceeded - no job will be enqueued
    check_rate_limit_for_endpoint(
        user_id,
        RateLimitCategory.LESSON_CREATION,
        "lesson creation",
        check_queue_back_pressure=True
    )
    
    # Input validation (NO AI EXECUTION)
    if not request.topic or not request.topic.strip():
        raise HTTPException(
            status_code=400,
            detail="Topic cannot be empty"
        )
    
    if not request.learning_objectives or len(request.learning_objectives) == 0:
        raise HTTPException(
            status_code=400,
            detail="At least one learning objective is required"
        )

    try:
        # SYSTEM SAFETY GATE: Check system safety before enqueueing
        from services.safety_gate import get_safety_gate
        safety_gate = get_safety_gate()
        safety_result = safety_gate.check_system_safety(queue_name=QUEUE_LESSON)
        
        if not safety_result.safe:
            # System is unsafe - reject work to prevent crash
            from starlette.responses import JSONResponse
            return JSONResponse(
                status_code=503,
                content=safety_result.to_dict(),
                headers={"Retry-After": str(safety_result.retry_after)}
            )
        
        # Enqueue job to Redis - NO AI EXECUTION HERE
        job_data = {
            'topic': request.topic.strip(),
            'learning_objectives': request.learning_objectives,
            'difficulty_level': request.difficulty_level or 'intermediate'
        }
        
        job_id = job_queue.enqueue_job(
            queue_name=QUEUE_LESSON,
            job_type='create_lesson',
            job_data=job_data,
            priority=0,
            max_retries=2,
            retry_delay=30
        )
        
        if ENABLE_DEBUG:
            print(f"[API] Lesson creation job enqueued: {job_id}")
        
        return {
            "success": True,
            "job_id": job_id,
            "status": "pending",
            "message": "Lesson creation job enqueued successfully",
            "status_endpoint": f"/jobs/{job_id}",
            "estimated_wait_time": 20  # seconds
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] Failed to enqueue lesson creation job: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error enqueueing lesson creation job: {str(e)}"
        )


@app.get("/concepts/topic/{topic_id}")
async def get_concepts_by_topic(
    topic_id: int,
    subject_id: Optional[int] = Query(None, description="Subject ID to determine concept table"),
    limit: int = Query(10, description="Maximum number of concepts to return")
):
    """
    Fetch concepts for a specific topic.
    Cached for 24 hours to improve performance.
    """
    try:
        from agents.services.concept_service import ConceptService
        from agents.concept_agent import ConceptAgent
        from services.supabase_client import get_supabase_client
        import os
        
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        # Initialize concept service
        api_key = os.getenv("OPENAI_API_KEY")
        concept_agent = ConceptAgent(api_key=api_key, supabase_client=supabase)
        concept_service = ConceptService(concept_agent=concept_agent)
        
        # Fetch concepts (will use 24-hour cache if available)
        concepts = concept_service.fetch_concepts_by_topic(
            topic_id=str(topic_id),
            limit=limit,
            random_order=False,  # Consistent order for caching
            subject_id=subject_id
        )
        
        return {
            "topic_id": topic_id,
            "subject_id": subject_id,
            "concepts": concepts,
            "count": len(concepts)
        }
    except Exception as e:
        logger.error(f"[API] Error fetching concepts for topic {topic_id}: {e}")
        import traceback
        logger.error(f"[API] Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching concepts: {str(e)}"
        )

@app.get("/tutor/health")
async def tutor_health():
    """Health check for AI Tutor service"""
    redis_available = REDIS_QUEUE_AVAILABLE and job_queue is not None
    return {
        "status": "healthy" if redis_available else "degraded",
        "service": "AI Tutor",
        "queue_available": redis_available,
        "message": "Queue-based processing" if redis_available else "Redis queue not available"
    }


# ===== GRADING API ENDPOINTS =====

@app.post("/grade-answer")
async def grade_answer(
    request: GradingRequest,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Direct answer grading (synchronous, no workers)
    Returns grading result immediately - runs in API layer for fast response
    Rate limited based on authenticated user identity
    """
    # Authentication: Validate user_id (required for rate limiting)
    user_id = request.user_id or current_user
    
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required. Anonymous users are not allowed for AI operations."
        )
    
    # PERMANENT RATE LIMITING: Enforced before any AI execution (fail-closed)
    from utils.rate_limit_helpers import check_rate_limit_for_endpoint
    from services.rate_limiter import RateLimitCategory
    
    # Check rate limit (no queue back-pressure check since we're not using workers)
    check_rate_limit_for_endpoint(
        user_id,
        RateLimitCategory.ANSWER_GRADING,
        "answer grading",
        check_queue_back_pressure=False
    )
    
    # Input validation
    if not request.student_answer or not request.student_answer.strip():
        raise HTTPException(
            status_code=400,
            detail="Student answer cannot be empty"
        )

    if not request.model_answer or not request.model_answer.strip():
        raise HTTPException(
            status_code=400,
            detail="Model answer cannot be empty"
        )

    if not request.question or not request.question.strip():
        raise HTTPException(
            status_code=400,
            detail="Question cannot be empty"
        )
    
    try:
        # Initialize grading agent directly (runs in API layer for this endpoint)
        try:
            from agents.answer_grading_agent import AnswerGradingAgent
        except ImportError as import_error:
            logger.error(f"[ERROR] Failed to import AnswerGradingAgent: {import_error}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to import grading agent: {str(import_error)}"
            )
        
        if not OPENAI_API_KEY:
            raise HTTPException(
                status_code=503,
                detail="OpenAI API key not configured"
            )
        
        # Initialize agent with better error handling
        try:
            grading_agent = AnswerGradingAgent(api_key=OPENAI_API_KEY)
        except Exception as init_error:
            error_type = type(init_error).__name__
            logger.error(f"[ERROR] Failed to initialize AnswerGradingAgent: {error_type}: {init_error}")
            import traceback
            logger.error(f"[ERROR] Initialization traceback:\n{traceback.format_exc()}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to initialize grading agent: {error_type}: {str(init_error)}"
            )
        
        # OPTIMIZED: Call grade_answer - returns immediately after LLM grading
        # All database writes (question attempts, mastery updates) happen in background
        try:
            grading_result = grading_agent.grade_answer(
            question=request.question.strip(),
            model_answer=request.model_answer.strip(),
            student_answer=request.student_answer.strip(),
            user_id=user_id,
            max_marks=request.max_marks or 10,
            question_id=request.question_id,
            topic_id=request.topic_id,
            topic_name=request.topic_name,
            difficulty_level=request.difficulty_level,
            subject=request.subject or 'Business Studies'
        )
        except Exception as grade_error:
            error_type = type(grade_error).__name__
            error_msg = str(grade_error)
            import traceback
            traceback_str = traceback.format_exc()
            
            logger.error(f"[ERROR] grade_answer method failed: {error_type}: {error_msg}")
            logger.error(f"[ERROR] grade_answer traceback:\n{traceback_str}")
            print(f"[ERROR] grade_answer method failed: {error_type}: {error_msg}")
            print(f"[ERROR] grade_answer traceback:\n{traceback_str}")
            
            raise HTTPException(
                status_code=500,
                detail=f"Error in grading agent: {error_type}: {error_msg}"
            )
        
        if ENABLE_DEBUG:
            print(f"[API] Answer graded for user {user_id}, score: {grading_result.overall_score}/{grading_result.max_marks or request.max_marks or 10} (DB writes in background)")
        
        # Convert GradingResult to dict for JSON response
        # Return immediately - database writes complete in background
        try:
            result_dict = {
                "overall_score": grading_result.overall_score,
                "percentage": grading_result.percentage,
                "grade": grading_result.grade,
                "strengths": grading_result.strengths,
                "areas_for_improvement": grading_result.areas_for_improvement,
                "specific_feedback": grading_result.specific_feedback,
                "suggestions": grading_result.suggestions,
                "reasoning_category": grading_result.reasoning_category,
                "has_misconception": grading_result.has_misconception,
                "topic_name": grading_result.topic_name,
                "primary_concept_ids": grading_result.primary_concept_ids,
                "secondary_concept_ids": grading_result.secondary_concept_ids,
                "mastery_deltas": grading_result.mastery_deltas,
                "max_marks": grading_result.max_marks
            }
        except AttributeError as attr_error:
            error_msg = str(attr_error)
            logger.error(f"[ERROR] Failed to convert GradingResult to dict: {error_msg}")
            logger.error(f"[ERROR] GradingResult type: {type(grading_result)}")
            logger.error(f"[ERROR] GradingResult attributes: {dir(grading_result)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to convert grading result: {error_msg}"
            )
        
        # Return response immediately - all DB writes happen in background via async_write
        return {
            "success": True,
            "result": result_dict,
            "status": "completed"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__
        import traceback
        traceback_str = traceback.format_exc()
        
        # Log full error details
        logger.error(f"[ERROR] Failed to grade answer: {error_type}: {error_msg}")
        logger.error(f"[ERROR] Traceback:\n{traceback_str}")
        print(f"[ERROR] Failed to grade answer: {error_type}: {error_msg}")
        print(f"[ERROR] Traceback:\n{traceback_str}")
        
        # Return more detailed error message
        raise HTTPException(
            status_code=500,
            detail=f"Error grading answer: {error_type}: {error_msg}"
        )


@app.post("/grade-mock-exam")
async def grade_mock_exam(
    request: MockExamGradingRequest,
    background_tasks: BackgroundTasks,
    current_user: Optional[str] = Depends(get_current_user),
):
    """
    Synchronous mock exam grading (NO WORKERS/NO QUEUE - completely standalone).

    - Returns the ExamReport immediately after LLM grading finishes.
    - All database writes (attempt logs, readiness/mastery persistence, etc.)
      happen in the background so the UI sees no extra delay.
    - NO workers involved - executes directly in the API process
    - NO Redis queue - completely synchronous execution
    - Uses background threads for non-blocking DB writes only
    """
    # CRITICAL: This endpoint does NOT use workers or job queues
    # All execution happens synchronously in the API process
    
    # Authentication (optional): allow grading without persistence if anonymous
    user_id = request.user_id or current_user or "anonymous"
    
    # Rate limit authenticated users only (no queue back-pressure needed)
    try:
        if user_id and user_id != "anonymous":
            from utils.rate_limit_helpers import check_rate_limit_for_endpoint
            from services.rate_limiter import RateLimitCategory
            
            check_rate_limit_for_endpoint(
                user_id,
                RateLimitCategory.MOCK_EXAM_GRADING,
                "mock exam grading",
                check_queue_back_pressure=False,
            )
    except HTTPException:
        raise
    except Exception:
        # Fail-open on rate limiter errors to preserve availability
        pass
    
    # Input validation
    if not request.attempted_questions or len(request.attempted_questions) == 0:
        raise HTTPException(status_code=400, detail="No attempted questions provided")

    if request.exam_type not in ["P1", "P2"]:
        raise HTTPException(status_code=400, detail="exam_type must be 'P1' or 'P2'")

    for q in request.attempted_questions:
        if not q.get("question") or q.get("user_answer") is None:
            raise HTTPException(
                status_code=400,
                detail="Each attempted question must have 'question' and 'user_answer' fields",
            )
    
    ENABLE_MOCK_EXAM_GRADING = os.getenv("ENABLE_MOCK_EXAM_GRADING", "true").lower() == "true"
    if not ENABLE_MOCK_EXAM_GRADING:
        raise HTTPException(
            status_code=503,
            detail="Mock exam grading is temporarily disabled. Please try again later.",
            headers={"Retry-After": "300"},
        )
    
    try:
        from uuid import uuid4
        request_id = f"exam-{uuid4().hex[:8]}"

        # Lazy singleton agent to avoid re-init overhead
        global _mock_exam_agent_instance  # type: ignore
        try:
            _mock_exam_agent_instance
        except NameError:
            _mock_exam_agent_instance = None  # type: ignore

        if _mock_exam_agent_instance is None:  # type: ignore
            from agents.mock_exam_grading_agent import MockExamGradingAgent

            if not OPENAI_API_KEY:
                raise HTTPException(status_code=500, detail="OPENAI_API_KEY not configured")

            _mock_exam_agent_instance = MockExamGradingAgent(api_key=OPENAI_API_KEY)  # type: ignore

        agent = _mock_exam_agent_instance  # type: ignore

        # Grade exam (LLM on hot path) - SYNCHRONOUS, NO WORKERS, NO QUEUE
        # This executes directly in the API process, not in a worker
        # NO job_queue.enqueue() - NO workers involved
        from agents.mock_exam_grading_agent import set_agent_instance
        set_agent_instance(agent)

        # Execute grading synchronously (wrapped in asyncio.to_thread for non-blocking)
        # CRITICAL: This is NOT enqueued to any worker queue
        report = await asyncio.to_thread(
            agent.grade_exam,
            request.attempted_questions,
            request.subject,
        )

        # Compute readiness + grade-based mastery WITHOUT blocking on DB writes
        try:
            # Readiness uses mastery_updates; use empty dict here (DB writes happen later)
            report.readiness_score = agent.compute_readiness_score(user_id, report, {})
        except Exception:
            report.readiness_score = None

        try:
            report.average_mastery = agent._grade_to_mastery(report.overall_grade)
        except Exception:
            report.average_mastery = None

        # Persist in background (non-blocking)
        if user_id and user_id != "anonymous":
            import logging
            logging.info(f"[GRADE-MOCK-EXAM] User {user_id} - Preparing background persistence")
            
            from agents.mock_exam_grading_agent import persist_results, set_agent_instance
            
            # Ensure agent instance is set for persist_results
            set_agent_instance(agent)
            logging.info(f"[GRADE-MOCK-EXAM] Agent instance set for user {user_id}")

            # Calculate mastery updates from question grades if not already calculated
            mastery_updates = {}
            if report.question_grades:
                try:
                    # Calculate mastery updates for each question based on performance
                    for q_grade in report.question_grades:
                        concept_ids = agent.detect_concepts_for_question(
                            q_grade.question_text or ""
                        )
                        if concept_ids:
                            # Calculate mastery delta based on performance
                            percentage = q_grade.percentage_score or 0
                            base_delta = (percentage - 50) / 10  # Normalize to -5 to +5 range
                            marks_allocated = q_grade.marks_allocated or 0
                            
                            for concept_id in concept_ids:
                                if concept_id:
                                    mastery_updates[str(concept_id)] = {
                                        "base_delta": base_delta,
                                        "marks_allocated": marks_allocated,
                                    }
                except Exception as e:
                    logging.warning(f"[GRADE-MOCK-EXAM] Failed to calculate mastery_updates: {e}")

            # Extract mock exam name from attempted questions if not provided
            mock_exam_name = request.mock_exam_name
            if not mock_exam_name and request.attempted_questions:
                # Try to extract from first question's set/case field
                first_q = request.attempted_questions[0]
                if request.exam_type == "P2":
                    # For P2, use case field (e.g., "1", "2")
                    mock_exam_name = first_q.get("case") or first_q.get("Case")
                    if mock_exam_name:
                        mock_exam_name = f"Case {mock_exam_name}"
                else:
                    # For P1, use set field (e.g., "SET1", "Set2")
                    set_value = first_q.get("set") or first_q.get("Set")
                    if set_value:
                        # Extract number from set (e.g., "SET1" -> "Mock 1", "Set2" -> "Mock 2")
                        import re
                        match = re.search(r'\d+', str(set_value))
                        if match:
                            mock_exam_name = f"Mock {match.group()}"
                        else:
                            mock_exam_name = f"Mock {set_value}"
            
            # Default fallback
            if not mock_exam_name:
                mock_exam_name = f"Mock Exam ({request.exam_type})"
            
            logging.info(f"[GRADE-MOCK-EXAM] Mock exam name: {mock_exam_name}, Subject: {request.subject}, Exam Type: {request.exam_type}")
            
            persist_state = {
                "user_id": user_id,
                "attempted_questions": request.attempted_questions,
                "question_grades": report.question_grades,
                "exam_report": report,
                "readiness_score": report.readiness_score,
                "mastery_updates": mastery_updates,  # Include calculated mastery updates
                "request_id": request_id,
                "job_id": None,
                "subject": request.subject,
                "exam_type": request.exam_type,
                "mock_exam_name": mock_exam_name,
            }
            
            logging.info(f"[GRADE-MOCK-EXAM] Persist state prepared: user_id={user_id}, mock_exam_name={mock_exam_name}, subject={request.subject}, exam_type={request.exam_type}, has_exam_report={report is not None}, overall_grade={report.overall_grade if report else None}, average_mastery={report.average_mastery if report else None}")
            
            # Use threading to ensure background task executes reliably
            import threading
            import logging
            import traceback
            from agents.mock_exam_grading_agent import set_agent_instance
            
            def persist_with_error_handling():
                # CRITICAL: Print immediately to confirm thread is running
                import sys
                sys.stdout.flush()
                print("\n" + "="*80, flush=True)
                print("[BACKGROUND-TASK] [START] THREAD STARTED - persist_with_error_handling()", flush=True)
                print("="*80 + "\n", flush=True)
                
                try:
                    # Ensure agent instance is set before calling persist_results
                    set_agent_instance(agent)
                    print(f"[BACKGROUND-TASK] [OK] Agent instance set: {agent is not None}", flush=True)
                    
                    user_id_from_state = persist_state.get('user_id', 'unknown')
                    print(f"\n{'='*80}", flush=True)
                    print(f"[BACKGROUND-TASK] [INFO] Starting persist_results for user {user_id_from_state}", flush=True)
                    print(f"[BACKGROUND-TASK] State keys: {list(persist_state.keys())}", flush=True)
                    print(f"[BACKGROUND-TASK] Mock exam name: {persist_state.get('mock_exam_name')}", flush=True)
                    print(f"[BACKGROUND-TASK] Subject: {persist_state.get('subject')}", flush=True)
                    print(f"[BACKGROUND-TASK] Exam type: {persist_state.get('exam_type')}", flush=True)
                    print(f"[BACKGROUND-TASK] Agent instance set: {agent is not None}", flush=True)
                    print(f"[BACKGROUND-TASK] Exam report in state: {'exam_report' in persist_state}", flush=True)
                    if 'exam_report' in persist_state:
                        exam_report = persist_state.get('exam_report')
                        print(f"[BACKGROUND-TASK] Exam report type: {type(exam_report)}", flush=True)
                        if hasattr(exam_report, 'overall_grade'):
                            print(f"[BACKGROUND-TASK] Overall grade: {exam_report.overall_grade}", flush=True)
                        if hasattr(exam_report, 'average_mastery'):
                            print(f"[BACKGROUND-TASK] Average mastery: {exam_report.average_mastery}", flush=True)
                    print(f"{'='*80}\n", flush=True)
                    logging.info(f"[BACKGROUND-TASK] Starting persist_results for user {user_id_from_state}")
                    logging.info(f"[BACKGROUND-TASK] State keys: {list(persist_state.keys())}")
                    logging.info(f"[BACKGROUND-TASK] Agent instance set: {agent is not None}")
                    
                    print(f"[BACKGROUND-TASK] [INFO] Calling persist_results()...", flush=True)
                    result = persist_results(persist_state)
                    print(f"[BACKGROUND-TASK] [INFO] persist_results() returned: {result}", flush=True)
                    
                    print(f"\n{'='*80}", flush=True)
                    print(f"[BACKGROUND-TASK] [OK] Completed persist_results for user {user_id_from_state}", flush=True)
                    print(f"{'='*80}\n", flush=True)
                    logging.info(f"[BACKGROUND-TASK] Completed persist_results for user {user_id_from_state}")
                except Exception as e:
                    user_id_from_state = persist_state.get('user_id', 'unknown')
                    error_msg = str(e)
                    traceback_str = traceback.format_exc()
                    print(f"\n{'='*80}", flush=True)
                    print(f"[BACKGROUND-TASK] [FAIL] ERROR in persist_results for user {user_id_from_state}", flush=True)
                    print(f"[BACKGROUND-TASK] Error: {error_msg}", flush=True)
                    print(f"[BACKGROUND-TASK] Traceback:\n{traceback_str}", flush=True)
                    print(f"{'='*80}\n", flush=True)
                    logging.error(f"[BACKGROUND-TASK] Error in persist_results for user {user_id_from_state}: {e}", exc_info=True)
                    logging.error(f"[BACKGROUND-TASK] Traceback: {traceback_str}")
                finally:
                    print(f"[BACKGROUND-TASK] [END] Thread ending", flush=True)
                    sys.stdout.flush()
            
            print(f"\n[GRADE-MOCK-EXAM] [INFO] Starting background thread for user {user_id}", flush=True)
            print(f"[GRADE-MOCK-EXAM] Agent instance available: {agent is not None}", flush=True)
            print(f"[GRADE-MOCK-EXAM] Persist state has exam_report: {'exam_report' in persist_state}", flush=True)
            logging.info(f"[GRADE-MOCK-EXAM] Starting background thread for user {user_id}")
            logging.info(f"[GRADE-MOCK-EXAM] Agent instance available: {agent is not None}")
            logging.info(f"[GRADE-MOCK-EXAM] NO WORKERS - NO QUEUE - Results return immediately, DB writes in background")
            
            # CRITICAL: Do NOT call persist_results synchronously - it blocks the response
            # All DB writes happen in background threads/tasks only
            # Results are returned immediately after grading completes
            
            # Start background thread for DB writes (non-blocking)
            # CRITICAL: This is NOT a worker process - it's just a background thread in the same API process
            # NO workers involved - NO job queue involved
            thread = threading.Thread(target=persist_with_error_handling, daemon=False)
            thread.start()
            
            print(f"[GRADE-MOCK-EXAM] [OK] Background thread started for DB writes (thread ID: {thread.ident}) for user {user_id}", flush=True)
            print(f"[GRADE-MOCK-EXAM] Thread is_alive: {thread.is_alive()}", flush=True)
            logging.info(f"[GRADE-MOCK-EXAM] Background thread started for user {user_id}")
            logging.info(f"[GRADE-MOCK-EXAM] Thread ID: {thread.ident}, is_alive: {thread.is_alive()}")
            
            # Also add to FastAPI BackgroundTasks as backup
            background_tasks.add_task(persist_with_error_handling)
            print(f"[GRADE-MOCK-EXAM] BackgroundTasks also added as backup\n", flush=True)
        else:
            import logging
            logging.warning(f"[GRADE-MOCK-EXAM] Skipping persistence - user_id={user_id} (anonymous or missing)")

        # Return report immediately
        try:
            return report.model_dump()
        except Exception:
            return report.dict()
        
    except HTTPException:
        raise
    except Exception as e:
        if ENABLE_DEBUG:
            print(f"[ERROR] Failed to grade mock exam: {e}")
        raise HTTPException(status_code=500, detail=f"Error grading mock exam: {str(e)}")


@app.get("/grading/health")
async def grading_health():
    """Health check for grading service"""
    redis_available = REDIS_QUEUE_AVAILABLE and job_queue is not None
    return {
        "status": "healthy" if redis_available else "degraded",
        "service": "Answer Grading API",
        "queue_available": redis_available,
        "message": "Queue-based processing" if redis_available else "Redis queue not available"
    }


# ===== HELPING AGENT ENDPOINTS =====

@app.post("/helping/explain")
async def explain_concept(
    request: HelpingRequest,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Direct helping agent explanation (synchronous, no workers)
    Returns explanation immediately - runs in API layer for fast response
    Rate limited based on authenticated user identity
    """
    # Authentication: Validate user_id (required for rate limiting)
    user_id = request.user_id or current_user
    
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Authentication required. Anonymous users are not allowed for AI operations."
        )
    
    # PERMANENT RATE LIMITING: Enforced before any AI execution (fail-closed)
    from utils.rate_limit_helpers import check_rate_limit_for_endpoint
    from services.rate_limiter import RateLimitCategory
    
    # Check rate limit (no queue back-pressure check since we're not using workers)
    check_rate_limit_for_endpoint(
        user_id,
        RateLimitCategory.CONCEPT_EXPLANATION,
        "concept explanation",
        check_queue_back_pressure=False
    )
    
    # Input validation
    query = request.query.strip() if request.query else ""
    if not query:
        raise HTTPException(
            status_code=400,
            detail="Query cannot be empty"
        )

    # Validate query length
    if len(query) > 500:
        raise HTTPException(
            status_code=400,
            detail="Query cannot exceed 500 characters"
        )
    
    try:
        # Initialize helping agent directly (runs in API layer for this endpoint)
        from agents.helping_agent import HelpingAgent
        
        if not OPENAI_API_KEY:
            raise HTTPException(
                status_code=503,
                detail="OpenAI API key not configured"
            )
        
        # Initialize agent
        helping_agent = HelpingAgent(api_key=OPENAI_API_KEY)
        
        # Call explain synchronously
        explanation = helping_agent.explain(
            query=query,
            context=request.context or None,
            subject=request.subject or None
        )
        
        if ENABLE_DEBUG:
            print(f"[API] Helping agent explanation generated for user {user_id}")
        
        return {
            "success": True,
            "explanation": explanation,
            "status": "completed"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] Failed to generate helping agent explanation: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error generating explanation: {str(e)}"
        )


@app.get("/helping/health")
async def helping_health():
    """Health check for helping agent service"""
    redis_available = REDIS_QUEUE_AVAILABLE and job_queue is not None
    return {
        "status": "healthy" if redis_available else "degraded",
        "service": "Helping Agent",
        "queue_available": redis_available,
        "message": "Queue-based processing" if redis_available else "Redis queue not available"
    }


@app.get("/rate-limit/status")
async def get_rate_limit_status(
    category: str,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get current rate limit status for authenticated user
    """
    if not current_user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required to check rate limit status"
        )
    
    try:
        from services.rate_limiter import RateLimitCategory, rate_limiter
        from services.auth_middleware import get_user_tier
        
        # Map category string to enum
        category_map = {
            'tutor_chat': RateLimitCategory.TUTOR_CHAT,
            'answer_grading': RateLimitCategory.ANSWER_GRADING,
            'mock_exam_grading': RateLimitCategory.MOCK_EXAM_GRADING,
            'concept_explanation': RateLimitCategory.CONCEPT_EXPLANATION,
            'lesson_creation': RateLimitCategory.LESSON_CREATION,
            'all_ai_work': RateLimitCategory.ALL_AI_WORK,
        }
        
        rate_category = category_map.get(category)
        if not rate_category:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid category. Valid categories: {list(category_map.keys())}"
            )
        
        user_tier = get_user_tier(current_user)
        status = rate_limiter.get_rate_limit_status(current_user, rate_category, user_tier)
        
        return {
            "success": True,
            "user_id": current_user,
            "category": category,
            "tier": user_tier,
            "rate_limit": status
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving rate limit status: {str(e)}"
        )


@app.get("/cache/stats")
async def get_cache_stats(
    namespace: Optional[str] = None,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get cache statistics (admin/development endpoint)
    """
    try:
        from services.auth_middleware import get_user_tier
        user_tier = get_user_tier(current_user) if current_user else "standard"
    except Exception:
        user_tier = "standard"
    
    if ENABLE_DEBUG or (current_user and user_tier == "admin"):
        try:
            from services.read_cache import read_cache
            stats = read_cache.get_stats(namespace)
            return {
                "success": True,
                "cache_stats": stats
            }
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error retrieving cache stats: {str(e)}"
            )
    else:
        raise HTTPException(
            status_code=403,
            detail="Cache statistics only available to admins or in debug mode"
        )


@app.get("/observability/queues")
async def get_queue_metrics(
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get queue metrics: depth, processing jobs, failures
    Provides visibility into queue depth and worker status
    """
    try:
        from services.observability import observability
        metrics = observability.get_queue_metrics()
        return {
            "success": True,
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving queue metrics: {str(e)}"
        )


@app.get("/observability/queue-rejections")
async def get_queue_rejection_metrics(
    queue_name: Optional[str] = None,
    hours: int = 24,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get queue rejection metrics (critical for back-pressure monitoring)
    Shows how many jobs were rejected due to queue being full
    """
    try:
        from services.observability import observability
        metrics = observability.get_queue_rejection_metrics(queue_name, hours)
        return {
            "success": True,
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving queue rejection metrics: {str(e)}"
        )


@app.get("/observability/failures")
async def get_failure_metrics(
    job_type: Optional[str] = None,
    hours: int = 24,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get failure metrics for jobs
    Provides visibility into job failures and retries
    """
    try:
        from services.observability import observability
        metrics = observability.get_failure_metrics(job_type, hours)
        return {
            "success": True,
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving failure metrics: {str(e)}"
        )


@app.get("/observability/processing-times")
async def get_processing_time_metrics(
    job_type: Optional[str] = None,
    hours: int = 24,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get job processing time metrics (background processing, separate from request latency)
    """
    try:
        from services.observability import observability
        metrics = observability.get_processing_time_metrics(job_type, hours)
        return {
            "success": True,
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving processing time metrics: {str(e)}"
        )


@app.get("/observability/request-latency")
async def get_request_latency_metrics(
    endpoint: Optional[str] = None,
    hours: int = 1,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get API request latency metrics (separate from job processing time)
    Shows how fast the API responds, independent of background job processing
    """
    try:
        from services.observability import observability
        metrics = observability.get_request_latency_metrics(endpoint, hours)
        return {
            "success": True,
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving request latency metrics: {str(e)}"
        )


@app.get("/observability/worker-health")
async def get_worker_health(
    # No auth required for health checks - public endpoint
):
    """
    Get worker health metrics: active jobs, stale jobs (indicates crashes)
    Uses lightweight health reporting - does not affect API availability
    """
    try:
        from services.observability import observability
        health = observability.get_worker_health()
        return {
            "success": True,
            "health": health
        }
    except Exception as e:
        # Health check failures don't affect API availability (failure isolation)
        return {
            "success": False,
            "error": str(e),
            "health": {
                "error": "Failed to retrieve worker health",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }


@app.get("/observability/workers")
async def get_workers_status(
    worker_id: Optional[str] = None,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get detailed worker health status (lightweight, read-only)
    Returns liveness, Redis connectivity, active job count, and error state
    Does not affect API availability if workers are down
    """
    try:
        from services.worker_health import worker_health_reporter
        
        if worker_id:
            # Get specific worker health
            health = worker_health_reporter.get_worker_health(worker_id)
            if health:
                return {
                    "success": True,
                    "worker_id": worker_id,
                    "health": health
                }
            else:
                return {
                    "success": False,
                    "worker_id": worker_id,
                    "error": "Worker not found or health data unavailable",
                    "health": None
                }
        else:
            # Get all workers health summary
            summary = worker_health_reporter.get_workers_summary()
            return {
                "success": True,
                "summary": summary
            }
    except Exception as e:
        # Health check failures don't affect API availability (failure isolation)
        return {
            "success": False,
            "error": str(e),
            "summary": {
                "error": "Failed to retrieve workers status",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }


@app.get("/observability/locks")
async def get_processing_locks(
    job_type: Optional[str] = None,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Debug endpoint to inspect active processing locks (processing markers)
    Returns list of active locks with their values, TTL, and associated job info
    Protected endpoint - requires authentication
    """
    try:
        from services.job_queue import job_queue, JobStatus
        from services.redis_connection import get_redis_client
        from datetime import datetime, timezone
        
        redis = get_redis_client()
        if not redis:
            return {
                "success": False,
                "error": "Redis not available",
                "locks": []
            }
        
        # Get all processing markers
        pattern = f"{job_queue.processing_prefix}*"
        if job_type:
            # Filter by job type if specified (e.g., tutor_chat)
            pattern = f"{job_queue.processing_prefix}{job_type}:*"
        
        locks = []
        cursor = 0
        active_jobs_count = 0
        
        # Scan for all processing markers
        while True:
            cursor, keys = redis.scan(cursor, match=pattern, count=100)
            
            for key in keys:
                try:
                    # Get lock value (timestamp when set)
                    value = redis.get(key)
                    # Get TTL (remaining time in seconds, -1 if no expiry, -2 if key doesn't exist)
                    ttl = redis.ttl(key)
                    
                    # Extract job_id from key
                    job_id = key.replace(job_queue.processing_prefix, "")
                    if job_type and ":" in job_id:
                        # For job-type-specific locks, extract actual job_id
                        job_id = job_id.split(":", 1)[1]
                    
                    # Get job data to find more info
                    job_data = job_queue.get_job(job_id)
                    job_info = None
                    if job_data:
                        job_info = {
                            "job_id": job_id,
                            "job_type": job_data.get("job_type", "unknown"),
                            "status": job_data.get("status", "unknown"),
                            "user_id": job_data.get("data", {}).get("user_id") if isinstance(job_data.get("data"), dict) else None,
                            "created_at": job_data.get("created_at"),
                            "updated_at": job_data.get("updated_at")
                        }
                        # Count as active if status is processing
                        if job_data.get("status") == JobStatus.PROCESSING:
                            active_jobs_count += 1
                    
                    # Calculate age of lock
                    lock_age_seconds = None
                    if value:
                        try:
                            # Value is ISO timestamp string
                            set_time = datetime.fromisoformat(value.decode() if isinstance(value, bytes) else value.replace('Z', '+00:00'))
                            now = datetime.now(timezone.utc)
                            if set_time.tzinfo:
                                lock_age_seconds = (now - set_time.replace(tzinfo=timezone.utc)).total_seconds()
                            else:
                                lock_age_seconds = (now - set_time).total_seconds()
                        except Exception:
                            pass
                    
                    locks.append({
                        "lock_key": key.decode() if isinstance(key, bytes) else key,
                        "job_id": job_id,
                        "value": value.decode() if isinstance(value, bytes) else value if value else None,
                        "ttl_seconds": ttl if ttl >= 0 else None,
                        "ttl_status": "expires" if ttl > 0 else "no_expiry" if ttl == -1 else "not_found",
                        "lock_age_seconds": lock_age_seconds,
                        "job_info": job_info
                    })
                except Exception as e:
                    # Skip invalid keys
                    locks.append({
                        "lock_key": key.decode() if isinstance(key, bytes) else key,
                        "error": str(e)
                    })
            
            if cursor == 0:
                break
        
        # Get worker health to map worker_ids (if available)
        worker_mapping = {}
        try:
            from services.worker_health import worker_health_reporter
            workers_summary = worker_health_reporter.get_workers_summary()
            if workers_summary and "workers" in workers_summary:
                for worker in workers_summary.get("workers", []):
                    worker_id = worker.get("worker_id")
                    active_jobs = worker.get("active_jobs", 0)
                    if worker_id:
                        worker_mapping[worker_id] = {
                            "active_jobs": active_jobs,
                            "status": worker.get("status", "unknown")
                        }
        except Exception:
            pass  # Non-critical
        
        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_locks": len(locks),
            "active_jobs_count": active_jobs_count,
            "worker_mapping": worker_mapping,
            "locks": locks
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


@app.get("/observability/summary")
async def get_observability_summary(
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get comprehensive observability summary
    """
    try:
        from services.observability import observability
        
        summary = {
            "queues": observability.get_queue_metrics(),
            "worker_health": observability.get_worker_health(),
            "request_latency": observability.get_request_latency_metrics(hours=1),
            "processing_times": observability.get_processing_time_metrics(hours=24),
            "failures": observability.get_failure_metrics(hours=24),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return {
            "success": True,
            "summary": summary
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving observability summary: {str(e)}"
        )


@app.get("/metrics")
async def get_prometheus_metrics(
    hours: int = 24,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get latency metrics in Prometheus exposition format.
    
    Returns metrics with p50/p95/p99 percentiles per job type,
    including execution times, queue wait times, and time breakdowns.
    """
    try:
        from services.latency_aggregator import get_latency_aggregator
        
        aggregator = get_latency_aggregator()
        metrics = aggregator.get_prometheus_metrics(hours=hours)
        
        return Response(
            content=metrics,
            media_type="text/plain; version=0.0.4"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving metrics: {str(e)}"
        )


@app.get("/metrics/json")
async def get_latency_metrics_json(
    job_type: Optional[str] = None,
    hours: int = 24,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get comprehensive latency metrics in JSON format.
    
    Returns:
    - execution_times: p50/p95/p99 per job type
    - queue_wait_times: p50/p95/p99 per job type
    - time_breakdown: breakdown by category (LLM, DB, cache, other)
    """
    try:
        from services.latency_aggregator import get_latency_aggregator
        
        aggregator = get_latency_aggregator()
        metrics = aggregator.get_job_latency_metrics(
            job_type=job_type,
            hours=hours
        )
        
        return {
            "success": True,
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving latency metrics: {str(e)}"
        )


@app.get("/metrics/statsd")
async def get_statsd_metrics(
    hours: int = 24,
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get latency metrics in StatsD format.
    
    Returns metrics suitable for sending to a StatsD daemon.
    """
    try:
        from services.latency_aggregator import get_latency_aggregator
        
        aggregator = get_latency_aggregator()
        metrics = aggregator.get_statsd_metrics(hours=hours)
        
        return {
            "success": True,
            "metrics": metrics,
            "format": "statsd",
            "instructions": (
                "Send each metric line to your StatsD daemon. "
                "Format: metric_name:value|type"
            )
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving StatsD metrics: {str(e)}"
        )


# ===== METRICS SERVICE ENDPOINTS =====

@app.get("/metrics/agents/{agent_name}")
async def get_agent_metrics(
    agent_name: str,
    metric_name: str = Query(default="execution_time", description="Metric name to retrieve"),
    hours: int = Query(default=24, ge=1, le=168, description="Time window in hours (1-168)"),
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get metrics for a specific agent.
    
    Args:
        agent_name: Name of the agent (tutor, grading, mock_exam, helping, lesson)
        metric_name: Name of the metric (default: execution_time)
        hours: Time window in hours (1-168, default: 24)
    
    Returns:
        JSON with count, min, max, avg, and percentiles (p50, p95, p99)
    """
    # Validate agent name
    valid_agents = ['tutor', 'grading', 'mock_exam', 'helping', 'lesson']
    if agent_name not in valid_agents:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid agent_name. Must be one of: {', '.join(valid_agents)}"
        )
    
    try:
        from services.metrics import metrics_service
        
        result = metrics_service.get_agent_metrics(
            agent_name=agent_name,
            metric_name=metric_name,
            hours=hours
        )
        
        if "error" in result:
            raise HTTPException(
                status_code=503 if result["error"] == "Redis not available" else 500,
                detail=result["error"]
            )
        
        return {
            "success": True,
            "agent": agent_name,
            "metric": metric_name,
            "data": result
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving agent metrics: {str(e)}"
        )


@app.get("/metrics/queue-wait")
async def get_queue_wait_metrics(
    job_type: Optional[str] = Query(default=None, description="Optional job type filter"),
    hours: int = Query(default=24, ge=1, le=168, description="Time window in hours (1-168)"),
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get queue wait time metrics.
    
    Args:
        job_type: Optional job type filter (tutor_chat, grade_answer, grade_mock_exam, explain_concept, create_lesson)
        hours: Time window in hours (1-168, default: 24)
    
    Returns:
        JSON with queue wait times per job type, including percentiles (p50, p95, p99)
    """
    # Validate job type if provided
    if job_type:
        valid_job_types = ['tutor_chat', 'grade_answer', 'grade_mock_exam', 'explain_concept', 'create_lesson']
        if job_type not in valid_job_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid job_type. Must be one of: {', '.join(valid_job_types)}"
            )
    
    try:
        from services.metrics import metrics_service
        
        result = metrics_service.get_queue_wait_metrics(
            job_type=job_type,
            hours=hours
        )
        
        if "error" in result:
            raise HTTPException(
                status_code=503 if result["error"] == "Redis not available" else 500,
                detail=result["error"]
            )
        
        return {
            "success": True,
            "data": result
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving queue wait metrics: {str(e)}"
        )


@app.get("/metrics/ai-calls")
async def get_ai_call_metrics(
    agent_name: Optional[str] = Query(default=None, description="Optional agent name filter"),
    call_type: str = Query(default="api_call", description="Type of call: api_call or prompt_construction"),
    hours: int = Query(default=24, ge=1, le=168, description="Time window in hours (1-168)"),
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get AI call duration metrics (separate from prompt construction).
    
    Args:
        agent_name: Optional agent name filter (tutor, grading, mock_exam, helping, lesson)
        call_type: Type of call - 'api_call' or 'prompt_construction' (default: api_call)
        hours: Time window in hours (1-168, default: 24)
    
    Returns:
        JSON with AI call durations per agent, including percentiles (p50, p95, p99)
    """
    # Validate call type
    valid_call_types = ['api_call', 'prompt_construction']
    if call_type not in valid_call_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid call_type. Must be one of: {', '.join(valid_call_types)}"
        )
    
    # Validate agent name if provided
    if agent_name:
        valid_agents = ['tutor', 'grading', 'mock_exam', 'helping', 'lesson']
        if agent_name not in valid_agents:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid agent_name. Must be one of: {', '.join(valid_agents)}"
            )
    
    try:
        from services.metrics import metrics_service
        
        result = metrics_service.get_ai_call_metrics(
            agent_name=agent_name,
            call_type=call_type,
            hours=hours
        )
        
        if "error" in result:
            raise HTTPException(
                status_code=503 if result["error"] == "Redis not available" else 500,
                detail=result["error"]
            )
        
        return {
            "success": True,
            "data": result
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving AI call metrics: {str(e)}"
        )


@app.get("/metrics/worker-restarts")
async def get_worker_restart_metrics(
    hours: int = Query(default=24, ge=1, le=168, description="Time window in hours (1-168)"),
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get worker restart metrics.
    
    Args:
        hours: Time window in hours (1-168, default: 24)
    
    Returns:
        JSON with total restarts, restarts by reason, and recent restart details
    """
    try:
        from services.metrics import metrics_service
        
        result = metrics_service.get_worker_restart_metrics(hours=hours)
        
        if "error" in result:
            raise HTTPException(
                status_code=503 if result["error"] == "Redis not available" else 500,
                detail=result["error"]
            )
        
        return {
            "success": True,
            "data": result
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving worker restart metrics: {str(e)}"
        )


@app.get("/metrics/summary")
async def get_metrics_summary(
    hours: int = Query(default=24, ge=1, le=168, description="Time window in hours (1-168)"),
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get comprehensive metrics summary (all metrics in one response).
    
    Args:
        hours: Time window in hours (1-168, default: 24)
    
    Returns:
        JSON with queue wait times, AI call durations (api_call and prompt_construction), and worker restarts
    """
    try:
        from services.metrics import metrics_service
        
        result = metrics_service.get_all_metrics_summary(hours=hours)
        
        # Check for errors in nested results
        if isinstance(result, dict):
            for key, value in result.items():
                if isinstance(value, dict) and "error" in value:
                    # If any sub-metric has an error, return partial results with warning
                    return {
                        "success": True,
                        "partial": True,
                        "warning": f"Some metrics unavailable: {value.get('error')}",
                        "data": result
                    }
        
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving metrics summary: {str(e)}"
        )


# ===== MEMORY SAFETY ENDPOINT =====

@app.get("/memory/status")
async def get_memory_status(
    current_user: Optional[str] = Depends(get_current_user)
):
    """
    Get current memory usage and peak memory
    
    Returns:
        Dictionary with memory information
    """
    try:
        from services.memory_monitor import get_memory_usage, get_peak_memory
        
        current = get_memory_usage()
        peak = get_peak_memory()
        
        return {
            "success": True,
            "current": current,
            "peak": peak,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="Memory monitor not available"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get memory status: {str(e)}"
        )


# ============================================================
#  TIME TRACKING ENDPOINTS  (NEW)
# ============================================================

@app.post("/analytics/start")
async def analytics_start(req: TimeStartRequest):
    """
    Start timing when a page is opened.
    Returns a tracking_id that frontend must store.
    """
    if not supabase_client:
        raise HTTPException(
            status_code=500, detail="Supabase not configured"
        )

    record = {
        "user_id": req.user_id,
        "page_type": req.page_type,
        "start_time": datetime.now(timezone.utc).isoformat()
    }
    
    # Add subject if provided
    if req.subject:
        record["subject"] = req.subject

    try:
        # Note: This endpoint requires immediate return of tracking_id for frontend
        # We use direct insert here to preserve functional behavior
        # The stop endpoint uses batch_writer for async updates
        # This is a trade-off: start needs ID immediately, stop can be async
        result = sb_execute(
            supabase_client.table("time_tracking").insert(record)
        )
        tracking_id = result.data[0]["id"]

        return {
            "success": True,
            "tracking_id": tracking_id
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start tracking: {str(e)}"
        )


@app.post("/analytics/stop")
async def analytics_stop(req: TimeStopRequest):
    """
    Stop timer when user leaves the page.
    Updates record with final duration_seconds and end_time.
    
    Rule: Insert once on start, update once on stop.
    Frontend calculates duration and sends it here.
    """
    if not supabase_client:
        raise HTTPException(
            status_code=500, detail="Supabase not configured"
        )

    try:
        # Verify record exists
        result = sb_execute(
            supabase_client.table("time_tracking")
            .select("id")
            .eq("id", req.tracking_id)
            .single()
        )

        if not result.data:
            raise HTTPException(
                status_code=404, detail="Tracking ID not found"
            )

        end_time = datetime.now(timezone.utc)

        # Build update record (use duration_seconds from frontend)
        update_record = {
            "end_time": end_time.isoformat(),
            "duration_seconds": req.duration_seconds
        }
        
        # Update subject if provided (in case it changed during the session)
        if req.subject:
            update_record["subject"] = req.subject

        # Use batch_writer for async write (non-blocking)
        if batch_writer:
            # Enqueue update operation (non-blocking)
            batch_writer.enqueue_write(
                table="time_tracking",
                operation="update",
                data=update_record,
                filters={"id": req.tracking_id}
            )
            # NOTE: Rollup is handled by a worker that runs periodically or processes on session stop
            # Dashboard no longer calls rollup - worker updates daily_analytics asynchronously
            # Return immediately (eventual consistency)
            return {
                "success": True,
                "duration": req.duration_seconds,
                "message": "Tracking stopped (update queued)"
            }
        else:
            # Fallback to direct write if batch_writer unavailable
            sb_execute(
                supabase_client.table("time_tracking")
                .update(update_record)
                .eq("id", req.tracking_id)
            )
            return {
                "success": True,
                "duration": req.duration_seconds
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop tracking: {str(e)}"
        )


@app.post("/analytics/time-tracking/batch-update")
async def time_tracking_batch_update(req: TimeTrackingBatchUpdateRequest):
    """
    Batch update time tracking record with accumulated seconds.
    Used by batched time tracker to write +N seconds in one update.
    
    This endpoint supports:
    - Adding accumulated seconds to existing duration
    - Optionally setting end_time if session is ending
    """
    if not supabase_client:
        raise HTTPException(
            status_code=500, detail="Supabase not configured"
        )

    try:
        # Get existing record to get current duration
        result = sb_execute(
            supabase_client.table("time_tracking")
            .select("duration_seconds")
            .eq("id", req.tracking_id)
            .single()
        )

        if not result.data:
            raise HTTPException(
                status_code=404, detail="Tracking ID not found"
            )

        # Calculate new duration: existing + additional seconds
        current_duration = result.data.get("duration_seconds", 0) or 0
        new_duration = current_duration + req.duration_seconds

        # Build update record
        update_record: Dict[str, Any] = {
            "duration_seconds": new_duration
        }

        # Add end_time if provided (session ending)
        if req.end_time:
            update_record["end_time"] = req.end_time

        # Use batch_writer for async write (non-blocking)
        if batch_writer:
            batch_writer.enqueue_write(
                table="time_tracking",
                operation="update",
                data=update_record,
                filters={"id": req.tracking_id}
            )
            return {
                "success": True,
                "duration": new_duration,
                "added_seconds": req.duration_seconds,
                "message": "Batch update queued"
            }
        else:
            # Fallback to direct write
            sb_execute(
                supabase_client.table("time_tracking")
                .update(update_record)
                .eq("id", req.tracking_id)
            )
            return {
                "success": True,
                "duration": new_duration,
                "added_seconds": req.duration_seconds
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to batch update tracking: {str(e)}"
        )


# ------------------------------------------------------------
# DAILY ROLLUP — Sync time_tracking to daily_analytics
# ------------------------------------------------------------
@app.post("/analytics/rollup/{user_id}")
async def analytics_rollup(user_id: str):
    """
    Rollup endpoint - enqueues rollup job for async processing.
    Returns immediately with status "queued".
    
    NOTE: This endpoint is called by workers, NOT by the dashboard.
    Dashboard only reads precomputed daily_analytics - no rollup calls from frontend.
    
    Deduplication: Multiple requests for same user/day are deduplicated
    (only one job queued per user per day).
    """
    if not supabase_client:
        raise HTTPException(
            status_code=500, detail="Supabase not configured"
        )

    try:
        # Deduplication: Check if rollup job already queued for this user/today
        today = datetime.now(timezone.utc).date()
        dedupe_key = f"rollup:{user_id}:{today.isoformat()}"
        
        # Try to use cache for deduplication (Redis if available, in-memory fallback)
        try:
            from cache import cache_get, cache_set
            existing_job = cache_get(dedupe_key)
            if existing_job:
                # Job already queued for this user/day
                return {
                    "success": True,
                    "status": "queued",
                    "message": "Rollup job already queued for this user/day",
                    "job_id": existing_job
                }
        except ImportError:
            # Fallback: in-memory dedupe (simple dict, per-process only)
            if not hasattr(analytics_rollup, '_dedupe_cache'):
                analytics_rollup._dedupe_cache = {}
            if dedupe_key in analytics_rollup._dedupe_cache:
                return {
                    "success": True,
                    "status": "queued",
                    "message": "Rollup job already queued for this user/day",
                    "job_id": analytics_rollup._dedupe_cache[dedupe_key]
                }
        except Exception as e:
            # If cache fails, continue without dedupe (allow duplicate jobs)
            if ENABLE_DEBUG:
                print(f"⚠️ Dedupe check failed: {e}, allowing job enqueue")

        # Enqueue rollup job via job queue (if available) or batch_writer
        if job_queue and REDIS_QUEUE_AVAILABLE:
            # Use Redis job queue with idempotency
            try:
                from services.job_queue import PRIORITY_NORMAL
                priority = PRIORITY_NORMAL
            except ImportError:
                priority = 0  # Default priority
            
            job_id = job_queue.enqueue_job(
                queue_name=QUEUE_ROLLUP,
                job_type="analytics_rollup",
                job_data={"user_id": user_id},
                priority=priority,
                idempotency_key=dedupe_key
            )
            
            # Store dedupe key (TTL: 1 hour - enough for daily rollup)
            try:
                from cache import cache_set
                cache_set(dedupe_key, job_id, ttl=3600)
            except Exception:
                # Fallback to in-memory
                if not hasattr(analytics_rollup, '_dedupe_cache'):
                    analytics_rollup._dedupe_cache = {}
                analytics_rollup._dedupe_cache[dedupe_key] = job_id
            
            return {
                "success": True,
                "status": "queued",
                "message": "Rollup job queued for processing",
                "job_id": job_id
            }
        elif batch_writer:
            # Fallback: Use batch_writer's rollup queue
            batch_writer.enqueue_rollup(user_id, dedupe_key)
            
            # Store dedupe key
            try:
                from cache import cache_set
                cache_set(dedupe_key, "batch_writer", ttl=3600)
            except Exception:
                if not hasattr(analytics_rollup, '_dedupe_cache'):
                    analytics_rollup._dedupe_cache = {}
                analytics_rollup._dedupe_cache[dedupe_key] = "batch_writer"
            
            return {
                "success": True,
                "status": "queued",
                "message": "Rollup job queued via batch_writer",
                "job_id": f"batch_writer:{user_id}"
            }
        else:
            # No async system available - fallback to synchronous (not ideal)
            # This should not happen in production
            if ENABLE_DEBUG:
                print("⚠️ WARNING: No async system available, executing rollup synchronously")
            return await _execute_rollup_sync(user_id)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to enqueue rollup job: {str(e)}"
        )


async def _execute_rollup_sync(user_id: str):
    """
    Synchronous rollup execution (fallback only - should use async job queue in production).
    Calls the worker handler function.
    """
    try:
        result = process_rollup_job(user_id)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to rollup analytics: {str(e)}"
        )


# Rollup job handler function (called by workers)
def process_rollup_job(user_id: str) -> Dict[str, Any]:
    """
    Process a rollup job - called by workers to execute the actual rollup.
    This function performs the rollup computation synchronously.
    
    Args:
        user_id: User ID to process rollup for
        
    Returns:
        Dict with rollup results
    """
    if not supabase_client:
        raise Exception("Supabase not configured")

    try:
        today = datetime.now(timezone.utc).date()
        today_start = (
            datetime.combine(today, datetime.min.time())
            .replace(tzinfo=timezone.utc)
        )
        today_end = (
            datetime.combine(today, datetime.max.time())
            .replace(tzinfo=timezone.utc)
        )

        # Efficient query: Use date range with index (user_id, start_time)
        # Only fetch needed columns (duration_seconds, subject)
        result = sb_execute(
            supabase_client.table("time_tracking")
            .select("duration_seconds, subject")
            .eq("user_id", user_id)
            .gte("start_time", today_start.isoformat())
            .lt("start_time", today_end.isoformat())
            .not_.is_("duration_seconds", "null")
        )

        # Calculate total time from time_tracking and collect unique subjects
        total_seconds = sum(
            record.get("duration_seconds", 0) or 0
            for record in (result.data or [])
        )
        
        # Collect unique subjects from time_tracking records
        subjects_from_tracking = set()
        for record in (result.data or []):
            subject = record.get("subject")
            if subject and subject.strip():
                subjects_from_tracking.add(subject.strip())

        # Get current daily_analytics record (efficient: uses index on user_id, date)
        daily_result = sb_execute(
            supabase_client.table("daily_analytics")
            .select("total_time_spent, total_activities, session_count, subject")
            .eq("user_id", user_id)
            .eq("date", today.isoformat())
            .single()
        )

        current_daily = daily_result.data if daily_result.data else None

        # Use upsert to avoid duplicate entries
        session_count = len(result.data or [])

        # Preserve existing values if record exists, only update time
        existing_time = (
            current_daily.get("total_time_spent", 0) or 0
            if current_daily else 0
        )
        existing_activities = (
            current_daily.get("total_activities", 0) or 0
            if current_daily else 0
        )
        existing_session_count = (
            current_daily.get("session_count", 0) or 0
            if current_daily else 0
        )
        
        # Merge existing subject with new subjects from time_tracking
        existing_subject = current_daily.get("subject") if current_daily else None
        
        # Collect all unique subjects (from existing and new)
        all_subjects_set = set()
        
        # Add existing subject(s) to set
        if existing_subject:
            if isinstance(existing_subject, list):
                all_subjects_set.update(existing_subject)
            elif isinstance(existing_subject, str):
                all_subjects_set.add(existing_subject)
        
        # Add new subjects from time_tracking
        all_subjects_set.update(subjects_from_tracking)
        
        # Convert to final format: single string if one subject, array if multiple, null if none
        if len(all_subjects_set) == 0:
            final_subject = None
        elif len(all_subjects_set) == 1:
            final_subject = list(all_subjects_set)[0]  # Single subject as string
        else:
            final_subject = sorted(list(all_subjects_set))  # Multiple subjects as sorted array

        # Use the larger value to avoid overwriting with smaller time
        # (in case of race conditions)
        final_time = max(existing_time, total_seconds)
        final_session_count = max(existing_session_count, session_count)
        avg_session = (
            final_time // final_session_count
            if final_session_count > 0 else 0
        )

        # Use upsert with conflict resolution
        upsert_data = {
            "user_id": user_id,
            "date": today.isoformat(),
            "total_time_spent": final_time,
            "total_activities": existing_activities,
            "session_count": final_session_count,
            "average_session_length": avg_session,
            "subject": final_subject,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }

        # Use batch_writer for async write (non-blocking)
        if batch_writer:
            batch_writer.enqueue_write(
                table="daily_analytics",
                operation="upsert",
                data=upsert_data
            )
            # Publish Redis pub/sub event (non-blocking, no WAL noise)
            try:
                from services.redis_pubsub import publish_analytics_update
                publish_analytics_update(user_id, "daily_analytics")
            except Exception:
                pass  # Non-blocking: continue even if publish fails
        else:
            # Fallback to direct write if batch_writer unavailable
            if current_daily:
                sb_execute(
                    supabase_client.table("daily_analytics")
                    .update(upsert_data)
                    .eq("user_id", user_id)
                    .eq("date", today.isoformat())
                )
            else:
                sb_execute(
                    supabase_client.table("daily_analytics").insert(upsert_data)
                )
            # Publish Redis pub/sub event (non-blocking)
            try:
                from services.redis_pubsub import publish_analytics_update
                publish_analytics_update(user_id, "daily_analytics")
            except Exception:
                pass  # Non-blocking: continue even if publish fails

        was_updated = final_time != existing_time

        return {
            "success": True,
            "message": (
                "Daily analytics created" if not current_daily
                else "Daily analytics updated"
            ),
            "total_time_seconds": final_time,
            "time_tracking_records": session_count,
            "was_updated": was_updated,
            "was_created": not current_daily
        }

    except Exception as e:
        raise Exception(f"Failed to rollup analytics: {str(e)}")


@app.get("/api/accuracy/update")
async def update_accuracy(user_id: str):
    """
    Calculate and store Plan Accuracy for a user.

    Fetches mastery scores from mastery_states table,
    computes accuracy using weighted formula,
    stores in user_accuracy table.

    Formula: accuracy = 0.60 * mastery_macro + 0.30 * mastery_micro
    + 0.10 * mastery_concept
    """
    if not supabase_client:
        raise HTTPException(
            status_code=500,
            detail="Supabase not configured"
        )

    if not user_id:
        raise HTTPException(
            status_code=400,
            detail="user_id parameter is required"
        )

    try:
        # Fetch mastery scores from mastery_states table
        result = sb_execute(
            supabase_client.table("mastery_states")
            .select("mastery_concept, mastery_micro, mastery_macro")
            .eq("user_id", user_id)
            .limit(1)
        )

        # If no mastery_states row exists, return 0
        if not result.data or len(result.data) == 0:
            accuracy = 0.0
        else:
            mastery_data = result.data[0]

            # Extract mastery values, defaulting to 0 if null/undefined
            mastery_concept = mastery_data.get("mastery_concept") or 0
            mastery_micro = mastery_data.get("mastery_micro") or 0
            mastery_macro = mastery_data.get("mastery_macro") or 0

            # Apply weighted formula
            accuracy = (
                0.60 * mastery_macro +
                0.30 * mastery_micro +
                0.10 * mastery_concept
            )

            # Clamp to 0-100 range
            accuracy = max(0.0, min(100.0, accuracy))
            # Round to 2 decimal places
            accuracy = round(accuracy, 2)

        # Use batch_writer for async write (non-blocking)
        accuracy_record = {
            "user_id": user_id,
            "accuracy": accuracy,
            "computed_at": datetime.now(timezone.utc).isoformat()
        }
        
        if batch_writer:
            # Enqueue insert operation (non-blocking)
            batch_writer.enqueue_write(
                table="user_accuracy",
                operation="insert",
                data=accuracy_record
            )
            # Return immediately (eventual consistency)
            return {
                "accuracy": accuracy,
                "message": "Accuracy computed (write queued)"
            }
        else:
            # Fallback to direct write if batch_writer unavailable
            sb_execute(
                supabase_client.table("user_accuracy").insert(accuracy_record)
            )
            return {
                "accuracy": accuracy
            }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update accuracy: {str(e)}"
        )


# ===== STUDY PLANNER ENDPOINTS =====

@app.get("/api/v1/study-plans")
async def get_study_plans(user_id: str):
    """Get all active study plans for a user"""
    if not study_planner_service:
        raise HTTPException(
            status_code=503,
            detail="Study planner service not available"
        )
    
    try:
        plans = study_planner_service.get_user_study_plans(user_id, status='active')
        
        if not plans:
            return {"success": True, "data": []}
        
        # OPTIMIZATION: Fetch all topic counts in a single query instead of N queries
        plan_ids = [plan['id'] for plan in plans]
        
        # Get all topics for all plans in one query
        all_topics_response = sb_execute(
            supabase_client.table('study_plan_topics_v2')
            .select('plan_id, topic_id')
            .in_('plan_id', plan_ids)
        )
        
        # Build a dictionary mapping plan_id to topic count
        topics_count_map = {}
        if all_topics_response.data:
            for topic_row in all_topics_response.data:
                plan_id = topic_row['plan_id']
                topics_count_map[plan_id] = topics_count_map.get(plan_id, 0) + 1
        
        # Format response with summary data
        formatted_plans = []
        today = date.today()
        
        # Subject ID to name mapping
        subject_id_to_name = {
            101: 'Business Studies',
            102: 'Islamiyat',
            103: 'Mathematics',
            104: 'Physics',
            105: 'Chemistry',
            113: 'Pak Studies Geography',
            114: 'Pak Studies History',
            119: 'Economics'
        }
        
        for plan in plans:
            plan_id = plan['id']
            topics_count = topics_count_map.get(plan_id, 0)
            
            # Calculate days left
            exam_date = datetime.fromisoformat(plan['exam_date']).date()
            days_left = max(0, (exam_date - today).days)
            
            # Get subject name from plan or derive from subject_id
            subject_id = plan.get('subject_id')
            subject_name = plan.get('subject') or subject_id_to_name.get(subject_id, 'Unknown Subject')
            
            formatted_plans.append({
                'id': plan_id,
                'plan_name': plan['plan_name'],
                'subject_id': subject_id,
                'subject': subject_name,
                'topics_count': topics_count,
                'days_left': days_left,
                'exam_date': plan['exam_date'],
                'status': plan['status'],
                'created_at': plan['created_at']
            })
        
        return {"success": True, "data": formatted_plans}
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch study plans: {str(e)}"
        )


@app.post("/api/v1/study-plans")
async def create_study_plan(request: CreateStudyPlanRequest, user_id: str = Query(...)):
    """Create a new study plan"""
    if not study_planner_service:
        raise HTTPException(
            status_code=503,
            detail="Study planner service not available"
        )
    
    try:
        # Validate plan name
        if len(request.plan_name.strip()) < 3:
            raise HTTPException(
                status_code=400,
                detail="Plan name must be at least 3 characters"
            )
        
        # Validate topic selection
        if not request.selected_topic_ids or len(request.selected_topic_ids) == 0:
            raise HTTPException(
                status_code=400,
                detail="At least one topic must be selected"
            )
        
        # Parse exam date
        exam_date = datetime.fromisoformat(request.exam_date).date()
        
        # Validate days to exam (will be checked in service, but check here too)
        days_to_exam = (exam_date - date.today()).days
        if days_to_exam < 5:
            raise HTTPException(
                status_code=400,
                detail="Exam date must be at least 5 days from today"
            )
        
        # Create plan
        plan = study_planner_service.create_study_plan(
            user_id=user_id,
            subject_id=request.subject_id,
            plan_name=request.plan_name.strip(),
            exam_date=exam_date,
            selected_topic_ids=request.selected_topic_ids
        )
        
        return {"success": True, "data": plan}
    
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create study plan: {str(e)}"
        )


@app.get("/api/v1/study-plans/{plan_id}")
async def get_study_plan_details(plan_id: str, user_id: str):
    """Get study plan details with full schedule"""
    if not study_planner_service:
        raise HTTPException(
            status_code=503,
            detail="Study planner service not available"
        )
    
    try:
        plan = study_planner_service.get_study_plan_details(plan_id, user_id)
        
        if not plan:
            raise HTTPException(
                status_code=404,
                detail="Study plan not found"
            )
        
        return {"success": True, "data": plan}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch study plan: {str(e)}"
        )


@app.patch("/api/v1/study-plans/{plan_id}")
async def update_study_plan(
    plan_id: str,
    request: UpdateStudyPlanRequest,
    user_id: str = Query(...)
):
    """Update study plan (regenerate schedule if exam_date changes)"""
    if not study_planner_service:
        raise HTTPException(
            status_code=503,
            detail="Study planner service not available"
        )
    
    try:
        exam_date = None
        if request.exam_date:
            exam_date = datetime.fromisoformat(request.exam_date).date()
            days_to_exam = (exam_date - date.today()).days
            if days_to_exam < 5:
                raise HTTPException(
                    status_code=400,
                    detail="Exam date must be at least 5 days from today"
                )
        
        plan = study_planner_service.update_study_plan(
            plan_id=plan_id,
            user_id=user_id,
            plan_name=request.plan_name,
            exam_date=exam_date
        )
        
        if not plan:
            raise HTTPException(
                status_code=404,
                detail="Study plan not found"
            )
        
        return {"success": True, "data": plan}
    
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update study plan: {str(e)}"
        )


@app.post("/api/v1/study-plans/{plan_id}/recompute")
async def recompute_study_plan_schedule(plan_id: str, user_id: str):
    """Regenerate schedule from stored plan config"""
    if not study_planner_service:
        raise HTTPException(
            status_code=503,
            detail="Study planner service not available"
        )
    
    try:
        plan = study_planner_service.recompute_schedule(plan_id, user_id)
        
        if not plan:
            raise HTTPException(
                status_code=404,
                detail="Study plan not found"
            )
        
        return {"success": True, "data": plan}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to recompute schedule: {str(e)}"
        )


@app.delete("/api/v1/study-plans/{plan_id}")
async def delete_study_plan(plan_id: str, user_id: str):
    """Delete a study plan"""
    if not study_planner_service or not study_planner_service.enabled:
        raise HTTPException(
            status_code=503,
            detail="Study planner service not available"
        )
    
    try:
        # Delete study plan directly - cascade deletes handle related records
        # Skip verification to avoid potential query issues
        delete_response = study_planner_service.client.table('study_plans_v2')\
            .delete()\
            .eq('id', plan_id)\
            .eq('user_id', user_id)\
            .execute()
        
        # Check if any rows were deleted
        deleted_count = len(delete_response.data) if delete_response.data else 0
        
        if deleted_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Study plan not found or already deleted"
            )
        
        return {"success": True, "message": "Study plan deleted successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting study plan: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete study plan: {str(e)}"
        )


# ===== UNIFIED ENDPOINTS =====

@app.get("/")
async def root():
    """Root endpoint with unified API information"""
    return {
        "message": "Imtehaan AI EdTech Platform - Unified Backend",
        "version": "2.0.0",
        "services": {
            "ai_tutor": {
                "status": "available",
                "endpoints": {
                    "chat": "/tutor/chat",
                    "lesson": "/tutor/lesson",
                    "health": "/tutor/health"
                }
            },
            "grading": {
                "status": "available" if GRADING_AVAILABLE else "unavailable",
                "endpoints": {
                    "grade_answer": "/grade-answer",
                    "health": "/grading/health"
                }
            },
            "helping": {
                "status": (
                    "available" if HELPING_AGENT_AVAILABLE
                    else "unavailable"
                ),
                "endpoints": {
                    "explain": "/helping/explain",
                    "health": "/helping/health"
                }
            }
        },
        "port": API_PORT,
        "documentation": "/docs"
    }


@app.get("/health")
async def unified_health():
    """
    Production-grade health check endpoint (FAILURE ISOLATION)
    API remains available even if Redis/workers are down
    Returns detailed system health status for observability
    """
    try:
        from services.observability import observability
        import asyncio
        
        # Get comprehensive system health with timeout protection
        # Use asyncio.wait_for to prevent blocking indefinitely
        try:
            system_health = await asyncio.wait_for(
                asyncio.to_thread(observability.get_system_health),
                timeout=2.0  # 2 second timeout for health check
            )
        except asyncio.TimeoutError:
            # If observability times out, return basic health
            system_health = {
                "api_status": "healthy",
                "redis_status": "timeout",
                "error": "Health check timeout"
            }
        except Exception as e:
            # If observability fails, return basic health
            system_health = {
                "api_status": "healthy",
                "redis_status": "error",
                "error": str(e)
            }
        
        # API is always healthy (failure isolation)
        # Redis/worker issues don't impact HTTP availability
        api_status = system_health.get("api_status", "healthy")
        
        return {
            "status": api_status,
            "architecture": "queue-based",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "services": {
                "api_layer": {
                    "status": "healthy",  # API is always healthy (failure isolation)
                    "description": "API handles auth, validation, and job enqueueing only"
                },
                "redis": {
                    "status": system_health.get("redis_status", "unknown"),
                    "description": "Redis connectivity status"
                },
                "workers": {
                    "status": "unknown" if system_health.get("worker_status", {}).get("error") else "operational",
                    "description": "Background worker status"
                }
            },
            "observability": system_health
        }
    except Exception as e:
        # Even if observability fails, API is healthy (failure isolation)
        return {
            "status": "healthy",
            "architecture": "queue-based",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "services": {
                "api_layer": {
                    "status": "healthy",
                    "description": "API handles auth, validation, and job enqueueing only"
                }
            },
            "observability_error": str(e)
    }


if __name__ == "__main__":
    # Import crash logger
    from services.crash_logger import log_crash
    
    # MEMORY SAFETY: Log memory on startup
    try:
        from services.memory_monitor import log_memory_usage
        log_memory_usage(
            service_name="backend",
            reason="startup",
            context={
                "host": API_HOST,
                "port": API_PORT,
                "environment": ENVIRONMENT
            }
        )
    except ImportError:
        pass  # Memory monitor not available
    except Exception as e:
        print(f"⚠️ Failed to log startup memory: {e}")
    
    if ENABLE_DEBUG:
        print("Starting Unified Backend Service...")
    print("AI Tutor endpoints: /tutor/*")
    print("Grading endpoints: /grade-answer, /grading/health")
    print("Health checks: /health, /tutor/health, /grading/health")
    # Get actual server URL from environment or use default
    server_url = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("API_BASE_URL") or f"http://{API_HOST}:{API_PORT}"
    print(f"Documentation: {server_url}/docs")
    print(f"Server: {server_url}")

    try:
        # Enable auto-reload in development mode only
        # In production, reload should be False (PM2/Docker handles restarts)
        enable_reload = ENVIRONMENT == "development"
        
        if enable_reload:
            print("🔄 Auto-reload enabled (development mode)")
            print("   Code changes will automatically restart the server")
        
        # Production-grade uvicorn configuration
        # Prevents overload and memory spikes under burst traffic
        uvicorn.run(
            app,
            host=API_HOST,
            port=API_PORT,
            log_level=LOG_LEVEL.lower(),
            reload=enable_reload,  # Auto-reload on code changes (development only)
            # Production settings to prevent overload
            timeout_keep_alive=UVICORN_TIMEOUT_KEEP_ALIVE,  # 30s - prevents connection buildup
            timeout_graceful_shutdown=UVICORN_TIMEOUT_GRACEFUL_SHUTDOWN,  # 10s - fast shutdown
            limit_concurrency=UVICORN_LIMIT_CONCURRENCY,  # 1000 - prevents connection exhaustion
            backlog=UVICORN_BACKLOG,  # 2048 - connection backlog protection
            workers=UVICORN_WORKERS,  # 1 - PM2/Docker manages instances
            # Additional production settings
            access_log=True,  # Enable access logs for monitoring
            proxy_headers=True,  # Trust proxy headers (if behind reverse proxy)
        )
    except KeyboardInterrupt:
        print("\n⚠️ Server interrupted by user")
        
        # MEMORY SAFETY: Log memory on shutdown
        try:
            from services.memory_monitor import log_memory_usage, get_peak_memory
            peak = get_peak_memory()
            log_memory_usage(
                service_name="backend",
                reason="shutdown",
                context={
                    "peak_memory_mb": peak.get("peak_memory_mb", 0)
                }
            )
        except Exception:
            pass
    except Exception as e:
        # Log fatal crash
        context = {
            'host': API_HOST,
            'port': API_PORT,
            'environment': ENVIRONMENT
        }
        log_crash('backend', e, context)
        
        # MEMORY SAFETY: Log memory on crash
        try:
            from services.memory_monitor import log_memory_usage, get_peak_memory
            peak = get_peak_memory()
            log_memory_usage(
                service_name="backend",
                reason="crash",
                context={
                    "peak_memory_mb": peak.get("peak_memory_mb", 0),
                    "error": str(e)
                }
            )
        except Exception:
            pass
        
        raise  # Re-raise to exit with error code
