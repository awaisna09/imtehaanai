from typing import TypedDict, Dict, List, Optional  # noqa: F401
from langgraph.graph import StateGraph, END  # noqa: F401
from agents.ai_tutor_agent import AITutorAgent  # to load services only
from dotenv import load_dotenv
# Import Supabase operations helper for concurrency limiting
from services.supabase_ops import sb_execute
import os
import logging
import hashlib
import time
import threading
import asyncio
from datetime import datetime
from uuid import uuid4
from contextlib import contextmanager

# Optional LangChain support
try:
    import langchain_openai  # noqa: F401
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

# Import cache for round-robin position tracking and other caching needs
# Uses Redis when available, falls back to in-memory cache
try:
    from cache import cache_get, cache_set, cache_delete
    REDIS_CACHE_AVAILABLE = True
except ImportError:
    REDIS_CACHE_AVAILABLE = False
    def cache_get(key): return None
    def cache_set(key, value, ttl=3600): return False
    def cache_delete(key): return False

logger = logging.getLogger(__name__)

# Configure logging to show DEBUG messages
logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG", "0") == "1" else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True  # Force reconfiguration
)

# Debug mode toggle
DEBUG_MODE = os.getenv("DEBUG", "0") == "1"
if DEBUG_MODE:
    logger.info("="*60)
    logger.info("[DEBUG MODE ENABLED] LangGraph Tutor debug logging is active")
    logger.info(f"[DEBUG] Environment DEBUG value: {os.getenv('DEBUG', '0')}")
    logger.info("="*60)

# Fallback in-memory cache (only used if Redis unavailable)
conversation_cache_fallback = {}


def async_write(fn, *args, **kwargs):
    """
    Fire-and-forget wrapper for Supabase writes.
    Executes the function in a background thread to avoid blocking.
    Includes error handling to log exceptions.
    """
    def wrapped_fn():
        try:
            fn(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"[ERROR] async_write failed for {fn.__name__}: {e}",
                exc_info=True
            )
    
    threading.Thread(
        target=wrapped_fn, daemon=True
    ).start()


@contextmanager
def timeout_context(seconds):
    """
    Context manager for timeout protection on blocking operations.
    Works on Windows using threading.Timer.
    """
    if seconds is None or seconds <= 0:
        yield
        return

    timeout_occurred = threading.Event()

    def timeout_handler():
        timeout_occurred.set()

    timer = threading.Timer(seconds, timeout_handler)
    timer.start()

    try:
        yield timeout_occurred
    finally:
        timer.cancel()


def safe_supabase_query(query_func, timeout=10, default_return=None):
    """
    Execute a Supabase query with timeout protection.
    Returns default_return if query times out or fails.

    Args:
        query_func: Function that executes the Supabase query
        timeout: Timeout in seconds (default: 10, increased from 3)
        default_return: Value to return on timeout/error

    Returns:
        Query result or default_return on timeout/error
    """
    if timeout <= 0:
        # No timeout, execute directly
        try:
            return query_func()
        except Exception as e:
            logger.error(f"Supabase query failed: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return default_return

    result_container = {"value": None, "error": None, "completed": False}

    def execute_query():
        try:
            result_container["value"] = query_func()
            result_container["completed"] = True
        except Exception as e:
            result_container["error"] = e
            result_container["completed"] = True

    query_thread = threading.Thread(target=execute_query, daemon=True)
    query_thread.start()
    query_thread.join(timeout=timeout)

    if not result_container["completed"]:
        logger.error(
            f"Supabase query timed out after {timeout}s"
        )
        return default_return

    if result_container["error"]:
        err_msg = str(result_container["error"])
        logger.error(f"Supabase query error: {err_msg}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return default_return

    return result_container["value"]


# Import performance instrumentation
try:
    from services.performance_instrumentation import (
        timed_operation, StageType, extract_trace_id_from_state,
        extract_job_id_from_state
    )
    PERFORMANCE_INSTRUMENTATION_AVAILABLE = True
except ImportError:
    PERFORMANCE_INSTRUMENTATION_AVAILABLE = False
    # Fallback if instrumentation not available
    def timed_operation(*args, **kwargs):
        from contextlib import nullcontext
        return nullcontext()


def timed_node(fn):
    """
    Decorator to wrap LangGraph nodes with timing and logging.
    Measures execution time and logs to Supabase tutor_traces table.
    Also handles errors and logs to tutor_errors table.
    Now uses new performance instrumentation for structured logging.
    """
    def wrapper(state):
        node_name = fn.__name__
        trace_id = state.get("trace_id")
        job_id = state.get("job_id")
        correlation_id = state.get("correlation_id", "unknown")
        user_id = state.get("user_id")
        topic = state.get("topic")
        
        # Use performance instrumentation (non-blocking)
        with timed_operation(
            stage_name=node_name,
            stage_type=StageType.PIPELINE_NODE,
            job_id=job_id,
            trace_id=trace_id,
            additional_context={
                "user_id": user_id,
                "topic": topic,
                "node_name": node_name,
                "correlation_id": correlation_id
            }
        ):
            start_time = time.time()
            logger.info(f"[Node Start] {node_name}, job_id: {job_id}, correlation_id: {correlation_id}")
            
            # Structured logging for node start
            try:
                from services.structured_logging import structured_logger
                structured_logger.log_langgraph_node(
                    event="node_start",
                    node_name=node_name,
                    job_id=job_id,
                    correlation_id=correlation_id
                )
            except Exception:
                pass  # Non-critical

            try:
                result = fn(state)
                end_time = time.time()
                duration_ms = int((end_time - start_time) * 1000)
                logger.info(f"[Node Complete] {node_name}, job_id: {job_id}, correlation_id: {correlation_id}, duration: {duration_ms}ms")
                
                # Structured logging for node end
                try:
                    from services.structured_logging import structured_logger
                    structured_logger.log_langgraph_node(
                        event="node_end",
                        node_name=node_name,
                        job_id=job_id,
                        correlation_id=correlation_id,
                        duration_ms=duration_ms
                    )
                except Exception:
                    pass  # Non-critical

                # Log timing to Supabase (async fire-and-forget) - legacy support
                if DEBUG_MODE and supabase_client:
                    trace_data = {
                        "user_id": user_id,
                        "topic": topic,
                        "node_name": node_name,
                        "duration_ms": duration_ms,
                        "timestamp": datetime.now().isoformat(),
                        "trace_id": trace_id,
                        "correlation_id": correlation_id
                    }
                    async_write(
                        lambda: sb_execute(
                            supabase_client.table("tutor_traces").insert(trace_data)
                        )
                    )

                return result

            except Exception as e:
                end_time = time.time()
                duration_ms = int((end_time - start_time) * 1000)

                # Log error timing to Supabase (async fire-and-forget) - legacy support
                if DEBUG_MODE and supabase_client:
                    trace_data = {
                        "user_id": user_id,
                        "topic": topic,
                        "node_name": node_name,
                        "duration_ms": duration_ms,
                        "timestamp": datetime.now().isoformat(),
                        "trace_id": trace_id,
                        "correlation_id": correlation_id,
                        "error": str(e)
                    }
                    async_write(
                        lambda: sb_execute(
                            supabase_client.table("tutor_traces").insert(trace_data)
                        )
                    )

                # Enhanced error logging with stack trace and categorization
                import traceback
                error_trace = traceback.format_exc()
                error_type = type(e).__name__
                
                # Categorize error
                error_category = "unknown"
                if "timeout" in str(e).lower() or isinstance(e, TimeoutError):
                    error_category = "timeout"
                elif "api" in str(e).lower() or "openai" in str(e).lower():
                    error_category = "api_error"
                elif "database" in str(e).lower() or "supabase" in str(e).lower() or "connection" in str(e).lower():
                    error_category = "database_error"
                elif "validation" in str(e).lower() or "value" in str(e).lower():
                    error_category = "validation_error"
                elif "network" in str(e).lower() or "connection" in str(e).lower():
                    error_category = "network_error"
                
                # Log to console with full trace
                logger.error(f"[Node Error] {node_name}, job_id: {job_id}, correlation_id: {correlation_id}, error_type: {error_type}, error: {e}")
                logger.error(f"[Node Error Trace] {node_name}, correlation_id: {correlation_id}:\n{error_trace}")
                
                # Structured logging for node error
                try:
                    from services.structured_logging import structured_logger
                    structured_logger.log_langgraph_node(
                        event="node_error",
                        node_name=node_name,
                        job_id=job_id,
                        correlation_id=correlation_id,
                        duration_ms=duration_ms,
                        error=str(e),
                        error_type=error_type
                    )
                except Exception:
                    pass  # Non-critical

                # Log structured analytics to Supabase with enhanced details (async fire-and-forget)
                if supabase_client:
                    error_data = {
                        "node": node_name,
                        "user_id": user_id,
                        "topic": topic,
                        "error": str(e),
                        "error_type": error_type,
                        "error_category": error_category,
                        "stack_trace": error_trace,
                        "job_id": job_id,
                        "trace_id": trace_id,
                        "correlation_id": correlation_id,
                        "duration_ms": duration_ms,
                        "severity": "high" if error_category in ["timeout", "database_error", "api_error"] else "medium"
                    }
                    async_write(
                        lambda: sb_execute(
                            supabase_client.table("tutor_errors").insert(error_data)
                        )
                    )

                return {}
    return wrapper


def safe_node(fn):
    """
    Decorator to wrap LangGraph nodes with error handling.
    Catches exceptions and logs them, returning empty dict to allow
    pipeline to continue.
    Also logs structured analytics to Supabase tutor_errors table.

    Note: Use timed_node instead for timing + error handling.
    """
    def wrapper(state):
        try:
            return fn(state)
        except Exception as e:
            # Enhanced error logging with stack trace and categorization
            import traceback
            error_trace = traceback.format_exc()
            error_type = type(e).__name__
            
            # Categorize error
            error_category = "unknown"
            if "timeout" in str(e).lower() or isinstance(e, TimeoutError):
                error_category = "timeout"
            elif "api" in str(e).lower() or "openai" in str(e).lower():
                error_category = "api_error"
            elif "database" in str(e).lower() or "supabase" in str(e).lower() or "connection" in str(e).lower():
                error_category = "database_error"
            elif "validation" in str(e).lower() or "value" in str(e).lower():
                error_category = "validation_error"
            elif "network" in str(e).lower() or "connection" in str(e).lower():
                error_category = "network_error"
            
            # Log to console with full trace
            logger.error(f"[Node Error] {fn.__name__}: {error_type}: {e}")
            logger.error(f"[Node Error Trace] {fn.__name__}:\n{error_trace}")

            # Log structured analytics to Supabase with enhanced details (async fire-and-forget)
            if supabase_client:
                error_data = {
                    "node": fn.__name__,
                    "user_id": state.get("user_id"),
                    "topic": state.get("topic"),
                    "error": str(e),
                    "error_type": error_type,
                    "error_category": error_category,
                    "stack_trace": error_trace,
                    "job_id": state.get("job_id"),
                    "trace_id": state.get("trace_id"),
                    "severity": "high" if error_category in ["timeout", "database_error", "api_error"] else "medium"
                }
                async_write(
                    lambda: sb_execute(
                        supabase_client.table("tutor_errors").insert(error_data)
                    )
                )

            return {}
    return wrapper


# Load environment variables from config.env
load_dotenv('config.env')

# Initialize Supabase client if available (singleton)
try:
    from services.supabase_client import get_supabase_client
    supabase_client = get_supabase_client()
    if DEBUG_MODE and supabase_client:
        logger.info("[OK] Supabase client initialized for LangGraph tutor")
except Exception as e:
    logger.error(f"[ERROR] Error initializing Supabase client: {e}")
    supabase_client = None

# Initialize AITutorAgent to build all services
api_key = os.getenv("OPENAI_API_KEY")
agent = AITutorAgent(
    api_key=api_key,
    supabase_client=supabase_client
)

# Get all services from agent
services = agent.build_services()
lesson_service = services["lesson"]
concept_service = services["concepts"]
history_service = services["history"]
llm_service = services["llm"]
mastery_service = services["mastery"]
readiness_service = services["readiness"]
message_service = services["messages"]
student_service = services["student"]


# Unified state object passed across LangGraph nodes
class TutorState(TypedDict):
    user_message: str
    topic: str
    user_id: str
    conversation_id: str
    explanation_style: str
    trace_id: str
    job_id: Optional[str]  # Added for instrumentation correlation
    subject_id: Optional[int]

    # Data retrieved from AITutorAgent internals
    lesson_text: Optional[str]
    lesson_chunks: List[Dict]
    concept_rows: List[Dict]
    history: List[Dict]
    condensed_history: Optional[str]
    reasoning_label: str
    llm_response: str
    token_usage: Dict
    mastery_updates: List[Dict]
    readiness: Optional[Dict]
    learning_path: Optional[Dict]


# -----------------------------------------------------
# Node 0: LogUserMessage
# -----------------------------------------------------
def LogUserMessage(state: TutorState):
    """
    Log the student's message into Supabase at the start of the pipeline.
    This ensures the user message is stored before processing begins.
    """
    # Extract concept IDs (will be empty at this point, but structure is ready)
    concept_ids = []
    
    # Get subject_id and convert to subject name if needed
    subject_id = state.get("subject_id")
    subject_name = None
    if subject_id is not None:
        subject_map = {
            101: "Business Studies",
            102: "Islamiyat",
            103: "Mathematics",
            104: "Physics",
            105: "Chemistry",
            113: "Pak Studies Geography",
            114: "Pak Studies History",
            119: "Economics"
        }
        subject_name = subject_map.get(subject_id)

    # Write the user message to Supabase (async fire-and-forget)
    async_write(
        message_service.log,
        user_id=state["user_id"],
        lesson_topic=state["topic"],
        conversation_id=state["conversation_id"],
        role="user",
        content=state["user_message"],
        concept_ids=concept_ids,
        subject=subject_name,
        subject_id=subject_id
    )

    # Update Redis conversation cache
    conversation_id = state["conversation_id"]
    cache_key = f"conversation_history:{conversation_id}"
    
    # Get existing history from Redis or fallback
    if REDIS_CACHE_AVAILABLE:
        existing_history = cache_get(cache_key) or []
    else:
        existing_history = conversation_cache_fallback.get(conversation_id, [])

    # Add user message to cache
    existing_history.append({
        "role": "user",
        "content": state["user_message"]
    })

    # Keep only last 20 messages in cache
    existing_history = existing_history[-20:]
    
    # Store back in Redis or fallback
    if REDIS_CACHE_AVAILABLE:
        cache_set(cache_key, existing_history, ttl=3600)
    else:
        conversation_cache_fallback[conversation_id] = existing_history

    return {}


# -----------------------------------------------------
# Node 0.5: ValidateInput
# -----------------------------------------------------
def ValidateInput(state: TutorState):
    """
    Validate and limit input sizes to prevent token overflow.

    Limits:
    - user_message: 800 tokens (summarize if over)
    - lesson_text: 4000 tokens (truncate if over)
    - lesson_chunks: 2000 tokens total (truncate if over)
    - concept descriptions: 500 tokens combined (truncate if over)

    Returns updated state with validated/truncated inputs.
    """
    # Helper function to estimate tokens (rough: 1 token ≈ 4 characters)
    def estimate_tokens(text: str) -> int:
        if not text:
            return 0
        return len(text) // 4

    # Helper function to truncate text to token limit
    def truncate_to_tokens(text: str, max_tokens: int) -> str:
        if not text:
            return text
        max_chars = max_tokens * 4
        if len(text) <= max_chars:
            return text
        # Truncate and add ellipsis
        return text[:max_chars - 3] + "..."

    updated_state = {}

    # 0. Subject-aware off-topic validation
    user_message = state.get("user_message", "")
    user_message_lower = user_message.lower()
    subject_id = state.get("subject_id")
    
    # Get subject name for logging
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
    current_subject = subject_map.get(subject_id, "Business Studies")
    
    # Subject-specific off-topic keywords (IMPROVED: More context-aware)
    def get_off_topic_keywords(subject_id: Optional[int]) -> List[str]:
        """Get off-topic keywords based on current subject
        
        IMPORTANT: Only flag obvious off-topic questions.
        Use phrase matching to avoid false positives.
        Let LLM handle edge cases with its judgment.
        """
        
        # Common off-topic keywords for ALL subjects (obvious non-academic)
        common_off_topic = [
            "write code", "programming language", "python script", "javascript code",
            "html page", "css styling", "write a poem", "poetry writing",
            "novel writing", "translate to", "translate from", "grammar rules",
            "vocabulary words", "spell this", "how to code", "programming tutorial"
        ]
        
        if subject_id == 101:  # Business Studies
            # REMOVED: "economics", "demand", "supply", "inflation", "gdp" - these ARE relevant to Business Studies!
            # Only flag CLEARLY unrelated subjects and math problem-solving
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate the area",
                "find the derivative", "solve for x", "algebra problem", "geometry proof",
                "calculus problem", "physics experiment", "chemistry reaction",
                "biology experiment", "science lab", "history of ancient",
                "world war details", "medieval period", "pakistan independence movement",
                "pakistan geography", "physical geography", "islamiyat topics",
                "quranic verses", "hadith interpretation", "islamic law details"
            ]
        elif subject_id == 119:  # Economics
            # REMOVED: "business organization", "marketing", "human resources" - these ARE relevant to Economics!
            # Only flag CLEARLY unrelated subjects
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate the area",
                "find the derivative", "solve for x", "algebra problem", "geometry proof",
                "calculus problem", "physics experiment", "chemistry reaction",
                "biology experiment", "science lab", "pakistan independence",
                "muhammad ali jinnah", "pakistan geography", "physical geography",
                "mountains of pakistan", "rivers of pakistan", "islamiyat topics",
                "quranic verses", "hadith interpretation", "islamic law details"
            ]
        elif subject_id == 114:  # Pak Studies History
            # REMOVED: "geography", "mountains", "rivers", "climate" - these can be relevant in historical context!
            # Only flag CLEARLY unrelated subjects and pure geography questions
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate",
                "algebra", "geometry", "calculus", "physics experiment",
                "chemistry reaction", "biology experiment", "pure geography",
                "geography of other countries", "physical geography details",
                "business management", "marketing strategies", "economics calculations",
                "islamiyat topics", "quranic verses", "hadith interpretation",
                "islamic law details", "religious rulings"
            ]
        elif subject_id == 113:  # Pak Studies Geography
            # REMOVED: "pakistan history", "independence", "muhammad ali jinnah" - these can be relevant in geographic context!
            # Only flag CLEARLY unrelated subjects and pure history questions
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate",
                "algebra", "geometry", "calculus", "physics experiment",
                "chemistry reaction", "biology experiment", "pure history",
                "history of other countries", "ancient history", "medieval history",
                "world war details", "business management", "marketing strategies",
                "islamiyat topics", "quranic verses", "hadith interpretation",
                "islamic law details", "religious rulings"
            ]
        elif subject_id == 102:  # Islamiyat
            # REMOVED: "pakistan history", "geography", "mountains", "rivers" - these can be relevant in Islamic historical context!
            # Only flag CLEARLY unrelated subjects
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate",
                "algebra", "geometry", "calculus", "physics experiment",
                "chemistry reaction", "biology experiment", "pure geography",
                "geography of other countries", "business management",
                "marketing strategies", "economics calculations", "pakistan independence movement",
                "muhammad ali jinnah biography"  # Only specific non-Islamic history
            ]
        elif subject_id == 103:  # Mathematics
            return common_off_topic + [
                "history of", "pakistan history", "geography", "business",
                "marketing", "economics concepts", "islamiyat", "physics experiment",
                "chemistry reaction", "biology experiment"
            ]
        elif subject_id == 104:  # Physics
            return common_off_topic + [
                "history of", "pakistan history", "geography", "business",
                "marketing", "economics", "islamiyat", "chemistry reaction",
                "biology experiment", "solve math", "algebra problem"
            ]
        elif subject_id == 105:  # Chemistry
            return common_off_topic + [
                "history of", "pakistan history", "geography", "business",
                "marketing", "economics", "islamiyat", "physics experiment",
                "biology experiment", "solve math", "algebra problem"
            ]
        else:
            # Default: reject only obvious non-academic topics
            return common_off_topic + [
                "solve this math problem", "write code", "programming"
            ]
    
    # Get off-topic keywords for current subject
    # Use local function (same logic as LLM service for consistency)
    off_topic_keywords = get_off_topic_keywords(subject_id)
    
    # IMPROVED: Use phrase matching instead of simple keyword matching
    # Only flag if message contains COMPLETE phrases, not just individual words
    # This prevents false positives (e.g., "demand" in "what is demand in business" should NOT be flagged)
    is_off_topic = False
    
    # Check for phrase matches (more accurate) - only flag obvious off-topic phrases
    for keyword_phrase in off_topic_keywords:
        # Use phrase matching for better accuracy (check complete phrase, not just word)
        if keyword_phrase in user_message_lower:
            # CRITICAL: Check if keyword appears WITH subject-relevant context
            # If it does, it's likely a valid question even if it contains the keyword
            # Example: "demand in business" should NOT be flagged for Business Studies
            # Example: "supply chain in business" should NOT be flagged
            # Only flag if it's clearly a standalone off-topic phrase
            
            # Subject-relevant context indicators
            subject_context_indicators = []
            if subject_id == 101:  # Business Studies
                subject_context_indicators = ["business", "market", "company", "firm", "enterprise", "organization", "commerce", "trade", "industry"]
            elif subject_id == 119:  # Economics - EXPANDED
                subject_context_indicators = [
                    # Core economic concepts
                    "economic", "economy", "economics", "microeconomics", "macroeconomics",
                    # Market and price concepts
                    "market", "markets", "price", "prices", "demand", "supply", "equilibrium",
                    "price determination", "price changes", "price elasticity", "ped", "pes",
                    "price elasticity of demand", "price elasticity of supply",
                    # Production and costs
                    "production", "factors of production", "opportunity cost", "ppc",
                    "production possibility curve", "costs", "revenue", "profit",
                    "fixed costs", "variable costs", "marginal costs", "average costs",
                    "firms", "firms and production", "firms costs revenue and objectives",
                    # Economic systems
                    "market economic system", "mixed economic system", "command economy",
                    "free market", "mixed economy",
                    # Market structure and failure
                    "market structure", "market failure", "perfect competition", "monopoly",
                    "oligopoly", "monopolistic competition", "externalities", "public goods",
                    # Economic agents
                    "workers", "households", "trade unions", "firms",
                    # Government and policy
                    "government", "role of government", "macroeconomic aims of government",
                    "monetary policy", "fiscal policy", "supply-side policy",
                    "central bank", "interest rates", "money supply", "taxation",
                    "government spending", "budget",
                    # Macroeconomic concepts
                    "economic growth", "gdp", "gross domestic product", "employment",
                    "unemployment", "inflation", "deflation", "price stability",
                    "living standards", "population", "poverty",
                    # International economics
                    "international trade", "globalisation", "globalization", "free trade",
                    "protection", "protectionism", "tariffs", "quotas", "trade barriers",
                    "foreign exchange", "exchange rates", "balance of payments",
                    "current account", "exports", "imports", "trade balance",
                    "international specialisation", "specialization", "comparative advantage",
                    # Development economics
                    "developed economies", "less-developed economies", "developing economies",
                    "economic development", "development indicators",
                    # Money and banking
                    "money", "banking", "money and banking", "central bank",
                    "commercial banks", "functions of money",
                    # Resource allocation
                    "allocating resources", "role of markets in allocating resources",
                    "resource allocation", "price mechanism", "market forces",
                    # Nature of economic problem
                    "nature of the economic problem", "scarcity", "choice", "wants", "needs"
                ]
            elif subject_id == 114:  # Pak Studies History - EXPANDED
                subject_context_indicators = [
                    # Historical periods and eras
                    "history", "historical", "era", "period", "colonial", "pre-partition", "post-independence",
                    # Pakistan and Indian subcontinent
                    "pakistan", "india", "indian", "subcontinent", "south asia",
                    # Independence and partition
                    "independence", "partition", "1947", "partition of", "radcliffe", "boundary commission",
                    # Pakistan Movement
                    "pakistan movement", "pakistan resolution", "lahore resolution", "two-nation theory",
                    "all india muslim league", "muslim league", "resolution 1940",
                    # Key historical figures (ALL from syllabus)
                    "jinnah", "quaid-e-azam", "muhammad ali jinnah", "allama iqbal", "iqbal",
                    "liaquat", "bhutto", "benazir", "nawaz sharif", "zia", "ayub khan",
                    "shah waliullah", "haji shariatullah", "syed ahmed", "barelvi", "titu mir",
                    "shivaji", "rani of jhansi",
                    # British colonial period (ALL acts and reforms)
                    "british", "east india company", "colonial", "british expansion", "black hole",
                    "pitt's india act", "vernacular press act", "rowlatt act", "morley-minto reforms",
                    "montague chelmsford reforms", "government of india act", "simon commission",
                    "cripps mission", "cabinet mission",
                    # Wars and conflicts
                    "war of independence", "1857", "war", "conflict", "battle", "independence war",
                    # Political developments (ALL conferences, pacts, reports)
                    "reform", "act", "conference", "election", "constitution", "pact", "agreement",
                    "round table", "simla", "lucknow pact", "nehru report", "jinnah's points", "14 points",
                    # Constitutional and political
                    "constitutional", "constitutional crisis", "basic principles committee",
                    "objectives resolution", "one unit scheme", "proda", "public and representative officers",
                    # Prime Ministers and Presidents (ALL leaders)
                    "prime minister", "president", "khawaja nazimuddin", "zia-ul-haq", "general elections",
                    # Movements and events (ALL movements)
                    "khilafat", "caliphate", "quit india", "direct action", "gandhi-jinnah talks",
                    "simla conference", "3rd june plan", "world war", "world war i", "world war ii",
                    "bengal partition", "partition of bengal", "why was bengal partitioned",
                    # Government and policy
                    "government", "policy", "domestic policy", "economic policy", "political policy",
                    "agricultural reforms", "land reforms", "industrial reforms", "constitutional reforms",
                    # Foreign relations (ALL countries)
                    "foreign relations", "relations with", "pakistan's relations", "simla agreement",
                    "liaquat-nehru pact", "afghan miracle",
                    # Post-independence leaders (ALL eras)
                    "nawaz sharif", "benazir bhutto", "zulfiqar ali bhutto", "decade of progress",
                    "fall of ayub khan", "why was martial law", "why was nawaz sharif", "why did benazir",
                    # Language development
                    "urdu", "punjabi", "sindhi", "language development", "how has the pakistan government promoted",
                    # Bangladesh
                    "bangladesh", "creation of bangladesh", "reasons for the creation",
                    # General historical terms
                    "mughal empire", "downfall", "factors", "geographical factors", "political factors",
                    "social factors", "military factors", "main events", "services", "achievements",
                    "biography", "earlier life", "building a nation", "building a government",
                    "establishing national security", "anti-muslim attitudes", "reaction", "response",
                    "hindus' response", "reaction of muslims", "downfall of mughal", "east india company",
                    "black hole", "pitt's india", "vernacular press", "vernacular", "rowlatt",
                    "morley-minto", "montague chelmsford", "government of india act", "simon commission",
                    "cripps", "cabinet mission", "round table conference", "round table conferences",
                    "simla deputation", "simla conference", "lucknow", "nehru", "allama iqbal",
                    "chaudri rehmat ali", "liaquat ali khan", "khawaja nazimuddin", "ayub khan",
                    "zulfiqar ali bhutto", "zia-ul-haq", "zia ul haq", "benazir", "nawaz sharif",
                    "objectives resolution", "basic principles", "one unit", "constitutional crisis",
                    "pakistan resolution", "pakistan resolution 1940", "the pakistan resolution",
                    "jallianwala bagh", "amritsar massacre", "jallianwala", "amritsar",
                    "gandhi-jinnah", "direct action day", "3rd june", "radcliffe award", "boundary commission",
                    "partition and nascent pakistan", "quaid-e-azam", "quaid e azam", "political achievements",
                    "presidential address at allahabad", "allahabad 1930", "pakistan movement 1933",
                    "liaquat-nehru", "public and representative officers", "representative officers disqualification",
                    "reasons for the creation of bangladesh", "reasons for creation of bangladesh",
                    "why was bengal partitioned", "why was martial law imposed", "why did benazir fall",
                    "why was nawaz sharif's first government dismissed", "how has the pakistan government promoted",
                    "promoted the development of urdu", "promoted the development of punjabi",
                    "promoted the development of sindhi", "pakistan's relation", "relations with india",
                    "relations with bangladesh", "relations with afghanistan", "relations with iran",
                    "relations with china", "relations with ussr", "relations with usa",
                    "agricultural reforms", "constitutional reforms", "land reforms", "industrial reforms",
                    "the decade of progress", "fall of ayub khan", "anti-muslim", "anti muslim", "elections of 1937",
                    "elections of 1985", "general elections", "election", "building a nation", "building a government"
                ]
            elif subject_id == 113:  # Pak Studies Geography - EXPANDED
                subject_context_indicators = [
                    # Core geography terms
                    "geography", "geographical", "geographic", "pakistan", "pakistani",
                    # Administrative and cities
                    "administrative areas", "major cities", "islamabad", "karachi", "lahore",
                    "cities", "urban", "rural",
                    # Physical geography - Rivers
                    "rivers", "indus", "jhelum", "chenab", "ravi", "sutlej", "beas",
                    "major rivers", "river systems", "river basin",
                    # Physical geography - Mountains and Passes
                    "mountains", "mountain passes", "karakoram", "himalayas", "siwaliks",
                    "lesser himalayas", "central himalayas", "hindu kush", "safed koh",
                    "waziristan hills", "sulaiman", "kirthar", "khunjerab pass", "bolan pass",
                    "khyber pass", "karakoram pass",
                    # Physical geography - Plateaus and Plains
                    "plateaus", "balochistan plateau", "potwar plateau", "potohar plateau",
                    "basins", "hamuns", "badland topography", "indus plain", "upper indus plain",
                    "lower indus plain", "deltaic plains", "doabs", "bars",
                    # Physical geography - Deserts
                    "deserts", "thal desert", "thar desert", "cholistan desert", "sand dunes",
                    "rolling sand dunes",
                    # Climate and Weather
                    "climate", "climatic zones", "highland climate", "lowland climate",
                    "coastal climate", "arid climate", "weather", "monsoon", "monsoon rainfall",
                    "western depressions", "convectional currents", "relief rainfall",
                    "sources of rainfall", "temperature", "rainfall",
                    # Environmental hazards
                    "environmental hazards", "floods", "droughts", "causes of floods",
                    "effects of floods", "causes of droughts", "effects of droughts",
                    "natural disasters",
                    # Water resources
                    "water", "sources of water", "groundwater", "surface water", "water bodies",
                    "rivers", "lakes", "streams", "aquifers", "waterlogging", "salinity",
                    "siltation", "water issues", "indus waters treaty",
                    # Forests
                    "forests", "productive forests", "protective forests", "forestry",
                    "conservation",
                    # Energy resources
                    "energy", "oil", "natural gas", "coal", "petroleum", "non-renewable",
                    "renewable", "hydroelectric power", "thermal power", "nuclear energy",
                    "electricity sources", "rural electrification", "sustainable development",
                    "load shedding", "power theft", "economic effects of load shedding",
                    # Oil and gas
                    "oil refineries", "karachi refinery", "attock refinery", "mehmood kot",
                    "uses of oil", "extraction of oil", "uses of natural gas",
                    "extraction of natural gas",
                    # Agriculture
                    "agriculture", "agricultural inputs", "natural inputs", "soil", "rain",
                    "human inputs", "capital", "machinery", "hyv seeds", "high yielding varieties",
                    "fertilizers", "pesticides", "food crops", "wheat", "rice", "cultivation",
                    "cash crops", "cotton", "sugar cane", "sugarcane", "fruit", "mangoes",
                    "bananas", "apples", "poultry farming", "livestock", "nomadic",
                    "settled", "transhumance", "semi-nomadic", "livestock systems",
                    # Irrigation
                    "irrigation", "need for irrigation", "modern irrigation", "conventional irrigation",
                    "karez", "shaduf", "persian wheels", "water infrastructure", "large dams",
                    "small dams", "barrages", "tarbela", "mangla", "kalabagh", "canal systems",
                    # Land reforms
                    "land reforms", "1959 land reforms", "1972 land reforms", "1977 land reforms",
                    # Industry
                    "industry", "industrial", "formal sector", "informal sector", "cottage sector",
                    "cottage industry", "industrial estates", "site", "noorabad", "hub",
                    "export processing zones", "epz", "special economic zones",
                    # Transportation
                    "transportation", "roads", "kutcha roads", "pucca roads", "nha",
                    "national highway authority", "railways", "railway problems", "corruption",
                    "worn-out rails", "dry ports", "public sector", "private sector",
                    "seaports", "karachi port", "port qasim", "gwadar port", "gwadar",
                    # Communication
                    "communication", "telecommunication", "ptcl", "pta", "pakistan telecommunication authority",
                    "ntc", "national telecommunication corporation", "e-commerce", "call centres",
                    "call centers", "it sector",
                    # Location and borders
                    "latitudes", "longitudes", "neighboring countries", "borders", "geographic location",
                    # Population
                    "population", "population geography", "birth rate", "death rate",
                    "growth rate", "density", "population density", "population structure",
                    "population pyramids", "age structure", "dependency ratio",
                    "high birth rates", "high death rates", "control measures",
                    "rural-urban migration", "urban infrastructure", "migration",
                    "urbanization", "demography", "demographic",
                    # Trade
                    "trade", "imports", "exports", "balance of trade", "wto",
                    "world trade organisation", "world trade organization",
                    "trading blocs", "saarc", "south asian association for regional cooperation",
                    "eco", "economic cooperation organization", "challenges for pakistan",
                    # Tourism
                    "tourism", "tourist destinations", "cultural attractions",
                    "archaeological attractions", "modern attractions", "heritage sites",
                    # Geographic features
                    "indus river", "karakoram range", "thar desert", "k2", "tarbela dam",
                    "mangla dam", "plateaus", "valleys", "provinces", "territories",
                    "administrative divisions", "districts", "regions"
                ]
            elif subject_id == 102:  # Islamiyat - EXPANDED
                subject_context_indicators = [
                    # Core Islamic terms
                    "islam", "islamic", "muslim", "islamiyat", "deen", "religion",
                    # Quran and revelation
                    "quran", "qur'an", "qur'anic", "revelation", "revelation of quran",
                    "surah", "surahs", "ayat", "ayah", "verses", "ayat-ul-kursi",
                    "surah al-baqarah", "surah al-an'aam", "surah fussilat", "surah shura",
                    "surah ikhlas", "surah fatiha", "surah al-alaq", "surah az-zilzaal",
                    "surah naas", "surah al-maidah", "surah duha", "surah kauthar",
                    "quran as primary source", "preservation of quran", "compilation of quran",
                    "under uthman", "during prophet's lifetime", "under abu bakr",
                    "early transmission", "scribes of quran", "ways quran was revealed",
                    # Hadith and Sunnah
                    "hadith", "hadiths", "sunnah", "sunnah of prophet", "traditions",
                    "need for hadith", "compilation of hadith", "early transmission",
                    "musnad", "musannaf", "authenticity of hadith", "isnad", "matn",
                    "sunni hadith collections", "shi'a hadith collections", "shi'a hadith",
                    "hadith as source of islamic law", "major teachings in hadiths",
                    "importance of hadith in muslim life",
                    # Islamic law and sources
                    "islamic law", "shariah", "sharia", "fiqh", "jurisprudence",
                    "sources of islamic law", "quran", "hadith", "ijma", "qiyas",
                    "consensus", "analogical reasoning", "ijtihad",
                    # Themes and passages
                    "theme 1", "allah in himself", "theme 2", "created world",
                    "theme 3", "messengers", "passage 1", "passage 2", "passage 3",
                    "passage 4", "passage 5", "passage 6", "passage 7", "passage 8",
                    "passage 9", "passage 10", "passage 11", "passage 12", "passage 13",
                    "passage 14", "passage 15",
                    # Prophets and messengers
                    "prophet", "prophets", "prophet muhammad", "prophet adam",
                    "prophet ibrahim", "prophet isa", "messenger", "messengers",
                    "final messenger", "seal of prophets", "importance of prophet",
                    "prophet as role model", "prophet as leader",
                    # Life of Prophet Muhammad
                    "early life before prophethood", "first revelation", "makkan period",
                    "makkah", "madinan period", "madinah", "hijrah", "hijrah to madinah",
                    "migration to abyssinia", "isra and mi'raj", "night journey",
                    "opposition and persecution", "boycott of banu hashim",
                    "conquest of makkah", "farewell sermon", "final sermon",
                    "battles", "battle of badr", "battle of uhud", "battle of khandaq",
                    "battle of trench", "treaties", "treaty of hudaybiyyah",
                    "leadership", "character of prophet", "prophet's family",
                    # Companions and early Muslims
                    "companions", "sahabah", "ansar", "muhajirun", "ten blessed companions",
                    "asharah mubashsharah", "abu bakr", "umar", "uthman", "ali",
                    "importance and contribution of companions", "wives", "mothers of believers",
                    "children", "grandchildren", "hasan", "husayn", "descendants",
                    "imams", "shi'a perspective",
                    # Rashidun Caliphs
                    "caliphs", "rashidun caliphs", "caliphate", "khilafat",
                    "abu bakr ra", "umar ra", "uthman ra", "ali ra",
                    # Articles of Faith
                    "articles of faith", "belief in allah", "tawheed", "oneness of allah",
                    "angels", "holy books", "prophets", "predestination", "qadr",
                    "resurrection", "last day", "day of judgment", "afterlife",
                    # Pillars of Islam
                    "pillars of islam", "five pillars", "shahadah", "declaration of faith",
                    "salah", "prayer", "namaz", "zakah", "zakat", "charity", "sadaqah",
                    "sawm", "fasting", "ramadan", "hajj", "pilgrimage", "umrah",
                    # Islamic practices
                    "islamic practices", "rituals", "worship", "ibadah", "ibadat",
                    "individual conduct", "life in community", "community life",
                    "social responsibilities", "ethics", "akhlaq", "character",
                    "moral values", "islamic ethics",
                    # Jihad
                    "jihad", "struggle", "spiritual jihad", "physical jihad",
                    "greater jihad", "lesser jihad", "jihad meanings",
                    # Islamic concepts
                    "islamic civilization", "islamic culture", "islamic history",
                    "islamic education", "knowledge", "ilm", "seeking knowledge",
                    "islamic scholarship", "islamic scholars",
                    # Location and context
                    "makkah", "madinah", "arabia", "arabian peninsula",
                    "mecca", "medina"
                ]
            
            # Check if keyword phrase appears WITH subject context (likely valid question)
            has_subject_context = any(
                indicator in user_message_lower for indicator in subject_context_indicators
            ) if subject_context_indicators else False
            
            # Only flag as off-topic if NO subject context is present
            # This prevents false positives
            if not has_subject_context:
                is_off_topic = True
                if DEBUG_MODE:
                    logger.info(
                        f"[DEBUG] ValidateInput: Found off-topic phrase '{keyword_phrase}' "
                        f"in message for {current_subject} (no subject context detected)"
                    )
                break  # Found one match without context, flag as potentially off-topic
            else:
                if DEBUG_MODE:
                    logger.info(
                        f"[DEBUG] ValidateInput: Found phrase '{keyword_phrase}' but WITH "
                        f"{current_subject} context - likely valid question, not flagging as off-topic"
                    )
    
    # NOTE: We're being much more lenient - only flag obvious off-topic WITHOUT subject context
    # Let LLM handle final judgment with its context awareness
    # Even if flagged here, LLM may still accept if it detects relevance
    if is_off_topic:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] ValidateInput: Detected potential off-topic phrase "
                f"for {current_subject}. LLM will make final judgment - may still accept if contextually relevant."
            )
        # Add flag to state so LLM knows to reject
        updated_state["_off_topic_detected"] = True
        updated_state["_current_subject"] = current_subject

    # 1. Validate user_message (800 tokens limit)
    user_tokens = estimate_tokens(user_message)
    if user_tokens > 800:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] User message exceeds 800 tokens ({user_tokens}). "
                f"Summarizing..."
            )
        # Summarize using llm_service
        try:
            summarized = llm_service.summarize_history(user_message)
            updated_state["user_message"] = summarized
            if DEBUG_MODE:
                new_tokens = estimate_tokens(summarized)
                logger.info(
                    f"[DEBUG] Summarized to {new_tokens} tokens "
                    f"(from {user_tokens})"
                )
        except Exception as e:
            # Fallback: truncate if summarization fails
            logger.warning(f"Summarization failed: {e}, truncating instead")
            updated_state["user_message"] = truncate_to_tokens(
                user_message, 800
            )
    else:
        updated_state["user_message"] = user_message

    # 2. Validate lesson_text (4000 tokens limit)
    lesson_text = state.get("lesson_text")
    if lesson_text:
        lesson_tokens = estimate_tokens(lesson_text)
        if lesson_tokens > 4000:
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] Lesson text exceeds 4000 tokens "
                    f"({lesson_tokens}). Truncating..."
                )
            updated_state["lesson_text"] = truncate_to_tokens(
                lesson_text, 4000
            )
        else:
            updated_state["lesson_text"] = lesson_text

    # 3. Validate lesson_chunks (2000 tokens total limit)
    lesson_chunks = state.get("lesson_chunks", [])
    if lesson_chunks:
        total_chunk_tokens = sum(
            estimate_tokens(chunk.get("chunk_text", ""))
            for chunk in lesson_chunks
        )
        if total_chunk_tokens > 2000:
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] Lesson chunks exceed 2000 tokens "
                    f"({total_chunk_tokens}). Truncating..."
                )
            # Truncate chunks, keeping most relevant (first ones)
            truncated_chunks = []
            remaining_tokens = 2000
            for chunk in lesson_chunks:
                chunk_text = chunk.get("chunk_text", "")
                chunk_tokens = estimate_tokens(chunk_text)
                if chunk_tokens <= remaining_tokens:
                    truncated_chunks.append(chunk)
                    remaining_tokens -= chunk_tokens
                else:
                    # Truncate this chunk to fit remaining tokens
                    if remaining_tokens > 0:
                        truncated_chunk = chunk.copy()
                        truncated_chunk["chunk_text"] = truncate_to_tokens(
                            chunk_text, remaining_tokens
                        )
                        truncated_chunks.append(truncated_chunk)
                    break
            updated_state["lesson_chunks"] = truncated_chunks
        else:
            updated_state["lesson_chunks"] = lesson_chunks

    # 4. Validate concept descriptions (500 tokens combined limit)
    concept_rows = state.get("concept_rows", [])
    if concept_rows:
        total_desc_tokens = sum(
            estimate_tokens(
                f"{c.get('name', '')} {c.get('description', '')}"
            )
            for c in concept_rows
        )
        if total_desc_tokens > 500:
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] Concept descriptions exceed 500 tokens "
                    f"({total_desc_tokens}). Truncating..."
                )
            # Truncate descriptions proportionally
            truncated_concepts = []
            tokens_per_concept = 500 // len(concept_rows)
            for concept in concept_rows:
                truncated_concept = concept.copy()
                name = concept.get("name", "")
                desc = concept.get("description", "")
                combined = f"{name} {desc}"
                combined_tokens = estimate_tokens(combined)
                if combined_tokens > tokens_per_concept:
                    # Truncate description to fit
                    desc_tokens = tokens_per_concept - estimate_tokens(name)
                    if desc_tokens > 0:
                        truncated_concept["description"] = truncate_to_tokens(
                            desc, desc_tokens
                        )
                    else:
                        truncated_concept["description"] = ""
                truncated_concepts.append(truncated_concept)
            updated_state["concept_rows"] = truncated_concepts
        else:
            updated_state["concept_rows"] = concept_rows

    return updated_state


# -----------------------------------------------------
# Node 1: FetchDataParallel (NEW - Parallel DB Reads)
# -----------------------------------------------------
def FetchDataParallel(state: TutorState):
    """
    Fetch lesson, history, and concepts in parallel.
    
    PARALLELIZATION: All three independent database reads execute concurrently
    to maximize throughput. Concurrency is limited to respect connection pool limits.
    
    Error Handling: Partial failures are handled gracefully - one read failure doesn't
    cancel others. Failed reads return empty/default values.
    
    This replaces the sequential FetchLesson → RetrieveHistory → FetchConcepts flow.
    """
    job_id = state.get("job_id")
    trace_id = state.get("trace_id")
    user_id = state.get("user_id")
    
    # Import performance instrumentation
    try:
        from services.performance_instrumentation import (
            timed_operation, StageType, time_db_read
        )
        PERFORMANCE_INSTRUMENTATION_AVAILABLE = True
    except ImportError:
        PERFORMANCE_INSTRUMENTATION_AVAILABLE = False
        def timed_operation(*args, **kwargs):
            from contextlib import nullcontext
            return nullcontext()
        def time_db_read(*args, **kwargs):
            from contextlib import nullcontext
            return nullcontext()
    
    # Get connection pool limit from environment
    MAX_DB_CONNECTIONS = int(os.getenv("MAX_DB_CONNECTIONS", 10))
    # Limit concurrent DB reads to respect connection pool
    # Use conservative limit: MAX_DB_CONNECTIONS // 2 to leave room for writes
    max_concurrent_reads = min(3, MAX_DB_CONNECTIONS // 2)
    
    if DEBUG_MODE:
        logger.info(
            f"[FetchDataParallel] Parallelizing 3 DB reads with "
            f"max_concurrency={max_concurrent_reads} "
            f"(MAX_DB_CONNECTIONS={MAX_DB_CONNECTIONS})"
        )
    
    # Extract state values needed for parallel reads
    topic = state.get("topic")
    conversation_id = state.get("conversation_id")
    user_message = state.get("user_message", "")
    subject_id = state.get("subject_id")
    
    # Helper to extract core logic from FetchLesson
    def _fetch_lesson_core(topic_id: str) -> Dict:
        """Core lesson fetching logic (extracted from FetchLesson)"""
        # FAST PATH: If lesson_text already exists in state and hash matches
        if state.get("lesson_text") and state.get("last_lesson_hash"):
            return {
                "lesson_text": state.get("lesson_text"),
                "lesson_chunks": [],
                "last_lesson_hash": state.get("last_lesson_hash")
            }
        
        # Check cache
        lesson_cache_key = f"lesson:{topic_id}"
        lesson_text = cache_get(lesson_cache_key)
        
        if lesson_text is None:
            # Fetch from service
            if DEBUG_MODE:
                logger.info(f"[FetchDataParallel] Fetching lesson for topic_id: {topic_id}")
            lesson_text = lesson_service.fetch_lesson_content(topic_id)
            if lesson_text:
                cache_set(lesson_cache_key, lesson_text, ttl=3600)
        
        # FIX: Handle case where lesson_text might be a list (from old cache format)
        # Convert list to string if needed
        if isinstance(lesson_text, list):
            # If it's a list, join it into a string
            lesson_text = "\n\n".join(str(item) for item in lesson_text) if lesson_text else ""
        elif not isinstance(lesson_text, str):
            # If it's not a string or list, convert to string
            lesson_text = str(lesson_text) if lesson_text else ""
        
        text_hash = (
            hashlib.md5(lesson_text.encode()).hexdigest()
            if lesson_text else None
        )
        return {
            "lesson_text": lesson_text or "",
            "lesson_chunks": [],
            "last_lesson_hash": text_hash
        }
    
    # Helper to extract core logic from RetrieveHistory
    def _fetch_history_core(conv_id: str) -> Dict:
        """Core history fetching logic (extracted from RetrieveHistory)"""
        # Check if history already exists in state
        if "history" in state and len(state.get("history", [])) > 0:
            if DEBUG_MODE:
                logger.info(
                    f"[FetchDataParallel] Using history from state: "
                    f"{len(state['history'])} messages"
                )
            return {}
        
        # Check Redis cache
        cache_key = f"conversation_history:{conv_id}"
        if REDIS_CACHE_AVAILABLE:
            cached_history = cache_get(cache_key)
            if cached_history is not None and len(cached_history) > 0:
                if DEBUG_MODE:
                    logger.info(
                        f"[FetchDataParallel] Using Redis cached history: "
                        f"{len(cached_history)} messages"
                    )
                return {"history": cached_history}
        
        # Check fallback in-memory cache
        if conv_id in conversation_cache_fallback:
            cached_history = conversation_cache_fallback[conv_id]
            if len(cached_history) > 0:
                if DEBUG_MODE:
                    logger.info(
                        f"[FetchDataParallel] Using in-memory cached history: "
                        f"{len(cached_history)} messages"
                    )
                return {"history": cached_history}
        
        # Fetch from DB
        if DEBUG_MODE:
            logger.info(
                "[FetchDataParallel] Cache miss, fetching last 2 messages from Supabase"
            )
        
        history = history_service.get_recent_messages(
            conversation_id=conv_id,
            limit=2
        )
        
        if history:
            # Store in Redis or fallback
            if REDIS_CACHE_AVAILABLE:
                cache_set(cache_key, history[:10], ttl=3600)
            else:
                conversation_cache_fallback[conv_id] = history[:10]
        
        return {"history": history or []}
    
    # Helper to extract core logic from FetchConcepts (simplified - only topic-based fetch)
    def _fetch_concepts_core(topic_id: str, user_msg: str, subj_id: Optional[int]) -> Dict:
        """Core concepts fetching logic (extracted from FetchConcepts - topic-based only)"""
        concept_rows = None
        
        if topic_id:
            try:
                # Determine subject_id if not provided
                if not subj_id:
                    topic_id_int = int(topic_id) if isinstance(topic_id, str) else topic_id
                    if 200 <= topic_id_int <= 302:
                        subj_id = 114  # History
                    elif 305 <= topic_id_int <= 400:
                        subj_id = 113  # Geography
                    elif 500 <= topic_id_int <= 699:
                        subj_id = 119  # Economics
                    elif 100 <= topic_id_int <= 199:
                        subj_id = 102  # Islamiyat
                    else:
                        subj_id = 101  # Business Studies
                
                # Fetch concepts by topic
                concept_rows = concept_service.fetch_concepts_by_topic(
                    topic_id=str(topic_id),
                    limit=10,
                    random_order=True,
                    subject_id=subj_id,
                    job_id=job_id,
                    trace_id=trace_id
                )
                
                if concept_rows and len(concept_rows) > 0:
                    if DEBUG_MODE:
                        logger.info(
                            f"[FetchDataParallel] Fetched {len(concept_rows)} "
                            f"concepts for topic_id: {topic_id}"
                        )
                    return {"concept_rows": concept_rows}
            except Exception as e:
                if DEBUG_MODE:
                    logger.error(
                        f"[FetchDataParallel] Error fetching concepts by topic: {e}"
                    )
        
        # Return empty if topic fetch failed
        return {"concept_rows": []}
    
    # Create async wrappers for DB reads
    async def fetch_lesson_async() -> Dict:
        """Async wrapper for lesson fetch"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, _fetch_lesson_core, topic
            )
            return result
        except Exception as e:
            logger.error(
                f"[FetchDataParallel] Lesson fetch failed: {e}",
                exc_info=True
            )
            return {"lesson_text": "", "lesson_chunks": [], "last_lesson_hash": None}
    
    async def fetch_history_async() -> Dict:
        """Async wrapper for history fetch"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, _fetch_history_core, conversation_id
            )
            return result
        except Exception as e:
            logger.error(
                f"[FetchDataParallel] History fetch failed: {e}",
                exc_info=True
            )
            return {"history": []}
    
    async def fetch_concepts_async() -> Dict:
        """Async wrapper for concepts fetch"""
        try:
            loop = asyncio.get_event_loop()
            # _fetch_concepts_core uses job_id and trace_id from closure, only pass 3 args
            result = await loop.run_in_executor(
                None,
                _fetch_concepts_core,
                topic,
                user_message,
                subject_id
            )
            return result
        except Exception as e:
            logger.error(
                f"[FetchDataParallel] Concepts fetch failed: {e}",
                exc_info=True
            )
            return {"concept_rows": []}
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(max_concurrent_reads)
    
    async def fetch_with_limit(fetch_func, name: str):
        """Fetch with concurrency limit"""
        async with semaphore:
            if PERFORMANCE_INSTRUMENTATION_AVAILABLE:
                with time_db_read(
                    f"fetch_{name}",
                    job_id=job_id,
                    trace_id=trace_id,
                    table=name
                ):
                    return await fetch_func()
            else:
                return await fetch_func()
    
    # Execute all reads concurrently
    try:
        # Instrument parallel execution
        with timed_operation(
            stage_name="fetch_data_parallel",
            stage_type=StageType.PIPELINE_NODE,
            job_id=job_id,
            trace_id=trace_id,
            additional_context={
                "user_id": user_id,
                "max_concurrent_reads": max_concurrent_reads,
                "max_db_connections": MAX_DB_CONNECTIONS
            }
        ):
            # OPTIMIZED: Use existing event loop if available, otherwise create new one
            # This avoids the overhead of creating a new event loop each time
            try:
                # Try to get existing event loop
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    # Loop is closed, create new one
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except RuntimeError:
                # No event loop exists, create new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Create tasks
            async def fetch_all_parallel():
                return await asyncio.gather(
                    fetch_with_limit(fetch_lesson_async, "lesson"),
                    fetch_with_limit(fetch_history_async, "history"),
                    fetch_with_limit(fetch_concepts_async, "concepts"),
                    return_exceptions=True
                )
            
            # Run async code using existing loop (faster than asyncio.run)
            if loop.is_running():
                # If loop is already running, use run_in_executor
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, fetch_all_parallel())
                    results = future.result(timeout=10)
            else:
                # Loop not running, can use run_until_complete (faster than asyncio.run)
                results = loop.run_until_complete(fetch_all_parallel())
        
        # Process results, handling exceptions
        lesson_result, history_result, concepts_result = results
        
        # Merge results safely
        merged_state = {}
        
        # Handle lesson result
        if isinstance(lesson_result, Exception):
            logger.error(
                f"[FetchDataParallel] Lesson fetch raised exception: {lesson_result}",
                exc_info=True
            )
            merged_state.update({"lesson_text": "", "lesson_chunks": [], "last_lesson_hash": None})
        else:
            merged_state.update(lesson_result)
        
        # Handle history result
        if isinstance(history_result, Exception):
            logger.error(
                f"[FetchDataParallel] History fetch raised exception: {history_result}",
                exc_info=True
            )
            merged_state.update({"history": []})
        else:
            merged_state.update(history_result)
        
        # Handle concepts result
        if isinstance(concepts_result, Exception):
            logger.error(
                f"[FetchDataParallel] Concepts fetch raised exception: {concepts_result}",
                exc_info=True
            )
            merged_state.update({"concept_rows": []})
        else:
            merged_state.update(concepts_result)
        
        # Log summary
        if DEBUG_MODE:
            logger.info(
                f"[FetchDataParallel] Parallel fetch completed - "
                f"lesson={'✓' if not isinstance(lesson_result, Exception) else '✗'}, "
                f"history={'✓' if not isinstance(history_result, Exception) else '✗'}, "
                f"concepts={'✓' if not isinstance(concepts_result, Exception) else '✗'}"
            )
        
        return merged_state
        
    except Exception as e:
        # If parallel execution itself fails, fall back to sequential
        logger.error(
            f"[FetchDataParallel] Parallel execution failed, falling back to sequential: {e}",
            exc_info=True
        )
        
        # Sequential fallback
        merged_state = {}
        try:
            merged_state.update(_fetch_lesson_core(topic))
        except Exception as e2:
            logger.error(f"[FetchDataParallel] Sequential lesson fetch failed: {e2}")
            merged_state.update({"lesson_text": "", "lesson_chunks": [], "last_lesson_hash": None})
        
        try:
            merged_state.update(_fetch_history_core(conversation_id))
        except Exception as e2:
            logger.error(f"[FetchDataParallel] Sequential history fetch failed: {e2}")
            merged_state.update({"history": []})
        
        try:
            merged_state.update(
                _fetch_concepts_core(topic, user_message, subject_id, job_id, trace_id)
            )
        except Exception as e2:
            logger.error(
                f"[FetchDataParallel] Sequential concepts fetch failed: {e2}"
            )
            merged_state.update({"concept_rows": []})
        
        return merged_state


# -----------------------------------------------------
# Node 1: FetchLesson (DEPRECATED - Use FetchDataParallel)
# -----------------------------------------------------
def FetchLesson(state: TutorState):
    """
    Retrieve lesson content from Supabase lessons table
    based on topic_id (state['topic']), using LessonService.

    OPTIMIZED: Skip lesson chunks entirely for speed.
    Only fetch lesson text if not already in state.
    """
    topic = state["topic"]

    # FAST PATH: If lesson_text already exists in state and hash matches,
    # return immediately without any DB queries
    if state.get("lesson_text") and state.get("last_lesson_hash"):
        return {
            "lesson_text": state.get("lesson_text"),
            "lesson_chunks": []  # Skip chunks for speed
        }

    # Check cache for lesson content
    lesson_cache_key = f"lesson:{topic}"
    lesson_text = cache_get(lesson_cache_key)

    if lesson_text is None:
        # Fetch from service if not in cache
        if DEBUG_MODE:
            logger.info(f"[FetchLesson] Fetching lesson for topic_id: {topic}")
        lesson_text = lesson_service.fetch_lesson_content(topic)
        if lesson_text:
            if DEBUG_MODE:
                logger.info(
                    f"[FetchLesson] Successfully fetched lesson "
                    f"({len(lesson_text)} chars)"
                )
            # Cache for 1 hour (3600 seconds)
            cache_set(lesson_cache_key, lesson_text, ttl=3600)
        else:
            if DEBUG_MODE:
                logger.warning(
                    f"[FetchLesson] No lesson content returned for "
                    f"topic_id: {topic}"
                )

    # Skip lesson chunks entirely for speed (not critical for responses)
    # FIX: Handle case where lesson_text might be a list (from old cache format)
    # Convert list to string if needed
    if isinstance(lesson_text, list):
        # If it's a list, join it into a string
        lesson_text = "\n\n".join(str(item) for item in lesson_text) if lesson_text else ""
    elif not isinstance(lesson_text, str):
        # If it's not a string or list, convert to string
        lesson_text = str(lesson_text) if lesson_text else ""
    
    # Compute hash and update state
    text_hash = (
        hashlib.md5(lesson_text.encode()).hexdigest()
        if lesson_text else None
    )
    updated_state = {
        "lesson_text": lesson_text or "",
        "lesson_chunks": [],  # Always empty for speed
        "last_lesson_hash": text_hash
    }

    return updated_state


# -----------------------------------------------------
# Node 2: FetchConcepts
# -----------------------------------------------------
def FetchConcepts(state: TutorState):
    """
    Retrieve concepts for the current topic.

    Priority (ALWAYS use topic_id when available):
    1. ALWAYS fetch concepts directly by topic_id if available
       (returns concepts in random order from database)
       This works for EVERY message, regardless of message length or content
    2. If topic_id fetch fails or returns empty, use pgvector similarity search
    3. Fallback to keyword matching if no concepts found

    The result is stored in state['concept_rows'].
    """
    user_message = state["user_message"]
    topic_id = state.get("topic")

    # Debug: Log topic_id value and type
    if DEBUG_MODE:
        logger.info(
            f"[DEBUG] FetchConcepts: topic_id from state: {topic_id} "
            f"(type: {type(topic_id).__name__})"
        )

    # Initialize concept_rows to track if we found concepts
    concept_rows = None

    # PRIORITY 1: ALWAYS try topic_id fetch first if available
    # This is the PRIMARY method and should work for EVERY message
    if topic_id:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] FetchConcepts: ALWAYS using topic_id: {topic_id} "
                f"for message: '{user_message[:50]}...'"
            )
        try:
            # Get subject_id from state for table selection
            subject_id = state.get("subject_id")
            
            # CRITICAL FIX: Validate or determine subject_id from topic_id
            # This prevents wrong table queries (e.g., concepts instead of concepts_history)
            if topic_id:
                try:
                    topic_id_int = int(topic_id) if isinstance(topic_id, str) else topic_id
                    
                    # Determine expected subject_id from topic_id range
                    expected_subject_id = None
                    if 200 <= topic_id_int <= 302:
                        expected_subject_id = 114  # History
                    elif 305 <= topic_id_int <= 400:
                        expected_subject_id = 113  # Geography
                    elif 500 <= topic_id_int <= 699:
                        expected_subject_id = 119  # Economics
                    elif 100 <= topic_id_int <= 199:
                        expected_subject_id = 102  # Islamiyat
                    else:
                        expected_subject_id = 101  # Business Studies (default)
                    
                    # CRITICAL: If subject_id is None or doesn't match expected, use expected_subject_id
                    # This prevents wrong table queries (e.g., concepts instead of concepts_history)
                    if not subject_id or subject_id != expected_subject_id:
                        if subject_id and subject_id != expected_subject_id:
                            if DEBUG_MODE:
                                logger.warning(
                                    f"[DEBUG] FetchConcepts: MISMATCH! subject_id={subject_id} "
                                    f"doesn't match topic_id={topic_id_int} "
                                    f"(expected={expected_subject_id}). "
                                    f"Using expected_subject_id={expected_subject_id}."
                                )
                        else:
                            if DEBUG_MODE:
                                logger.warning(
                                    f"[DEBUG] FetchConcepts: subject_id is None, determined from "
                                    f"topic_id={topic_id_int}: subject_id={expected_subject_id}"
                                )
                        subject_id = expected_subject_id
                    else:
                        if DEBUG_MODE:
                            logger.info(
                                f"[DEBUG] FetchConcepts: subject_id={subject_id} matches "
                                f"topic_id={topic_id_int} range (correct)"
                            )
                except Exception as e:
                    if DEBUG_MODE:
                        logger.error(
                            f"[DEBUG] FetchConcepts: Error validating subject_id from topic_id: {e}"
                        )
                    # Fallback to Business Studies if validation fails
                    # BUT: Don't override if we already determined it from topic_id
                    if not subject_id:
                        # Only default to 101 if we couldn't determine from topic_id
                        if expected_subject_id:
                            subject_id = expected_subject_id
                            if DEBUG_MODE:
                                logger.info(
                                    f"[DEBUG] FetchConcepts: Using expected_subject_id={expected_subject_id} "
                                    f"as fallback (determined from topic_id range)"
                                )
                        else:
                            subject_id = 101
                            if DEBUG_MODE:
                                logger.warning(
                                    f"[DEBUG] FetchConcepts: Could not determine subject_id, "
                                    f"defaulting to 101 (Business Studies)"
                                )
            
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] FetchConcepts: Using subject_id: {subject_id} "
                    f"for topic_id: {topic_id}"
                )
                # Additional debug for Economics
                if subject_id == 119:  # Economics
                    topic_id_int_check = int(topic_id) if isinstance(topic_id, str) else topic_id
                    logger.info(
                        f"[DEBUG] FetchConcepts Economics: topic_id={topic_id_int_check}, "
                        f"range check: {500 <= topic_id_int_check <= 699}, "
                        f"will query table: concepts_economics"
                    )
            
            # Fetch concepts directly from database for this topic
            # This works regardless of message length or content
            # Uses subject-specific concept tables
            concept_rows = concept_service.fetch_concepts_by_topic(
                topic_id=str(topic_id),
                limit=10,  # Return 10 concepts in random order
                random_order=True,
                subject_id=subject_id,  # Pass subject_id to select correct table
                job_id=state.get("job_id"),
                trace_id=state.get("trace_id")
            )
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] FetchConcepts: Fetched {len(concept_rows)} "
                    f"concepts for topic_id: {topic_id}, subject_id: {subject_id}"
                )
                if concept_rows:
                    concept_names = [
                        c.get('name', 'N/A') for c in concept_rows[:3]
                    ]
                    logger.info(
                        f"[DEBUG] FetchConcepts: Concepts found: "
                        f"{concept_names}"
                    )
                else:
                    logger.warning(
                        f"[DEBUG] FetchConcepts: NO CONCEPTS FOUND for "
                        f"topic_id: {topic_id}, subject_id: {subject_id}. "
                        f"This indicates either: "
                        f"1) No concepts exist in concepts_economics table for this topic_id, "
                        f"2) Topic_id is not in Economics range (500-699), or "
                        f"3) Table query failed silently."
                    )

            # If we got concepts from topic, return them immediately
            # This is the PRIMARY path - topic_id should ALWAYS work
            if concept_rows and len(concept_rows) > 0:
                if DEBUG_MODE:
                    logger.info(
                        f"[DEBUG] FetchConcepts: SUCCESS - Returning "
                        f"{len(concept_rows)} concepts from topic-based fetch "
                        f"(topic_id: {topic_id})"
                    )
                return {"concept_rows": concept_rows}
            else:
                # Topic fetch returned empty - log warning and try fallbacks
                if DEBUG_MODE:
                    logger.warning(
                        f"[DEBUG] FetchConcepts: Topic fetch returned empty "
                        f"for topic_id: {topic_id}. "
                        f"Trying fallback methods (embedding/keyword search)."
                    )
                # concept_rows is already None, continue to fallbacks
        except Exception as e:
            if DEBUG_MODE:
                logger.error(
                    f"[DEBUG] FetchConcepts: Error fetching by "
                    f"topic_id {topic_id}: {e}"
                )
                import traceback
                logger.error(f"[DEBUG] Traceback: {traceback.format_exc()}")
            # concept_rows is already None, continue to fallbacks
    else:
        # No topic_id available - log and use fallback methods
        if DEBUG_MODE:
            logger.warning(
                "[DEBUG] FetchConcepts: No topic_id in state. "
                "Using fallback methods (embedding/keyword search)."
            )

    # Priority 2: Use pgvector similarity search (original method)
    # Skip embeddings/RPC for short queries, but still try keyword matching
    subject_id = state.get("subject_id") or 101

    # Only try embedding search for messages with 4+ words
    # AND if we don't already have concepts from topic fetch
    needs_embedding = (
        len(user_message.split()) >= 4 and
        (concept_rows is None or len(concept_rows) == 0)
    )
    if needs_embedding:
        # Hash user message for cache key
        message_hash = hashlib.md5(
            user_message.encode('utf-8')
        ).hexdigest()
        concept_cache_key = f"concepts:{subject_id}:{message_hash}"

        # Check cache for concept rows
        cached_result = cache_get(concept_cache_key)

        # Only use cache if it has actual results (not empty list)
        concept_rows_from_cache = (
            cached_result
            if (cached_result and len(cached_result) > 0)
            else None
        )

        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] FetchConcepts: Cache check - "
                f"cached={cached_result is not None}, "
                f"cached_count={len(cached_result) if cached_result else 0}, "
                f"using_cache={concept_rows_from_cache is not None}"
            )

        if concept_rows_from_cache is None:
            # Fetch from service if not in cache
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] FetchConcepts: Fetching from service - "
                    f"message='{user_message[:50]}...', "
                    f"subject_id={subject_id}, "
                    f"topic_id={state.get('topic')}"
                )
            concept_rows_from_cache = concept_service.find_related_concepts(
                message_text=user_message,
                subject_id=subject_id,  # From state (backend)
                topic_id=state.get("topic"),
                k=7,
                min_similarity=0.18,
                job_id=state.get("job_id"),
                trace_id=state.get("trace_id")
            )
            if DEBUG_MODE:
                count = (
                    len(concept_rows_from_cache)
                    if concept_rows_from_cache else 0
                )
                logger.info(
                    f"[DEBUG] FetchConcepts: Embedding search returned "
                    f"{count} concepts"
                )
            if concept_rows_from_cache:
                # Cache for 2 hours (7200 seconds)
                cache_set(concept_cache_key, concept_rows_from_cache, ttl=7200)

        # Use embedding search results if found
        if concept_rows_from_cache:
            concept_rows = concept_rows_from_cache
    else:
        # Short message - skip embedding search, will try keyword matching
        if DEBUG_MODE:
            word_count = len(user_message.split())
            logger.info(
                f"[DEBUG] FetchConcepts: Message too short "
                f"({word_count} words), skipping embedding search, "
                f"will try keyword matching..."
            )

    # Priority 3: Fallback to keyword matching if no concepts found
    if not concept_rows or len(concept_rows) == 0:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] FetchConcepts: No concepts found, "
                f"trying keyword_match fallback - "
                f"message='{user_message}', subject_id={subject_id}, "
                f"topic_id={topic_id}"
            )
        try:
            # Ensure subject_id is converted to string if it's an int
            subject_id_str = str(subject_id) if subject_id else None
            # Get topic_id from state (from topic selection)
            topic_id_str = (
                str(state.get("topic")) if state.get("topic") else None
            )
            concept_rows = concept_service.keyword_match(
                message_text=user_message,
                subject_id=subject_id_str,
                topic_id=topic_id_str,
                job_id=state.get("job_id"),
                trace_id=state.get("trace_id")
            )
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] FetchConcepts: Keyword match returned "
                    f"{len(concept_rows) if concept_rows else 0} concepts"
                )
                if concept_rows:
                    concept_names = [
                        c.get('name', 'N/A') for c in concept_rows[:3]
                    ]
                    logger.info(
                        f"[DEBUG] FetchConcepts: Keyword match found "
                        f"concepts: {concept_names}"
                    )
        except Exception as e:
            if DEBUG_MODE:
                logger.error(
                    f"[DEBUG] FetchConcepts: Error in keyword_match: {e}"
                )
            concept_rows = []

    # Ensure concept_rows is always a list, never None
    if concept_rows is None:
        concept_rows = []

    # Debug: Embedding results
    if DEBUG_MODE:
        logger.info(
            f"[DEBUG] Embedding results: Found {len(concept_rows)} concepts"
        )
        for c in concept_rows[:3]:  # Show first 3
            logger.info(
                f"  - {c.get('name', 'N/A')} "
                f"(distance: {c.get('distance', 'N/A')})"
            )

    return {"concept_rows": concept_rows or []}


# -----------------------------------------------------
# Node 3: RetrieveHistory
# -----------------------------------------------------
def RetrieveHistory(state: TutorState):
    """
    Retrieve conversation history with optimized caching.

    Priority:
    1. Use history from state if already present (from frontend)
    2. Use in-memory conversation_cache if available
    3. Fetch only last 3 messages from Supabase (fallback)

    The method returns a list of dicts:
        { "role": "user" | "assistant", "content": str }

    These messages preserve the conversation context for the LLM.
    They are stored in state['history'].
    """
    conversation_id = state["conversation_id"]

    # 1. Check if history already exists in state (from frontend)
    if "history" in state and len(state.get("history", [])) > 0:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] Using history from state: "
                f"{len(state['history'])} messages"
            )
        return {}  # No update needed

    # 2. Check Redis cache (scalable, persistent across workers)
    cache_key = f"conversation_history:{conversation_id}"
    if REDIS_CACHE_AVAILABLE:
        cached_history = cache_get(cache_key)
        if cached_history is not None and len(cached_history) > 0:
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] Using Redis cached history: "
                    f"{len(cached_history)} messages"
                )
            return {"history": cached_history}
    
    # 2b. Fallback to in-memory cache if Redis unavailable
    if conversation_id in conversation_cache_fallback:
        cached_history = conversation_cache_fallback[conversation_id]
        if len(cached_history) > 0:
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] Using in-memory cached history: "
                    f"{len(cached_history)} messages"
                )
            return {"history": cached_history}

    # 3. Fallback: Fetch only last 2 messages from Supabase (further reduced)
    # Skip DB query if cache exists (even if empty) to save time
    if DEBUG_MODE:
        logger.info(
            "[DEBUG] Cache miss, fetching last 2 messages from Supabase"
        )

    history = history_service.get_recent_messages(
        conversation_id=conversation_id,
        limit=2  # Reduced to 2 for speed
    )

    # Update cache with fetched messages (store up to 10 for speed)
    if history:
        # Store in Redis (1 hour TTL for conversation history)
        if REDIS_CACHE_AVAILABLE:
            cache_set(cache_key, history[:10], ttl=3600)
        # Also update fallback cache
        conversation_cache_fallback[conversation_id] = history[:10]
        # Limit fallback cache size to prevent memory issues
        if len(conversation_cache_fallback) > 100:
            # Remove oldest entries (simple FIFO)
            oldest_key = next(iter(conversation_cache_fallback))
            del conversation_cache_fallback[oldest_key]

    return {"history": history or []}


# -----------------------------------------------------
# Node 3.5: SummarizeHistory
# -----------------------------------------------------
def SummarizeHistory(state: TutorState):
    """
    Summarize conversation history if it exceeds token limits.

    If total tokens > 6000:
        Use llm_service.summarize_history() to summarize into a short abstract.
    Only summarizes every 3 turns to reduce LLM calls.
    Otherwise:
        condensed_history = the concatenated history.
    """
    history = state.get("history", [])

    if not history:
        return {"condensed_history": None}

    # Build history text
    history_text = "\n".join([
        f"{m['role']}: {m['content']}"
        for m in history
    ])

    # Estimate tokens (rough: 1 token ≈ 4 characters)
    total_chars = sum(
        len(msg.get("content", "")) for msg in history
    )
    estimated_tokens = total_chars // 4

    # Debug: History token count
    if DEBUG_MODE:
        logger.info(
            f"[DEBUG] History token count: {estimated_tokens} tokens "
            f"({len(history)} messages)"
        )

    # Fast path: If history is small, return immediately without LLM call
    # Increased threshold to 8000 tokens to skip summarization more often
    if estimated_tokens <= 8000:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] History within limit ({estimated_tokens} tokens) - "
                f"skipping summarization"
            )
        return {"condensed_history": history_text}

    # Only summarize every 5 turns to reduce LLM calls (increased from 3)
    if len(history) % 5 != 0:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] Skipping summarization (turn {len(history)} "
                f"not divisible by 5, tokens: {estimated_tokens})"
            )
        # Use last 8000 tokens worth of history instead of full summary
        # Approximate: keep last N messages that fit in 8000 tokens
        remaining_tokens = 8000
        trimmed_messages = []
        for msg in reversed(history):
            msg_tokens = len(msg.get("content", "")) // 4
            if remaining_tokens >= msg_tokens:
                trimmed_messages.insert(0, msg)
                remaining_tokens -= msg_tokens
            else:
                break
        trimmed_history = "\n".join([
            f"{m['role']}: {m['content']}"
            for m in trimmed_messages
        ])
        return {"condensed_history": trimmed_history}

    # Only summarize if tokens exceed threshold AND it's the right turn
    if estimated_tokens > 8000:
        # Summarize using llm_service with SHORT input (first 1500 chars)
        summary = llm_service.summarize_history(history_text[:1500])
        return {"condensed_history": summary}
    else:
        # Use concatenated history as-is
        return {"condensed_history": history_text}


# -----------------------------------------------------
# Node 4: ClassifyReasoning
# -----------------------------------------------------
def ClassifyReasoning(state: TutorState):
    """
    Classify the student's reasoning quality using MasteryService.

    The method analyzes the student message and returns EXACT labels:
      - 'good'
      - 'neutral'
      - 'confused'

    The result is stored in state['reasoning_label'].

    OPTIMIZATION: If no concepts found, skip classification and return
    'neutral' to avoid unnecessary LLM call.
    """
    # Fast path: Skip classification if no concepts (will be neutral anyway)
    concept_rows = state.get("concept_rows", [])
    if not concept_rows or len(concept_rows) == 0:
        if DEBUG_MODE:
            logger.info(
                "[DEBUG] No concepts found - skipping reasoning "
                "classification, returning 'neutral'"
            )
        return {"reasoning_label": "neutral"}

    # Get subject_name from subject_id for subject-aware classification
    subject_id = state.get("subject_id")
    subject_name = None
    if subject_id is not None:
        subject_map = {
            101: "Business Studies",
            102: "Islamiyat",
            103: "Mathematics",
            104: "Physics",
            105: "Chemistry",
            113: "Pak Studies Geography",
            114: "Pak Studies History",
            119: "Economics"
        }
        subject_name = subject_map.get(subject_id)

    label = mastery_service.classify_student_reasoning(
        message_text=state["user_message"],
        subject_name=subject_name,
        job_id=state.get("job_id"),
        trace_id=state.get("trace_id")
    )

    # Debug: Reasoning label
    if DEBUG_MODE:
        logger.info(f"[DEBUG] Reasoning label: {label}")

    return {"reasoning_label": label}


# -----------------------------------------------------
# Node 5: GenerateLLMResponse
# -----------------------------------------------------
def GenerateLLMResponse(state: TutorState):
    """
    Generate the AI tutor's detailed explanation using LLMService.

    This method uses:
      - lesson_text
      - related concepts
      - conversation history
      - student question
    to produce the final answer.

    The result is stored in state['llm_response'].
    """
    # OPTIMIZED: Skip student profile fetch entirely for speed
    # Use default profile to avoid DB query
    student_profile = {
        "learning_style": "visual",
        "speed": "moderate",
        "grade_level": "intermediate",
        "subject_strengths": []
    }

    # OPTIMIZED: Balance between context and speed
    # 1. Limit lesson_text to first 800 chars (increased from 500 for better context)
    lesson_text = (state.get("lesson_text") or "")[:800]

    # 2. Limit history to last 3 messages (increased from 2 for better context)
    trimmed_history = state["history"][-3:] if state.get("history") else []

    # 3. Skip lesson_chunks entirely (always empty for speed)
    trimmed_chunks = []

    # Trim context to fit within token budget (2500 tokens - increased for better responses)
    trimmed_history_final, trimmed_lesson_final, trimmed_chunks_final = (
        llm_service.trim_context(
            history=trimmed_history,
            lesson_text=lesson_text,
            chunks=trimmed_chunks,
            max_tokens=2500  # Increased for better context while maintaining speed
        )
    )

    # Get subject_id and subject_name from state
    subject_id = state.get("subject_id")
    subject_name = None
    
    # Get subject name from subject_id if available
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

    # Performance measurement: Combined AI call (response + reasoning)
    import time
    llm_start_time = time.time()
    
    try:
        # COMBINED AI CALL: Generate response and classify reasoning in one call
        # This reduces from 2 LLM calls to 1 LLM call, saving ~300-1200ms latency
        response_text, token_usage, reasoning_label = (
            llm_service.generate_reply(
                message=state["user_message"],
                topic=state["topic"],
                learning_level=student_profile.get("grade_level", "intermediate"),
                conversation_history=trimmed_history_final,
                lesson_content=trimmed_lesson_final,
                concept_rows=state["concept_rows"],
                explanation_style=state["explanation_style"],
                lesson_chunks=trimmed_chunks_final,
                condensed_history=state.get("condensed_history"),
                student_profile=student_profile,
                subject_id=subject_id,
                subject_name=subject_name,
                job_id=state.get("job_id"),
                trace_id=state.get("trace_id")
            )
        )
        llm_elapsed = time.time() - llm_start_time
        
        # Log performance metrics
        if DEBUG_MODE:
            logger.info(
                f"[PERF] Combined AI call completed in {llm_elapsed:.2f}s "
                f"(response + reasoning classification)"
            )
            logger.info(
                f"[PERF] Token usage: {token_usage.get('total_tokens', 0)} "
                f"(prompt: {token_usage.get('prompt_tokens', 0)}, "
                f"completion: {token_usage.get('completion_tokens', 0)})"
            )
    except Exception as e:
        llm_elapsed = time.time() - llm_start_time
        logger.error(f"[LLM Failure] {e} (after {llm_elapsed:.2f}s)")
        response_text = llm_service.fallback_reply(
            message=state["user_message"],
            topic=state["topic"]
        )
        token_usage = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }
        reasoning_label = "neutral"  # Default for fallback

    # Debug: Token usage and reasoning
    if DEBUG_MODE:
        logger.info(
            f"[DEBUG] Token Usage: "
            f"Input={token_usage.get('prompt_tokens', 0)}, "
            f"Output={token_usage.get('completion_tokens', 0)}, "
            f"Total={token_usage.get('total_tokens', 0)}"
        )
        logger.info(
            f"[DEBUG] Reasoning Label (from combined call): {reasoning_label}"
        )

    return {
        "llm_response": response_text,
        "token_usage": token_usage,
        "reasoning_label": reasoning_label  # Include reasoning_label in state
    }


# -----------------------------------------------------
# Node 6: UpdateMastery
# -----------------------------------------------------
def UpdateMastery(state: TutorState):
    """
    Update student mastery scores based on reasoning quality.

    Steps:
      1. Convert reasoning label → mastery delta using MasteryService
      2. Create update objects for each detected concept
      3. Call MasteryService.apply_mastery_updates() to write updates
         into Supabase tables (student_mastery, student_weaknesses,
         student_trends).
      4. Store updates inside state['mastery_updates'].
    """

    # FIXED: Only update mastery for ONE concept per message
    # Use the first concept from concept_rows (most relevant to current message)
    # This prevents creating 10 entries for all concepts in the topic
    concept_id = None
    if state.get("concept_rows") and len(state["concept_rows"]) > 0:
        # Get the first concept (most relevant to the current message)
        first_concept = state["concept_rows"][0]
        concept_id_raw = first_concept.get("concept_id")
        if concept_id_raw not in (None, "None", ""):
            try:
                concept_id = int(concept_id_raw)
            except (ValueError, TypeError):
                concept_id = None

    # Return empty updates if no concept_id found
    if not concept_id:
        return {"mastery_updates": []}

    label = state["reasoning_label"]

    # Convert label -> mastery delta using MasteryService
    delta = mastery_service.label_to_delta(label)

    # FIXED: Only create ONE update for the primary concept
    updates = [{
        "concept_id": concept_id,
            "delta": delta,
            "reason": f"tutor_chat_{label}"
    }]

    # Get subject_id and subject_name from state
    subject_id = state.get("subject_id")
    subject_name = None
    
    # Get subject name from subject_id if available
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

    # Apply full update logic using MasteryService
    # FIXED: Only update ONE concept per message (not all concepts in topic)
    # This ensures only one entry is created in mastery table per response
    # OPTIMIZED: Use async_write to move mastery updates to background (non-blocking)
    # This allows response to be returned immediately while updates happen in background
    if len(updates) > 0:
        # OPTIMIZED: Move mastery updates to background thread (non-blocking)
        # This ensures response is returned immediately while updates complete in background
        async_write(
            mastery_service.apply_mastery_updates,
            user_id=state["user_id"],
            updates=updates,
            subject_id=subject_id,
            subject_name=subject_name
        )
        if DEBUG_MODE:
                logger.info(
                f"[DEBUG] Mastery update queued (async) for "
                    f"user {state['user_id']}, concept_id={concept_id}, delta={delta}"
            )

    # Invalidate readiness cache if updates were made (delta != 0)
    if delta != 0:
        # Invalidate readiness cache for this user/concept combination
        # This ensures readiness reflects updated mastery scores
        # FIXED: Use single concept_id instead of all concept_ids
        concept_ids_str = [str(concept_id)]
        
        # Use centralized cache invalidation if available
        try:
            from services.deterministic_cache import (
                invalidate_cache, CacheOperation
            )
            # Invalidate using centralized cache utility
            invalidated = invalidate_cache(
                CacheOperation.READINESS_ASSESSMENT,
                state["user_id"],
                concept_ids_str
            )
            # Log invalidation at INFO level for production monitoring
            logger.info(
                f"[CACHE INVALIDATE] readiness_assessment - "
                f"user_id: {state['user_id']}, "
                f"concept_count: {len(concept_ids_str)}, "
                f"success: {invalidated}"
            )
            if DEBUG_MODE:
                logger.info(
                    "[DEBUG] Invalidated readiness cache (centralized) after mastery updates"
                )
        except ImportError:
            # Fallback to legacy cache invalidation
            # Invalidate cache using both cache key formats
            # Format 1: readiness_agent format (sorted concept ID hash)
            try:
                from cache import _hash_string
                concept_id_hash = _hash_string(str(concept_id))
                cache_key_1 = f"readiness:{state['user_id']}:{concept_id_hash}"
                cache_delete(cache_key_1)
            except Exception:
                pass
            # Format 2: langgraph_tutor format (hash of concept_id)
            try:
                cache_key_2 = (
                    f"readiness:{state['user_id']}:{hash(str(concept_id))}"
                )
                cache_delete(cache_key_2)
            except Exception:
                pass

            if DEBUG_MODE:
                logger.info(
                    "[DEBUG] Invalidated readiness cache (legacy) after mastery updates"
                )

    # Debug: Mastery deltas
    if DEBUG_MODE:
        logger.info(f"[DEBUG] Mastery update: concept_id={concept_id}, delta={delta:+d}, reason={updates[0]['reason'] if updates else 'N/A'}")

    # Save updates (may be empty)
    return {"mastery_updates": updates}


# -----------------------------------------------------
# Node 7: LogMessage
# -----------------------------------------------------
def LogMessage(state: TutorState):
    """
    Log the final AI message into Supabase using:
        MessageService.log()

    Stores:
      - user_id
      - lesson_topic (topic)
      - conversation_id
      - role="assistant"
      - AI message text
      - the list of related concept IDs detected during FetchConcepts
      - subject (subject name if available)
      - subject_id (subject ID if available)

    Also invalidates relevant caches to ensure fresh data after updates.
    """

    # Extract concept IDs from concept_rows
    concept_ids = [
        row.get("concept_id")
        for row in state["concept_rows"]
        if row.get("concept_id")
    ]
    
    # Get subject_id and convert to subject name if needed
    subject_id = state.get("subject_id")
    subject_name = None
    if subject_id is not None:
        subject_map = {
            101: "Business Studies",
            102: "Islamiyat",
            103: "Mathematics",
            104: "Physics",
            105: "Chemistry",
            113: "Pak Studies Geography",
            114: "Pak Studies History",
            119: "Economics"
        }
        subject_name = subject_map.get(subject_id)

    # Write the assistant message to Supabase (async fire-and-forget)
    async_write(
        message_service.log,
        user_id=state["user_id"],
        lesson_topic=state["topic"],
        conversation_id=state["conversation_id"],
        role="assistant",
        content=state["llm_response"],
        concept_ids=concept_ids,
        subject=subject_name,
        subject_id=subject_id
    )

    # OPTIMIZED: Move mastery fetching to background (non-blocking)
    # This improves response time by not waiting for mastery data (saves 3-4 seconds)
    # Fetch and store mastery values for this tutor output (background)
    # This ensures every tutor response has mastery data recorded
    if concept_ids and supabase_client and state["user_id"]:
        # Move entire mastery fetching to background - pass all needed state
        # Use lambda to properly pass keyword arguments
        async_write(
            lambda: _fetch_and_store_mastery_background(
                user_id=state["user_id"],
                conversation_id=state["conversation_id"],
                topic=state["topic"],
                concept_ids=concept_ids,
                supabase_client=supabase_client,
                mastery_updates=state.get("mastery_updates", []),
                reasoning_label=state.get("reasoning_label", "neutral")
            )
        )
    
    return {"message_logged": True}


def _fetch_and_store_mastery_background(
    user_id, conversation_id, topic, concept_ids, supabase_client,
    mastery_updates, reasoning_label
):
    """
    Background function to fetch and store mastery values.
    This runs asynchronously and doesn't block the response.
    """
    try:
        # Convert concept_ids to integers for database query
        concept_ids_int = [
            int(cid) for cid in concept_ids
            if cid and str(cid).strip() and str(cid) != "None"
        ]

        if concept_ids_int:
                # PARALLEL DB READS: Fetch mastery and mastery_states concurrently
                # These two reads are independent and can run in parallel
                def fetch_mastery_func():
                    return sb_execute(
                        supabase_client.table("student_mastery")
                        .select("concept_id, mastery_score")
                    .eq("user_id", user_id)
                        .in_("concept_id", concept_ids_int)
                    )

                def fetch_mastery_states_func():
                    return sb_execute(
                        supabase_client.table("mastery_states")
                        .select(
                            "mastery_concept, mastery_micro, mastery_macro"
                        )
                    .eq("user_id", user_id)
                        .limit(1)
                    )

                # Execute both reads concurrently
                async def fetch_mastery_async():
                    loop = asyncio.get_event_loop()
                    return await loop.run_in_executor(
                        None,
                        lambda: safe_supabase_query(
                            fetch_mastery_func, timeout=5, default_return={"data": []}
                        )
                    )

                async def fetch_mastery_states_async():
                    loop = asyncio.get_event_loop()
                    return await loop.run_in_executor(
                        None,
                        lambda: safe_supabase_query(
                            fetch_mastery_states_func,
                            timeout=5,
                            default_return={"data": []}
                        )
                    )

                # Run both reads in parallel
                # CRITICAL FIX: Don't use asyncio.run() in a thread - it can cause deadlocks
                # Instead, use sequential fallback or create new event loop properly
                try:
                    # Try to get existing event loop, create new one if none exists
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_closed():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                    except RuntimeError:
                        # No event loop in this thread - create one
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    
                    # Run async operations
                    mastery_result, mastery_states_result = loop.run_until_complete(
                        asyncio.gather(
                            fetch_mastery_async(),
                            fetch_mastery_states_async(),
                            return_exceptions=True
                        )
                    )

                    # Handle exceptions
                    if isinstance(mastery_result, Exception):
                        logger.warning(
                            f"[LogMessage] Mastery fetch failed: {mastery_result}"
                        )
                        mastery_result = {"data": []}
                    if isinstance(mastery_states_result, Exception):
                        logger.warning(
                            f"[LogMessage] Mastery states fetch failed: {mastery_states_result}"
                        )
                        mastery_states_result = {"data": []}
                except Exception as e:
                    logger.warning(
                        f"[LogMessage] Parallel mastery fetch failed, using sequential: {e}"
                    )
                    # Sequential fallback
                    mastery_result = safe_supabase_query(
                        fetch_mastery_func, timeout=5, default_return={"data": []}
                    )
                    mastery_states_result = safe_supabase_query(
                        fetch_mastery_states_func,
                        timeout=5,
                        default_return={"data": []}
                    )

                # Handle Supabase APIResponse object (has .data attribute, not dict)
                mastery_data = []
                if mastery_result:
                    # Check if it's an APIResponse object (has .data attribute)
                    if hasattr(mastery_result, 'data'):
                        mastery_data = mastery_result.data or []
                    # Check if it's a dict with "data" key (fallback)
                    elif isinstance(mastery_result, dict) and "data" in mastery_result:
                        mastery_data = mastery_result.get("data", [])
                    # If it's already a list (from default_return)
                    elif isinstance(mastery_result, list):
                        mastery_data = mastery_result
                
                if mastery_data:
                    # Build mastery mapping: concept_id -> mastery_score
                    mastery_map = {
                        row["concept_id"]: row.get("mastery_score", 50)
                        for row in mastery_data
                    }
                else:
                    mastery_map = {}

                mastery_states = {}
                if mastery_states_result:
                    # Check if it's an APIResponse object (has .data attribute)
                    if hasattr(mastery_states_result, 'data'):
                        states_data = mastery_states_result.data or []
                    # Check if it's a dict with "data" key (fallback)
                    elif isinstance(mastery_states_result, dict) and "data" in mastery_states_result:
                        states_data = mastery_states_result.get("data", [])
                    else:
                        states_data = []
                    
                    if states_data and len(states_data) > 0:
                        states_row = states_data[0]
                        # Handle both dict and object responses
                        if isinstance(states_row, dict):
                            mastery_states = {
                                "mastery_concept": states_row.get("mastery_concept", 0),
                                "mastery_micro": states_row.get("mastery_micro", 0),
                                "mastery_macro": states_row.get("mastery_macro", 0),
                            }
                        else:
                            # Object with attributes
                            mastery_states = {
                                "mastery_concept": getattr(states_row, "mastery_concept", 0),
                                "mastery_micro": getattr(states_row, "mastery_micro", 0),
                                "mastery_macro": getattr(states_row, "mastery_macro", 0),
                            }

                # Calculate average mastery for concepts
                if mastery_map:
                    avg_mastery = (
                        sum(mastery_map.values()) / len(mastery_map)
                    )
                else:
                    avg_mastery = 50.0  # Default baseline

                # Store mastery entry with tutor message reference
                # This creates a record linking the tutor output to
                # mastery state
                mastery_entry = {
                    "user_id": user_id,
                    "conversation_id": conversation_id,
                    "topic": topic,
                    "concept_masteries": mastery_map,  # JSON object
                    "average_mastery": avg_mastery,
                    "mastery_states": mastery_states,  # JSON object
                    "mastery_updates_applied": mastery_updates,
                    "reasoning_label": reasoning_label,
                    "timestamp": datetime.now().isoformat(),
                }

                def insert_mastery_tracking_func():
                    # Store in tutor_mastery_tracking table
                    # If table doesn't exist, we'll log the data
                    return (
                        supabase_client.table(
                            "tutor_mastery_tracking"
                        )
                        .insert(mastery_entry)
                    )

                # Try to insert mastery tracking, but don't fail if
                # table doesn't exist
                try:
                    safe_supabase_query(
                        insert_mastery_tracking_func,
                        timeout=5,
                        default_return=None
                    )
                    if DEBUG_MODE:
                        logger.info(
                            f"[DEBUG] Mastery tracking entry created for "
                            f"user {user_id}, "
                            f"avg_mastery={avg_mastery:.2f}"
                        )
                except Exception as e:
                    # If table doesn't exist, log to console for now
                    if DEBUG_MODE:
                        logger.warning(
                            f"[DEBUG] Could not store mastery tracking "
                            f"(table may not exist): {e}. "
                            f"Mastery data: {mastery_entry}"
                        )  # noqa: E501
                    # Still log the mastery data for debugging
                    logger.info(
                        f"[MASTERY_TRACKING] User: {user_id}, "
                        f"Avg Mastery: {avg_mastery:.2f}, "
                        f"Concepts: {list(mastery_map.keys())}, "
                        f"States: {mastery_states}"
                    )

    except Exception as e:
        logger.warning(
            f"[WARNING] Failed to fetch/store mastery for tutor output: "
            f"{e}",
            exc_info=True
        )
        # Continue execution even if mastery tracking fails


# -----------------------------------------------------
# Node 8: ComputeReadiness
# -----------------------------------------------------
def ComputeReadiness(state: TutorState):
    """
    Compute the student's readiness level for the detected concepts.

    Uses:
        readiness_service.compute_readiness_signal(user_id, concept_ids)

    Produces:
        state['readiness'] = {
            "overall_readiness": str,
            "concept_readiness": [...],
            "average_mastery": float,
            "min_mastery": int
        }
    """

    # Extract concept_ids and convert to strings
    # (required by readiness service)
    concept_ids = [
        str(row.get("concept_id"))
        for row in state["concept_rows"]
        if row.get("concept_id") not in (None, "None", "")
    ]

    # FAST PATH: Return default readiness immediately if no concepts
    # This avoids DB query and computation
    if not concept_ids:
        if DEBUG_MODE:
            logger.info(
                "[DEBUG] ComputeReadiness: No concept_ids found! "
                "Returning default readiness immediately."
            )
        return {
            "readiness": {
                "overall_readiness": "unknown",
                "concept_readiness": [],
                "average_mastery": 50.0,  # Default baseline
                "min_mastery": 50
            }
        }

    # NOTE: Caching is now handled by readiness_service.compute_readiness_signal()
    # which uses centralized caching with @cached_operation decorator.
    # Cache keys include user_id and concept_ids, TTL is 15 minutes (900s),
    # and cache is invalidated on mastery updates.
    # No need for manual cache check here - readiness_service handles it.

    # Debug: Concept IDs extraction
    if DEBUG_MODE:
        logger.info(
            f"[DEBUG] ComputeReadiness: Found {len(concept_ids)} concept IDs"
        )
        if len(concept_ids) > 0:
            logger.info(
                f"[DEBUG] Concept IDs (first 3): {concept_ids[:3]}"
            )
        else:
            logger.warning(
                "[DEBUG] WARNING: No concept_ids found! "
                "Check FetchConcepts node."
            )
            logger.info(
                f"[DEBUG] concept_rows count: "
                f"{len(state.get('concept_rows', []))}"
            )
            if state.get("concept_rows"):
                logger.info(
                    f"[DEBUG] Sample concept_row: "
                    f"{state['concept_rows'][0]}"
                )

    # Apply mastery updates from current session before computing readiness
    # This ensures readiness reflects mastery changes from this conversation
    mastery_updates = state.get("mastery_updates", [])
    if mastery_updates and len(mastery_updates) > 0:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] ComputeReadiness: Applying {len(mastery_updates)} "
                f"mastery updates from current session"
            )
        # Compute readiness with mastery updates applied
        readiness = readiness_service.compute_readiness_signal(
            user_id=state["user_id"],
            concept_ids=concept_ids,
            mastery_updates=mastery_updates,
            job_id=state.get("job_id"),
            trace_id=state.get("trace_id")
        )
    else:
        # No mastery updates in this session, use standard calculation
        readiness = readiness_service.compute_readiness_signal(
            user_id=state["user_id"],
            concept_ids=concept_ids,
            job_id=state.get("job_id"),
            trace_id=state.get("trace_id")
        )

    # Debug: Readiness computation
    if DEBUG_MODE:
        if readiness:
            logger.info(
                f"[DEBUG] Readiness computation: "
                f"overall={readiness.get('overall_readiness', 'N/A')}, "
                f"avg_mastery={readiness.get('average_mastery', 'N/A')}, "
                f"min_mastery={readiness.get('min_mastery', 'N/A')}, "
                f"concept_readiness_count="
                f"{len(readiness.get('concept_readiness', []))}"
            )

    # NOTE: Caching is handled by readiness_service (centralized caching)
    # Cache TTL: 15 minutes (900s), invalidated on mastery updates
    # Cache keys include user_id and concept_ids for proper scoping
    # Cache hit/miss logging is at INFO level for production monitoring
    return {"readiness": readiness}


# -----------------------------------------------------
# Node 9: ComputeLearningPath
# -----------------------------------------------------
def ComputeLearningPath(state: TutorState):
    """
    Determine the student's next recommended learning step.

    Uses:
        readiness_service.compute_next_learning_step(readiness, concept_ids)
    """
    # Extract concept_ids and ensure they're strings
    concept_ids = [
        str(row.get("concept_id"))
        for row in state["concept_rows"]
        if row.get("concept_id") not in (None, "None", "")
    ]

    # Return empty learning path if no concept_ids found
    if not concept_ids:
        if DEBUG_MODE:
            logger.warning(
                "[DEBUG] ComputeLearningPath: No concept_ids found! "
                "Returning unknown learning path."
            )
        return {
            "learning_path": {
                "decision": "unknown",
                "recommended_concept": None,
                "recommended_concept_name": None,
                "details": "No concepts available."
            }
        }

    # NO-REPEAT ROTATION: Track shown concepts per user/topic
    # Ensures all 10 concepts are shown before any repeat
    user_id = state.get("user_id", "")
    topic_id = state.get("topic", "")
    subject_id = state.get("subject_id")  # Get subject_id for table selection
    
    # CRITICAL FIX: If subject_id is None, try to determine it from topic_id
    # This ensures concepts are fetched from the correct table for all 5 subjects
    if not subject_id and topic_id:
        if DEBUG_MODE:
            logger.warning(
                f"[DEBUG] ComputeLearningPath: subject_id is None in state, "
                f"attempting to determine from topic_id: {topic_id}"
            )
        try:
            topic_id_int = int(topic_id) if isinstance(topic_id, str) else topic_id
            
            # Determine subject_id from topic_id ranges (same logic as FetchConcepts)
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
            
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] ComputeLearningPath: Determined subject_id: {subject_id} "
                    f"from topic_id range: {topic_id_int}"
                )
        except Exception as e:
            if DEBUG_MODE:
                logger.error(
                    f"[DEBUG] ComputeLearningPath: Error determining subject_id from topic_id: {e}"
                )
            # Fallback to Business Studies if determination fails
            subject_id = 101

    # Fetch ALL concepts for this topic (up to 10) to ensure we have
    # the complete set for rotation
    # Use a persistent cache key that includes subject_id to avoid collisions
    # between subjects with same topic_id
    subject_key = f"subject_{subject_id}" if subject_id else "no_subject"
    all_concepts_cache_key = f"all_concepts_ordered:{subject_key}:{topic_id}"
    all_topic_concepts = cache_get(all_concepts_cache_key)

    if all_topic_concepts is None:
        # Fetch from database if not in cache
        try:
            all_topic_concepts = concept_service.fetch_concepts_by_topic(
                topic_id=str(topic_id),
                limit=10,
                random_order=False,  # Get in consistent order
                subject_id=subject_id  # Pass subject_id to use correct table
            )
            # Sort by concept_id to ensure consistent order
            all_topic_concepts.sort(
                key=lambda x: x.get("concept_id", 0)
            )
            # Cache the ordered list for 24 hours
            cache_set(all_concepts_cache_key, all_topic_concepts, ttl=86400)
            
            if DEBUG_MODE:
                logger.info(
                    f"[DEBUG] ComputeLearningPath: ✓ Fetched and cached "
                    f"{len(all_topic_concepts)} concepts for topic {topic_id} "
                    f"(subject_id: {subject_id})"
                )
                if all_topic_concepts:
                    concept_names = [
                        c.get('name', 'N/A') for c in all_topic_concepts[:3]
                    ]
                    logger.info(
                        f"[DEBUG] ComputeLearningPath: Sample concepts: {concept_names}"
                    )
                else:
                    logger.warning(
                        f"[DEBUG] ComputeLearningPath: NO CONCEPTS FOUND for "
                        f"topic_id: {topic_id}, subject_id: {subject_id}. "
                        f"Falling back to concepts from state."
                    )
        except Exception as e:
            if DEBUG_MODE:
                logger.error(
                    f"[DEBUG] ComputeLearningPath: Error fetching all "
                    f"concepts: {e}"
                )
                import traceback
                logger.error(f"[DEBUG] Traceback: {traceback.format_exc()}")
            # Fallback to concepts from state
            all_topic_concepts = state.get("concept_rows", [])
            # Sort by concept_id for consistency
            all_topic_concepts.sort(
                key=lambda x: x.get("concept_id", 0)
            )
    else:
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] ComputeLearningPath: Using cached ordered "
                f"concepts ({len(all_topic_concepts)} concepts) for "
                f"topic {topic_id} (subject_id: {subject_id})"
            )

    # Extract concept_ids from all topic concepts (already sorted)
    all_concept_ids = [
        str(c.get("concept_id"))
        for c in all_topic_concepts
        if c.get("concept_id") not in (None, "None", "")
    ]

    # If we have concepts from state but not from fetch, use state concepts
    if not all_concept_ids:
        all_concept_ids = concept_ids
        all_topic_concepts = state.get("concept_rows", [])
        # Sort by concept_id for consistency
        all_topic_concepts.sort(
            key=lambda x: x.get("concept_id", 0)
        )
        all_concept_ids = [
            str(c.get("concept_id"))
            for c in all_topic_concepts
            if c.get("concept_id") not in (None, "None", "")
        ]

    # SIMPLE ROUND-ROBIN: Assign concepts positions 1-10, cycle through sequentially
    # On first response: show concept at position 1
    # On next response: show concept at position 2
    # Continue until position 10, then cycle back to 1
    
    if all_concept_ids and len(all_concept_ids) > 0:
        # Get current position (1-10) from cache
        position_key = f"concept_position:{user_id}:{subject_key}:{topic_id}"
        current_position = cache_get(position_key)
        
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] ComputeLearningPath: Round-robin cache lookup - "
                f"key={position_key}, cached_position={current_position}, "
                f"all_concept_ids={all_concept_ids[:5]}..."
            )
        
        # Initialize to 1 if not set
        if current_position is None:
            current_position = 1
            if DEBUG_MODE:
                logger.info(f"[DEBUG] ComputeLearningPath: No cached position, initializing to 1")
        else:
            try:
                current_position = int(current_position)
                if DEBUG_MODE:
                    logger.info(f"[DEBUG] ComputeLearningPath: Retrieved position from cache: {current_position}")
            except (ValueError, TypeError):
                current_position = 1
                if DEBUG_MODE:
                    logger.warning(f"[DEBUG] ComputeLearningPath: Invalid cached position, resetting to 1")
        
        # Ensure position is within valid range (1-10)
        # If more than 10 concepts, limit to first 10
        max_concepts = min(10, len(all_concept_ids))
        if current_position > max_concepts:
            if DEBUG_MODE:
                logger.info(f"[DEBUG] ComputeLearningPath: Position {current_position} > max_concepts {max_concepts}, resetting to 1")
            current_position = 1
        
        # Select concept at current position (0-indexed, so subtract 1)
        concept_index = current_position - 1
        recommended_concept_id = all_concept_ids[concept_index]
        recommended_concept_id_str = str(recommended_concept_id)
        
        # Increment position for next time (cycle back to 1 after max_concepts)
        next_position = current_position + 1
        if next_position > max_concepts:
            next_position = 1
        
        # Store next position in cache (24 hour TTL to persist across sessions)
        cache_success = cache_set(position_key, next_position, ttl=86400)
        
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] ComputeLearningPath: Round-robin selection - "
                f"Selected concept {recommended_concept_id} at position {current_position}/{max_concepts}, "
                f"next position: {next_position}, cache_set success: {cache_success}"
            )
    else:
        recommended_concept_id = None

    # Find the concept details from all_topic_concepts
    recommended_concept_id_str = (
        str(recommended_concept_id) if recommended_concept_id else None
    )
    concept_name = None
    for concept in all_topic_concepts:
        if str(concept.get("concept_id")) == recommended_concept_id_str:
            concept_name = concept.get("name", "")
            break

    # If we couldn't find the name, try to get it from concept service
    if not concept_name and recommended_concept_id:
        try:
            concept_details = concept_service.fetch_concept_details(
                [recommended_concept_id]
            )
            if (concept_details and
                    recommended_concept_id in concept_details):
                concept_name = (
                    concept_details[recommended_concept_id].get("name", "")
                )
        except Exception as e:
            if DEBUG_MODE:
                logger.warning(
                    f"[DEBUG] ComputeLearningPath: Error fetching concept "
                    f"details: {e}"
                )

    # Debug: Learning path computation
    if DEBUG_MODE:
        all_concept_names = [
            c.get('name', 'N/A') for c in all_topic_concepts[:10]
        ]
        logger.info(
            f"[DEBUG] ComputeLearningPath: "
            f"all_concept_ids={len(all_concept_ids)}, "
            f"concept_names={all_concept_names}, "
            f"recommended_concept_id={recommended_concept_id}, "
            f"recommended_concept_name={concept_name}"
        )

    # Build learning path using readiness-based decision tree
    # Follow documented logic: readiness determines decision, sequential rotation provides concept
    readiness = state.get("readiness")
    overall_readiness = None
    
    if readiness and isinstance(readiness, dict):
        overall_readiness = readiness.get("overall_readiness") or readiness.get("overall")
    
    # Decision tree based on readiness (as documented):
    # 1. If no concepts → "explore_topic"
    # 2. If readiness unknown → "learn_next_concept" (recommend unseen concept)
    # 3. If readiness = "review_prerequisites": check prerequisites → recommend
    # 4. If readiness = "needs_reinforcement": → "reinforce" (current concept)
    # 5. If readiness = "almost_ready": check next concepts → recommend
    # 6. If readiness = "ready": check next concepts → "advance"
    
    if not recommended_concept_id:
        # No concept found
        learning_path = {
            "decision": "explore_topic" if len(all_concept_ids) == 0 else "unknown",
            "recommended_concept": None,
            "recommended_concept_name": None,
            "details": "No concepts available." if len(all_concept_ids) == 0 else "Continue exploring the current topic and ask more questions to identify key concepts."
        }
    elif not overall_readiness or overall_readiness == "unknown":
        # Readiness unknown - recommend next concept
        if concept_name:
            details = (
                f"Continue exploring concepts in this topic. "
                f"Try asking questions about '{concept_name}' "
                f"to deepen your understanding."
            )
        else:
            details = (
                "Continue exploring concepts in this topic. "
                "Try asking questions about the recommended concept "
                "to deepen your understanding."
            )
        learning_path = {
            "decision": "learn_next_concept",
            "recommended_concept": recommended_concept_id,
            "recommended_concept_name": concept_name,
            "details": details
        }
    elif overall_readiness == "review_prerequisites":
        # Check for prerequisite concepts
        if concept_service:
            try:
                concept_graph = concept_service.get_prerequisites_and_next_concepts(
                    concept_ids
                )
                prereqs = concept_graph.get("prerequisites", [])
                if len(prereqs) > 0:
                    # Recommend first prerequisite
                    prereq_concept = prereqs[0]
                    prereq_id = str(prereq_concept.get("concept_id", ""))
                    prereq_name = prereq_concept.get("name", "")
                    learning_path = {
                        "decision": "review_prerequisite",
                        "recommended_concept": prereq_id,
                        "recommended_concept_name": prereq_name,
                        "details": "Mastery too low; review prerequisite concept first."
                    }
                else:
                    # No prerequisites found - reinforce current concept
                    learning_path = {
                        "decision": "reinforce",
                        "recommended_concept": recommended_concept_id,
                        "recommended_concept_name": concept_name,
                        "details": "Insufficient mastery; reinforce current concept."
                    }
            except Exception as e:
                if DEBUG_MODE:
                    logger.warning(f"[DEBUG] Error fetching prerequisites: {e}")
                # Fallback to reinforce
                learning_path = {
                    "decision": "reinforce",
                    "recommended_concept": recommended_concept_id,
                    "recommended_concept_name": concept_name,
                    "details": "Insufficient mastery; reinforce current concept."
                }
        else:
            # No concept agent - reinforce current concept
            learning_path = {
                "decision": "reinforce",
                "recommended_concept": recommended_concept_id,
                "recommended_concept_name": concept_name,
                "details": "Insufficient mastery; reinforce current concept."
            }
    elif overall_readiness == "needs_reinforcement":
        # Reinforce current concept
        learning_path = {
            "decision": "reinforce",
            "recommended_concept": recommended_concept_id,
            "recommended_concept_name": concept_name,
            "details": "Student needs reinforcement before progressing."
        }
    elif overall_readiness == "almost_ready":
        # Check for next concepts
        if concept_service:
            try:
                concept_graph = concept_service.get_prerequisites_and_next_concepts(
                    concept_ids
                )
                next_concepts = concept_graph.get("next_concepts", [])
                if len(next_concepts) > 0:
                    # Recommend first next concept
                    next_concept = next_concepts[0]
                    next_id = str(next_concept.get("concept_id", ""))
                    next_name = next_concept.get("name", "")
                    learning_path = {
                        "decision": "learn_next_concept",
                        "recommended_concept": next_id,
                        "recommended_concept_name": next_name,
                        "details": "Student is almost ready; consider preparing next concept."
                    }
                else:
                    # No next concepts - reinforce current
                    learning_path = {
                        "decision": "reinforce",
                        "recommended_concept": recommended_concept_id,
                        "recommended_concept_name": concept_name,
                        "details": "No next concept found; strengthen understanding."
                    }
            except Exception as e:
                if DEBUG_MODE:
                    logger.warning(f"[DEBUG] Error fetching next concepts: {e}")
                # Fallback to reinforce
                learning_path = {
                    "decision": "reinforce",
                    "recommended_concept": recommended_concept_id,
                    "recommended_concept_name": concept_name,
                    "details": "No next concept found; strengthen understanding."
                }
        else:
            # No concept agent - reinforce current
            learning_path = {
                "decision": "reinforce",
                "recommended_concept": recommended_concept_id,
                "recommended_concept_name": concept_name,
                "details": "No next concept found; strengthen understanding."
            }
    elif overall_readiness == "ready":
        # Check for next concepts
        if concept_service:
            try:
                concept_graph = concept_service.get_prerequisites_and_next_concepts(
                    concept_ids
                )
                next_concepts = concept_graph.get("next_concepts", [])
                if len(next_concepts) > 0:
                    # Recommend first next concept
                    next_concept = next_concepts[0]
                    next_id = str(next_concept.get("concept_id", ""))
                    next_name = next_concept.get("name", "")
                    learning_path = {
                        "decision": "advance",
                        "recommended_concept": next_id,
                        "recommended_concept_name": next_name,
                        "details": "Student has high mastery and is ready to advance."
                    }
                else:
                    # No next concepts - advance without specific concept
                    learning_path = {
                        "decision": "advance",
                        "recommended_concept": None,
                        "recommended_concept_name": None,
                        "details": "No next concept found, but mastery indicates readiness to move ahead."
                    }
            except Exception as e:
                if DEBUG_MODE:
                    logger.warning(f"[DEBUG] Error fetching next concepts: {e}")
                # Fallback to advance
                learning_path = {
                    "decision": "advance",
                    "recommended_concept": None,
                    "recommended_concept_name": None,
                    "details": "No next concept found, but mastery indicates readiness to move ahead."
                }
        else:
            # No concept agent - advance without specific concept
            learning_path = {
                "decision": "advance",
                "recommended_concept": None,
                "recommended_concept_name": None,
                "details": "No next concept found, but mastery indicates readiness to move ahead."
            }
    else:
        # Unknown readiness value - default to learn_next_concept
        if concept_name:
            details = (
                f"Continue exploring concepts in this topic. "
                f"Try asking questions about '{concept_name}' "
                f"to deepen your understanding."
            )
        else:
            details = (
                "Continue exploring concepts in this topic. "
                "Try asking questions about the recommended concept "
                "to deepen your understanding."
            )
        learning_path = {
            "decision": "learn_next_concept",
            "recommended_concept": recommended_concept_id,
            "recommended_concept_name": concept_name,
            "details": details
        }

    # Debug: Learning path result
    if DEBUG_MODE:
        decision = learning_path.get('decision', 'N/A')
        recommended = learning_path.get('recommended_concept', 'N/A')
        logger.info(
            f"[DEBUG] ComputeLearningPath result: "
            f"decision={decision}, recommended_concept={recommended}, "
            f"concept_name={concept_name}, "
            f"(shown: {len(shown_concept_ids)}/{len(all_concept_ids)})"
        )

    return {"learning_path": learning_path}


# -----------------------------------------------------
# BUILD LANGGRAPH PIPELINE
# -----------------------------------------------------

# Node execution order:
#   0. LogUserMessage     (log student message to Supabase)
#   0.5. ValidateInput    (validate and limit input token sizes)
#   1. FetchLesson        (load lesson content from Supabase)
#   2. RetrieveHistory    (load last N messages)
#   3. SummarizeHistory   (condense history if needed)
#   4. FetchConcepts      (pgvector similarity search)
#   5. ClassifyReasoning  (LLM classify reasoning)
#   6. GenerateLLMResponse (main ChatGPT tutor response)
#   7. UpdateMastery      (update mastery scores)
#   8. ComputeReadiness   (compute readiness levels)
#   9. ComputeLearningPath (determine next learning step)
#   10. LogMessage         (store AI message)

graph = StateGraph(TutorState)

# All nodes wrapped with timed_node for timing and error handling
graph.add_node("LogUserMessage", timed_node(LogUserMessage))
graph.add_node("ValidateInput", timed_node(ValidateInput))
graph.add_node("FetchDataParallel", timed_node(FetchDataParallel))  # NEW: Parallel DB reads
graph.add_node("FetchLesson", timed_node(FetchLesson))  # DEPRECATED: Use FetchDataParallel
graph.add_node("RetrieveHistory", timed_node(RetrieveHistory))  # DEPRECATED: Use FetchDataParallel
graph.add_node("SummarizeHistory", timed_node(SummarizeHistory))
graph.add_node("FetchConcepts", timed_node(FetchConcepts))  # DEPRECATED: Use FetchDataParallel
graph.add_node("ClassifyReasoning", timed_node(ClassifyReasoning))  # DEPRECATED: Now combined with GenerateLLMResponse
graph.add_node("GenerateLLMResponse", timed_node(GenerateLLMResponse))  # COMBINED: Now returns reasoning_label
graph.add_node("UpdateMastery", timed_node(UpdateMastery))
graph.add_node("ComputeReadiness", timed_node(ComputeReadiness))
graph.add_node("ComputeLearningPath", timed_node(ComputeLearningPath))
graph.add_node("LogMessage", timed_node(LogMessage))

# ------------------------------
# Define the linear flow
# ------------------------------
graph.set_entry_point("LogUserMessage")
graph.add_edge("LogUserMessage", "ValidateInput")
graph.add_edge("ValidateInput", "FetchDataParallel")  # NEW: Parallel fetch replaces sequential flow
graph.add_edge("FetchDataParallel", "SummarizeHistory")  # SummarizeHistory needs history from FetchDataParallel
graph.add_edge("SummarizeHistory", "GenerateLLMResponse")  # Skip ClassifyReasoning (now combined with GenerateLLMResponse)
graph.add_edge("GenerateLLMResponse", "UpdateMastery")
graph.add_edge("UpdateMastery", "ComputeReadiness")
graph.add_edge("ComputeReadiness", "ComputeLearningPath")
graph.add_edge("ComputeLearningPath", "LogMessage")
graph.add_edge("LogMessage", END)

# Compile the graph into an executable app
tutor_app = graph.compile()


# -----------------------------------------------------
# FUNCTION: run_tutor_graph(input)
# -----------------------------------------------------
def run_tutor_graph(
    user_id: str,
    topic: str,
    message: str,
    conversation_id: str = None,
    explanation_style: str = "default",
    mode: str = "tutor",
    subject_id: Optional[int] = None,
    conversation_history: Optional[List[Dict[str, str]]] = None,
    job_id: Optional[str] = None,
    correlation_id: Optional[str] = None
):
    """
    High-level function to execute the LangGraph tutor pipeline.

    Inputs:
        - user_id         (str) required
        - topic            (str) topic_id from Supabase
        - message          (str) student's message/question
        - conversation_id  (optional str)
              if none provided → auto-generate "student_topic"
        - explanation_style (str) optional, default="default"
              Options: default, simple, detailed, steps, table,
              diagram, comparison, visual_prompt
        - mode             (str) optional, default="tutor"
              Options: "tutor", "exam", "essay"
        - subject_id       (int) optional, subject ID from Supabase
        - job_id           (str) optional, job ID for correlation
        - correlation_id   (str) optional, correlation ID for end-to-end tracing

    Returns:
        dict containing:
          llm_response (or essay feedback for essay mode)
          related_concepts
          reasoning_label
          mastery_updates
          readiness
          learning_path
          conversation_id
    """
    # Import performance instrumentation
    try:
        from services.performance_instrumentation import (
            timed_operation, StageType
        )
        PERFORMANCE_INSTRUMENTATION_AVAILABLE = True
    except ImportError:
        PERFORMANCE_INSTRUMENTATION_AVAILABLE = False
        def timed_operation(*args, **kwargs):
            from contextlib import nullcontext
            return nullcontext()

        # Ensure topic is always a string
        topic = str(topic)

    if DEBUG_MODE:
        logger.info("")
        logger.info("="*60)
        logger.info("[DEBUG] Starting LangGraph Tutor Pipeline")
        logger.info("="*60)
        logger.info(f"[DEBUG] User ID: {user_id}")
        logger.info(f"[DEBUG] Topic: {topic}")
        msg_preview = (
            f"{message[:100]}..." if len(message) > 100 else message
        )
        logger.info(f"[DEBUG] Message: {msg_preview}")
        logger.info(f"[DEBUG] Mode: {mode}")
        logger.info(f"[DEBUG] Explanation Style: {explanation_style}")
        logger.info(f"[DEBUG] Correlation ID: {correlation_id}")
        logger.info("="*60)
        logger.info("")

    if conversation_id is None:
        conversation_id = f"{user_id}_{topic}"

    # Generate unique trace_id for this request
    trace_id = str(uuid4())

    if DEBUG_MODE:
        logger.info(f"[DEBUG] Conversation ID: {conversation_id}")
        logger.info(f"[DEBUG] Trace ID: {trace_id}")
        logger.info(f"[DEBUG] Correlation ID: {correlation_id}")
        logger.info("")

    # Route based on mode
    if mode == "tutor":
        # Default tutor mode - run current graph
        # Use conversation_history from frontend if provided
        # Format: [{role: "user", content: "..."}, ...]
        history_from_frontend = conversation_history or []

        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] Using conversation history from frontend: "
                f"{len(history_from_frontend)} messages"
            )

        initial_state = {
            "user_id": user_id,
            "topic": topic,
            "user_message": message,
            "conversation_id": conversation_id,
            "explanation_style": explanation_style,
            "trace_id": trace_id,
            "job_id": job_id,  # Add job_id to state for instrumentation
            "correlation_id": correlation_id,  # Add correlation_id for end-to-end tracing
            "subject_id": subject_id,
            "lesson_text": None,
            "lesson_chunks": [],
            "history": history_from_frontend,  # Use history from frontend
            "condensed_history": None,
            "concept_rows": [],
            "reasoning_label": "neutral",
            "llm_response": "",
            "token_usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0
            },
            "mastery_updates": [],
            "readiness": None,
            "learning_path": None,
        }

        # Add timeout protection for graph execution
        import threading
        import time
        invoke_container = {
            "value": None, "error": None, "completed": False
        }

        if DEBUG_MODE:
            logger.info("[DEBUG] Starting graph execution with timeout...")

        start_time = time.time()

        def invoke_graph():
            try:
                logger.info(f"[GRAPH] Graph invoke thread started (job_id: {job_id}, correlation_id: {correlation_id})")
                if DEBUG_MODE:
                    logger.info(f"[DEBUG] Graph invoke thread started (correlation_id: {correlation_id})")
                # Log before invoke to track when it starts
                logger.info(f"[GRAPH] Calling tutor_app.invoke() (job_id: {job_id}, correlation_id: {correlation_id})")
                invoke_container["value"] = tutor_app.invoke(initial_state)
                invoke_container["completed"] = True
                elapsed = time.time() - start_time
                logger.info(f"[GRAPH] Graph execution completed in {elapsed:.2f}s (job_id: {job_id})")
                if DEBUG_MODE:
                    logger.info(
                        f"[DEBUG] Graph execution completed in {elapsed:.2f}s"
                    )
            except Exception as e:
                invoke_container["error"] = e
                invoke_container["completed"] = True
                elapsed = time.time() - start_time
                logger.error(
                    f"[ERROR] Graph execution failed after "
                    f"{elapsed:.2f}s (job_id: {job_id}): {e}"
                )
                import traceback
                logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")
                if DEBUG_MODE:
                    logger.error(
                        f"[ERROR] Graph execution failed after "
                        f"{elapsed:.2f}s: {e}"
                    )

        invoke_thread = threading.Thread(target=invoke_graph, daemon=True)
        invoke_thread.start()
        # Add timeout: 180 seconds (3 minutes) - reduced from 25 minutes for faster failure
        # This ensures jobs fail fast if there's an issue, rather than hanging for 25 minutes
        # Normal jobs take 15-45 seconds, complex ones up to 90 seconds, so 3 minutes is reasonable
        logger.info(f"[TIMEOUT] Starting graph execution with 180s timeout (job_id: {job_id})")
        invoke_thread.join(timeout=180)  # 3 minutes timeout - fast failure

        elapsed = time.time() - start_time

        # CRITICAL FIX: Check if thread is still running (timed out)
        # If thread timed out, set result in container and mark as completed
        # This allows the worker to detect the timeout result even if thread is still alive
        if invoke_thread.is_alive():
            logger.error(
                f"[ERROR] Graph execution thread timed out after {elapsed:.2f}s "
                f"(thread still alive, job_id: {job_id})"
            )
            logger.error(
                f"[ERROR] Graph execution timed out after {elapsed:.2f}s "
                f"(25 minute limit exceeded, job_id: {job_id})"
            )
            # Set timeout result in container so worker can detect it
            timeout_result = {
                "response": (
                    "I apologize, but the request is taking longer than expected. "
                    "This may be due to high server load. Please try asking your question again."
                ),
                "suggestions": [],
                "related_concepts": [],
                "concept_ids": [],
                "reasoning_label": "neutral",
                "mastery_updates": [],
                "readiness": None,
                "learning_path": None,
                "token_usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0
                }
            }
            # Set result in container so worker can detect it even if thread is alive
            invoke_container["value"] = timeout_result
            invoke_container["completed"] = True
            invoke_container["error"] = TimeoutError(f"Graph execution timed out after {elapsed:.2f}s")
            return timeout_result

        if not invoke_container["completed"]:
            logger.error(
                f"[ERROR] Graph execution did not complete after "
                f"{elapsed:.2f}s (job_id: {job_id})"
            )
            # Log traceback if available
            if invoke_container.get("error"):
                import traceback
                logger.error(f"[ERROR] Exception in graph execution: {invoke_container['error']}")
                logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")
            # Return a fallback response instead of raising
            return {
                "response": (
                    "I apologize, but I'm experiencing a delay. "
                    "Please try asking your question again."
                ),
                "suggestions": [],
                "related_concepts": [],
                "concept_ids": [],
                "reasoning_label": "neutral",
                "mastery_updates": [],
                "readiness": None,
                "learning_path": None,
                "token_usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0
                }
            }

        if invoke_container.get("error"):
            error = invoke_container["error"]
            logger.error(
                f"[ERROR] Graph execution failed after {elapsed:.2f}s (job_id: {job_id}): "
                f"{error}"
            )
            import traceback
            logger.error(f"[ERROR] Full traceback: {traceback.format_exc()}")
            raise error

        final_state = invoke_container["value"]

        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] Graph execution successful, total time: "
                f"{elapsed:.2f}s"
            )

        if DEBUG_MODE:
            logger.info("")
            logger.info("="*60)
            logger.info("[DEBUG] Pipeline Execution Complete")
            logger.info("="*60)
            response_len = len(final_state.get('llm_response', ''))
            logger.info(f"[DEBUG] Response Length: {response_len} chars")
            concepts_count = len(final_state.get('concept_rows', []))
            logger.info(f"[DEBUG] Related Concepts: {concepts_count}")
            reasoning = final_state.get('reasoning_label', 'N/A')
            logger.info(f"[DEBUG] Reasoning Label: {reasoning}")
            mastery_count = len(final_state.get('mastery_updates', []))
            logger.info(f"[DEBUG] Mastery Updates: {mastery_count}")
            readiness_data = final_state.get('readiness')
            readiness_val = (
                readiness_data.get('overall_readiness', 'N/A')
                if readiness_data else 'N/A'
            )
            logger.info(f"[DEBUG] Readiness: {readiness_val}")
            learning_path_data = final_state.get('learning_path')
            learning_path_val = (
                learning_path_data.get('decision', 'N/A')
                if learning_path_data else 'N/A'
            )
            logger.info(f"[DEBUG] Learning Path: {learning_path_val}")
            logger.info("="*60)
            logger.info("")

        # Build standard API response
        # Ensure suggestions is always a list
        suggestions = final_state.get("suggestions", [])
        if not isinstance(suggestions, list):
            # Convert single suggestion to list if needed
            suggestions = [suggestions] if suggestions else []

        # Extract related concepts from concept_rows
        concept_rows = final_state.get("concept_rows", [])
        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] Building response: concept_rows count = "
                f"{len(concept_rows)}"
            )
            if concept_rows:
                logger.info(
                    f"[DEBUG] Sample concept_row: {concept_rows[0]}"
                )
                # Check for Economics specifically
                subject_id = final_state.get("subject_id")
                if subject_id == 119:
                    logger.info(
                        f"[DEBUG] Economics concepts check: "
                        f"concept_rows={len(concept_rows)}, "
                        f"first concept keys: {list(concept_rows[0].keys()) if concept_rows else []}"
                    )

        related_concepts_list = [
            row.get("name") for row in concept_rows
            if row.get("name")
        ]
        concept_ids_list = [
            str(row.get("concept_id"))
            for row in concept_rows
            if row.get("concept_id") is not None
        ]

        if DEBUG_MODE:
            logger.info(
                f"[DEBUG] Extracted {len(related_concepts_list)} related "
                f"concepts and {len(concept_ids_list)} concept IDs"
            )
            if subject_id == 119 and len(related_concepts_list) == 0 and len(concept_rows) > 0:
                logger.error(
                    f"[DEBUG] ⚠️ Economics concepts issue: "
                    f"concept_rows has {len(concept_rows)} items but extracted 0 names. "
                    f"Sample row: {concept_rows[0] if concept_rows else 'None'}"
                )

        return {
            "response": final_state["llm_response"],
            "suggestions": suggestions,
            "related_concepts": related_concepts_list,
            "concept_ids": concept_ids_list,
            "reasoning_label": final_state["reasoning_label"],
            "mastery_updates": final_state["mastery_updates"],
            "readiness": final_state.get("readiness"),
            "learning_path": final_state.get("learning_path"),
            "token_usage": final_state.get("token_usage", {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0
            }),
            "conversation_id": final_state["conversation_id"],
            "lesson_chunks": final_state.get("lesson_chunks", [])
        }

    elif mode == "exam":
        # TODO: Build exam graph and run it
        # For now, return placeholder response
        logger.warning(
            "[TODO] Exam mode not yet implemented - "
            "returning placeholder response"
        )
        return {
            "response": (
                "Exam mode is not yet implemented. "
                "This will run a specialized exam graph in the future."
            ),
            "suggestions": [],
            "related_concepts": [],
            "concept_ids": [],
            "reasoning_label": "neutral",
            "mastery_updates": [],
            "readiness": None,
            "learning_path": None,
            "conversation_id": conversation_id
        }

    elif mode == "essay":
        # Call essay marker from LLM service
        try:
            essay_feedback = llm_service.essay_marker(
                essay_text=message,
                topic=topic,
                user_id=user_id
            )
            # Ensure suggestions is always a list
            essay_suggestions = essay_feedback.get("suggestions", [])
            if not isinstance(essay_suggestions, list):
                essay_suggestions = (
                    [essay_suggestions] if essay_suggestions else []
                )

            return {
                "response": essay_feedback.get("feedback", ""),
                "suggestions": essay_suggestions,
                "related_concepts": [],
                "concept_ids": [],
                "reasoning_label": "neutral",
                "mastery_updates": [],
                "readiness": None,
                "learning_path": None,
                "conversation_id": conversation_id,
                "score": essay_feedback.get("score")
            }
        except AttributeError:
            # essay_marker method doesn't exist yet
            logger.warning(
                "[TODO] essay_marker() not yet implemented in LLMService"
            )
            return {
                "response": (
                    "Essay marking is not yet implemented. "
                    "This will call llm_service.essay_marker() in the future."
                ),
                "suggestions": [],
                "related_concepts": [],
                "concept_ids": [],
                "reasoning_label": "neutral",
                "mastery_updates": [],
                "readiness": None,
                "learning_path": None,
                "conversation_id": conversation_id
            }

    else:
        # Invalid mode - default to tutor
        logger.warning(
            f"Invalid mode '{mode}' - defaulting to 'tutor' mode"
        )
        return run_tutor_graph(
            user_id=user_id,
            topic=topic,
            message=message,
            conversation_id=conversation_id,
            explanation_style=explanation_style,
            mode="tutor"
        )
