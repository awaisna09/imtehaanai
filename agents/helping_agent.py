#!/usr/bin/env python3
"""
Helping Agent - A standalone agent for quick concept/word explanations
Designed for students to ask about words or concepts during practice
Supports all subjects: Business Studies, Economics, Geography, History, Islamiyat
Restricted to the current subject - only answers questions about that subject
Responses are limited to 50 words maximum
"""

import os
import logging
import time
import hashlib
from typing import Optional, Tuple
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config.env')

# Import cache utilities
try:
    from cache import cache_get, cache_set
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("Cache module not available - caching disabled")

# Try to import LangChain
try:
    from langchain_openai import ChatOpenAI
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    ChatOpenAI = None

logger = logging.getLogger(__name__)


class HelpingAgent:
    """
    Standalone helping agent for quick concept/word explanations.
    Supports all subjects (Business Studies, Economics, Geography, History, Islamiyat).
    Restricted to current subject - only answers questions about that subject.
    Designed to provide concise, helpful responses (max 50 words).
    """

    def __init__(
        self,
        api_key: str,
        model: str = None,
        temperature: float = None,
        max_tokens: int = None
    ):
        """Initialize the helping agent with configuration"""
        self.api_key = api_key

        # Use fastest model - gpt-4o-mini is fastest and cheapest
        self.model = model or os.getenv(
            'HELPING_MODEL', 'gpt-4o-mini'
        )
        # Fast model for simple queries - use same model for consistency
        self.fast_model = os.getenv(
            'HELPING_FAST_MODEL', 'gpt-4o-mini'  # Use same model for speed
        )
        
        # Lower temperature for faster, more deterministic responses
        self.temperature = temperature or float(
            os.getenv('HELPING_TEMPERATURE', '0.3')
        )
        # Reduced tokens: 50 words * 2.5 tokens/word = 125 max (faster)
        # Using 100 for maximum speed (less tokens = faster generation)
        self.max_tokens = max_tokens or int(
            os.getenv('HELPING_MAX_TOKENS', '100')  # Reduced from 150 to 100
        )
        
        # Model selection configuration - DISABLED by default for speed
        # Query classification adds latency, so disable unless explicitly enabled
        self.enable_model_selection = (
            os.getenv('HELPING_ENABLE_MODEL_SELECTION', 'false').lower() == 'true'
        )
        self.min_confidence_for_fast = float(
            os.getenv('HELPING_MIN_CONFIDENCE_FAST', '0.7')
        )

        # Disable LangSmith tracing by default for speed
        # Only enable if explicitly requested
        langsmith_enabled = (
            os.getenv('LANGSMITH_TRACING', 'false').lower() == 'true'
        )
        if langsmith_enabled:
            os.environ['LANGSMITH_TRACING'] = 'true'
            os.environ['LANGSMITH_ENDPOINT'] = os.getenv(
                'LANGSMITH_ENDPOINT', 'https://api.smith.langchain.com'
            )
            os.environ['LANGSMITH_API_KEY'] = os.getenv(
                'LANGSMITH_API_KEY', ''
            )
            os.environ['LANGSMITH_PROJECT'] = os.getenv(
                'LANGSMITH_PROJECT', 'imtehaan-helping-agent'
            )
        else:
            # Explicitly disable tracing for speed
            os.environ['LANGSMITH_TRACING'] = 'false'

        # System prompt will be generated dynamically based on subject
        # Store a default one, but it will be regenerated per request
        self._default_system_prompt = None

        # Initialize query classifier
        try:
            from agents.query_classifier import get_classifier
            self.query_classifier = get_classifier()
        except ImportError:
            logger.warning(
                "Query classifier not available - model selection disabled"
            )
            self.query_classifier = None
            self.enable_model_selection = False

        # Initialize standard LLM with speed optimizations
        if LANGCHAIN_AVAILABLE and self.api_key:
            try:
                self.llm = ChatOpenAI(
                    model=self.model,
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    openai_api_key=self.api_key,
                    timeout=8,  # Reduced to 8s for faster failure
                    max_retries=0,  # No retries - fail fast
                    streaming=False,  # Explicitly disable streaming
                    request_timeout=8,  # Request timeout
                    model_kwargs={
                        'frequency_penalty': 0,  # No penalties for speed
                        'presence_penalty': 0
                    }
                )
                
                # Initialize fast LLM for simple queries
                if self.enable_model_selection:
                    try:
                        self.fast_llm = ChatOpenAI(
                            model=self.fast_model,
                            temperature=self.temperature,
                            max_tokens=self.max_tokens,
                            openai_api_key=self.api_key,
                            timeout=6,  # Even faster timeout for simple queries
                            max_retries=0,
                            streaming=False,
                            request_timeout=6,
                            model_kwargs={
                                'frequency_penalty': 0,
                                'presence_penalty': 0
                            }
                        )
                        logger.info(
                            f"✅ Helping Agent initialized with model selection "
                            f"(Standard: {self.model}, Fast: {self.fast_model})"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to initialize fast model, "
                            f"using standard only: {e}"
                        )
                        self.fast_llm = None
                        self.enable_model_selection = False
                else:
                    self.fast_llm = None
                    logger.info(
                        f"✅ Helping Agent initialized "
                        f"(Model: {self.model}, Selection: disabled)"
                    )
            except Exception as e:
                logger.error(f"Error initializing helping agent LLM: {e}")
                self.llm = None
                self.fast_llm = None
        else:
            self.llm = None
            self.fast_llm = None
            if not LANGCHAIN_AVAILABLE:
                logger.warning("LangChain not available for helping agent")
            if not self.api_key:
                logger.warning("OpenAI API key not provided for helping agent")

    def _get_system_prompt_optimized(self, subject: str = "Business Studies") -> str:
        """Get optimized system prompt (shorter = faster) - dynamic based on subject
        Optimized for speed: minimal words, clear instructions
        Supports: Business Studies, Economics, Geography, History, Islamiyat"""
        # Normalize subject name - handle all variations
        subject_normalized = subject.strip() if subject else "Business Studies"
        subject_lower = subject_normalized.lower()
        
        # Comprehensive subject mapping - handles all variations
        if "economics" in subject_lower:
            subject_display = "Economics"
        elif "geography" in subject_lower or "pak studies geography" in subject_lower or "pakstudiesgeography" in subject_lower.replace(" ", ""):
            subject_display = "Geography"
        elif "history" in subject_lower or "pak studies history" in subject_lower or "pakstudieshistory" in subject_lower.replace(" ", ""):
            subject_display = "History"
        elif "islamiyat" in subject_lower or "islamiat" in subject_lower:
            subject_display = "Islamiyat"
        elif "business" in subject_lower:
            subject_display = "Business Studies"
        else:
            # Default to Business Studies if unclear
            subject_display = "Business Studies"
        
        # Ultra-short prompt for maximum speed
        return (
            f"IGCSE {subject_display} assistant. "
            f"Answer ONLY {subject_display} questions (max 50 words). "
            f"If not {subject_display}, say: 'Please ask only about IGCSE {subject_display} topics.' "
            "Be concise."
        )

    def explain(
        self,
        query: str,
        context: Optional[str] = None,
        subject: Optional[str] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> str:
        """
        Explain a word, concept, or term in a concise manner (max 50 words).

        Args:
            query: The word, concept, or question the student is asking about
            context: Optional context (e.g., current question text)
            subject: Optional subject name (e.g., "Business Studies")
            job_id: Optional job ID for instrumentation
            trace_id: Optional trace ID for distributed tracing

        Returns:
            A concise explanation (max 50 words)
        """
        if not self.llm:
            return (
                "Sorry, I'm not available right now. "
                "Please try again later."
            )

        if not query or not query.strip():
            return (
                "Please ask me about a word or concept "
                "you'd like to understand."
            )

        # CACHE CHECK - Fast path for repeated queries
        if CACHE_AVAILABLE:
            query_clean = query.strip().lower()
            # Normalize subject for cache key (consistent with prompt generation)
            subject_normalized = (subject or "Business Studies").strip()
            subject_lower = subject_normalized.lower()
            
            # Normalize subject name for cache (must match _get_system_prompt_optimized)
            if "economics" in subject_lower:
                subject_key = "Economics"
            elif "geography" in subject_lower or "pak studies geography" in subject_lower or "pakstudiesgeography" in subject_lower.replace(" ", ""):
                subject_key = "Geography"
            elif "history" in subject_lower or "pak studies history" in subject_lower or "pakstudieshistory" in subject_lower.replace(" ", ""):
                subject_key = "History"
            elif "islamiyat" in subject_lower or "islamiat" in subject_lower:
                subject_key = "Islamiyat"
            elif "business" in subject_lower:
                subject_key = "Business Studies"
            else:
                subject_key = "Business Studies"
            
            cache_key = f"helping_agent:{hashlib.md5(f'{query_clean}:{subject_key}'.encode()).hexdigest()}"
            
            cached_response = cache_get(cache_key)
            if cached_response is not None:
                logger.info(f"[CACHE HIT] Helping agent - query: '{query_clean[:50]}...' subject: {subject_key}")
                return cached_response

        # Import performance instrumentation
        try:
            from services.performance_instrumentation import (
                time_prompt_construction,
                time_ai_call,
                time_response_parsing
            )
            INSTRUMENTATION_AVAILABLE = True
        except ImportError:
            INSTRUMENTATION_AVAILABLE = False
            # Create no-op context managers if instrumentation unavailable
            from contextlib import nullcontext
            time_prompt_construction = lambda *args, **kwargs: nullcontext()
            time_ai_call = lambda *args, **kwargs: nullcontext()
            time_response_parsing = lambda *args, **kwargs: nullcontext()

        try:
            # PHASE 1: Prompt Construction
            with time_prompt_construction(
                stage_name="helping_agent_prompt_construction",
                job_id=job_id,
                trace_id=trace_id
            ):
                # Get subject from parameter or default to "Business Studies"
                subject_name = subject if subject else "Business Studies"
                
                # Generate system prompt dynamically based on subject (handles all normalization)
                system_prompt = self._get_system_prompt_optimized(subject=subject_name)
                
                # Get normalized subject display name for cache key (reuse the same logic)
                subject_normalized = subject_name.strip() if subject_name else "Business Studies"
                subject_lower = subject_normalized.lower()
                
                # Normalize subject for cache key (must match _get_system_prompt_optimized logic)
                if "economics" in subject_lower:
                    subject_display = "Economics"
                elif "geography" in subject_lower or "pak studies geography" in subject_lower or "pakstudiesgeography" in subject_lower.replace(" ", ""):
                    subject_display = "Geography"
                elif "history" in subject_lower or "pak studies history" in subject_lower or "pakstudieshistory" in subject_lower.replace(" ", ""):
                    subject_display = "History"
                elif "islamiyat" in subject_lower or "islamiat" in subject_lower:
                    subject_display = "Islamiyat"
                elif "business" in subject_lower:
                    subject_display = "Business Studies"
                else:
                    subject_display = "Business Studies"
                
                # Ultra-optimized prompt building (minimal string operations)
                query_clean = query.strip()

                # Ultra-short user prompt for maximum speed (skip context)
                user_prompt = query_clean

                # Use LangChain message format (HumanMessage/SystemMessage)
                try:
                    from langchain_core.messages import HumanMessage, SystemMessage
                    messages = [
                        SystemMessage(content=system_prompt),
                        HumanMessage(content=user_prompt)
                    ]
                except ImportError:
                    # Fallback to dict format if langchain_core not available
                    messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ]
                prompt_size = len(system_prompt) + len(user_prompt)

            # PHASE 2: Model Selection and API Call
            # Classify query to determine if we should use fast model
            model_used = self.model
            classification = 'complex'
            confidence = 0.0
            classification_metadata = {}
            use_fast_model = False
            
            if self.enable_model_selection and self.query_classifier:
                try:
                    classification, confidence, classification_metadata = (
                        self.query_classifier.classify(query_clean, context)
                    )
                    
                    # Use fast model if query is simple and confidence is high
                    if (classification == 'simple' and 
                            confidence >= self.min_confidence_for_fast and
                            self.fast_llm is not None):
                        use_fast_model = True
                        model_used = self.fast_model
                except Exception as e:
                    logger.warning(
                        f"Query classification failed, using standard model: {e}"
                    )
            
            # Log model selection decision
            logger.info(
                f"[MODEL_SELECTION] query='{query_clean[:50]}...' "
                f"classification={classification} confidence={confidence:.2f} "
                f"model={model_used} use_fast={use_fast_model}",
                extra={
                    'job_id': job_id,
                    'trace_id': trace_id,
                    'query_length': len(query_clean),
                    'classification': classification,
                    'confidence': confidence,
                    'model_selected': model_used,
                    'use_fast_model': use_fast_model,
                    'classification_metadata': classification_metadata
                }
            )
            
            # Select LLM instance
            llm_to_use = self.fast_llm if use_fast_model else self.llm
            
            # Track timing for comparison
            api_call_start = time.time()
            
            with time_ai_call(
                stage_name="helping_agent_api_call",
                job_id=job_id,
                trace_id=trace_id,
                model=model_used,
                prompt_tokens=prompt_size // 4  # Rough estimate
            ):
                # Get response from LLM (synchronous, optimized)
                response = llm_to_use.invoke(messages)
            
            api_call_duration = time.time() - api_call_start

            # PHASE 3: Response Parsing and Validation
            with time_response_parsing(
                stage_name="helping_agent_response_parsing",
                job_id=job_id,
                trace_id=trace_id,
                response_size=len(response.content) if hasattr(response, 'content') else None
            ):
                # Fast content extraction
                explanation = (
                    response.content.strip()
                    if hasattr(response, 'content')
                    else str(response).strip()
                )

                # Fast word count check (only if needed)
                word_count = len(explanation.split())
                if word_count > 50:
                    # Fast truncation
                    words = explanation.split()[:50]
                    explanation = " ".join(words) + "..."

                # CACHE THE RESPONSE - Cache for 1 hour (3600 seconds)
                # Use subject_display that was normalized earlier (defined in prompt construction phase)
                if CACHE_AVAILABLE:
                    query_clean_for_cache = query.strip().lower()
                    # subject_display is already defined above in the prompt construction phase
                    cache_key = f"helping_agent:{hashlib.md5(f'{query_clean_for_cache}:{subject_display}'.encode()).hexdigest()}"
                    cache_set(cache_key, explanation, ttl=3600)  # Cache for 1 hour
                    logger.info(f"[CACHE SET] Helping agent - query: '{query_clean_for_cache[:50]}...' subject: {subject_display}")
            
            # Quality assessment (simple heuristic)
            explanation_length = len(explanation)
            explanation_words = len(explanation.split())
            quality_score = min(1.0, explanation_length / 100.0)  # Simple heuristic
            
            # Log comprehensive comparison metrics
            logger.info(
                f"[MODEL_COMPARISON] query='{query_clean[:50]}...' "
                f"model={model_used} duration={api_call_duration:.3f}s "
                f"response_length={explanation_length} words={explanation_words} "
                f"quality_score={quality_score:.2f}",
                extra={
                    'job_id': job_id,
                    'trace_id': trace_id,
                    'query': query_clean,
                    'classification': classification,
                    'confidence': confidence,
                    'model_used': model_used,
                    'use_fast_model': use_fast_model,
                    'api_call_duration_seconds': api_call_duration,
                    'response_length': explanation_length,
                    'response_word_count': explanation_words,
                    'quality_score': quality_score,
                    'classification_metadata': classification_metadata
                }
            )
            
            # Fallback logic: If fast model response seems inadequate, retry with standard
            if (use_fast_model and 
                    explanation_length < 20 and 
                    confidence < 0.85):
                logger.warning(
                    f"[FALLBACK] Fast model response too short, "
                    f"retrying with standard model",
                    extra={
                        'job_id': job_id,
                        'trace_id': trace_id,
                        'query': query_clean,
                        'fast_response_length': explanation_length
                    }
                )
                try:
                    fallback_start = time.time()
                    with time_ai_call(
                        stage_name="helping_agent_fallback_api_call",
                        job_id=job_id,
                        trace_id=trace_id,
                        model=self.model,
                        prompt_tokens=prompt_size // 4
                    ):
                        fallback_response = self.llm.invoke(messages)
                    
                    fallback_duration = time.time() - fallback_start
                    fallback_explanation = (
                        fallback_response.content.strip()
                        if hasattr(fallback_response, 'content')
                        else str(fallback_response).strip()
                    )
                    
                    # Use fallback if it's better
                    if len(fallback_explanation) > explanation_length:
                        explanation = fallback_explanation
                        model_used = self.model
                        logger.info(
                            f"[FALLBACK_SUCCESS] Using standard model response "
                            f"(duration={fallback_duration:.3f}s)",
                            extra={
                                'job_id': job_id,
                                'trace_id': trace_id,
                                'fallback_duration': fallback_duration,
                                'final_model': self.model
                            }
                        )
                except Exception as e:
                    logger.error(
                        f"[FALLBACK_FAILED] Could not retry with standard model: {e}",
                        extra={
                            'job_id': job_id,
                            'trace_id': trace_id,
                            'error': str(e)
                        }
                    )
                    # Continue with fast model response

            return explanation

        except Exception as e:
            logger.error(f"Error in helping agent explain: {e}")
            return (
                "Sorry, I encountered an error. "
                "Please try again with a different question."
            )

    def is_available(self) -> bool:
        """Check if the helping agent is available"""
        return self.llm is not None
