#!/usr/bin/env python3
"""
AI Tutor Agent - Service Factory
Initializes and provides access to all specialized services.
"""

import os
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from pydantic import BaseModel
import logging

# Import agents (wrapped by services)
from agents.concept_agent import ConceptAgent
from agents.mastery_agent import MasteryAgent
from agents.readiness_agent import ReadinessAgent

# Import services
from agents.services.lesson_service import LessonService
from agents.services.concept_service import ConceptService
from agents.services.history_service import HistoryService
from agents.services.llm_service import LLMService
from agents.services.mastery_service import MasteryService
from agents.services.readiness_service import ReadinessService
from agents.services.message_service import MessageService
from agents.services.student_service import StudentService

# Import cache
try:
    from cache import cache_get, cache_set, cache_delete
except ImportError:
    def cache_get(key): return None
    def cache_set(key, value, ttl=3600): return False
    def cache_delete(key): return False

# Optional LangChain support
try:
    from langchain_openai import ChatOpenAI
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

# Load environment variables
load_dotenv('config.env')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TutorRequest(BaseModel):
    """Request model for AI Tutor"""
    message: str
    topic: str
    lesson_content: Optional[str] = None
    user_id: Optional[str] = None
    conversation_history: Optional[List[Dict[str, str]]] = []
    conversation_id: Optional[str] = None
    explanation_style: Optional[str] = "default"  # noqa: E501


class TutorResponse(BaseModel):
    """Response model for AI Tutor"""
    response: str
    suggestions: List[str]
    related_concepts: List[str]
    related_concept_ids: List[str] = []
    confidence_score: float
    reasoning_label: str = "neutral"
    mastery_updates: List[Dict] = []
    readiness: Dict = None
    learning_path: Dict = None


class LessonRequest(BaseModel):
    """Request model for lesson generation"""
    topic: str
    learning_objectives: List[str]
    difficulty_level: str = "intermediate"


class LessonResponse(BaseModel):
    """Response model for lesson generation"""
    lesson_content: str
    key_points: List[str]
    practice_questions: List[str]
    estimated_duration: int


class AITutorAgent:
    """
    AI Tutor Agent - Service Factory
    Initializes and provides access to all specialized services.
    """

    def __init__(
        self,
        api_key: str = None,
        model: str = None,
        temperature: float = None,
        max_tokens: int = None,
        supabase_client: Optional[Any] = None
    ):
        """
        Initialize the AI Tutor agent and all services.

        Args:
            api_key: OpenAI API key
            model: Model to use (ignored - forced to gpt-4o-mini)
            temperature: Temperature for responses (default: 1)
            max_tokens: Maximum tokens per response (default: 4000)
            supabase_client: Optional Supabase client instance
        """
        # Load configuration
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        # Use gpt-3.5-turbo for fastest performance (2-4s vs 6-11s)
        self.model = "gpt-3.5-turbo"
        self.temperature = temperature or float(
            os.getenv('TUTOR_TEMPERATURE', '1')
        )
        self.max_tokens = max_tokens or int(
            os.getenv('TUTOR_MAX_TOKENS', '4000')
        )
        self.supabase = supabase_client

        # Initialize specialized agents (wrapped by services)
        self.concept_agent = ConceptAgent(
            api_key=self.api_key,
            supabase_client=supabase_client
        )
        self.mastery_agent = MasteryAgent(
            api_key=self.api_key,
            supabase_client=supabase_client
        )
        self.readiness_agent = ReadinessAgent(
            supabase_client=supabase_client,
            concept_agent=self.concept_agent
        )

        # Initialize LLM for LLMService - Use gpt-3.5-turbo for fastest performance
        llm = None
        if LANGCHAIN_AVAILABLE and self.api_key:
            try:
                llm = ChatOpenAI(
                    model="gpt-3.5-turbo",  # Fastest OpenAI model (2-4s response time)
                    temperature=self.temperature,
                    # Increased to 3000 for more comprehensive responses
                    max_tokens=min(self.max_tokens, 3000),
                    openai_api_key=self.api_key,
                    timeout=20,  # 20 second timeout (faster failure detection)
                    max_retries=1  # Reduced retries for faster failures
                )
                # Only log in debug mode
                if os.getenv("DEBUG", "0") == "1":
                    logger.info(
                        f"✅ LLM initialized with LangChain "
                        f"(Model: {self.model})"
                    )
            except Exception as e:
                logger.error(f"Error initializing LLM: {e}")
                llm = None

        # Initialize all services
        self.lesson_service = LessonService(
            supabase_client=supabase_client,
            concept_agent=self.concept_agent,
            cache_get=cache_get,
            cache_set=cache_set
        )
        self.concept_service = ConceptService(
            concept_agent=self.concept_agent,
            cache_get=cache_get,
            cache_set=cache_set
        )
        self.history_service = HistoryService(
            supabase_client=supabase_client,
            cache_get=cache_get,
            cache_set=cache_set,
            cache_delete=cache_delete
        )
        self.llm_service = LLMService(
            llm=llm,
            langchain_available=LANGCHAIN_AVAILABLE and llm is not None
        )
        self.mastery_service = MasteryService(
            mastery_agent=self.mastery_agent
        )
        self.readiness_service = ReadinessService(
            readiness_agent=self.readiness_agent
        )
        self.message_service = MessageService(
            supabase_client=supabase_client,
            cache_delete=cache_delete
        )
        self.student_service = StudentService(
            supabase_client=supabase_client,
            cache_get=cache_get,
            cache_set=cache_set
        )

        # Set up LangSmith tracing if enabled
        if os.getenv('LANGSMITH_TRACING', 'false').lower() == 'true':
            os.environ['LANGSMITH_TRACING'] = 'true'
            os.environ['LANGSMITH_ENDPOINT'] = os.getenv(
                'LANGSMITH_ENDPOINT', 'https://api.smith.langchain.com'
            )
            os.environ['LANGSMITH_API_KEY'] = os.getenv(
                'LANGSMITH_API_KEY', ''
            )
            os.environ['LANGSMITH_PROJECT'] = os.getenv(
                'LANGSMITH_PROJECT', 'imtehaan-ai-tutor'
            )
            logger.info("🔍 LangSmith tracing enabled for AI Tutor")

    def build_services(self) -> Dict:
        """
        Build and return a structured dict of all services.

        Returns:
            Dict containing all initialized services:
            {
                "lesson": LessonService,
                "concepts": ConceptService,
                "history": HistoryService,
                "llm": LLMService,
                "mastery": MasteryService,
                "readiness": ReadinessService,
                "messages": MessageService
            }
        """
        return {
            "lesson": self.lesson_service,
            "concepts": self.concept_service,
            "history": self.history_service,
            "llm": self.llm_service,
            "mastery": self.mastery_service,
            "readiness": self.readiness_service,
            "messages": self.message_service,
            "student": self.student_service
        }
