#!/usr/bin/env python3
"""
Mock Exam Grading Agent
This agent grades complete mock exams by evaluating all attempted questions
and updating adaptive learning signals (mastery, weaknesses, readiness).
"""

import os
import json
import asyncio
import statistics
import time
from typing import List, Dict, Optional, TypedDict
from datetime import datetime, timedelta
from uuid import uuid4
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from pydantic import BaseModel, Field, field_validator, model_validator
import logging
from collections import defaultdict
from functools import wraps

# LangGraph imports
try:
    from langgraph.graph import StateGraph, END
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False
    StateGraph = None
    END = None

# FastAPI imports
try:
    from fastapi import FastAPI, HTTPException, Request, Depends
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    FastAPI = None
    Depends = None
    JSONResponse = None

# Load environment variables
load_dotenv("config.env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Metrics tracking
_metrics = {
    "jobs_created": 0,
    "jobs_completed": 0,
    "jobs_failed": 0,
    "questions_graded": 0,
    "api_requests": 0,
    "api_errors": 0,
    "supabase_retries": 0,
    "supabase_failures": 0,
}


def log_metric(metric_name: str, value: int = 1):
    """Log a metric."""
    if metric_name in _metrics:
        _metrics[metric_name] += value
    else:
        _metrics[metric_name] = value


def get_metrics() -> Dict:
    """Get current metrics."""
    return _metrics.copy()


# Retry decorator for Supabase operations
def retry_supabase_operation(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
):
    """Decorator to retry Supabase operations with exponential backoff."""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = delay * (backoff ** attempt)
                        logger.warning(
                            f"Supabase operation failed (attempt "
                            f"{attempt + 1}/{max_retries}): {e}. "
                            f"Retrying in {wait_time}s..."
                        )
                        log_metric("supabase_retries")
                        await asyncio.sleep(wait_time)
                    else:
                        log_metric("supabase_failures")
                        logger.error(
                            f"Supabase operation failed after "
                            f"{max_retries} attempts: {e}"
                        )
            raise last_exception

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = delay * (backoff ** attempt)
                        logger.warning(
                            f"Supabase operation failed (attempt "
                            f"{attempt + 1}/{max_retries}): {e}. "
                            f"Retrying in {wait_time}s..."
                        )
                        log_metric("supabase_retries")
                        time.sleep(wait_time)
                    else:
                        log_metric("supabase_failures")
                        logger.error(
                            f"Supabase operation failed after "
                            f"{max_retries} attempts: {e}"
                        )
            raise last_exception

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    return decorator


# ============================================================================
# Data Models
# ============================================================================

class QuestionGrade(BaseModel):
    """Grade for a single question."""
    question_id: int
    question_number: int = 1
    part: str = ""
    question_text: str
    student_answer: str
    model_answer: str
    marks_allocated: int
    marks_awarded: float = Field(description="Marks awarded to the student")
    percentage_score: float = Field(
        description="Percentage score for this question"
    )
    feedback: str = Field(description="Detailed feedback on the answer")
    strengths: List[str] = Field(description="List of strengths in the answer")
    improvements: List[str] = Field(
        description="Areas that need improvement"
    )
    concept_ids: List[str] = Field(
        default_factory=list,
        description="List of concept IDs detected for this question"
    )
    mastery_score: Optional[float] = Field(
        default=None,
        description="Mastery score for this question (0-100)"
    )


class ExamReport(BaseModel):
    """Complete exam grading report."""
    total_questions: int
    questions_attempted: int
    total_marks: int
    marks_obtained: float
    percentage_score: float
    overall_grade: str = Field(
        description="Letter grade: A+, A, B+, B, C+, C, D, F"
    )
    question_grades: List[QuestionGrade]
    overall_feedback: str = Field(
        description="Overall exam performance feedback"
    )
    recommendations: List[str] = Field(
        description="Recommendations for improvement"
    )
    strengths_summary: List[str] = Field(description="Overall strengths")
    weaknesses_summary: List[str] = Field(
        description="Overall weaknesses"
    )
    readiness_score: Optional[float] = Field(
        default=None, description="Readiness score (0-100)"
    )
    average_mastery: Optional[float] = Field(
        default=None,
        description=(
            "Mastery score based on overall grade achieved (50-70 scale)"
        )
    )


# ============================================================================
# Core Agent
# ============================================================================

class MockExamGradingAgent:
    """
    Agent for grading complete mock exams and updating adaptive signals.
    
    NOTE: This agent handles written-answer mock exams only. MCQ-based exams
    (such as Economics P1) are graded automatically in the frontend and do
    not use this agent.
    
    Supported exam types:
    - Business Studies P1 & P2 (written answers)
    - Economics P2 (written answers)
    - Geography P1 & P2 (written answers, via Pakistan Studies)
    - History P1 & P2 (written answers, via Pakistan Studies)
    - Islamiyat P1 & P2 (written answers)
    
    NOT supported (MCQ-based, graded in frontend):
    - Economics P1 (MCQ-based)
    """

    def __init__(self, api_key: str):
        """Initialize the grading agent."""
        self.api_key = api_key
        self.llm = ChatOpenAI(
            model=os.getenv("GRADING_MODEL", "gpt-5-nano-2025-08-07"),
            temperature=0.3,
            max_tokens=4000,
            openai_api_key=api_key,
        )

        # Initialize embeddings for concept detection
        try:
            self.embeddings = OpenAIEmbeddings(openai_api_key=api_key)
        except Exception as e:
            logger.warning(f"Could not initialize embeddings: {e}")
            self.embeddings = None

        # Initialize Supabase client (singleton)
        try:
            from services.supabase_client import get_supabase_client
            self.supabase = get_supabase_client()
            if self.supabase:
                logger.info(
                    "✅ Supabase client initialized for Mock Exam Grading Agent"
                )
            else:
                logger.warning(
                    "⚠️ Supabase credentials not found - persistence disabled"
                )
        except ImportError:
            logger.warning(
                "⚠️ Supabase Python client not installed - "
                "persistence disabled"
            )
            self.supabase = None
        except Exception as e:
            logger.warning(f"[WARN] Error initializing Supabase: {e}")
            self.supabase = None

        logger.info("[OK] Mock Exam Grading Agent initialized")

    # ---------------------------------------------------------------------
    # Concept Detection (for adaptive learning, not grading itself)
    # ---------------------------------------------------------------------
    def detect_concepts_for_question(self, question_text: str) -> List[str]:
        """
        Detect concept IDs for a question using embeddings and pgvector search.

        Args:
            question_text: The question text to analyze

        Returns:
            List of concept IDs (as strings)
        """
        if not self.supabase or not self.embeddings:
            return []

        try:
            # Generate embedding for question
            embedding = self.embeddings.embed_query(question_text)

            # Use Supabase RPC for pgvector similarity search
            # Uses concept_embeddings table (correct source of vectors)
            result = self.supabase.rpc(
                "match_concepts",
                {
                    "query_embedding": embedding,
                    "match_threshold": 0.7,
                    "match_count": 5,
                },
            ).execute()

            if result.data:
                concept_ids = [
                    item.get("concept_id")
                    for item in result.data
                    if item.get("concept_id") is not None
                ]
                return [str(cid) for cid in concept_ids]

            # No matches or RPC not wired – just return empty
            return []
        except Exception as e:
            logger.warning(
                f"Error detecting concepts for question: {e}",
                exc_info=True
            )
            # Fallback: try direct table query if RPC doesn't exist
            try:
                if self.supabase:
                    # Simple fallback - just return empty for now
                    logger.debug(
                        "RPC match_concepts failed, using fallback "
                        "(returning empty list)"
                    )
            except Exception as fallback_error:
                logger.debug(f"Fallback also failed: {fallback_error}")
            return []

    # ---------------------------------------------------------------------
    # Mastery / Weakness / Readiness logic
    # ---------------------------------------------------------------------
    def difficulty_weight_from_marks(self, marks: int) -> float:
        """Calculate difficulty weight based on marks allocated."""
        if marks <= 5:
            return 1.0
        elif marks <= 10:
            return 1.2
        else:
            return 1.5

    def base_delta_from_question_score(self, percentage_score: float) -> float:
        """Convert percentage score to mastery delta (-10 to +10 range)."""
        # Map 0–100% to -10 to +10 delta (50% = 0 delta)
        return (percentage_score - 50.0) / 5.0

    @retry_supabase_operation(max_retries=3, delay=1.0, backoff=2.0)
    def apply_mastery_update(
        self,
        user_id: str,
        concept_id: str,
        base_delta: float,
        marks_allocated: int,
    ) -> float:
        """
        Apply mastery update to Supabase with retry logic.

        Returns:
            New mastery value (0–100)
        """
        if not self.supabase:
            logger.warning(
                f"Supabase not available - skipping mastery update for "
                f"user {user_id}, concept {concept_id}"
            )
            return 0.0

        try:
            difficulty_weight = self.difficulty_weight_from_marks(
                marks_allocated
            )
            time_weight = 1.0  # Fixed for now
            exam_weight = 1.3  # Exam weighting factor

            final_delta = (
                base_delta * difficulty_weight * time_weight * exam_weight
            )

            # Get current mastery
            # Actual schema: mastery_score (INTEGER), id (BIGINT PK),
            # user_id (TEXT)
            result = (
                self.supabase.table("student_mastery")
                .select("mastery_score")
                .eq("user_id", user_id)
                .eq("concept_id", concept_id)
                .limit(1)
                .execute()
            )

            current_mastery = 50.0  # Default
            if result.data:
                current_mastery = float(
                    result.data[0].get("mastery_score", 50.0)
                )

            # Calculate new mastery
            new_mastery = max(0.0, min(100.0, current_mastery + final_delta))

            # Upsert mastery (actual schema uses mastery_score, id as PK)
            # Check if record exists first using id
            existing = (
                self.supabase.table("student_mastery")
                .select("id")
                .eq("user_id", user_id)
                .eq("concept_id", concept_id)
                .limit(1)
                .execute()
            )

            # Prepare update/insert data
            update_data = {
                "mastery_score": int(round(new_mastery)),
                "updated_at": datetime.now().isoformat(),
            }
            insert_data = {
                "user_id": user_id,
                "concept_id": concept_id,
                "mastery_score": int(round(new_mastery)),
                "updated_at": datetime.now().isoformat(),
            }
            
            # Note: subject is not available in apply_mastery_update
            # It's handled at the exam level in compute_mastery_and_readiness
            
            if existing.data:
                # Update existing record
                try:
                    (
                        self.supabase.table("student_mastery")
                        .update(update_data)
                        .eq("user_id", user_id)
                        .eq("concept_id", concept_id)
                        .execute()
                    )
                except Exception as e:
                    # Handle any update errors
                    logger.warning(f"Error updating mastery: {e}")
                    raise
            else:
                # Insert new record
                try:
                    (
                        self.supabase.table("student_mastery")
                        .insert(insert_data)
                        .execute()
                    )
                except Exception as e:
                    # Handle any insert errors
                    logger.warning(f"Error inserting mastery: {e}")
                    raise

            logger.info(
                f"✅ Mastery updated: user={user_id}, concept={concept_id}, "
                f"old={current_mastery:.2f}, new={new_mastery:.2f}"
            )
            return new_mastery
        except Exception as e:
            logger.error(
                f"❌ Error applying mastery update for user {user_id}, "
                f"concept {concept_id}: {e}",
                exc_info=True
            )
            return 0.0

    def classify_weakness_level(
        self, percentage_score: float
    ) -> Optional[str]:
        """Classify weakness level from percentage score."""
        if percentage_score < 30:
            return "critical"
        elif percentage_score < 40:
            return "high"
        elif percentage_score < 50:
            return "moderate"
        elif percentage_score < 60:
            return "low"
        else:
            return None

    def compute_readiness_score(
        self,
        user_id: str,
        exam_report: ExamReport,
        mastery_updates: Dict[str, float],
    ) -> float:
        """
        Compute readiness score using formula:
        R = 0.35M + 0.25E + 0.15T + 0.10C + 0.10D + 0.05S
        """
        # M = average mastery from mastery_updates
        if mastery_updates:
            M = sum(mastery_updates.values()) / len(mastery_updates)
        else:
            M = 50.0  # Default baseline

        # E = exam percentage_score
        E = exam_report.percentage_score

        # T = trend (simplified: 100 if E > 60, else 50)
        T = 100.0 if E > 60 else 50.0

        # C = confidence (100 if all questions attempted)
        if exam_report.questions_attempted == exam_report.total_questions:
            C = 100.0
        else:
            C = 70.0

        # D = difficulty (normalized from total_marks, assuming max 100)
        D = min(100.0, (exam_report.total_marks / 100.0) * 100.0)

        # S = consistency (std dev of question scores, normalized)
        if exam_report.question_grades:
            scores = [q.percentage_score for q in exam_report.question_grades]
            if len(scores) > 1:
                std_dev = statistics.stdev(scores)
                # Normalize: lower std dev = higher consistency
                S = max(0.0, 100.0 - (std_dev * 2))
            else:
                S = 50.0
        else:
            S = 50.0

        readiness = (
            0.35 * M
            + 0.25 * E
            + 0.15 * T
            + 0.10 * C
            + 0.10 * D
            + 0.05 * S
        )
        return max(0.0, min(100.0, readiness))

    # ---------------------------------------------------------------------
    # Grading (per exam and per question)
    # ---------------------------------------------------------------------
    def grade_exam(self, attempted_questions: List[Dict], subject: Optional[str] = None) -> ExamReport:
        """
        Grade a complete mock exam (written answers only).
        
        NOTE: This method is for written-answer exams only. MCQ-based exams
        (e.g., Economics P1) are graded in the frontend and should not call
        this agent.

        Args:
            attempted_questions: List of attempted questions with:
                - question
                - user_answer
                - solution / model_answer
                - marks
            subject: Optional subject name (e.g., "Business Studies", "Economics", 
                     "Geography", "History", "Islamiyat")
                     
                     NOTE: Economics P1 is MCQ-based and not handled by this agent.

        Returns:
            ExamReport with detailed grading results.
        """
        try:
            logger.info(
                f"📝 Grading exam with "
                f"{len(attempted_questions)} attempted questions"
                f" (subject: {subject or 'Business Studies (default)'})"
            )

            # Calculate total marks
            total_marks = sum(q.get("marks", 0) for q in attempted_questions)

            # Grade each question
            question_grades: List[QuestionGrade] = []
            for q in attempted_questions:
                grade = self._grade_single_question(
                    q, subject=subject, job_id=None, trace_id=None
                )
                question_grades.append(grade)

            # Calculate overall scores
            marks_obtained = sum(g.marks_awarded for g in question_grades)
            percentage_score = (
                (marks_obtained / total_marks * 100) if total_marks > 0 else 0
            )

            # Generate overall feedback
            overall_feedback = self._generate_overall_feedback(
                question_grades, percentage_score, subject=subject
            )

            # Generate recommendations
            recommendations = self._generate_recommendations(
                question_grades, percentage_score, subject=subject
            )

            # Generate strengths and weaknesses summary
            strengths, weaknesses = self._generate_summaries(question_grades)

            # Determine overall grade
            overall_grade = self._calculate_grade(percentage_score)

            report = ExamReport(
                total_questions=len(attempted_questions),
                questions_attempted=len(attempted_questions),
                total_marks=total_marks,
                marks_obtained=marks_obtained,
                percentage_score=round(percentage_score, 2),
                overall_grade=overall_grade,
                question_grades=question_grades,
                overall_feedback=overall_feedback,
                recommendations=recommendations,
                strengths_summary=strengths,
                weaknesses_summary=weaknesses,
            )

            logger.info(
                f"✅ Exam graded successfully. "
                f"Score: {percentage_score}% ({overall_grade})"
            )
            return report

        except Exception as e:
            logger.error(f"[FAIL] Error grading exam: {e}")
            return self._create_fallback_report(attempted_questions)

    def _grade_single_question(
        self,
        question: Dict,
        subject: Optional[str] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> QuestionGrade:
        """Grade a single question with concept detection (no RAG)."""
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
            question_id = question.get("question_id", 0)
            question_text = question.get("question", "")
            student_answer = question.get("user_answer", "")
            model_answer = (
                question.get("solution") or question.get("model_answer", "")
            )
            marks = question.get("marks", 0)
            part = question.get("part", "")
            question_number = (
                question.get("question_number", 0)
                if "question_number" in question
                else question_id
            )

            # Detect concepts for this question (for adaptive layer)
            concept_ids = self.detect_concepts_for_question(question_text)

            # Determine subject from parameter or default to Business Studies
            # Support all subjects: Economics, Geography, History, Islamiyat, Business Studies
            # Note: "Pakistan Studies" can refer to Geography (P2) or History/Geography (P1)
            if subject:
                subject_normalized = str(subject).strip().lower()
                # Map subject variations to display names
                if "economics" in subject_normalized:
                    subject_display = "Economics"
                elif "geography" in subject_normalized or "pak studies geography" in subject_normalized:
                    subject_display = "Geography"
                elif "history" in subject_normalized or "pak studies history" in subject_normalized:
                    subject_display = "History"
                elif "islamiyat" in subject_normalized or "islamiat" in subject_normalized:
                    subject_display = "Islamiyat"
                elif "pakistan studies" in subject_normalized or "pak studies" in subject_normalized:
                    # Pakistan Studies: P2 is Geography, P1 could be Geography or History
                    # Default to Geography for now (can be refined based on exam_type if needed)
                    subject_display = "Geography"
                elif "business" in subject_normalized:
                    subject_display = "Business Studies"
                else:
                    # Default to Business Studies if unclear
                    subject_display = "Business Studies"
            else:
                subject_display = "Business Studies"
                subject_normalized = ""
            
            logger.info(
                f"Grading question {question_id} (Part {part}): "
                f"subject={subject}, subject_display={subject_display}, "
                f"normalized={subject_normalized}"
            )

            if not model_answer:
                # If no model answer provided, award marks based on effort
                return QuestionGrade(
                    question_id=question_id,
                    question_number=question_number,
                    part=part,
                    question_text=question_text,
                    student_answer=student_answer,
                    model_answer="No model answer available",
                    marks_allocated=marks,
                    marks_awarded=marks * 0.5 if student_answer.strip() else 0,
                    percentage_score=50.0 if student_answer.strip() else 0.0,
                    feedback=(
                        "Your answer has been recorded. Detailed grading "
                        "requires a model answer."
                    ),
                    strengths=(
                        ["Answer submitted"]
                        if student_answer.strip()
                        else ["Attempt made"]
                    ),
                    improvements=(
                        ["Keep practicing"]
                        if student_answer.strip()
                        else ["Try to provide an answer"]
                    ),
                    concept_ids=concept_ids,
                )

            # PHASE 1: Prompt Construction
            with time_prompt_construction(
                stage_name="mock_exam_grading_prompt_construction",
                job_id=job_id,
                trace_id=trace_id
            ):
                # Build grading prompt (no lesson/RAG context here)
                # Set terminology text based on subject
                if subject_display == "Economics":
                    terminology_text = "economics terminology"
                elif subject_display == "Geography":
                    terminology_text = "geography terminology"
                elif subject_display == "History":
                    terminology_text = "history terminology"
                elif subject_display == "Islamiyat":
                    terminology_text = "islamiyat terminology"
                else:
                    terminology_text = "business terminology"
                
                # Calculate word count for student answer (for word count evaluation)
                student_answer_word_count = len(student_answer.split()) if student_answer else 0
                
                # Determine expected word count range based on marks allocated
                # This helps the agent evaluate if the answer length is appropriate
                word_count_ranges = {
                    1: (5, 10, "One fact / one correct point"),
                    2: (15, 25, "Definition or two brief points"),
                    3: (25, 40, "One explained point OR two simple points"),
                    4: (40, 60, "Two explained points"),
                    5: (55, 80, "Explanation + some application"),
                    6: (70, 100, "Two developed points (KAA)"),
                    7: (90, 120, "Explanation + analysis"),
                    8: (110, 140, "Balanced analysis, cause–effect"),
                    10: (140, 180, "Analysis + evaluation"),
                    14: (220, 280, "Full evaluation + justified judgement")
                }
                
                # Get expected range for this question's marks
                expected_min_words, expected_max_words, expected_description = word_count_ranges.get(
                    marks, (0, 0, "Standard answer length")
                )
                
                # Determine if word count is appropriate
                word_count_status = ""
                if expected_min_words > 0 and expected_max_words > 0:
                    if student_answer_word_count < expected_min_words:
                        word_count_status = (
                            f"⚠️ TOO SHORT: Student wrote {student_answer_word_count} words, "
                            f"but {marks}-mark questions typically require {expected_min_words}-{expected_max_words} words "
                            f"({expected_description}). This may indicate insufficient detail or depth."
                        )
                    elif student_answer_word_count > expected_max_words:
                        word_count_status = (
                            f"⚠️ TOO LONG: Student wrote {student_answer_word_count} words, "
                            f"but {marks}-mark questions typically require {expected_min_words}-{expected_max_words} words "
                            f"({expected_description}). Check for repetition or lack of focus."
                        )
                    else:
                        word_count_status = (
                            f"✅ APPROPRIATE LENGTH: Student wrote {student_answer_word_count} words, "
                            f"which is within the expected range of {expected_min_words}-{expected_max_words} words "
                            f"for {marks}-mark questions ({expected_description})."
                        )
                else:
                    # For marks not in the table, provide general guidance
                    word_count_status = (
                        f"Student wrote {student_answer_word_count} words for a {marks}-mark question. "
                        f"Evaluate if the length is appropriate for the mark allocation."
                    )
                
                # Add subject-specific context for better grading
                if subject_display == "Economics":
                    subject_context = """
ECONOMICS IGCSE CONTEXT:
You are grading an IGCSE Economics exam. Consider Economics concepts such as:
- Microeconomics: demand, supply, price determination, elasticity, market structures, market failure
- Macroeconomics: economic growth, inflation, unemployment, monetary policy, fiscal policy, supply-side policy
- Economic concepts: scarcity, opportunity cost, factors of production, PPC diagrams, economic systems
- Economic agents: workers, firms, households, trade unions, government
- International economics: globalization, free trade, protection, exchange rates, balance of payments
- Development economics: developed/less-developed economies, poverty, living standards, population

GRADING CRITERIA FOR ECONOMICS:
- Knowledge: Correct use of Economics concepts and terminology (demand, supply, elasticity, market structures, etc.)
- Application: Ability to apply Economics concepts to real-world scenarios and case studies
- Analysis: Identifying relationships, causes, and effects in economic contexts
- Evaluation: Making judgments about economic policies, solutions, and trade-offs
"""
                elif subject_display == "Geography":
                    subject_context = """
GEOGRAPHY IGCSE CONTEXT:
You are grading an IGCSE Geography exam. Consider Geography concepts such as:
- Physical geography: rivers, coasts, weather, climate, ecosystems, natural hazards
- Human geography: population, settlement, migration, urbanization, economic activities
- Environmental geography: resource management, sustainability, environmental issues
- Map skills: reading maps, interpreting data, using geographical tools
- Case studies: real-world examples from different regions

GRADING CRITERIA FOR GEOGRAPHY:
- Knowledge: Correct use of Geography concepts and terminology (erosion, deposition, migration, urbanization, etc.)
- Application: Ability to apply Geography concepts to real-world case studies and examples
- Analysis: Identifying patterns, relationships, and processes in geographical contexts
- Evaluation: Making judgments about geographical issues, solutions, and their impacts
"""
                elif subject_display == "History":
                    subject_context = """
HISTORY IGCSE CONTEXT:
You are grading an IGCSE History exam. Consider History concepts such as:
- Historical events: causes, consequences, significance
- Historical sources: primary and secondary sources, reliability, bias
- Historical analysis: chronology, causation, change and continuity
- Historical interpretation: different perspectives, historical debates
- Historical context: understanding events within their time period

GRADING CRITERIA FOR HISTORY:
- Knowledge: Accurate recall of historical facts, dates, events, and figures
- Application: Ability to use historical knowledge to explain and analyze events
- Analysis: Identifying causes, consequences, and relationships between historical events
- Evaluation: Making judgments about historical significance, reliability of sources, and different interpretations
"""
                elif subject_display == "Islamiyat":
                    subject_context = """
ISLAMIYAT IGCSE CONTEXT:
You are grading an IGCSE Islamiyat exam. Consider Islamiyat concepts such as:
- Islamic beliefs: Tawheed, Risalah, Akhirah, Angels, Books, Predestination
- Islamic practices: Five Pillars (Shahadah, Salah, Zakat, Sawm, Hajj)
- Islamic history: Life of Prophet Muhammad (PBUH), early Islamic history, Caliphate
- Islamic sources: Quran, Hadith, Sunnah, Ijma, Qiyas
- Islamic ethics: moral values, social justice, human rights in Islam

GRADING CRITERIA FOR ISLAMIYAT:
- Knowledge: Correct understanding of Islamic beliefs, practices, and historical events
- Application: Ability to apply Islamic teachings to real-world situations and case studies
- Analysis: Identifying relationships between Islamic concepts and their significance
- Evaluation: Making judgments about Islamic perspectives on contemporary issues
"""
                else:
                    subject_context = """
BUSINESS STUDIES IGCSE CONTEXT:
You are grading an IGCSE Business Studies exam. Consider Business concepts such as:
- Business organization and structure
- Marketing, finance, operations, human resources
- External influences on business
- Business growth and strategy

GRADING CRITERIA FOR BUSINESS STUDIES:
- Knowledge: Correct use of business terminology and concepts
- Application: Ability to apply business concepts to real-world scenarios
- Analysis: Identifying relationships and implications
- Evaluation: Making business judgments and recommendations
"""
                
                grading_prompt = f"""
You are an expert Cambridge IGCSE {subject_display} examiner grading a mock exam question.
Please evaluate the student's answer comprehensively using IGCSE {subject_display} standards.

{subject_context}

────────────────────────────────────────
WORD COUNT GUIDELINES FOR OPTIMAL GRADING
────────────────────────────────────────
The length of the student's answer is an important factor in grading.
Use these word count ranges as a guide when evaluating responses:

| Marks | Ideal Word Count  | What the Examiner Expects                |
|-------|-------------------|-------------------------------------------|
|   **1 mark** | **5–10 words**    | One fact / one correct point             |
|  **2 marks** | **15–25 words**   | Definition or two brief points           |
|  **3 marks** | **25–40 words**   | One explained point OR two simple points |
|  **4 marks** | **40–60 words**   | Two explained points                     |
|  **5 marks** | **55–80 words**   | Explanation + some application           |
|  **6 marks** | **70–100 words**  | Two developed points (KAA)               |
|  **7 marks** | **90–120 words**  | Explanation + analysis                   |
|  **8 marks** | **110–140 words** | Balanced analysis, cause–effect          |
| **10 marks** | **140–180 words** | Analysis + evaluation                    |
| **14 marks** | **220–280 words** | Full evaluation + justified judgement    |

IMPORTANT WORD COUNT EVALUATION RULES:
• The student's answer word count will be provided below.
• Answers significantly below the ideal range may lack depth and detail.
• Answers significantly above the ideal range may be repetitive or unfocused.
• Consider word count when determining if the answer has sufficient:
  - Detail for the mark allocation
  - Development of points
  - Analysis and evaluation (for higher mark questions)
• If the answer is too short for the mark allocation, mention this in
  "improvements" and suggest expanding with more detail.
• If the answer is too long and repetitive, mention this in feedback.
• Word count is a GUIDE, not a strict requirement - content quality matters most.
  However, answers that are far outside the ideal range should be flagged.

────────────────────────────────────────
QUESTION DETAILS
────────────────────────────────────────
QUESTION:
{question_text}

MODEL ANSWER:
{model_answer}

STUDENT'S ANSWER:
{student_answer}

MARKS ALLOCATED: {marks}
STUDENT ANSWER WORD COUNT: {student_answer_word_count} words

────────────────────────────────────────
WORD COUNT EVALUATION FOR THIS QUESTION
────────────────────────────────────────
Expected Word Count Range for {marks} marks: {expected_min_words}-{expected_max_words} words
Expected Content: {expected_description}

{word_count_status}

IMPORTANT: Word count is a MINOR consideration - CONTENT QUALITY IS PARAMOUNT:
• Prioritize knowledge, understanding, and analysis quality over strict word count adherence.
• Only reduce marks for word count if:
  - The answer is SIGNIFICANTLY too short (less than 70% of minimum: {int(expected_min_words * 0.7)} words) AND lacks essential detail
  - The answer is SIGNIFICANTLY too long (more than 150% of maximum: {int(expected_max_words * 1.5)} words) AND contains substantial repetition
• If an answer demonstrates good understanding but is slightly outside the range, award marks based on content quality.
• Excellent answers should receive high marks even if slightly over/under the word count range.
• Use the word count guidelines as a reference, not a strict requirement.

Please provide:
1. Marks awarded (0 to {marks} - MUST NOT exceed {marks})
2. Percentage score (0 to 100)
3. Detailed feedback on the answer (mention specific {terminology_text} used correctly or incorrectly)
4. 2-3 key strengths (be specific about what the student did well)
5. 2-3 areas for improvement (be constructive and specific)

Be fair, constructive, and encouraging. Consider:
- Understanding of the {subject_display} topic and concepts
- Use of appropriate {terminology_text} (e.g., demand/supply, elasticity, market structures for Economics)
- Structure and clarity of response
- Relevance and accuracy of the content
- Depth of analysis and evaluation (where applicable for higher mark questions)
- Application of {subject_display} concepts to the specific question context
- **WORD COUNT REFERENCE**: The answer has {student_answer_word_count} words. For {marks} marks, 
  the ideal range is {expected_min_words}-{expected_max_words} words ({expected_description}). 
  Use this as a reference, but prioritize content quality. Only reduce marks if the answer is 
  significantly outside the range (less than 70% of minimum or more than 150% of maximum) AND 
  the length issue affects content quality (too short = lacks detail, too long = repetitive).

MARK ALLOCATION GUIDELINES:
- For {marks}-mark questions, assess based on the mark allocation (typically Knowledge for 2 marks, Knowledge+Application for 4 marks, Knowledge+Application+Analysis for 6 marks, etc.)
- Be strict but fair - award marks only when {subject_display} concepts are correctly understood and applied
- **WORD COUNT GUIDANCE**: Word count is a minor factor. Award marks primarily based on content quality:
  - Knowledge and understanding of {subject_display} concepts
  - Accuracy and relevance of the answer
  - Depth of analysis and explanation
  - Use of appropriate {terminology_text}
- Only consider word count if the answer is significantly outside the range AND it affects quality.
- The word count evaluation above shows: {word_count_status}
- REMEMBER: Excellent content should receive high marks regardless of being slightly outside the word count range.

Return your response in this JSON format:
{{
    "marks_awarded": <number between 0 and {marks}>,
    "percentage_score": <number between 0 and 100>,
    "feedback": "<detailed feedback mentioning specific {terminology_text} and {subject_display} concepts>",
    "strengths": ["strength1", "strength2", "strength3"],
    "improvements": ["improvement1", "improvement2"]
}}
"""
                prompt_size = len(grading_prompt)

            # PHASE 2: API Call
            with time_ai_call(
                stage_name="mock_exam_grading_api_call",
                job_id=job_id,
                trace_id=trace_id,
                model="gpt-4o-mini",
                prompt_tokens=prompt_size // 4  # Rough estimate
            ):
                response = self.llm.invoke(grading_prompt)

            # PHASE 3: Response Parsing and Validation
            with time_response_parsing(
                stage_name="mock_exam_grading_response_parsing",
                job_id=job_id,
                trace_id=trace_id,
                response_size=len(response.content) if hasattr(response, 'content') else None
            ):
                # Parse the response
                try:
                    result = json.loads(response.content)
                except json.JSONDecodeError:
                    # Try to extract JSON from the response
                    content = response.content
                    json_start = content.find("{")
                    json_end = content.rfind("}") + 1
                    if json_start >= 0 and json_end > json_start:
                        result = json.loads(content[json_start:json_end])
                    else:
                        raise ValueError("Could not parse JSON response")

            return QuestionGrade(
                question_id=question_id,
                question_number=question_number,
                part=part,
                question_text=question_text,
                student_answer=student_answer,
                model_answer=model_answer,
                marks_allocated=marks,
                marks_awarded=float(
                    result.get("marks_awarded", marks * 0.5)
                ),
                percentage_score=float(
                    result.get("percentage_score", 50.0)
                ),
                feedback=result.get(
                    "feedback", "Good effort on this question."
                ),
                strengths=result.get("strengths", ["Answer submitted"]),
                improvements=result.get("improvements", ["Keep practicing"]),
                concept_ids=concept_ids,
            )

        except Exception as e:
            logger.error(f"Error grading question {question_id}: {e}")
            model_ans = (
                question.get("solution")
                or question.get("model_answer", "No model answer")
            )
            return QuestionGrade(
                question_id=question.get("question_id", 0),
                question_number=question.get("question_number", 0),
                part=question.get("part", ""),
                question_text=question.get("question", ""),
                student_answer=question.get("user_answer", ""),
                model_answer=model_ans,
                marks_allocated=question.get("marks", 0),
                marks_awarded=0.0,
                percentage_score=0.0,
                feedback="Error in grading system. Please contact support.",
                strengths=["Answer submitted"],
                improvements=["Grading error occurred"],
                concept_ids=[],
            )

    # ---------------------------------------------------------------------
    # Report helpers
    # ---------------------------------------------------------------------
    def _generate_overall_feedback(
        self, question_grades: List[QuestionGrade], percentage: float, subject: Optional[str] = None
    ) -> str:
        """Generate overall feedback for the exam."""
        # Determine subject display name (handle None and normalize)
        if subject:
            subject_normalized = str(subject).strip().lower()
            subject_display = "Economics" if "economics" in subject_normalized else "Business Studies"
        else:
            subject_display = "Business Studies"
        if percentage >= 90:
            return (
                f"Outstanding performance! You scored {percentage}%, "
                f"demonstrating excellent mastery of {subject_display} "
                "concepts. Your understanding is exceptional across all "
                "topics covered."
            )
        elif percentage >= 80:
            return (
                f"Excellent work! Your score of {percentage}% shows strong "
                "understanding of the material. You have a solid grasp of "
                "key concepts and can apply them effectively."
            )
        elif percentage >= 70:
            return (
                f"Good performance with {percentage}%. You demonstrate a "
                "solid understanding of most concepts. With some focused "
                "practice, you can achieve even better results."
            )
        elif percentage >= 60:
            return (
                f"Satisfactory performance at {percentage}%. You understand "
                "the basics but need to strengthen your knowledge in several "
                "areas. Keep studying!"
            )
        elif percentage >= 50:
            return (
                f"Below expectations at {percentage}%. Focus on understanding "
                "core concepts and improving your answer structure. More "
                "practice will help you improve significantly."
            )
        else:
            return (
                f"Needs improvement at {percentage}%. Review the fundamental "
                "concepts and work on building your understanding. Don't "
                "give up - consistent effort will lead to progress."
            )

    def _generate_recommendations(
        self, question_grades: List[QuestionGrade], percentage: float, subject: Optional[str] = None
    ) -> List[str]:
        """Generate recommendations based on performance."""
        # Determine subject display name (handle None and normalize)
        if subject:
            subject_normalized = str(subject).strip().lower()
            # Map all supported subjects
            if "economics" in subject_normalized:
                subject_display = "Economics"
                terminology_text = "economics terminology"
            elif "islamiyat" in subject_normalized or "islamiat" in subject_normalized:
                subject_display = "Islamiyat"
                terminology_text = "Islamic terminology"
            elif "geography" in subject_normalized or "pak studies geography" in subject_normalized:
                subject_display = "Geography"
                terminology_text = "geographical terminology"
            elif "history" in subject_normalized or "pak studies history" in subject_normalized:
                subject_display = "History"
                terminology_text = "historical terminology"
            elif "business" in subject_normalized:
                subject_display = "Business Studies"
                terminology_text = "business terminology"
            elif "pakistan studies" in subject_normalized or "pak studies" in subject_normalized:
                # Default to Geography for Pakistan Studies (P2 is Geography)
                subject_display = "Geography"
                terminology_text = "geographical terminology"
            else:
                # Default fallback
                subject_display = "Business Studies"
                terminology_text = "business terminology"
        else:
            # Default fallback when subject is None
            subject_display = "Business Studies"
            terminology_text = "business terminology"
        
        recommendations: List[str] = []

        if percentage < 60:
            recommendations.append(
                f"Review fundamental {subject_display} concepts thoroughly"
            )
            recommendations.append(
                "Practice writing structured answers with clear points"
            )
            recommendations.append(
                f"Focus on using appropriate {terminology_text}"
            )
        elif percentage < 80:
            recommendations.append(
                "Strengthen understanding in weaker topic areas"
            )
            recommendations.append(
                "Practice providing more detailed analysis in answers"
            )
            recommendations.append(
                "Work on connecting concepts to real-world examples"
            )
        else:
            recommendations.append(
                "Continue practicing with more challenging questions"
            )
            recommendations.append(
                "Focus on refining your critical analysis skills"
            )
            recommendations.append("Maintain your excellent study habits")

        # NOTE: Intentionally do NOT add question-number-specific recommendations.
        # Users found "Pay special attention to Question X..." noisy; keep
        # recommendations generic and skills-focused instead.

        return recommendations

    def _generate_summaries(
        self, question_grades: List[QuestionGrade]
    ) -> tuple[List[str], List[str]]:
        """Generate strengths and weaknesses summaries."""
        # Analyze common patterns
        all_strengths = [s for g in question_grades for s in g.strengths]
        all_improvements = [
            i for g in question_grades for i in g.improvements
        ]

        strength_counts: Dict[str, int] = {}
        improvement_counts: Dict[str, int] = {}

        for s in all_strengths:
            strength_counts[s] = strength_counts.get(s, 0) + 1

        for i in all_improvements:
            improvement_counts[i] = improvement_counts.get(i, 0) + 1

        # Get top 3 strengths
        strengths_pairs = sorted(
            strength_counts.items(), key=lambda x: x[1], reverse=True
        )[:3]
        strengths = [s[0] for s in strengths_pairs]

        # Get top 3 areas for improvement
        weaknesses_pairs = sorted(
            improvement_counts.items(), key=lambda x: x[1], reverse=True
        )[:3]
        weaknesses = [w[0] for w in weaknesses_pairs]

        if not strengths:
            strengths = [
                "Consistent effort across questions",
                "Completed all questions",
            ]

        if not weaknesses:
            weaknesses = [
                "Continue practicing",
                "Maintain focus and effort",
            ]

        return strengths, weaknesses

    def _calculate_grade(self, percentage: float) -> str:
        """Calculate letter grade from percentage."""
        if percentage >= 97:
            return "A+"
        elif percentage >= 93:
            return "A"
        elif percentage >= 87:
            return "B+"
        elif percentage >= 83:
            return "B"
        elif percentage >= 77:
            return "C+"
        elif percentage >= 73:
            return "C"
        elif percentage >= 65:
            return "D"
        else:
            return "F"

    def _grade_to_mastery(self, grade: str) -> float:
        """Convert letter grade to mastery value.

        Args:
            grade: Letter grade (A+, A, B+, B, C+, C, D, F)

        Returns:
            Mastery value between 50 (F) and 70 (A+)
        """
        grade_mastery_map = {
            "A+": 70.0,
            "A": 67.0,
            "B+": 63.0,
            "B": 60.0,
            "C+": 58.0,
            "C": 55.0,
            "D": 52.0,
            "F": 50.0,
        }
        return grade_mastery_map.get(grade.upper(), 50.0)

    def _create_fallback_report(
        self, attempted_questions: List[Dict]
    ) -> ExamReport:
        """Create a fallback report when grading fails."""
        total_marks = sum(q.get("marks", 0) for q in attempted_questions)

        return ExamReport(
            total_questions=len(attempted_questions),
            questions_attempted=len(attempted_questions),
            total_marks=total_marks,
            marks_obtained=0.0,
            percentage_score=0.0,
            overall_grade="F",
            question_grades=[],
            overall_feedback=(
                "Error in grading system. Please try again or contact "
                "support."
            ),
            recommendations=[
                "Retry the grading",
                "Contact technical support",
            ],
            strengths_summary=["Answers submitted successfully"],
            weaknesses_summary=["Grading system error"],
        )


# ============================================================================
# LangGraph State and Nodes
# ============================================================================

class MockExamState(TypedDict, total=False):
    """State for LangGraph mock exam grading workflow."""
    user_id: str
    attempted_questions: List[Dict]
    question_grades: List[QuestionGrade]
    exam_report: Optional[ExamReport]
    mastery_updates: Dict[str, float]
    readiness_score: Optional[float]
    concept_ids: List[str]
    request_id: Optional[str]
    job_id: Optional[str]
    subject: Optional[str]  # Subject name (e.g., "Business Studies", "Economics")
    exam_type: Optional[str]  # Exam type (e.g., "P1", "P2")


# Global agent instance for LangGraph nodes
_agent_instance: Optional[MockExamGradingAgent] = None


def set_agent_instance(agent: MockExamGradingAgent):
    """Set the agent instance for use in LangGraph nodes."""
    global _agent_instance
    _agent_instance = agent


def load_exam(state: MockExamState) -> Dict:
    """Node: Load and validate exam data."""
    request_id = state.get("request_id", "unknown")
    job_id = state.get("job_id", "unknown")

    logger.info(
        json.dumps(
            {
                "request_id": request_id,
                "job_id": job_id,
                "user_id": state.get("user_id"),
                "step": "load_exam",
                "message": "Loading exam data",
            }
        )
    )

    attempted_questions = state.get("attempted_questions", [])

    if not attempted_questions:
        raise ValueError("No attempted questions provided")

    return {
        "attempted_questions": attempted_questions,
    }


def grade_questions(state: MockExamState) -> Dict:
    """
    Node: Grade all questions in parallel.
    
    PARALLELIZATION: All question gradings execute concurrently using asyncio.gather()
    to maximize throughput. Concurrency is limited to respect worker limits and rate limits.
    
    Error Handling: Partial failures are handled gracefully - one question failure doesn't
    cancel others. Failed questions receive fallback grades.
    """
    request_id = state.get("request_id", "unknown")
    job_id = state.get("job_id", "unknown")
    user_id = state.get("user_id", "")
    subject = state.get("subject")

    logger.info(
        json.dumps(
            {
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "grade_questions",
                "message": "Grading questions (parallel execution)",
                "subject": subject or "Business Studies (default)",
            }
        )
    )

    if not _agent_instance:
        raise ValueError("Agent instance not set")

    attempted_questions = state.get("attempted_questions", [])
    
    if not attempted_questions:
        return {
            "question_grades": [],
            "concept_ids": [],
        }
    
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
    
    # Get concurrency limit from environment (respects worker limits)
    import os
    from dotenv import load_dotenv
    load_dotenv('config.env')
    
    # Limit concurrent gradings to respect worker concurrency and rate limits
    # Use WORKER_CONCURRENCY as base limit, but allow up to question count
    WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", 3))
    # For mock exams, we can grade more questions concurrently than worker jobs
    # because each question is independent. Use min(worker_concurrency * 2, question_count)
    # to allow more parallelism while respecting overall system limits
    max_concurrent_gradings = min(
        WORKER_CONCURRENCY * 2,  # Allow 2x worker concurrency for question gradings
        len(attempted_questions)  # Don't exceed question count
    )
    
    logger.info(
        json.dumps({
            "request_id": request_id,
            "job_id": job_id,
            "user_id": user_id,
            "step": "grade_questions",
            "message": f"Parallelizing {len(attempted_questions)} questions with max_concurrency={max_concurrent_gradings}",
            "worker_concurrency": WORKER_CONCURRENCY,
            "max_concurrent_gradings": max_concurrent_gradings,
        })
    )
    
    def _create_fallback_question_grade(question: Dict, error: str = None) -> QuestionGrade:
        """Create a fallback grade for failed question"""
        question_id = question.get("question_id", 0)
        question_number = question.get("question_number", question_id)
        part = question.get("part", "")
        marks = question.get("marks", 0)
        
        return QuestionGrade(
            question_id=question_id,
            question_number=question_number,
            part=part,
            question_text=question.get("question", ""),
            student_answer=question.get("user_answer", ""),
            model_answer=question.get("solution") or question.get("model_answer", ""),
            marks_allocated=marks,
            marks_awarded=0.0,
            percentage_score=0.0,
            feedback=(
                f"Grading error occurred for this question. "
                f"{'Error: ' + error if error else 'Please contact support.'}"
            ),
            strengths=["Answer submitted"],
            improvements=["Grading system error - question will be reviewed"],
            concept_ids=[],
        )
    
    # Create async wrapper for _grade_single_question
    async def grade_single_question_async(question: Dict, question_index: int) -> QuestionGrade:
        """
        Async wrapper for _grade_single_question.
        Runs synchronous grading in thread pool to enable parallel execution.
        """
        try:
            # Extract job_id and trace_id from state for instrumentation
            job_id = state.get("job_id")
            trace_id = state.get("request_id")  # Use request_id as trace_id
            
            # Run synchronous grading in thread pool
            loop = asyncio.get_event_loop()
            grade = await loop.run_in_executor(
                None,  # Use default executor (ThreadPoolExecutor)
                _agent_instance._grade_single_question,
                question,
                subject,
                job_id,
                trace_id
            )
            
            # Log successful grading
            logger.info(
                json.dumps({
                    "request_id": request_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "step": "grade_questions",
                    "question_index": question_index,
                    "question_id": question.get("question_id"),
                    "message": "Question graded successfully",
                    "marks_awarded": grade.marks_awarded,
                    "percentage": grade.percentage_score,
                })
            )
            
            return grade
            
        except Exception as e:
            # Log error but don't fail entire exam
            logger.error(
                json.dumps({
                    "request_id": request_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "step": "grade_questions",
                    "question_index": question_index,
                    "question_id": question.get("question_id"),
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "message": "Question grading failed, creating fallback grade",
                }),
                exc_info=True
            )
            
            # Create fallback grade so exam can continue
            return _create_fallback_question_grade(question, error=str(e))
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(max_concurrent_gradings)
    
    async def grade_with_concurrency_limit(question: Dict, index: int) -> QuestionGrade:
        """Grade question with concurrency limit"""
        async with semaphore:
            return await grade_single_question_async(question, index)
    
    # Grade all questions concurrently with concurrency limiting
    # Use asyncio.gather with return_exceptions=True to handle partial failures
    # Since this is a synchronous LangGraph node, we use asyncio.run() to execute async code
    try:
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
        
        # Instrument parallel grading execution
        with timed_operation(
            stage_name="grade_questions_parallel",
            stage_type=StageType.PIPELINE_NODE,
            job_id=job_id,
            additional_context={
                "user_id": user_id,
                "question_count": len(attempted_questions),
                "subject": subject or "Business Studies (default)",
                "max_concurrency": max_concurrent_gradings
            }
        ):
            # Create tasks for all questions
            async def grade_all_questions():
                grading_tasks = [
                    grade_with_concurrency_limit(q, i)
                    for i, q in enumerate(attempted_questions)
                ]
                
                # Execute all gradings concurrently
                return await asyncio.gather(*grading_tasks, return_exceptions=True)
            
            # Run async code in synchronous context
            # Since this is a synchronous LangGraph node, we can safely use asyncio.run()
            # to execute the async code. This creates a new event loop.
            # Note: LangGraph nodes are synchronous, so we're not in an async context here.
            results = asyncio.run(grade_all_questions())
        
        # Process results, handling exceptions
        question_grades: List[QuestionGrade] = []
        all_concept_ids: List[str] = []
        failed_count = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Exception occurred - create fallback grade
                failed_count += 1
                logger.error(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "grade_questions",
                        "question_index": i,
                        "question_id": attempted_questions[i].get("question_id"),
                        "error": str(result),
                        "error_type": type(result).__name__,
                        "message": "Question grading raised exception, using fallback",
                    }),
                    exc_info=True
                )
                fallback_grade = _create_fallback_question_grade(
                    attempted_questions[i],
                    error=str(result)
                )
                question_grades.append(fallback_grade)
                all_concept_ids.extend(fallback_grade.concept_ids)
            else:
                # Success
                question_grades.append(result)
                all_concept_ids.extend(result.concept_ids)
        
        # Log summary
        logger.info(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "grade_questions",
                "message": "Parallel grading completed",
                "total_questions": len(attempted_questions),
                "successful": len(question_grades) - failed_count,
                "failed": failed_count,
                "max_concurrency": max_concurrent_gradings,
            })
        )
        
        # Preserve question order (results from gather maintain order)
        # Verify order matches input
        assert len(question_grades) == len(attempted_questions), \
            "Question count mismatch after parallel grading"
        
        return {
            "question_grades": question_grades,
            "concept_ids": list(set(all_concept_ids)),  # Deduplicate
        }
        
    except Exception as e:
        # If parallel execution itself fails, fall back to sequential
        logger.error(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "grade_questions",
                "error": str(e),
                "error_type": type(e).__name__,
                "message": "Parallel execution failed, falling back to sequential",
            }),
            exc_info=True
        )
        
        # Fallback to sequential execution
        question_grades: List[QuestionGrade] = []
        all_concept_ids: List[str] = []
        
        for q in attempted_questions:
            try:
                grade = _agent_instance._grade_single_question(
                    q, subject=subject, job_id=None, trace_id=None
                )
                question_grades.append(grade)
                all_concept_ids.extend(grade.concept_ids)
            except Exception as q_error:
                logger.error(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "grade_questions",
                        "question_id": q.get("question_id"),
                        "error": str(q_error),
                        "message": "Sequential fallback also failed for question",
                    })
                )
                fallback_grade = _create_fallback_question_grade(q, error=str(q_error))
                question_grades.append(fallback_grade)
                all_concept_ids.extend(fallback_grade.concept_ids)
        
        return {
            "question_grades": question_grades,
            "concept_ids": list(set(all_concept_ids)),
        }


def aggregate_results(state: MockExamState) -> Dict:
    """Node: Aggregate results into ExamReport."""
    request_id = state.get("request_id", "unknown")
    job_id = state.get("job_id", "unknown")
    user_id = state.get("user_id", "")

    logger.info(
        json.dumps(
            {
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "aggregate_results",
                "message": "Aggregating results",
            }
        )
    )

    if not _agent_instance:
        raise ValueError("Agent instance not set")

    question_grades = state.get("question_grades", [])
    attempted_questions = state.get("attempted_questions", [])

    # Calculate total marks
    total_marks = sum(q.get("marks", 0) for q in attempted_questions)

    # Calculate overall scores
    marks_obtained = sum(g.marks_awarded for g in question_grades)
    percentage_score = (
        (marks_obtained / total_marks * 100) if total_marks > 0 else 0
    )

    # Get subject from state
    subject = state.get("subject")

    # Generate overall feedback
    overall_feedback = _agent_instance._generate_overall_feedback(
        question_grades, percentage_score, subject=subject
    )

    # Generate recommendations
    recommendations = _agent_instance._generate_recommendations(
        question_grades, percentage_score, subject=subject
    )

    # Generate strengths and weaknesses summary
    strengths, weaknesses = _agent_instance._generate_summaries(
        question_grades
    )

    # Determine overall grade
    overall_grade = _agent_instance._calculate_grade(percentage_score)

    exam_report = ExamReport(
        total_questions=len(attempted_questions),
        questions_attempted=len(attempted_questions),
        total_marks=total_marks,
        marks_obtained=marks_obtained,
        percentage_score=round(percentage_score, 2),
        overall_grade=overall_grade,
        question_grades=question_grades,
        overall_feedback=overall_feedback,
        recommendations=recommendations,
        strengths_summary=strengths,
        weaknesses_summary=weaknesses,
        readiness_score=None,  # Will be set in next node
    )

    return {
        "exam_report": exam_report,
    }


def compute_mastery_and_readiness(state: MockExamState) -> Dict:
    """Node: Compute mastery updates and readiness score."""
    request_id = state.get("request_id", "unknown")
    job_id = state.get("job_id", "unknown")
    user_id = state.get("user_id", "")
    subject = state.get("subject")  # Get subject from state for mastery updates

    logger.info(
        json.dumps(
            {
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "subject": subject or "Not specified",
                "step": "compute_mastery_and_readiness",
                "message": "Computing mastery and readiness",
            }
        )
    )

    if not _agent_instance:
        raise ValueError("Agent instance not set")

    exam_report = state.get("exam_report")
    question_grades = state.get("question_grades", [])

    if not exam_report:
        return {
            "mastery_updates": {},
            "readiness_score": None,
        }

    # Compute mastery updates per concept
    mastery_updates: Dict[str, float] = {}
    weakness_updates: List[Dict[str, str]] = []

    for grade in question_grades:
        # Calculate mastery score for this question based on percentage_score
        # Mastery = percentage_score (0-100 scale)
        question_mastery = max(0.0, min(100.0, grade.percentage_score))
        grade.mastery_score = question_mastery

        logger.info(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "compute_mastery_and_readiness",
                "question_number": grade.question_number,
                "percentage_score": grade.percentage_score,
                "mastery_score": question_mastery,
                "message": "Calculated mastery for question",
            })
        )

        for concept_id in grade.concept_ids:
            try:
                # Calculate mastery delta
                base_delta = _agent_instance.base_delta_from_question_score(
                    grade.percentage_score
                )
                new_mastery = _agent_instance.apply_mastery_update(
                    user_id, concept_id, base_delta, grade.marks_allocated
                )
                mastery_updates[concept_id] = new_mastery

                logger.info(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "compute_mastery_and_readiness",
                        "concept_id": concept_id,
                        "base_delta": base_delta,
                        "new_mastery": new_mastery,
                        "message": "Mastery update applied successfully",
                    })
                )
            except Exception as e:
                logger.error(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "compute_mastery_and_readiness",
                        "concept_id": concept_id,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "message": "Failed to apply mastery update",
                    }),
                    exc_info=True
                )
                # Continue with next concept even if one fails
                continue

            # Check for weaknesses
            try:
                weakness_level = _agent_instance.classify_weakness_level(
                    grade.percentage_score
                )
                if weakness_level:
                    weakness_updates.append(
                        {
                            "concept_id": concept_id,
                            "level": weakness_level,
                        }
                    )
            except Exception as e:
                logger.warning(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "compute_mastery_and_readiness",
                        "concept_id": concept_id,
                        "error": str(e),
                        "message": "Failed to classify weakness level",
                    })
                )

    # Update weaknesses in Supabase with retry
    if _agent_instance.supabase and weakness_updates:
        @retry_supabase_operation(max_retries=3, delay=1.0, backoff=2.0)
        def insert_weakness(weakness_data: Dict):
            return (
                _agent_instance.supabase.table("student_weaknesses")
                .insert(weakness_data)
                .execute()
            )

        @retry_supabase_operation(max_retries=3, delay=1.0, backoff=2.0)
        def update_weakness(
            weakness_data: Dict, user_id: str, concept_id: str
        ):
            return (
                _agent_instance.supabase.table("student_weaknesses")
                .update(weakness_data)
                .eq("user_id", user_id)
                .eq("concept_id", concept_id)
                .execute()
            )

        for weakness in weakness_updates:
            try:
                # Actual schema: severity (not level), id (BIGINT PK),
                # created_at
                # Map level to severity
                severity_map = {
                    "critical": "critical",
                    "high": "high",
                    "moderate": "moderate",
                    "low": "low",
                }
                severity = severity_map.get(
                    weakness["level"],
                    weakness["level"]
                )

                # Check if record exists using id
                existing = (
                    _agent_instance.supabase.table("student_weaknesses")
                    .select("id")
                    .eq("user_id", user_id)
                    .eq("concept_id", weakness["concept_id"])
                    .limit(1)
                    .execute()
                )

                if existing.data:
                    # Update existing
                    update_weakness(
                        {
                            "severity": severity,
                            "created_at": datetime.now().isoformat(),
                        },
                        user_id,
                        weakness["concept_id"],
                    )
                else:
                    # Insert new
                    insert_weakness(
                        {
                            "user_id": user_id,
                            "concept_id": weakness["concept_id"],
                            "severity": severity,
                            "created_at": datetime.now().isoformat(),
                        }
                    )
            except Exception as e:
                logger.warning(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "compute_mastery_and_readiness",
                        "error": f"Error updating weakness: {e}",
                        "concept_id": weakness.get("concept_id"),
                    })
                )

    # Invalidate readiness cache before computing new readiness score
    # Readiness is deterministic but invalidated when mastery changes
    if mastery_updates and user_id:
        concept_ids_for_invalidation = list(mastery_updates.keys())
        try:
            from services.deterministic_cache import (
                invalidate_cache, CacheOperation
            )
            invalidated = invalidate_cache(
                CacheOperation.READINESS_ASSESSMENT,
                user_id,
                concept_ids_for_invalidation
            )
            # Log invalidation at INFO level for production monitoring
            logger.info(
                json.dumps({
                    "request_id": request_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "step": "compute_mastery_and_readiness",
                    "message": "[CACHE INVALIDATE] readiness_assessment",
                    "concept_count": len(concept_ids_for_invalidation),
                    "success": invalidated
                })
            )
        except ImportError:
            # Fallback: invalidate legacy cache if centralized cache not available
            try:
                from cache import cache_delete, _hash_string
                concept_ids_sorted = sorted(concept_ids_for_invalidation)
                concept_ids_hash = _hash_string(":".join(concept_ids_sorted))
                cache_key = f"readiness:{user_id}:{concept_ids_hash}"
                cache_delete(cache_key)
                logger.info(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "compute_mastery_and_readiness",
                        "message": "[CACHE INVALIDATE] readiness_assessment (legacy)",
                        "concept_count": len(concept_ids_for_invalidation)
                    })
                )
            except Exception:
                pass

    # Compute readiness score
    readiness_score = _agent_instance.compute_readiness_score(
        user_id, exam_report, mastery_updates
    )

    # Calculate mastery from overall grade (not average from questions)
    # Mastery is based on the letter grade achieved
    if exam_report and exam_report.overall_grade:
        grade_based_mastery = _agent_instance._grade_to_mastery(
            exam_report.overall_grade
        )
        exam_report.average_mastery = grade_based_mastery

        logger.info(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "compute_mastery_and_readiness",
                "overall_grade": exam_report.overall_grade,
                "grade_based_mastery": grade_based_mastery,
                "message": "Calculated mastery from overall grade",
            })
        )

        # Store grade-based mastery in student_mastery table with subject-specific
        # concept_id (e.g., "economics_p2_mock_exam" for Economics P2)
        if (
            _agent_instance.supabase
            and exam_report
            and exam_report.overall_grade
        ):
            try:
                # Determine subject-specific concept_id
                # NOTE: Economics P1 is MCQ-based and not handled by this agent.
                # Only Economics P2 (written answers) uses this agent.
                # For Business Studies, use "business_studies_mock_exam" or default
                exam_type_from_state = state.get("exam_type")
                
                if subject and "economics" in str(subject).lower():
                    # Get exam_type from state, or detect from question structure
                    # NOTE: Economics P1 should never reach this code path as it's MCQ-based
                    if exam_type_from_state:
                        exam_type_used = exam_type_from_state.upper()
                    else:
                        # Fallback: detect from question structure
                        # Default to P2 since P1 is MCQ-based and not handled here
                        attempted_questions_check = state.get("attempted_questions", [])
                        exam_type_used = "P2"  # Default (P1 is MCQ-based, not handled)
                        if attempted_questions_check:
                            first_q = attempted_questions_check[0]
                            if first_q.get("part"):
                                exam_type_used = "P2"
                            # Note: P1 detection removed - Economics P1 is MCQ-based
                    default_concept_id = f"economics_{exam_type_used.lower()}_mock_exam"
                elif subject and "business" in str(subject).lower():
                    default_concept_id = "business_studies_mock_exam"
                else:
                    # Default to generic concept_id if subject unknown
                    default_concept_id = "mock_exam_average"
                
                grade_based_mastery = exam_report.average_mastery
                mastery_score_int = int(round(grade_based_mastery))

                # Check if row exists for this user + concept_id
                existing = (
                    _agent_instance.supabase.table("student_mastery")
                    .select("id, mastery_score")
                    .eq("user_id", user_id)
                    .eq("concept_id", default_concept_id)
                    .limit(1)
                    .execute()
                )

                # Prepare update/insert data with subject (if available)
                update_data = {
                    "mastery_score": mastery_score_int,
                    "updated_at": datetime.now().isoformat()
                }
                insert_data = {
                    "user_id": user_id,
                    "concept_id": default_concept_id,
                    "mastery_score": mastery_score_int,
                    "updated_at": datetime.now().isoformat()
                }
                
                # Add subject if available (with retry logic if column doesn't exist)
                if subject:
                    subject_clean = str(subject).strip()
                    update_data["subject"] = subject_clean
                    insert_data["subject"] = subject_clean

                if existing.data and len(existing.data) > 0:
                    # Update existing row (with retry if subject column missing)
                    try:
                        (
                            _agent_instance.supabase.table("student_mastery")
                            .update(update_data)
                            .eq("user_id", user_id)
                            .eq("concept_id", default_concept_id)
                            .execute()
                        )
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "compute_mastery_and_readiness",
                                "concept_id": default_concept_id,
                                "subject": subject,
                                "overall_grade": exam_report.overall_grade,
                                "grade_based_mastery": grade_based_mastery,
                                "message": (
                                    "Updated grade-based mastery in "
                                    "student_mastery with subject"
                                ),
                            })
                        )
                    except Exception as update_error:
                        # If subject column doesn't exist, retry without it
                        error_str = str(update_error).lower()
                        if "subject" in error_str or "42703" in error_str:
                            logger.warning(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "compute_mastery_and_readiness",
                                    "message": (
                                        "Subject column not found, retrying "
                                        "without subject"
                                    ),
                                })
                            )
                            update_data_no_subject = {k: v for k, v in update_data.items() if k != "subject"}
                            (
                                _agent_instance.supabase.table("student_mastery")
                                .update(update_data_no_subject)
                                .eq("user_id", user_id)
                                .eq("concept_id", default_concept_id)
                                .execute()
                            )
                            logger.info(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "compute_mastery_and_readiness",
                                    "concept_id": default_concept_id,
                                    "overall_grade": exam_report.overall_grade,
                                    "grade_based_mastery": grade_based_mastery,
                                    "message": (
                                        "Updated grade-based mastery in "
                                        "student_mastery without subject"
                                    ),
                                })
                            )
                        else:
                            raise  # Re-raise if it's a different error
                else:
                    # Insert new row (with retry if subject column missing)
                    try:
                        (
                            _agent_instance.supabase.table("student_mastery")
                            .insert(insert_data)
                            .execute()
                        )
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "compute_mastery_and_readiness",
                                "concept_id": default_concept_id,
                                "subject": subject,
                                "overall_grade": exam_report.overall_grade,
                                "grade_based_mastery": grade_based_mastery,
                                "message": (
                                    "Inserted grade-based mastery in "
                                    "student_mastery with subject"
                                ),
                            })
                        )
                    except Exception as insert_error:
                        # If subject column doesn't exist, retry without it
                        error_str = str(insert_error).lower()
                        if "subject" in error_str or "42703" in error_str:
                            logger.warning(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "compute_mastery_and_readiness",
                                    "message": (
                                        "Subject column not found, retrying "
                                        "without subject"
                                    ),
                                })
                            )
                            insert_data_no_subject = {k: v for k, v in insert_data.items() if k != "subject"}
                            (
                                _agent_instance.supabase.table("student_mastery")
                                .insert(insert_data_no_subject)
                                .execute()
                            )
                            logger.info(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "compute_mastery_and_readiness",
                                    "concept_id": default_concept_id,
                                    "overall_grade": exam_report.overall_grade,
                                    "grade_based_mastery": grade_based_mastery,
                                    "message": (
                                        "Inserted grade-based mastery in "
                                        "student_mastery without subject"
                                    ),
                                })
                            )
                        else:
                            raise  # Re-raise if it's a different error
            except Exception as e:
                logger.error(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "compute_mastery_and_readiness",
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "message": (
                            "Failed to store grade-based mastery in "
                            "student_mastery"
                        ),
                    }),
                    exc_info=True
                )

    # Update exam report with readiness score
    exam_report.readiness_score = readiness_score

    # Update mastery_states.mastery_macro with grade-based mastery
    # This is in addition to storing it in student_mastery table
    if exam_report and exam_report.overall_grade and _agent_instance.supabase:
        try:
            # Use the grade-based mastery (already calculated above)
            grade_based_mastery = exam_report.average_mastery

            # Check if row exists for this user
            existing_check = (
                _agent_instance.supabase.table("mastery_states")
                .select("user_id, mastery_macro")
                .eq("user_id", user_id)
                .limit(1)
                .execute()
            )

            if existing_check.data:
                # Update only mastery_macro column with grade-based mastery
                (
                    _agent_instance.supabase.table("mastery_states")
                    .update({
                        "mastery_macro": grade_based_mastery,
                        "updated_at": datetime.now().isoformat()
                    })
                    .eq("user_id", user_id)
                    .execute()
                )
                logger.info(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "compute_mastery_and_readiness",
                        "overall_grade": exam_report.overall_grade,
                        "mastery_macro": grade_based_mastery,
                        "source": "grade_based",
                        "message": (
                            "Updated mastery_states.mastery_macro with "
                            "grade-based mastery"
                        ),
                    })
                )
            else:
                # Insert new row with only mastery_macro filled
                (
                    _agent_instance.supabase.table("mastery_states")
                    .insert({
                        "user_id": user_id,
                        "mastery_concept": 0,
                        "mastery_micro": 0,
                        "mastery_macro": grade_based_mastery
                    })
                    .execute()
                )
                logger.info(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "compute_mastery_and_readiness",
                        "overall_grade": exam_report.overall_grade,
                        "mastery_macro": grade_based_mastery,
                        "source": "grade_based",
                        "message": (
                            "Inserted new mastery_states row with "
                            "grade-based mastery in mastery_macro"
                        ),
                    })
                )
        except Exception as e:
            logger.error(
                json.dumps({
                    "request_id": request_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "step": "compute_mastery_and_readiness",
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "message": (
                        "Failed to update mastery_states.mastery_macro with "
                        "grade-based mastery"
                    ),
                }),
                exc_info=True
            )

    return {
        "mastery_updates": mastery_updates,
        "readiness_score": readiness_score,
        "exam_report": exam_report,
    }


def _update_mastery_states_macro(agent_instance, user_id, mastery_score, subject, request_id, job_id, path_name):
    """Helper function to update mastery_states.mastery_macro with mastery_score value."""
    from services.supabase_ops import sb_execute
    
    logger.info(f"[{path_name}] [INFO] _update_mastery_states_macro called: user_id={user_id}, mastery_score={mastery_score}, subject={subject}")
    
    if not agent_instance or not agent_instance.supabase:
        logger.warning(f"[{path_name}] [WARN] Agent or supabase not available - skipping mastery_states update")
        return
    
    subject_to_use = str(subject).strip() if subject else "Business Studies"
    logger.info(f"[{path_name}] [INFO] Using subject: '{subject_to_use}'")
    
    try:
        import uuid
        uuid.UUID(user_id)  # Validate UUID
    except (ValueError, TypeError):
        logger.warning(f"[{path_name}] [WARN] Skipping mastery_states update - invalid user_id: {user_id}")
        return
    
    try:
        # Check if row exists
        existing = sb_execute(
            agent_instance.supabase.table("mastery_states")
            .select("user_id, mastery_micro, mastery_concept, mastery_macro, subject")
            .eq("user_id", user_id)
            .eq("subject", subject_to_use)
            .limit(1)
        )
        
        if existing.data and len(existing.data) > 0:
            # Update existing row
            update_data = {
                "mastery_macro": mastery_score,
                "subject": subject_to_use,
                "updated_at": datetime.now().isoformat()
            }
            try:
                sb_execute(
                    agent_instance.supabase.table("mastery_states")
                    .update(update_data)
                    .eq("user_id", user_id)
                    .eq("subject", subject_to_use)
                )
                logger.info(f"[{path_name}] [OK] Updated mastery_states.mastery_macro: {mastery_score} for user {user_id}, subject {subject_to_use}")
                print(f"[{path_name}] [OK] Updated mastery_states.mastery_macro: {mastery_score} for user {user_id}, subject {subject_to_use}")
            except Exception as e:
                logger.warning(f"[{path_name}] [WARN] Update failed, trying upsert: {e}")
                print(f"[{path_name}] [WARN] Update failed, trying upsert: {e}")
                # Try upsert fallback
                existing_row = existing.data[0]
                upsert_data = {
                    "user_id": user_id,
                    "mastery_macro": mastery_score,
                    "mastery_concept": existing_row.get("mastery_concept", 0),
                    "mastery_micro": existing_row.get("mastery_micro", 0),
                    "subject": subject_to_use,
                    "updated_at": datetime.now().isoformat()
                }
                try:
                    sb_execute(
                        agent_instance.supabase.table("mastery_states")
                        .upsert(upsert_data, on_conflict="user_id,subject")
                    )
                    logger.info(f"[{path_name}] [OK] Updated mastery_states.mastery_macro (upsert): {mastery_score}")
                    print(f"[{path_name}] [OK] Updated mastery_states.mastery_macro (upsert): {mastery_score}")
                except Exception as upsert_e:
                    logger.error(f"[{path_name}] [FAIL] Upsert also failed: {upsert_e}")
                    print(f"[{path_name}] [FAIL] Upsert also failed: {upsert_e}")
        else:
            # Insert new row
            insert_data = {
                "user_id": user_id,
                "mastery_macro": mastery_score,
                "mastery_concept": 0,
                "mastery_micro": 0,
                "subject": subject_to_use,
                "updated_at": datetime.now().isoformat()
            }
            try:
                sb_execute(
                    agent_instance.supabase.table("mastery_states")
                    .insert(insert_data)
                )
                logger.info(f"[{path_name}] [OK] Inserted mastery_states.mastery_macro: {mastery_score}")
                print(f"[{path_name}] [OK] Inserted mastery_states.mastery_macro: {mastery_score}")
            except Exception as e:
                logger.warning(f"[{path_name}] [WARN] Insert failed, trying upsert: {e}")
                print(f"[{path_name}] [WARN] Insert failed, trying upsert: {e}")
                # Try upsert fallback
                try:
                    sb_execute(
                        agent_instance.supabase.table("mastery_states")
                        .upsert(insert_data, on_conflict="user_id,subject")
                    )
                    logger.info(f"[{path_name}] [OK] Inserted mastery_states.mastery_macro (upsert): {mastery_score}")
                    print(f"[{path_name}] [OK] Inserted mastery_states.mastery_macro (upsert): {mastery_score}")
                except Exception as upsert_e:
                    logger.error(f"[{path_name}] [FAIL] Upsert also failed: {upsert_e}")
                    print(f"[{path_name}] [FAIL] Upsert also failed: {upsert_e}")
    except Exception as e:
        logger.warning(f"[{path_name}] [WARN] Failed to update mastery_states.mastery_macro: {e}")
        print(f"[{path_name}] [WARN] Failed to update mastery_states.mastery_macro: {e}")
        import traceback
        logger.error(f"[{path_name}] Traceback: {traceback.format_exc()}")
        print(f"[{path_name}] Traceback: {traceback.format_exc()}")


def persist_results(state: MockExamState) -> Dict:
    """Node: Persist results to Supabase with retry logic."""
    request_id = state.get("request_id", "unknown")
    job_id = state.get("job_id", "unknown")
    user_id = state.get("user_id", "")
    subject = state.get("subject")  # Get subject from state for mastery updates

    logger.info(
        json.dumps(
            {
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "persist_results",
                "message": "Persisting results to Supabase",
            }
        )
    )

    if not _agent_instance:
        logger.error(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "persist_results",
                "message": "Agent instance not set - cannot persist results",
            })
        )
        return {}
    
    if not _agent_instance.supabase:
        logger.warning(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "persist_results",
                "message": "Supabase not available - skipping persistence",
            })
        )
        return {}

    exam_report = state.get("exam_report")
    if not exam_report:
        logger.warning(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "persist_results",
                "message": "No exam report to persist",
                "state_keys": list(state.keys()),
            })
        )
        return {}
    
    # Log exam_report details for debugging
    overall_grade = getattr(exam_report, "overall_grade", None)
    average_mastery = getattr(exam_report, "average_mastery", None)
    
    logger.info(
        json.dumps({
            "request_id": request_id,
            "job_id": job_id,
            "user_id": user_id,
            "step": "persist_results",
            "overall_grade": overall_grade,
            "average_mastery": average_mastery,
            "subject": subject,
            "has_agent_instance": _agent_instance is not None,
            "has_supabase": _agent_instance.supabase is not None if _agent_instance else False,
            "message": "Checking conditions for mock_exam_mastery persistence",
        })
    )
    
    logger.info(
        json.dumps({
            "request_id": request_id,
            "job_id": job_id,
            "user_id": user_id,
            "step": "persist_results",
            "exam_report_type": type(exam_report).__name__,
            "has_overall_grade": hasattr(exam_report, 'overall_grade') if hasattr(exam_report, '__dict__') else 'overall_grade' in exam_report if isinstance(exam_report, dict) else False,
            "overall_grade": exam_report.overall_grade if hasattr(exam_report, 'overall_grade') else exam_report.get('overall_grade') if isinstance(exam_report, dict) else None,
            "has_average_mastery": hasattr(exam_report, 'average_mastery') if hasattr(exam_report, '__dict__') else 'average_mastery' in exam_report if isinstance(exam_report, dict) else False,
            "average_mastery": exam_report.average_mastery if hasattr(exam_report, 'average_mastery') else exam_report.get('average_mastery') if isinstance(exam_report, dict) else None,
            "message": "Exam report details",
        })
    )

    try:
        exam_id = str(uuid4())

        # Insert exam attempt with retry
        # Actual schema: exam_attempt_id (UUID PK), obtained_marks, percentage
        # user_id is UUID (must be valid UUID format)
        @retry_supabase_operation(max_retries=3, delay=1.0, backoff=2.0)
        def insert_exam_attempt():
            from services.supabase_ops import sb_execute
            # CRITICAL: Capture exam_id value at function definition time to avoid closure issues
            captured_exam_id = exam_id
            if not captured_exam_id:
                raise ValueError("exam_id is None - cannot insert exam attempt")
            insert_data = {
                        "exam_attempt_id": captured_exam_id,
                        "user_id": user_id,  # Must be valid UUID
                        "total_marks": exam_report.total_marks,
                        "obtained_marks": exam_report.marks_obtained,
                        "percentage": exam_report.percentage_score,
                        "overall_grade": exam_report.overall_grade,
                        "readiness_score": (
                            exam_report.readiness_score
                            if exam_report.readiness_score is not None
                            else None
                        ),
                        "created_at": datetime.now().isoformat(),
                    }
            print(f"\n[INSERT-EXAM-ATTEMPT] Attempting insert with data keys: {list(insert_data.keys())}")
            print(f"[INSERT-EXAM-ATTEMPT] user_id: {user_id}, exam_id: {captured_exam_id}")
            # Use sb_execute for proper error handling and retries
            result = sb_execute(
                _agent_instance.supabase.table("exam_attempts")
                .insert(insert_data)
            )
            # Return the exam_attempt_id
            if result.data and len(result.data) > 0:
                return result.data[0].get("exam_attempt_id", captured_exam_id)
            return captured_exam_id

        # Try to insert exam attempt, but don't fail if it doesn't work
        # (RLS permissions might prevent it, but we still want to save mastery)
        # CRITICAL: exam_id is already generated above - don't overwrite it with None
        inserted_exam_id = None
        print(f"\n[PERSIST-RESULTS] [INFO] Attempting to insert exam_attempts for user {user_id}, exam_id: {exam_id}")
        try:
            inserted_exam_id = insert_exam_attempt()
            # Use the returned ID (should be same as generated, but use returned value to be safe)
            if inserted_exam_id:
                exam_id = inserted_exam_id
            print(f"[PERSIST-RESULTS] [OK] Successfully inserted exam_attempts, exam_id: {exam_id}")
            logger.info(
                json.dumps({
                    "request_id": request_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "step": "persist_results",
                    "exam_id": exam_id,
                    "message": "Successfully inserted exam attempt",
                })
            )
        except Exception as exam_attempt_error:
            print(f"\n[PERSIST-RESULTS] [WARN] Failed to insert exam_attempts: {exam_attempt_error}")
            print(f"[PERSIST-RESULTS] [WARN] Continuing with mastery persistence anyway...\n")
            logger.warning(
                json.dumps({
                    "request_id": request_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "step": "persist_results",
                    "error": str(exam_attempt_error),
                    "error_type": type(exam_attempt_error).__name__,
                    "message": "Failed to insert exam_attempts (continuing with mastery persistence)",
                })
            )
            # Continue execution - mastery persistence is more important
            exam_id = None

        # Insert question results in batches (if many questions)
        # Actual schema: exam_attempt_id (not exam_id),
        # percentage (not percentage_score), concepts (not concept_ids),
        # user_id (UUID), and many additional fields
        question_results: List[Dict] = []  # Initialize outside if block
        if exam_id and exam_report.question_grades:
            for grade in exam_report.question_grades:
                # question_id is UUID in schema, but we have integer IDs
                # Since question_id is nullable, set to None if not valid UUID
                question_id_value = None
                if grade.question_id:
                    # Try to convert to UUID if it's already a UUID string
                    try:
                        from uuid import UUID
                        # If it's already a valid UUID string, use it
                        UUID(str(grade.question_id))
                        question_id_value = str(grade.question_id)
                    except (ValueError, AttributeError):
                        # If it's an integer or invalid UUID, set to None
                        # (question_id is nullable in schema)
                        question_id_value = None

                question_results.append(
                    {
                        "exam_attempt_id": exam_id,
                        "user_id": user_id,  # UUID format
                        "question_id": question_id_value,  # UUID or None
                        "question_number": grade.question_number,
                        "part": grade.part,
                        "question_text": grade.question_text,
                        "student_answer": grade.student_answer,
                        "model_answer": grade.model_answer,
                        "marks_allocated": grade.marks_allocated,
                        "marks_awarded": grade.marks_awarded,
                        "percentage": grade.percentage_score,
                        "feedback": grade.feedback,
                        "strengths": grade.strengths,  # Array field
                        "improvements": grade.improvements,  # Array field
                        # Array field (not concept_ids)
                        "concepts": grade.concept_ids,
                        "created_at": datetime.now().isoformat(),
                    }
                )

            if question_results:
                # SAFE BATCH INSERTS: Insert in small chunks (10-25 rows) with await between chunks
                # This prevents overwhelming Supabase with large batch inserts
                # Feature flag: Can disable mock-exam grading if DB is degraded
                ENABLE_MOCK_EXAM_GRADING = os.getenv("ENABLE_MOCK_EXAM_GRADING", "true").lower() == "true"
                
                if not ENABLE_MOCK_EXAM_GRADING:
                    logger.warning(
                        json.dumps({
                            "request_id": request_id,
                            "job_id": job_id,
                            "user_id": user_id,
                            "step": "persist_results",
                            "message": "Mock exam grading disabled via feature flag",
                            "question_results_count": len(question_results)
                        })
                    )
                    # Skip inserts but don't fail the job
                    return {}
                
                @retry_supabase_operation(
                    max_retries=3, delay=1.0, backoff=2.0
                )
                def insert_question_results_safe():
                    # SAFE BATCH SIZE: 10-25 rows per chunk (configurable)
                    batch_size = int(os.getenv("MOCK_EXAM_BATCH_INSERT_SIZE", "15"))  # Default 15
                    batch_size = max(10, min(25, batch_size))  # Clamp between 10-25
                    
                    # Use sb_execute for concurrency limiting and observability
                    from services.supabase_ops import sb_execute
                    
                    # Insert in chunks with delay between chunks
                    for i in range(0, len(question_results), batch_size):
                        batch = question_results[i:i + batch_size]
                        
                        result = sb_execute(
                            _agent_instance.supabase.table("exam_question_results")
                            .insert(batch)
                        )
                        
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "persist_results",
                                "batch_index": i // batch_size + 1,
                                "batch_size": len(batch),
                                "total_batches": (
                                    (len(question_results) + batch_size - 1) //
                                    batch_size
                                ),
                                "message": (
                                    f"Inserted batch {i // batch_size + 1} "
                                    f"of question results"
                                )
                            })
                        )
                        
                        # DELAY BETWEEN CHUNKS: Sleep to prevent overwhelming DB
                        if i + batch_size < len(question_results):  # Not last batch
                            delay_seconds = float(
                                os.getenv("MOCK_EXAM_BATCH_INSERT_DELAY", "0.5")
                            )  # Default 0.5s
                            time.sleep(delay_seconds)

                insert_question_results_safe()

        # Upsert readiness with retry
        # Actual schema: id (UUID PK), user_id (UUID), readiness_score
        # Try to get readiness_score from state or exam_report
        readiness_score = state.get("readiness_score")
        if readiness_score is None and exam_report:
            if isinstance(exam_report, dict):
                readiness_score = exam_report.get('readiness_score')
            else:
                readiness_score = getattr(exam_report, 'readiness_score', None)

        if readiness_score is not None:
            @retry_supabase_operation(max_retries=3, delay=1.0, backoff=2.0)
            def upsert_readiness():
                # Check if record exists using id (UUID PK)
                existing = (
                    _agent_instance.supabase.table("student_readiness")
                    .select("id")
                    .eq("user_id", user_id)  # user_id is UUID
                    .limit(1)
                    .execute()
                )

                if existing.data:
                    # Update existing record
                    return (
                        _agent_instance.supabase.table("student_readiness")
                        .update(
                            {
                                "readiness_score": readiness_score,
                                "updated_at": datetime.now().isoformat(),
                            }
                        )
                        .eq("user_id", user_id)
                        .execute()
                    )
                else:
                    # Insert new record
                    return (
                        _agent_instance.supabase.table("student_readiness")
                        .insert(
                            {
                                "user_id": user_id,  # UUID format
                                "readiness_score": readiness_score,
                                "updated_at": datetime.now().isoformat(),
                            }
                        )
                        .execute()
                    )

            try:
                upsert_readiness()
            except Exception as readiness_error:
                logger.warning(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "persist_results",
                        "error": str(readiness_error),
                        "error_type": type(readiness_error).__name__,
                        "message": "Failed to upsert readiness (continuing with mastery persistence)",
                    })
                )
                print(f"[PERSIST-RESULTS] [WARN] Failed to upsert readiness: {readiness_error}")

        print(f"\n[PERSIST-RESULTS] [OK] Reached mastery persistence section for user {user_id}")
        logger.info(f"[PERSIST-RESULTS] Reached mastery persistence section for user {user_id}")

        # Store grade-based mastery from exam report directly
        # This ensures every mock exam completion stores grade-based mastery
        # Handle both dict and object types for exam_report
        overall_grade = None
        average_mastery = None
        
        if isinstance(exam_report, dict):
            overall_grade = exam_report.get('overall_grade')
            average_mastery = exam_report.get('average_mastery')
        else:
            overall_grade = getattr(exam_report, 'overall_grade', None)
            average_mastery = getattr(exam_report, 'average_mastery', None)
        
        # Calculate average_mastery if not already set
        if overall_grade and average_mastery is None:
            try:
                calculated_mastery = _agent_instance._grade_to_mastery(overall_grade)
                average_mastery = calculated_mastery
                
                # Update exam_report object/dict
                if isinstance(exam_report, dict):
                    exam_report['average_mastery'] = calculated_mastery
                else:
                    exam_report.average_mastery = calculated_mastery
                
                logger.info(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "persist_results",
                        "overall_grade": overall_grade,
                        "calculated_average_mastery": calculated_mastery,
                        "message": "Calculated average_mastery from overall_grade",
                    })
                )
            except Exception as e:
                logger.error(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "persist_results",
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "message": "Failed to calculate average_mastery",
                    }),
                    exc_info=True
                )
        
        # Log the condition check for debugging
        print(f"\n[PERSIST-RESULTS] [INFO] Checking mastery persistence conditions:")
        print(f"  - overall_grade: {overall_grade}")
        print(f"  - average_mastery: {average_mastery}")
        print(f"  - has_agent_instance: {_agent_instance is not None}")
        print(f"  - has_supabase: {_agent_instance.supabase is not None if _agent_instance else False}")
        condition_met = bool(
            overall_grade
            and average_mastery is not None
            and _agent_instance
            and _agent_instance.supabase
        )
        print(f"  - condition_met: {condition_met}\n")
        
        logger.info(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "step": "persist_results",
                "overall_grade": overall_grade,
                "average_mastery": average_mastery,
                "has_agent_instance": _agent_instance is not None,
                "has_supabase": _agent_instance.supabase is not None if _agent_instance else False,
                "condition_met": condition_met,
                "message": "Checking conditions for mock_exam_mastery persistence",
            })
        )
        
        # Ensure we have a mastery value - use calculated_mastery if average_mastery is None
        if average_mastery is None and overall_grade:
            try:
                calculated_mastery = _agent_instance._grade_to_mastery(overall_grade) if _agent_instance else None
                if calculated_mastery is not None:
                    average_mastery = calculated_mastery
                    logger.info(
                        json.dumps({
                            "request_id": request_id,
                            "job_id": job_id,
                            "user_id": user_id,
                            "step": "persist_results",
                            "overall_grade": overall_grade,
                            "calculated_mastery": calculated_mastery,
                            "message": "Using calculated mastery as fallback",
                        })
                    )
            except Exception as e:
                logger.warning(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "persist_results",
                        "error": str(e),
                        "message": "Failed to calculate mastery fallback",
            })
        )
        
        if (
            overall_grade
            and average_mastery is not None
            and _agent_instance
            and _agent_instance.supabase
        ):
            try:
                # Get mock exam name from state
                mock_exam_name = state.get("mock_exam_name", "Unknown Mock Exam")
                exam_type = state.get("exam_type", "P1")
                grade_based_mastery = average_mastery
                
                logger.info(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "persist_results",
                        "mock_exam_name": mock_exam_name,
                        "grade_based_mastery": grade_based_mastery,
                        "overall_grade": overall_grade,
                        "subject": subject,
                        "exam_type": exam_type,
                        "message": "Starting mastery persistence to mock_exam_mastery table",
                    })
                )

                # Check if row exists for this user + mock_exam_name + exam_type + subject
                # CRITICAL: Include subject in all queries to ensure strict subject-based separation
                from services.supabase_ops import sb_execute
                subject_clean = str(subject).strip() if subject else "Unknown"
                
                # For History mock exams, use "Pak Studies History" as subject name
                if subject_clean.lower() in ["history", "pak studies history", "pak studies history p1", "pak studies history p2"]:
                    subject_clean = "Pak Studies History"
                
                # For Geography mock exams, use "Pak Studies Geography" as subject name
                if subject_clean.lower() in ["geography", "pak studies geography", "pak studies geography p1", "pak studies geography p2"]:
                    subject_clean = "Pak Studies Geography"
                
                existing_mock_mastery = sb_execute(
                    _agent_instance.supabase.table("mock_exam_mastery")
                    .select("id, mastery_score, subject")
                    .eq("user_id", user_id)
                    .eq("mock_exam_name", mock_exam_name)
                    .eq("exam_type", exam_type)
                    .eq("subject", subject_clean)
                    .limit(1)
                )

                # Prepare update/insert data for mock_exam_mastery table
                update_data = {
                            "mastery_score": int(round(grade_based_mastery)),
                            "subject": subject_clean,  # Ensure subject is always updated
                            "updated_at": datetime.now().isoformat()
                }
                insert_data = {
                    "user_id": user_id,
                    "mock_exam_name": mock_exam_name,
                    "mastery_score": int(round(grade_based_mastery)),
                    "subject": subject_clean,
                    "exam_type": exam_type,
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }

                if existing_mock_mastery.data and len(existing_mock_mastery.data) > 0:
                    # Update existing row (strictly matching user + mock_exam_name + exam_type + subject)
                    try:
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "persist_results",
                                "action": "updating_existing_mock_mastery",
                                "update_data": update_data,
                                "subject": subject_clean,
                                "message": "Updating existing mock exam mastery record (strictly by subject)",
                            })
                        )
                        result = sb_execute(
                            _agent_instance.supabase.table("mock_exam_mastery")
                            .update(update_data)
                            .eq("user_id", user_id)
                            .eq("mock_exam_name", mock_exam_name)
                            .eq("exam_type", exam_type)
                            .eq("subject", subject_clean)  # CRITICAL: Include subject in WHERE clause
                        )
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "persist_results",
                                "mock_exam_name": mock_exam_name,
                                "subject": subject,
                                "exam_type": exam_type,
                                "overall_grade": overall_grade,
                                "grade_based_mastery": grade_based_mastery,
                                "mastery_score": int(round(grade_based_mastery)),
                                "message": (
                                    "Updated mock exam mastery in "
                                    "mock_exam_mastery table"
                                ),
                            })
                        )
                        
                        # Immediately update mastery_states.mastery_macro with the same mastery_score value
                        # Use the mastery_score that was just saved to mock_exam_mastery
                        mastery_score_value = int(round(grade_based_mastery))
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "persist_results",
                                "action": "calling_update_mastery_states_macro",
                                "mastery_score_value": mastery_score_value,
                                "subject": subject_clean,
                                "path": "UPDATE path",
                                "message": "About to call _update_mastery_states_macro",
                            })
                        )
                        # CRITICAL: Update mastery_states.mastery_macro immediately after mock_exam_mastery update
                        # Use subject_clean (which has "Pak Studies History" for history mocks)
                        try:
                            _update_mastery_states_macro(
                                _agent_instance, user_id, mastery_score_value, subject_clean, 
                                request_id, job_id, "UPDATE path"
                            )
                            logger.info(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "persist_results",
                                    "message": "Successfully called _update_mastery_states_macro after UPDATE",
                                })
                            )
                        except Exception as macro_update_error:
                            logger.error(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "persist_results",
                                    "error": str(macro_update_error),
                                    "error_type": type(macro_update_error).__name__,
                                    "message": "CRITICAL: Failed to update mastery_states.mastery_macro after mock_exam_mastery UPDATE",
                                }),
                                exc_info=True
                            )
                            print(f"[CRITICAL] Failed to update mastery_states.mastery_macro: {macro_update_error}", flush=True)
                            import traceback
                            print(f"[CRITICAL] Traceback: {traceback.format_exc()}", flush=True)
                    except Exception as update_error:
                        logger.error(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "persist_results",
                                "error": str(update_error),
                                "error_type": type(update_error).__name__,
                                "message": "Error updating mock exam mastery",
                            }),
                            exc_info=True
                        )
                        raise
                else:
                    # Insert new row (strictly by subject)
                    # NOTE: Database constraint is (user_id, mock_exam_name, exam_type) - does NOT include subject
                    # If a record exists with same (user_id, mock_exam_name, exam_type) but different subject, insert will fail
                    # To allow separate entries per subject, the database constraint needs to be updated to include subject
                    try:
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                            "user_id": user_id,
                                "step": "persist_results",
                                "action": "inserting_new_mock_mastery",
                                "insert_data": insert_data,
                                "subject": subject_clean,
                                "message": "Inserting new mock exam mastery record (strictly by subject)",
                            })
                        )
                        result = sb_execute(
                            _agent_instance.supabase.table("mock_exam_mastery")
                            .insert(insert_data)
                        )
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "persist_results",
                                "mock_exam_name": mock_exam_name,
                                "subject": subject,
                                "exam_type": exam_type,
                                "overall_grade": overall_grade,
                                "grade_based_mastery": grade_based_mastery,
                                "mastery_score": int(round(grade_based_mastery)),
                                "message": (
                                    "Inserted mock exam mastery in "
                                    "mock_exam_mastery table (strictly by subject)"
                                ),
                            })
                        )
                        
                        # Immediately update mastery_states.mastery_macro with the same mastery_score value
                        # Use the mastery_score that was just saved to mock_exam_mastery
                        mastery_score_value = int(round(grade_based_mastery))
                        logger.info(
                            json.dumps({
                                "request_id": request_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "step": "persist_results",
                                "action": "calling_update_mastery_states_macro",
                                "mastery_score_value": mastery_score_value,
                                "subject": subject_clean,
                                "path": "INSERT path",
                                "message": "About to call _update_mastery_states_macro",
                            })
                        )
                        # CRITICAL: Update mastery_states.mastery_macro immediately after mock_exam_mastery insert
                        # Use subject_clean (which has "Pak Studies History" for history mocks)
                        try:
                            _update_mastery_states_macro(
                                _agent_instance, user_id, mastery_score_value, subject_clean, 
                                request_id, job_id, "INSERT path"
                            )
                            logger.info(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "persist_results",
                                    "message": "Successfully called _update_mastery_states_macro after INSERT",
                                })
                            )
                        except Exception as macro_update_error:
                            logger.error(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "persist_results",
                                    "error": str(macro_update_error),
                                    "error_type": type(macro_update_error).__name__,
                                    "message": "CRITICAL: Failed to update mastery_states.mastery_macro after mock_exam_mastery INSERT",
                                }),
                                exc_info=True
                            )
                            print(f"[CRITICAL] Failed to update mastery_states.mastery_macro: {macro_update_error}", flush=True)
                            import traceback
                            print(f"[CRITICAL] Traceback: {traceback.format_exc()}", flush=True)
                    except Exception as insert_error:
                        error_str = str(insert_error).lower()
                        error_code = str(insert_error) if hasattr(insert_error, 'code') else str(insert_error)
                        
                        # Handle duplicate key error gracefully (record already exists)
                        # Don't raise - just log and continue (don't block mastery_states update)
                        if "duplicate key" in error_str or "23505" in error_code:
                            logger.warning(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "persist_results",
                                    "subject": subject_clean,
                                    "mock_exam_name": mock_exam_name,
                                    "exam_type": exam_type,
                                    "message": (
                                        "Record already exists with same (user_id, mock_exam_name, exam_type). "
                                        "Skipping insert. Note: Database constraint does not include subject, "
                                        "so separate entries per subject require constraint update."
                                    ),
                                })
                            )
                            print(f"[PERSIST-RESULTS] [WARN] Duplicate key - record already exists. Skipping insert.", flush=True)
                            # Don't raise - just skip this insert and continue with mastery_states update
                        # Handle missing subject column
                        elif "subject" in error_str or "42703" in error_code:
                            logger.warning(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "persist_results",
                                    "message": (
                                        "Subject column not found, retrying "
                                        "without subject"
                                    ),
                                })
                            )
                            # This code path should not be reached for mock_exam_mastery table
                            # as subject is required. If it fails, log and continue.
                            print(f"[PERSIST-RESULTS] [WARN] Subject column issue: {insert_error}", flush=True)
                        else:
                            logger.error(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "persist_results",
                                    "error": str(insert_error),
                                    "error_type": type(insert_error).__name__,
                                    "message": "Error inserting mock exam mastery",
                                }),
                                exc_info=True
                            )
                            # Don't raise - log error but continue (don't block mastery_states update)
                            print(f"[PERSIST-RESULTS] [ERROR] Insert failed: {insert_error}", flush=True)
            except Exception as e:
                logger.warning(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "persist_results",
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "message": (
                            "Failed to store grade-based mastery in "
                            "student_mastery"
                        ),
                    }),
                    exc_info=True
                )

        # Store mastery tracking entry for this mock exam
        # This ensures every mock exam completion has mastery data recorded
        mastery_updates = state.get("mastery_updates", {})
        if mastery_updates and _agent_instance.supabase:
            try:
                # Get all concept IDs from mastery updates
                concept_ids = list(mastery_updates.keys())

                if concept_ids:
                    # Fetch current mastery values from student_mastery table
                    concept_ids_int = [
                        int(cid) for cid in concept_ids
                        if cid and str(cid).strip() and str(cid) != "None"
                    ]

                    if concept_ids_int:
                        @retry_supabase_operation(
                            max_retries=3, delay=1.0, backoff=2.0
                        )
                        def fetch_mastery_func():
                            return (
                                _agent_instance.supabase.table(
                                    "student_mastery"
                                )
                                .select("concept_id, mastery_score")
                                .eq("user_id", user_id)
                                .in_("concept_id", concept_ids_int)
                                .execute()
                            )

                        mastery_result = fetch_mastery_func()

                        mastery_map = {}
                        if mastery_result and mastery_result.get("data"):
                            mastery_data = mastery_result["data"]
                            # Build mastery mapping: concept_id -> score
                            mastery_map = {
                                row["concept_id"]: row.get(
                                    "mastery_score", 50
                                )
                                for row in mastery_data
                            }

                        # Fetch mastery_states for aggregate mastery
                        @retry_supabase_operation(
                            max_retries=3, delay=1.0, backoff=2.0
                        )
                        def fetch_mastery_states_func():
                            return (
                                _agent_instance.supabase.table(
                                    "mastery_states"
                                )
                                .select(
                                    (
                                        "mastery_concept, mastery_micro, "
                                        "mastery_macro"
                                    )
                                )
                                .eq("user_id", user_id)
                                .limit(1)
                                .execute()
                            )

                        mastery_states_result = fetch_mastery_states_func()

                        mastery_states = {}
                        if (
                            mastery_states_result
                            and mastery_states_result.get("data")
                        ):
                            states_row = mastery_states_result["data"][0]
                            mastery_states = {
                                "mastery_concept": states_row.get(
                                    "mastery_concept", 0
                                ),
                                "mastery_micro": states_row.get(
                                    "mastery_micro", 0
                                ),
                                "mastery_macro": states_row.get(
                                    "mastery_macro", 0
                                ),
                            }

                        # Use grade-based mastery from exam report
                        # (not average from updates)
                        if exam_report and exam_report.overall_grade:
                            grade_based_mastery = (
                                _agent_instance._grade_to_mastery(
                                    exam_report.overall_grade
                                )
                            )
                            avg_mastery = grade_based_mastery
                        else:
                            avg_mastery = 50.0  # Default baseline (F grade)

                        # Get readiness score
                        readiness_score = state.get("readiness_score")
                        if readiness_score is None:
                            readiness_score = (
                                exam_report.readiness_score
                                if exam_report
                                else None
                            )

                        # Store mastery tracking entry
                        mastery_entry = {
                            "user_id": user_id,
                            "conversation_id": f"mock_exam_{exam_id}",
                            "topic": None,  # Mock exams may not have a topic
                            "concept_masteries": mastery_map,
                            "average_mastery": avg_mastery,
                            "mastery_states": mastery_states,
                            "mastery_updates_applied": mastery_updates,
                            "reasoning_label": "mock_exam",
                            "exam_attempt_id": exam_id,
                            "exam_score": (
                                exam_report.percentage_score
                                if exam_report
                                else None
                            ),
                            "readiness_score": readiness_score,
                            "timestamp": datetime.now().isoformat(),
                        }

                        @retry_supabase_operation(
                            max_retries=3, delay=1.0, backoff=2.0
                        )
                        def insert_mastery_tracking_func():
                            # Store in mock_exam_mastery_tracking table
                            # or tutor_mastery_tracking if shared
                            return (
                                _agent_instance.supabase.table(
                                    "mock_exam_mastery_tracking"
                                )
                                .insert(mastery_entry)
                                .execute()
                            )

                        try:
                            insert_mastery_tracking_func()
                            logger.info(
                                json.dumps({
                                    "request_id": request_id,
                                    "job_id": job_id,
                                    "user_id": user_id,
                                    "step": "persist_results",
                                    "message": (
                                        "Mastery tracking entry created for "
                                        "mock exam"
                                    ),
                                    "avg_mastery": avg_mastery,
                                    "concepts_count": len(concept_ids),
                                })
                            )
                        except Exception:
                            # Try alternate table name if first fails
                            try:
                                (
                                    _agent_instance.supabase.table(
                                        "tutor_mastery_tracking"
                                    )
                                    .insert(mastery_entry)
                                    .execute()
                                )
                                logger.info(
                                    json.dumps({
                                        "request_id": request_id,
                                        "job_id": job_id,
                                        "user_id": user_id,
                                        "step": "persist_results",
                                        "message": (
                                            "Mastery tracking entry created "
                                            "(using tutor_mastery_tracking "
                                            "table)"
                                        ),
                                    })
                                )
                            except Exception as e2:
                                # Log mastery data even if table doesn't exist
                                logger.warning(
                                    json.dumps({
                                        "request_id": request_id,
                                        "job_id": job_id,
                                        "user_id": user_id,
                                        "step": "persist_results",
                                        "error": str(e2),
                                        "message": (
                                            "Could not store mastery tracking "
                                            "(table may not exist). "
                                            "Mastery data logged."
                                        ),
                                        "mastery_data": {
                                            "avg_mastery": avg_mastery,
                                            "concepts": list(concept_ids),
                                            "mastery_states": mastery_states,
                                        },
                                    })
                                )
                                # Still log the mastery data for debugging
                                logger.info(
                                    f"[MASTERY_TRACKING] Mock Exam - "
                                    f"User: {user_id}, "
                                    f"Avg Mastery: {avg_mastery:.2f}, "
                                    f"Concepts: {concept_ids}, "
                                    f"States: {mastery_states}"
                                )

            except Exception as e:
                logger.warning(
                    json.dumps({
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "step": "persist_results",
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "message": (
                            "Failed to create mastery tracking entry for "
                            "mock exam"
                        ),
                    }),
                    exc_info=True
                )
                # Continue execution even if mastery tracking fails

        logger.info(
            json.dumps(
                {
                    "request_id": request_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "step": "persist_results",
                    "message": "Results persisted successfully",
                    "exam_attempt_id": exam_id,
                    "questions_count": len(question_results),
                }
            )
        )

    except Exception as e:
        logger.error(
            json.dumps(
                {
                    "request_id": request_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "step": "persist_results",
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "message": (
                        "CRITICAL: Persistence failed - tables will not be "
                        "updated!"
                    ),
                }
            ),
            exc_info=True,
        )
        # Log to console as well for visibility
        print(
            f"\n[ERROR] Persistence failed for user {user_id}: {e}\n"
        )
        # Don't raise - allow workflow to complete even if persistence fails
        # But log the error prominently

    return {}


async def run_mock_exam_graph(
    agent: MockExamGradingAgent,
    user_id: str,
    attempted_questions: List[Dict],
    request_id: Optional[str] = None,
    job_id: Optional[str] = None,
    subject: Optional[str] = None,
    exam_type: Optional[str] = None,
) -> ExamReport:
    """
    Run the LangGraph workflow for mock exam grading.

    Args:
        agent: MockExamGradingAgent instance
        user_id: User ID
        attempted_questions: List of attempted questions
        request_id: Optional request ID for tracing
        job_id: Optional job ID for tracing
        subject: Optional subject name (e.g., "Economics", "Business Studies")

    Returns:
        ExamReport
    """
    if not LANGGRAPH_AVAILABLE:
        # Fallback to synchronous grade_exam if LangGraph not available
        logger.warning(
            "LangGraph not available - using synchronous grade_exam"
        )
        return agent.grade_exam(attempted_questions, subject=subject)

    # Set agent instance for nodes
    set_agent_instance(agent)

    # Initial state
    initial_state: MockExamState = {
        "user_id": user_id,
        "attempted_questions": attempted_questions,
        "question_grades": [],
        "exam_report": None,
        "mastery_updates": {},
        "readiness_score": None,
        "concept_ids": [],
        "request_id": request_id,
        "job_id": job_id,
        "subject": subject,
        "exam_type": exam_type,
    }

    logger.info(
        json.dumps({
            "request_id": request_id,
            "job_id": job_id,
            "user_id": user_id,
            "message": "Starting mock exam grading workflow",
            "questions_count": len(attempted_questions),
        })
    )

    # Build graph
    graph = StateGraph(MockExamState)
    graph.add_node("load_exam", load_exam)
    # grade_questions is now async-compatible (uses asyncio internally)
    graph.add_node("grade_questions", grade_questions)
    graph.add_node("aggregate_results", aggregate_results)
    graph.add_node(
        "compute_mastery_and_readiness", compute_mastery_and_readiness
    )
    graph.add_node("persist_results", persist_results)

    graph.set_entry_point("load_exam")
    graph.add_edge("load_exam", "grade_questions")
    graph.add_edge("grade_questions", "aggregate_results")
    graph.add_edge("aggregate_results", "compute_mastery_and_readiness")
    graph.add_edge("compute_mastery_and_readiness", "persist_results")
    graph.add_edge("persist_results", END)

    # Compile and run
    try:
        app = graph.compile()
        # Run graph synchronously (grade_questions handles async internally via asyncio.run)
        final_state = app.invoke(initial_state)

        logger.info(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "message": "Workflow completed successfully",
                "has_exam_report": final_state.get("exam_report") is not None,
                "has_mastery_updates": bool(
                    final_state.get("mastery_updates", {})
                ),
                "readiness_score": final_state.get("readiness_score"),
            })
        )

        exam_report = final_state.get("exam_report")
        if not exam_report:
            raise ValueError("Exam report not generated")

        return exam_report
    except Exception as e:
        logger.error(
            json.dumps({
                "request_id": request_id,
                "job_id": job_id,
                "user_id": user_id,
                "error": str(e),
                "error_type": type(e).__name__,
                "message": "Workflow failed",
            }),
            exc_info=True,
        )
        raise


# ============================================================================
# CLI Test Entry
# ============================================================================

def main():
    """Local example usage."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("[FAIL] OPENAI_API_KEY not found")
        return

    agent = MockExamGradingAgent(api_key)

    attempted_questions = [
        {
            "question_id": 1,
            "question": "Explain the concept of market segmentation.",
            "user_answer": (
                "Market segmentation is dividing customers into groups"
            ),
            "solution": (
                "Market segmentation is the process of dividing a market "
                "into groups of customers with similar needs and "
                "characteristics..."
            ),
            "marks": 10,
            "part": "A",
        }
    ]

    report = agent.grade_exam(attempted_questions)

    print("\n📊 EXAM REPORT")
    print("=" * 50)
    print(
        f"Score: {report.marks_obtained}/{report.total_marks} marks "
        f"({report.percentage_score}%)"
    )
    print(f"Grade: {report.overall_grade}")
    print(f"\nFeedback: {report.overall_feedback}")


if __name__ == "__main__":
    main()


# ============================================================================
# FastAPI Microservice
# ============================================================================

if FASTAPI_AVAILABLE:
    # In-memory job store (simple async job model)
    JOB_STORE: Dict[str, Dict] = {}
    JOB_EXPIRY_HOURS = int(os.getenv("JOB_EXPIRY_HOURS", "24"))

    # Rate limiting (simple in-memory)
    _rate_limit_store: Dict[str, List[float]] = defaultdict(list)
    RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
    RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))  # seconds

    app = FastAPI(
        title="Mock Exam Grading Service",
        version="1.0.0",
        description=(
            "FastAPI microservice for grading mock exams with LangGraph "
            "workflow. Supports async job processing, concept detection, "
            "mastery tracking, and readiness scoring."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # CORS middleware
    ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "*")
    ALLOWED_ORIGINS = [
        origin.strip() for origin in ALLOWED_ORIGINS_RAW.split(",")
    ]

    if "*" not in ALLOWED_ORIGINS:
        # Only add localhost for non-production environments
        environment = os.getenv("ENVIRONMENT", "development").lower()
        if environment != "production":
            localhost_origins = [
                "http://localhost:5173",
                "http://localhost:3000",
                "http://127.0.0.1:5173",
                "http://127.0.0.1:3000",
            ]
            for origin in localhost_origins:
                if origin not in ALLOWED_ORIGINS:
                    ALLOWED_ORIGINS.append(origin)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Request ID middleware
    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid4())
        request.state.request_id = request_id
        start_time = time.time()

        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Response-Time"] = str(
                round((time.time() - start_time) * 1000, 2)
            )
            log_metric("api_requests")
            return response
        except Exception as e:
            log_metric("api_errors")
            logger.error(
                json.dumps({
                    "request_id": request_id,
                    "error": str(e),
                    "path": request.url.path,
                })
            )
            raise

    # Rate limiting middleware
    @app.middleware("http")
    async def rate_limit_middleware(request: Request, call_next):
        """Simple rate limiting based on IP address."""
        if request.url.path.startswith("/health") or \
           request.url.path.startswith("/docs") or \
           request.url.path.startswith("/redoc") or \
           request.url.path.startswith("/openapi.json"):
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        now = time.time()

        # Clean old entries
        _rate_limit_store[client_ip] = [
            ts for ts in _rate_limit_store[client_ip]
            if now - ts < RATE_LIMIT_WINDOW
        ]

        # Check rate limit
        if len(_rate_limit_store[client_ip]) >= RATE_LIMIT_REQUESTS:
            logger.warning(
                json.dumps({
                    "message": "Rate limit exceeded",
                    "client_ip": client_ip,
                    "path": request.url.path,
                })
            )
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded",
                    "message": (
                        f"Maximum {RATE_LIMIT_REQUESTS} requests per "
                        f"{RATE_LIMIT_WINDOW} seconds"
                    ),
                    "retry_after": RATE_LIMIT_WINDOW,
                },
                headers={"Retry-After": str(RATE_LIMIT_WINDOW)},
            )

        # Add current request
        _rate_limit_store[client_ip].append(now)
        return await call_next(request)

    # Job cleanup background task
    async def cleanup_expired_jobs():
        """Periodically clean up expired jobs from JOB_STORE."""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                now = datetime.now()
                expired_jobs = []

                for job_id, job_data in JOB_STORE.items():
                    created_at_str = job_data.get("created_at")
                    if created_at_str:
                        try:
                            created_at = datetime.fromisoformat(
                                created_at_str.replace("Z", "+00:00")
                            )
                            if now - created_at > timedelta(
                                hours=JOB_EXPIRY_HOURS
                            ):
                                expired_jobs.append(job_id)
                        except (ValueError, TypeError):
                            # Invalid date format, mark for cleanup
                            expired_jobs.append(job_id)

                for job_id in expired_jobs:
                    del JOB_STORE[job_id]
                    logger.info(
                        json.dumps({
                            "message": "Cleaned up expired job",
                            "job_id": job_id,
                        })
                    )

                if expired_jobs:
                    logger.info(
                        f"Cleaned up {len(expired_jobs)} expired jobs"
                    )
            except Exception as e:
                logger.error(f"Error in job cleanup: {e}", exc_info=True)

    # Start cleanup task on FastAPI startup
    @app.on_event("startup")
    async def startup_event():
        """Start background tasks on FastAPI startup."""
        asyncio.create_task(cleanup_expired_jobs())

    # API models with validation
    class QuestionInput(BaseModel):
        """Validated question input."""
        question_id: int = Field(..., gt=0, description="Question ID")
        question: str = Field(..., min_length=1, description="Question text")
        user_answer: str = Field(default="", description="Student's answer")
        solution: Optional[str] = Field(
            default=None, description="Model answer/solution"
        )
        model_answer: Optional[str] = Field(
            default=None, description="Alternative model answer field"
        )
        marks: int = Field(..., gt=0, le=100, description="Marks allocated")
        part: str = Field(default="", description="Question part")
        question_number: Optional[int] = Field(
            default=None, description="Question number"
        )
        topic_id: Optional[int] = Field(
            default=None, description="Topic ID for RAG context"
        )

        @model_validator(mode='after')
        def validate_answer_or_solution(self):
            """Ensure at least one answer field is provided."""
            if not self.solution and not self.model_answer:
                raise ValueError(
                    "Either 'solution' or 'model_answer' must be provided"
                )
            return self

    class MockStartRequest(BaseModel):
        """Request to start a mock exam grading job."""
        user_id: str = Field(
            ..., min_length=1, description="User ID (UUID format recommended)"
        )
        attempted_questions: List[QuestionInput] = Field(
            ..., min_length=1, max_length=100,
            description="List of attempted questions (1-100 questions)"
        )

        @field_validator('user_id')
        @classmethod
        def validate_user_id(cls, v: str) -> str:
            """Validate user_id format."""
            if not v or not v.strip():
                raise ValueError("user_id cannot be empty")
            return v.strip()

        @field_validator('attempted_questions')
        @classmethod
        def validate_questions(
            cls, v: List[QuestionInput]
        ) -> List[QuestionInput]:
            """Validate questions list."""
            if not v:
                raise ValueError(
                    "attempted_questions cannot be empty"
                )
            if len(v) > 100:
                raise ValueError(
                    "Maximum 100 questions allowed per exam"
                )
            return v

    class MockStartResponse(BaseModel):
        job_id: str
        status: str

    class MockStatusResponse(BaseModel):
        job_id: str
        status: str
        result: Optional[ExamReport] = None
        error: Optional[str] = None

    # Singleton agent
    _grading_agent: Optional[MockExamGradingAgent] = None

    def get_agent() -> MockExamGradingAgent:
        """Get or create the grading agent."""
        global _grading_agent
        if _grading_agent is None:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY not found")
            _grading_agent = MockExamGradingAgent(api_key)
        return _grading_agent

    # Background task
    async def run_grading_task(
        job_id: str,
        user_id: str,
        attempted_questions: List[Dict],
        request_id: str,
    ):
        """Background grading workflow with improved error handling."""
        start_time = time.time()
        try:
            JOB_STORE[job_id]["status"] = "processing"
            JOB_STORE[job_id]["started_at"] = datetime.now().isoformat()

            agent = get_agent()

            # Convert QuestionInput to Dict for backward compatibility
            questions_dict = [
                q.model_dump() if isinstance(q, QuestionInput) else q
                for q in attempted_questions
            ]

            exam_report = await run_mock_exam_graph(
                agent, user_id, questions_dict, request_id, job_id
            )

            elapsed_time = time.time() - start_time
            JOB_STORE[job_id]["status"] = "completed"
            JOB_STORE[job_id]["result"] = exam_report
            JOB_STORE[job_id]["completed_at"] = datetime.now().isoformat()
            JOB_STORE[job_id]["duration_seconds"] = round(elapsed_time, 2)

            log_metric("jobs_completed")
            log_metric("questions_graded", len(questions_dict))

            logger.info(
                json.dumps(
                    {
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "status": "completed",
                        "message": "Grading completed successfully",
                        "duration_seconds": round(elapsed_time, 2),
                        "questions_count": len(questions_dict),
                        "percentage": exam_report.percentage_score,
                    }
                )
            )
        except Exception as e:
            elapsed_time = time.time() - start_time
            JOB_STORE[job_id]["status"] = "failed"
            JOB_STORE[job_id]["error"] = str(e)
            JOB_STORE[job_id]["failed_at"] = datetime.now().isoformat()
            JOB_STORE[job_id]["duration_seconds"] = round(elapsed_time, 2)

            log_metric("jobs_failed")

            logger.error(
                json.dumps(
                    {
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "status": "failed",
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "duration_seconds": round(elapsed_time, 2),
                    }
                ),
                exc_info=True,
            )

    # Endpoints
    @app.post(
        "/start",
        response_model=MockStartResponse,
        summary="Start Mock Exam Grading",
        description=(
            "Submit a mock exam for asynchronous grading. Returns a job_id "
            "that can be used to check the status and retrieve results."
        ),
        responses={
            200: {
                "description": "Job created successfully",
                "content": {
                    "application/json": {
                        "example": {
                            "job_id": "123e4567-e89b-12d3-a456-426614174000",
                            "status": "pending"
                        }
                    }
                }
            },
            400: {"description": "Invalid request data"},
            429: {"description": "Rate limit exceeded"},
        },
    )
    async def start_mock_exam(
        request: MockStartRequest, http_request: Request
    ):
        """
        Start a mock exam grading job.

        The grading process runs asynchronously. Use the returned job_id
        to check status via GET /api/v1/mock/status/{job_id}.
        """
        request_id = http_request.state.request_id

        try:
            job_id = str(uuid4())
            JOB_STORE[job_id] = {
                "status": "pending",
                "result": None,
                "error": None,
                "created_at": datetime.now().isoformat(),
                "user_id": request.user_id,
                "questions_count": len(request.attempted_questions),
            }

            asyncio.create_task(
                run_grading_task(
                    job_id,
                    request.user_id,
                    request.attempted_questions,
                    request_id,
                )
            )

            log_metric("jobs_created")

            logger.info(
                json.dumps(
                    {
                        "request_id": request_id,
                        "job_id": job_id,
                        "user_id": request.user_id,
                        "status": "pending",
                        "message": "Job created",
                        "questions_count": len(request.attempted_questions),
                    }
                )
            )

            return MockStartResponse(job_id=job_id, status="pending")
        except ValueError as e:
            logger.warning(
                json.dumps({
                    "request_id": request_id,
                    "error": str(e),
                    "user_id": request.user_id,
                })
            )
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(
                json.dumps({
                    "request_id": request_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                }),
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail="Internal server error. Please try again later."
            )

    @app.get(
        "/status/{job_id}",
        response_model=MockStatusResponse,
        summary="Get Job Status",
        description=(
            "Check the status of a mock exam grading job. Returns the "
            "current status (pending, processing, completed, or failed) "
            "and the result if available."
        ),
        responses={
            200: {"description": "Job status retrieved successfully"},
            404: {"description": "Job not found"},
        },
    )
    async def get_mock_status(job_id: str, http_request: Request):
        """
        Get the status of a mock exam grading job.

        Status values:
        - pending: Job is queued but not yet processing
        - processing: Job is currently being graded
        - completed: Job finished successfully (result available)
        - failed: Job encountered an error (error message available)
        """
        request_id = http_request.state.request_id

        if job_id not in JOB_STORE:
            logger.warning(
                json.dumps({
                    "request_id": request_id,
                    "job_id": job_id,
                    "message": "Job not found",
                })
            )
            raise HTTPException(
                status_code=404,
                detail=f"Job {job_id} not found. It may have expired or "
                       f"never existed."
            )

        job_data = JOB_STORE[job_id]

        return MockStatusResponse(
            job_id=job_id,
            status=job_data["status"],
            result=job_data.get("result"),
            error=job_data.get("error"),
        )

    @app.get(
        "/health",
        summary="Health Check",
        description="Check if the service is running and healthy.",
    )
    async def health_check():
        """Health check endpoint with metrics."""
        return {
            "status": "healthy",
            "service": "mock_exam_grading",
            "timestamp": datetime.now().isoformat(),
            "metrics": get_metrics(),
            "active_jobs": len([
                j for j in JOB_STORE.values()
                if j.get("status") in ["pending", "processing"]
            ]),
            "total_jobs": len(JOB_STORE),
        }

    @app.get(
        "/metrics",
        summary="Service Metrics",
        description="Get service metrics and statistics.",
    )
    async def get_service_metrics():
        """Get service metrics."""
        return {
            "metrics": get_metrics(),
            "job_store_size": len(JOB_STORE),
            "active_jobs": len([
                j for j in JOB_STORE.values()
                if j.get("status") in ["pending", "processing"]
            ]),
            "completed_jobs": len([
                j for j in JOB_STORE.values()
                if j.get("status") == "completed"
            ]),
            "failed_jobs": len([
                j for j in JOB_STORE.values()
                if j.get("status") == "failed"
            ]),
        }

    # LangSmith tracing (observability)
    if os.getenv("LANGSMITH_API_KEY"):
        os.environ["LANGSMITH_API_KEY"] = os.getenv("LANGSMITH_API_KEY")
        os.environ["LANGSMITH_PROJECT"] = os.getenv(
            "LANGSMITH_PROJECT", "imtehaan-mock-exam"
        )
        os.environ["LANGSMITH_ENDPOINT"] = os.getenv(
            "LANGSMITH_ENDPOINT", "https://api.smith.langchain.com"
        )
        os.environ["LANGSMITH_TRACING"] = os.getenv(
            "LANGSMITH_TRACING", "true"
        )
        logger.info("[OK] LangSmith tracing enabled")
