"""
AI Agents Package
Contains all AI agents for the Imtehaan EdTech Platform
"""

__version__ = "1.0.0"

from .ai_tutor_agent import (
    AITutorAgent, TutorRequest, TutorResponse,
    LessonRequest, LessonResponse
)
from .answer_grading_agent import (
    AnswerGradingAgent, GradingResult, GradingCriteria
)
from .mock_exam_grading_agent import (
    MockExamGradingAgent, ExamReport, QuestionGrade
)

__all__ = [
    # AI Tutor Agent
    'AITutorAgent',
    'TutorRequest',
    'TutorResponse',
    'LessonRequest',
    'LessonResponse',
    # Answer Grading Agent
    'AnswerGradingAgent',
    'GradingResult',
    'GradingCriteria',
    # Mock Exam Grading Agent
    'MockExamGradingAgent',
    'ExamReport',
    'QuestionGrade'
]
