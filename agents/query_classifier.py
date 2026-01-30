#!/usr/bin/env python3
"""
Query Classifier - Determines if explanation queries are simple or complex
Used for model selection in helping agent to route simple queries to faster models
"""

import re
import logging
from typing import Dict, Tuple, Optional

logger = logging.getLogger(__name__)


class QueryClassifier:
    """
    Classifies explanation queries as simple or complex.
    
    Simple queries are:
    - Short (typically < 50 characters)
    - Definition-based (asking "what is X", "define X", "explain X")
    - Non-reasoning-heavy (no "why", "how", "compare", "analyze")
    """
    
    # Patterns that indicate definition-based queries
    DEFINITION_PATTERNS = [
        r'^what\s+is\s+',
        r'^what\s+are\s+',
        r'^define\s+',
        r'^explain\s+',
        r'^meaning\s+of\s+',
        r'^tell\s+me\s+about\s+',
        r'^what\s+does\s+',
        r'^what\s+do\s+',
        r'^what\s+means?\s+',
    ]
    
    # Patterns that indicate complex/reasoning queries
    REASONING_PATTERNS = [
        r'\bwhy\b',
        r'\bhow\b',
        r'\bcompare\b',
        r'\bcontrast\b',
        r'\banalyze\b',
        r'\bevaluate\b',
        r'\bexplain\s+why\b',
        r'\bexplain\s+how\b',
        r'\bdifference\s+between\b',
        r'\bsimilarities?\b',
        r'\brelationship\s+between\b',
        r'\bcause\b',
        r'\beffect\b',
        r'\bimpact\b',
        r'\bconsequence\b',
    ]
    
    # Question words that suggest complexity
    COMPLEX_QUESTION_WORDS = [
        'why', 'how', 'when', 'where', 'which', 'whose'
    ]
    
    def __init__(
        self,
        max_simple_length: int = 50,
        min_confidence: float = 0.7
    ):
        """
        Initialize query classifier.
        
        Args:
            max_simple_length: Maximum character length for simple queries
            min_confidence: Minimum confidence threshold for simple classification
        """
        self.max_simple_length = max_simple_length
        self.min_confidence = min_confidence
        
        # Compile regex patterns for efficiency
        self.definition_regex = re.compile(
            '|'.join(self.DEFINITION_PATTERNS),
            re.IGNORECASE
        )
        self.reasoning_regex = re.compile(
            '|'.join(self.REASONING_PATTERNS),
            re.IGNORECASE
        )
    
    def classify(
        self,
        query: str,
        context: Optional[str] = None
    ) -> Tuple[str, float, Dict]:
        """
        Classify a query as 'simple' or 'complex'.
        
        Args:
            query: The user's query string
            context: Optional context (longer context suggests complexity)
        
        Returns:
            Tuple of (classification, confidence, metadata):
            - classification: 'simple' or 'complex'
            - confidence: float between 0.0 and 1.0
            - metadata: dict with classification details
        """
        if not query or not query.strip():
            return 'complex', 0.0, {'reason': 'empty_query'}
        
        query_clean = query.strip()
        query_lower = query_clean.lower()
        query_length = len(query_clean)
        
        # Initialize scoring
        simple_score = 0.0
        complex_score = 0.0
        reasons = []
        
        # Factor 1: Length (shorter = more likely simple)
        if query_length <= 30:
            simple_score += 0.4
            reasons.append('very_short')
        elif query_length <= self.max_simple_length:
            simple_score += 0.2
            reasons.append('short')
        else:
            complex_score += 0.3
            reasons.append('long')
        
        # Factor 2: Definition patterns (strong indicator of simple)
        if self.definition_regex.match(query_lower):
            simple_score += 0.4
            reasons.append('definition_pattern')
        elif any(
            query_lower.startswith(pattern.replace('^', '').replace('\\s+', ' '))
            for pattern in self.DEFINITION_PATTERNS
        ):
            simple_score += 0.3
            reasons.append('definition_like')
        
        # Factor 3: Reasoning patterns (strong indicator of complex)
        if self.reasoning_regex.search(query_lower):
            complex_score += 0.5
            reasons.append('reasoning_pattern')
        
        # Factor 4: Question words
        question_words_found = [
            word for word in self.COMPLEX_QUESTION_WORDS
            if word in query_lower
        ]
        if question_words_found:
            # "What" is often simple, but "why", "how" are complex
            if 'why' in question_words_found or 'how' in question_words_found:
                complex_score += 0.3
                reasons.append('complex_question_word')
            elif 'what' in question_words_found and query_length < 40:
                # "What is X" is often simple
                simple_score += 0.1
        
        # Factor 5: Word count (fewer words = more likely simple)
        word_count = len(query_clean.split())
        if word_count <= 3:
            simple_score += 0.2
            reasons.append('few_words')
        elif word_count <= 5:
            simple_score += 0.1
        elif word_count > 10:
            complex_score += 0.2
            reasons.append('many_words')
        
        # Factor 6: Context presence (context suggests complexity)
        if context and len(context) > 50:
            complex_score += 0.2
            reasons.append('has_context')
        
        # Factor 7: Special characters (questions marks, multiple sentences)
        if query_clean.count('?') > 1:
            complex_score += 0.1
            reasons.append('multiple_questions')
        
        if query_clean.count('.') > 1 or query_clean.count('!') > 0:
            complex_score += 0.1
            reasons.append('multiple_sentences')
        
        # Normalize scores
        total_score = simple_score + complex_score
        if total_score == 0:
            # Default to complex if no indicators
            return 'complex', 0.5, {'reason': 'no_indicators'}
        
        simple_ratio = simple_score / total_score
        complex_ratio = complex_score / total_score
        
        # Determine classification
        if simple_ratio > complex_ratio and simple_ratio >= self.min_confidence:
            classification = 'simple'
            confidence = simple_ratio
        elif complex_ratio > simple_ratio:
            classification = 'complex'
            confidence = complex_ratio
        else:
            # Ambiguous - default to complex for safety
            classification = 'complex'
            confidence = 0.5
        
        metadata = {
            'simple_score': simple_score,
            'complex_score': complex_score,
            'simple_ratio': simple_ratio,
            'complex_ratio': complex_ratio,
            'reasons': reasons,
            'query_length': query_length,
            'word_count': word_count,
            'has_context': bool(context and len(context) > 50)
        }
        
        return classification, confidence, metadata


# Global instance for reuse
_classifier_instance: Optional[QueryClassifier] = None


def get_classifier() -> QueryClassifier:
    """Get or create global query classifier instance"""
    global _classifier_instance
    if _classifier_instance is None:
        _classifier_instance = QueryClassifier()
    return _classifier_instance
