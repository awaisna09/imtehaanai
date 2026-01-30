#!/usr/bin/env python3
"""
Study Planner Service
Implements rules engine, schedule generation, and database operations for study plans
"""

import os
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv('config.env')

try:
    from supabase import create_client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False


@dataclass
class ContentRequirements:
    """Content requirements for a study plan based on rule band"""
    flash_per_topic: int
    topical_per_topic: int
    lessons_per_topic: int
    mocks_total: int
    rule_band: str


@dataclass
class PlanSummary:
    """Summary of a study plan"""
    total_flashcards: int
    total_topicals: int
    total_lessons: int
    total_mocks: int
    total_minutes: int
    minutes_per_day: int
    flash_per_topic: int
    topical_per_topic: int


@dataclass
class DaySchedule:
    """Schedule for a single day"""
    day_date: date
    day_index: int
    planned_flashcards: int
    planned_topicals: int
    planned_lessons: int
    planned_mock: bool
    planned_minutes: int
    topic_breakdown: List[Dict]  # List of {topic_id, flashcards, topicals, lesson}


class StudyPlannerService:
    """Service for managing study plans with rules engine and schedule generation"""
    
    def __init__(self):
        """Initialize Supabase client"""
        self.url = os.getenv("SUPABASE_URL")
        self.key = (
            os.getenv("SUPABASE_SERVICE_ROLE_KEY") or
            os.getenv("SUPABASE_ANON_KEY")
        )
        self.enabled = bool(self.url and self.key) and SUPABASE_AVAILABLE
        
        if self.enabled:
            self.client = create_client(self.url, self.key)
        else:
            self.client = None
    
    def determine_rule_band(self, days_to_exam: int) -> str:
        """
        Determine rule band based on days to exam
        
        Args:
            days_to_exam: Number of days until exam
            
        Returns:
            Rule band string: '>=60', '30-60', '15-30', or '5-15'
        """
        if days_to_exam >= 60:
            return ">=60"
        elif days_to_exam >= 30:
            return "30-60"
        elif days_to_exam >= 15:
            return "15-30"
        else:
            return "5-15"
    
    def get_content_requirements(self, days_to_exam: int, subject_id: int) -> ContentRequirements:
        """
        Get content requirements based on days to exam and subject
        
        Args:
            days_to_exam: Number of days until exam
            subject_id: Subject ID for subject-specific base values
            
        Returns:
            ContentRequirements with flash, topical, lessons, and mocks per topic
        """
        # Subject-specific base values per topic
        # Subject IDs: 101=Business Studies, 102=Islamiyat, 113=Pak Studies Geography, 
        # 114=Pak Studies History, 119=Economics
        subject_config = {
            102: {'flash_base': 30, 'topical_base': 30, 'mocks_p1': 10, 'mocks_p2': 10},  # Islamiyat
            113: {'flash_base': 50, 'topical_base': 50, 'mocks_p1': 0, 'mocks_p2': 10},   # Pak Studies Geography
            114: {'flash_base': 30, 'topical_base': 30, 'mocks_p1': 0, 'mocks_p2': 10},   # Pak Studies History
            119: {'flash_base': 50, 'topical_base': 50, 'mocks_p1': 10, 'mocks_p2': 10},  # Economics
            101: {'flash_base': 50, 'topical_base': 100, 'mocks_p1': 10, 'mocks_p2': 10}, # Business Studies
        }
        
        config = subject_config.get(subject_id, {'flash_base': 50, 'topical_base': 50, 'mocks_p1': 10, 'mocks_p2': 10})
        
        rule_band = self.determine_rule_band(days_to_exam)
        
        # Calculate multipliers based on days to exam (same logic as before)
        # Original logic used 100/50/20 as targets, so we use multipliers: 2x, 1x, 0.4x
        if rule_band == ">=60":
            flash_multiplier = 2  # 2x base (e.g., 50 -> 100, 30 -> 60)
            topical_multiplier = 2  # 2x base
            mocks_multiplier = 1  # Full mocks
        elif rule_band == "30-60":
            flash_multiplier = 1  # 1x base (e.g., 50 -> 50, 30 -> 30)
            topical_multiplier = 1  # 1x base
            mocks_multiplier = 0.5  # Half mocks
        elif rule_band == "15-30":
            flash_multiplier = 0.4  # 0.4x base (e.g., 50 -> 20, 30 -> 12)
            topical_multiplier = 0.4  # 0.4x base
            mocks_multiplier = 0.5  # Half mocks
        else:  # 5-15
            # Interpolation relative to base
            flash_target = max(5, min(days_to_exam, 15))
            topical_target = max(10, min(days_to_exam + 5, 20))
            flash_multiplier = flash_target / config['flash_base']
            topical_multiplier = topical_target / config['topical_base']
            mocks_multiplier = 0.3  # 30% of mocks
        
        # Apply multipliers to base values
        flash_per_topic = round(config['flash_base'] * flash_multiplier)
        topical_per_topic = round(config['topical_base'] * topical_multiplier)
        
        # Calculate total mocks based on subject-specific config and multiplier
        total_mocks_p1 = round(config['mocks_p1'] * mocks_multiplier)
        total_mocks_p2 = round(config['mocks_p2'] * mocks_multiplier)
        mocks_total = total_mocks_p1 + total_mocks_p2
        
        return ContentRequirements(
            flash_per_topic=flash_per_topic,
            topical_per_topic=topical_per_topic,
            lessons_per_topic=1,
            mocks_total=mocks_total,
            rule_band=rule_band
        )
    
    def calculate_plan_summary(
        self,
        num_topics: int,
        requirements: ContentRequirements
    ) -> PlanSummary:
        """
        Calculate plan summary totals
        
        Args:
            num_topics: Number of selected topics
            requirements: ContentRequirements object
            
        Returns:
            PlanSummary with all totals
        """
        # Per-topic minutes
        flash_minutes = requirements.flash_per_topic * 2
        topical_minutes = requirements.topical_per_topic * 4
        lesson_minutes = requirements.lessons_per_topic * 40
        per_topic_total = flash_minutes + topical_minutes + lesson_minutes
        
        # Totals
        total_flashcards = num_topics * requirements.flash_per_topic
        total_topicals = num_topics * requirements.topical_per_topic
        total_lessons = num_topics * requirements.lessons_per_topic
        total_mock_minutes = requirements.mocks_total * 180
        total_minutes = (num_topics * per_topic_total) + total_mock_minutes
        
        return PlanSummary(
            total_flashcards=total_flashcards,
            total_topicals=total_topicals,
            total_lessons=total_lessons,
            total_mocks=requirements.mocks_total,
            total_minutes=total_minutes,
            minutes_per_day=0,  # Will be calculated with days_to_exam
            flash_per_topic=requirements.flash_per_topic,
            topical_per_topic=requirements.topical_per_topic
        )
    
    def generate_schedule(
        self,
        start_date: date,
        exam_date: date,
        selected_topic_ids: List[int],
        requirements: ContentRequirements
    ) -> List[DaySchedule]:
        """
        Generate deterministic day-by-day schedule
        
        Args:
            start_date: Start date of plan (today)
            exam_date: Exam date
            selected_topic_ids: List of selected topic IDs
            requirements: ContentRequirements object
            
        Returns:
            List of DaySchedule objects
        """
        days_to_exam = (exam_date - start_date).days
        end_date = exam_date - timedelta(days=1)
        
        # Generate all study days (excluding exam day)
        study_days = []
        current_date = start_date
        day_index = 1
        
        while current_date <= end_date:
            study_days.append((current_date, day_index))
            current_date += timedelta(days=1)
            day_index += 1
        
        num_days = len(study_days)
        
        # Step 1: Place mock exams
        mock_day_indices = self._place_mock_exams(num_days, requirements.mocks_total)
        mock_days_set = set(mock_day_indices)
        
        # Step 2: Place lessons (first 20-30% of plan, round-robin, exclude mock days)
        lesson_window_days = max(2, int(num_days * 0.3))
        topic_lesson_days = self._place_lessons(
            num_days,
            selected_topic_ids,
            lesson_window_days,
            mock_days_set
        )
        
        # Step 3: Place flashcards and topicals for each topic
        # Spread evenly from lesson day to end, excluding mock days
        topic_schedules = {}
        for topic_id in selected_topic_ids:
            lesson_day = topic_lesson_days.get(topic_id, 1)
            allowed_days = [
                idx for idx in range(lesson_day, num_days + 1)
                if idx not in mock_days_set
            ]
            
            if allowed_days:
                flash_schedule = self._distribute_evenly(
                    requirements.flash_per_topic,
                    allowed_days
                )
                topical_schedule = self._distribute_evenly(
                    requirements.topical_per_topic,
                    allowed_days
                )
                
                topic_schedules[topic_id] = {
                    'lesson_day': lesson_day,
                    'flash_schedule': flash_schedule,
                    'topical_schedule': topical_schedule
                }
        
        # Step 4: Build day schedules
        day_schedules = []
        for day_date, day_idx in study_days:
            is_mock_day = day_idx in mock_days_set
            
            # Aggregate from all topics
            planned_flashcards = 0
            planned_topicals = 0
            planned_lessons = 0
            topic_breakdown = []
            
            for topic_id in selected_topic_ids:
                if topic_id in topic_schedules:
                    schedule = topic_schedules[topic_id]
                    flash_count = schedule['flash_schedule'].get(day_idx, 0)
                    topical_count = schedule['topical_schedule'].get(day_idx, 0)
                    has_lesson = schedule['lesson_day'] == day_idx and not is_mock_day
                    
                    if flash_count > 0 or topical_count > 0 or has_lesson:
                        planned_flashcards += flash_count
                        planned_topicals += topical_count
                        if has_lesson:
                            planned_lessons += 1
                        
                        topic_breakdown.append({
                            'topic_id': topic_id,
                            'flashcards_planned': flash_count,
                            'topicals_planned': topical_count,
                            'lesson_planned': has_lesson
                        })
            
            # Calculate total minutes
            planned_minutes = (
                planned_flashcards * 2 +
                planned_topicals * 4 +
                planned_lessons * 40 +
                (180 if is_mock_day else 0)
            )
            
            day_schedules.append(DaySchedule(
                day_date=day_date,
                day_index=day_idx,
                planned_flashcards=planned_flashcards,
                planned_topicals=planned_topicals,
                planned_lessons=planned_lessons,
                planned_mock=is_mock_day,
                planned_minutes=planned_minutes,
                topic_breakdown=topic_breakdown
            ))
        
        return day_schedules
    
    def _place_mock_exams(self, num_days: int, mocks_total: int) -> List[int]:
        """
        Place mock exams evenly spaced
        
        Args:
            num_days: Total number of study days
            mocks_total: Total number of mocks
            
        Returns:
            List of day indices (1-based) for mock exams
        """
        if mocks_total == 0:
            return []
        
        # Allowed day indices: 2..(num_days-1) (1-based, so indices 2 to num_days-1)
        # Formula: idx_k = round(2 + (k*(num_days-3))/(mocks_total-1)) for k=0..mocks_total-1
        mock_indices = []
        
        if mocks_total == 1:
            # Single mock: place in middle
            mock_indices.append(max(2, min(num_days - 1, num_days // 2)))
        else:
            for k in range(mocks_total):
                idx = round(2 + (k * (num_days - 3)) / (mocks_total - 1))
                # Clamp to allowed range
                idx = max(2, min(idx, num_days - 1))
                mock_indices.append(idx)
        
        # Ensure uniqueness (handle collisions)
        unique_indices = []
        for idx in mock_indices:
            while idx in unique_indices:
                idx += 1
                if idx > num_days - 1:
                    idx = 2  # Wrap around
            unique_indices.append(idx)
        
        return sorted(unique_indices)
    
    def _place_lessons(
        self,
        num_days: int,
        topic_ids: List[int],
        lesson_window_days: int,
        mock_days_set: set
    ) -> Dict[int, int]:
        """
        Place lessons round-robin in first window, excluding mock days
        
        Args:
            num_days: Total number of study days
            topic_ids: List of topic IDs
            lesson_window_days: Number of days in lesson window
            mock_days_set: Set of mock day indices to exclude
            
        Returns:
            Dict mapping topic_id to lesson day index
        """
        # Get available days in window (excluding mock days)
        available_days = [
            idx for idx in range(1, min(lesson_window_days + 1, num_days + 1))
            if idx not in mock_days_set
        ]
        
        if not available_days:
            # Fallback: use first non-mock day
            available_days = [idx for idx in range(1, num_days + 1) if idx not in mock_days_set]
            if not available_days:
                available_days = [1]  # Last resort
        
        # Round-robin assignment
        topic_lesson_days = {}
        for i, topic_id in enumerate(topic_ids):
            day_idx = available_days[i % len(available_days)]
            topic_lesson_days[topic_id] = day_idx
        
        return topic_lesson_days
    
    def _distribute_evenly(self, total: int, allowed_days: List[int]) -> Dict[int, int]:
        """
        Distribute items evenly across allowed days
        
        Args:
            total: Total number of items to distribute
            allowed_days: List of day indices (1-based) where items can be placed
            
        Returns:
            Dict mapping day_index to number of items
        """
        if not allowed_days or total == 0:
            return {}
        
        distribution = {}
        items_per_day = total // len(allowed_days)
        remainder = total % len(allowed_days)
        
        for i, day_idx in enumerate(allowed_days):
            count = items_per_day
            if i < remainder:
                count += 1
            if count > 0:
                distribution[day_idx] = count
        
        return distribution
    
    # Database operations
    def _get_subject_name(self, subject_id: int) -> str:
        """Get subject name from subject_id"""
        subject_map = {
            101: 'Business Studies',
            102: 'Islamiyat',
            103: 'Mathematics',
            104: 'Physics',
            105: 'Chemistry',
            113: 'Pak Studies Geography',
            114: 'Pak Studies History',
            119: 'Economics'
        }
        return subject_map.get(subject_id, 'Unknown Subject')
    
    def create_study_plan(
        self,
        user_id: str,
        subject_id: int,
        plan_name: str,
        exam_date: date,
        selected_topic_ids: List[int]
    ) -> Dict:
        """
        Create a new study plan with schedule
        
        Args:
            user_id: User UUID
            subject_id: Subject ID
            plan_name: Plan name
            exam_date: Exam date
            selected_topic_ids: List of selected topic IDs
            
        Returns:
            Dict with plan data
        """
        if not self.enabled:
            raise Exception("Supabase not available")
        
        # Get subject name
        subject_name = self._get_subject_name(subject_id)
        
        # Validate days to exam
        start_date = date.today()
        days_to_exam = (exam_date - start_date).days
        if days_to_exam < 5:
            raise ValueError("Days to exam must be at least 5")
        
        # Get requirements
        requirements = self.get_content_requirements(days_to_exam, subject_id)
        
        # Generate schedule
        day_schedules = self.generate_schedule(
            start_date,
            exam_date,
            selected_topic_ids,
            requirements
        )
        
        # Insert plan
        plan_data = {
            'user_id': user_id,
            'subject_id': subject_id,
            'subject': subject_name,
            'plan_name': plan_name,
            'start_date': start_date.isoformat(),
            'exam_date': exam_date.isoformat(),
            'days_to_exam': days_to_exam,
            'rule_band': requirements.rule_band,
            'flash_per_topic': requirements.flash_per_topic,
            'topical_per_topic': requirements.topical_per_topic,
            'lessons_per_topic': requirements.lessons_per_topic,
            'mocks_total': requirements.mocks_total,
            'status': 'active'
        }
        
        plan_response = self.client.table('study_plans_v2').insert(plan_data).execute()
        if not plan_response.data:
            raise Exception("Failed to create study plan")
        
        plan_id = plan_response.data[0]['id']
        
        # Insert plan topics
        plan_topics = [
            {'plan_id': plan_id, 'topic_id': topic_id, 'subject': subject_name}
            for topic_id in selected_topic_ids
        ]
        if plan_topics:
            self.client.table('study_plan_topics_v2').insert(plan_topics).execute()
        
        # Insert plan days
        plan_days = []
        for day_schedule in day_schedules:
            plan_day_data = {
                'plan_id': plan_id,
                'subject': subject_name,
                'day_date': day_schedule.day_date.isoformat(),
                'day_index': day_schedule.day_index,
                'planned_flashcards': day_schedule.planned_flashcards,
                'planned_topicals': day_schedule.planned_topicals,
                'planned_lessons': day_schedule.planned_lessons,
                'planned_mock': day_schedule.planned_mock,
                'planned_minutes': day_schedule.planned_minutes
            }
            plan_days.append(plan_day_data)
        
        if plan_days:
            days_response = self.client.table('study_plan_days_v2').insert(plan_days).execute()
            
            # Insert day topics
            day_topics = []
            for day_schedule in day_schedules:
                # Find the plan_day_id for this day
                day_data = next(
                    (d for d in days_response.data if d['day_date'] == day_schedule.day_date.isoformat()),
                    None
                )
                if day_data:
                    plan_day_id = day_data['id']
                    for topic_breakdown in day_schedule.topic_breakdown:
                        day_topic_data = {
                            'plan_day_id': plan_day_id,
                            'subject': subject_name,
                            'topic_id': topic_breakdown['topic_id'],
                            'flashcards_planned': topic_breakdown['flashcards_planned'],
                            'topicals_planned': topic_breakdown['topicals_planned'],
                            'lesson_planned': topic_breakdown['lesson_planned']
                        }
                        day_topics.append(day_topic_data)
            
            if day_topics:
                self.client.table('study_plan_day_topics_v2').insert(day_topics).execute()
        
        return plan_response.data[0]
    
    def get_user_study_plans(self, user_id: str, status: str = 'active') -> List[Dict]:
        """Get all study plans for a user"""
        if not self.enabled:
            return []
        
        response = self.client.table('study_plans_v2')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('status', status)\
            .order('created_at', desc=True)\
            .execute()
        
        return response.data if response.data else []
    
    def get_study_plan_details(self, plan_id: str, user_id: str) -> Optional[Dict]:
        """Get study plan with full schedule"""
        if not self.enabled:
            return None
        
        # Get plan - use limit(1) instead of single() to avoid exception on no results
        try:
            plan_response = self.client.table('study_plans_v2')\
                .select('*')\
                .eq('id', plan_id)\
                .eq('user_id', user_id)\
                .limit(1)\
                .execute()
        except Exception as e:
            print(f"Error fetching study plan: {e}")
            return None
        
        if not plan_response.data or len(plan_response.data) == 0:
            return None
        
        plan = plan_response.data[0]
        
        # Get topics
        topics_response = self.client.table('study_plan_topics_v2')\
            .select('topic_id')\
            .eq('plan_id', plan_id)\
            .execute()
        
        plan['topics'] = [t['topic_id'] for t in (topics_response.data or [])]
        
        # Get days
        days_response = self.client.table('study_plan_days_v2')\
            .select('*')\
            .eq('plan_id', plan_id)\
            .order('day_index')\
            .execute()
        
        days = days_response.data or []
        
        # OPTIMIZATION: Fetch all day topics in a single query instead of N queries
        if days:
            day_ids = [day['id'] for day in days]
            
            # Get all day topics for all days in one query
            all_day_topics_response = self.client.table('study_plan_day_topics_v2')\
                .select('*')\
                .in_('plan_day_id', day_ids)\
                .execute()
            
            # Build a dictionary mapping plan_day_id to list of topic breakdowns
            day_topics_map = {}
            if all_day_topics_response.data:
                for day_topic in all_day_topics_response.data:
                    plan_day_id = day_topic['plan_day_id']
                    if plan_day_id not in day_topics_map:
                        day_topics_map[plan_day_id] = []
                    day_topics_map[plan_day_id].append(day_topic)
            
            # Assign topic breakdowns to each day
            for day in days:
                day['topic_breakdown'] = day_topics_map.get(day['id'], [])
        
        plan['days'] = days
        
        return plan
    
    def update_study_plan(
        self,
        plan_id: str,
        user_id: str,
        plan_name: Optional[str] = None,
        exam_date: Optional[date] = None
    ) -> Optional[Dict]:
        """Update study plan (regenerate schedule if exam_date changes)"""
        if not self.enabled:
            return None
        
        # Get existing plan
        existing = self.get_study_plan_details(plan_id, user_id)
        if not existing:
            return None
        
        # Get subject_id from existing plan (needed for requirements calculation)
        subject_id = existing.get('subject_id')
        if not subject_id:
            raise ValueError("Subject ID not found in existing plan")
        
        update_data = {}
        if plan_name:
            update_data['plan_name'] = plan_name
        
        regenerate_schedule = False
        new_days = None
        if exam_date:
            new_days = (exam_date - date.today()).days
            if new_days < 5:
                raise ValueError("Days to exam must be at least 5")
            
            old_exam_date = datetime.fromisoformat(existing['exam_date']).date()
            if exam_date != old_exam_date:
                regenerate_schedule = True
                update_data['exam_date'] = exam_date.isoformat()
                update_data['days_to_exam'] = new_days
                
                # Recalculate requirements
                requirements = self.get_content_requirements(new_days, subject_id)
                update_data['rule_band'] = requirements.rule_band
                update_data['flash_per_topic'] = requirements.flash_per_topic
                update_data['topical_per_topic'] = requirements.topical_per_topic
                update_data['mocks_total'] = requirements.mocks_total
        
        # Ensure subject is always included in update
        subject_name = self._get_subject_name(subject_id)
        update_data['subject'] = subject_name
        
        if update_data:
            update_data['updated_at'] = datetime.now(timezone.utc).isoformat()
            self.client.table('study_plans_v2')\
                .update(update_data)\
                .eq('id', plan_id)\
                .eq('user_id', user_id)\
                .execute()
        
        if regenerate_schedule:
            # Delete old schedule
            self.client.table('study_plan_day_topics_v2')\
                .delete()\
                .in_('plan_day_id', 
                     self.client.table('study_plan_days_v2')
                     .select('id')
                     .eq('plan_id', plan_id)
                     .execute().data or [])
            
            self.client.table('study_plan_days_v2')\
                .delete()\
                .eq('plan_id', plan_id)\
                .execute()
            
            # Generate new schedule
            start_date = date.today()
            topic_ids = existing['topics']
            
            # Get requirements (subject_id already retrieved above)
            requirements = self.get_content_requirements(new_days, subject_id)
            
            # Get subject name (already retrieved above)
            
            day_schedules = self.generate_schedule(
                start_date,
                exam_date,
                topic_ids,
                requirements
            )
            
            # Insert new schedule (same as create_study_plan)
            plan_days = []
            for day_schedule in day_schedules:
                plan_day_data = {
                    'plan_id': plan_id,
                    'subject': subject_name,
                    'day_date': day_schedule.day_date.isoformat(),
                    'day_index': day_schedule.day_index,
                    'planned_flashcards': day_schedule.planned_flashcards,
                    'planned_topicals': day_schedule.planned_topicals,
                    'planned_lessons': day_schedule.planned_lessons,
                    'planned_mock': day_schedule.planned_mock,
                    'planned_minutes': day_schedule.planned_minutes
                }
                plan_days.append(plan_day_data)
            
            if plan_days:
                days_response = self.client.table('study_plan_days_v2').insert(plan_days).execute()
                
                # Insert day topics
                day_topics = []
                for day_schedule in day_schedules:
                    day_data = next(
                        (d for d in days_response.data if d['day_date'] == day_schedule.day_date.isoformat()),
                        None
                    )
                    if day_data:
                        plan_day_id = day_data['id']
                        for topic_breakdown in day_schedule.topic_breakdown:
                            day_topic_data = {
                                'plan_day_id': plan_day_id,
                                'subject': subject_name,
                                'topic_id': topic_breakdown['topic_id'],
                                'flashcards_planned': topic_breakdown['flashcards_planned'],
                                'topicals_planned': topic_breakdown['topicals_planned'],
                                'lesson_planned': topic_breakdown['lesson_planned']
                            }
                            day_topics.append(day_topic_data)
                
                if day_topics:
                    self.client.table('study_plan_day_topics_v2').insert(day_topics).execute()
        
        return self.get_study_plan_details(plan_id, user_id)
    
    def recompute_schedule(self, plan_id: str, user_id: str) -> Optional[Dict]:
        """Regenerate schedule from stored plan config"""
        if not self.enabled:
            return None
        
        existing = self.get_study_plan_details(plan_id, user_id)
        if not existing:
            return None
        
        # Delete old schedule
        day_ids_response = self.client.table('study_plan_days_v2')\
            .select('id')\
            .eq('plan_id', plan_id)\
            .execute()
        
        day_ids = [d['id'] for d in (day_ids_response.data or [])]
        
        if day_ids:
            self.client.table('study_plan_day_topics_v2')\
                .delete()\
                .in_('plan_day_id', day_ids)\
                .execute()
        
        self.client.table('study_plan_days_v2')\
            .delete()\
            .eq('plan_id', plan_id)\
            .execute()
        
        # Regenerate
        start_date = datetime.fromisoformat(existing['start_date']).date()
        exam_date = datetime.fromisoformat(existing['exam_date']).date()
        topic_ids = existing['topics']
        
        requirements = ContentRequirements(
            flash_per_topic=existing['flash_per_topic'],
            topical_per_topic=existing['topical_per_topic'],
            lessons_per_topic=existing['lessons_per_topic'],
            mocks_total=existing['mocks_total'],
            rule_band=existing['rule_band']
        )
        
        day_schedules = self.generate_schedule(
            start_date,
            exam_date,
            topic_ids,
            requirements
        )
        
        # Get subject name from existing plan
        subject_id = existing.get('subject_id')
        subject_name = self._get_subject_name(subject_id) if subject_id else 'Unknown Subject'
        
        # Insert new schedule
        plan_days = []
        for day_schedule in day_schedules:
            plan_day_data = {
                'plan_id': plan_id,
                'subject': subject_name,
                'day_date': day_schedule.day_date.isoformat(),
                'day_index': day_schedule.day_index,
                'planned_flashcards': day_schedule.planned_flashcards,
                'planned_topicals': day_schedule.planned_topicals,
                'planned_lessons': day_schedule.planned_lessons,
                'planned_mock': day_schedule.planned_mock,
                'planned_minutes': day_schedule.planned_minutes
            }
            plan_days.append(plan_day_data)
        
        if plan_days:
            days_response = self.client.table('study_plan_days_v2').insert(plan_days).execute()
            
            day_topics = []
            for day_schedule in day_schedules:
                day_data = next(
                    (d for d in days_response.data if d['day_date'] == day_schedule.day_date.isoformat()),
                    None
                )
                if day_data:
                    plan_day_id = day_data['id']
                    for topic_breakdown in day_schedule.topic_breakdown:
                        day_topic_data = {
                            'plan_day_id': plan_day_id,
                            'subject': subject_name,
                            'topic_id': topic_breakdown['topic_id'],
                            'flashcards_planned': topic_breakdown['flashcards_planned'],
                            'topicals_planned': topic_breakdown['topicals_planned'],
                            'lesson_planned': topic_breakdown['lesson_planned']
                        }
                        day_topics.append(day_topic_data)
            
            if day_topics:
                self.client.table('study_plan_day_topics_v2').insert(day_topics).execute()
        
        return self.get_study_plan_details(plan_id, user_id)

