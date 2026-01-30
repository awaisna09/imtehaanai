"""
AI Job Worker Service
Processes AI jobs from Redis queue asynchronously
"""

import os
import sys
import json
import signal
import time
import traceback
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv('config.env')

# Job timeout configuration (per job type)
JOB_TIMEOUT_TUTOR_CHAT = int(os.getenv("JOB_TIMEOUT_TUTOR_CHAT", "300"))  # 5 minutes default

# Import Redis queue service
from services.redis_queue import (
    job_queue, QUEUE_TUTOR, QUEUE_GRADING, QUEUE_MOCK_EXAM, QUEUE_HELPING, QUEUE_LESSON
)

# Import AI agents and workflows
try:
    from langgraph_tutor import run_tutor_graph
    from agents.mock_exam_grading_agent import run_mock_exam_graph, MockExamGradingAgent
    from agents.answer_grading_agent import AnswerGradingAgent
    AI_WORKFLOWS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ AI workflows not available: {e}")
    AI_WORKFLOWS_AVAILABLE = False


class AIWorker:
    """Worker that processes AI jobs from Redis queues"""
    
    def __init__(self, worker_id: str = None, queues: list = None):
        self.worker_id = worker_id or f"worker-{os.getpid()}"
        self.queues = queues or [QUEUE_TUTOR, QUEUE_GRADING, QUEUE_MOCK_EXAM, QUEUE_HELPING, QUEUE_LESSON]
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        
        # Initialize agents (lazy loading)
        self.tutor_agent = None
        self.grading_agent = None
        self.mock_exam_agent = None
        self.helping_agent = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print(f"🚀 AI Worker initialized: {self.worker_id}")
        print(f"📋 Monitoring queues: {self.queues}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def _initialize_agents(self):
        """Lazy initialization of AI agents"""
        if not AI_WORKFLOWS_AVAILABLE:
            return
        
        try:
            # Get OpenAI API key from environment
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                print("⚠️ OPENAI_API_KEY not found in environment - agents will not work")
                return
            
            # Initialize grading agent if not already done
            if self.grading_agent is None:
                from agents.answer_grading_agent import AnswerGradingAgent
                self.grading_agent = AnswerGradingAgent(api_key=api_key)
                print("✅ Grading agent initialized")
            
            # Initialize mock exam agent if not already done
            if self.mock_exam_agent is None:
                from agents.mock_exam_grading_agent import MockExamGradingAgent
                self.mock_exam_agent = MockExamGradingAgent(api_key=api_key)
                print("✅ Mock exam grading agent initialized")
            
            # Initialize helping agent if not already done
            if self.helping_agent is None:
                from agents.helping_agent import HelpingAgent
                self.helping_agent = HelpingAgent(api_key=api_key)
                print("✅ Helping agent initialized")
                
        except Exception as e:
            print(f"⚠️ Failed to initialize agents: {e}")
    
    def process_tutor_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process AI tutor chat job"""
        job_id = job_data['job_id']
        data = job_data.get('data', {})
        correlation_id = data.get('correlation_id', 'unknown')
        start_time = time.time()
        logger.info(f"[WORKER] Starting tutor job {job_id}, correlation_id: {correlation_id}")
        
        # Structured logging for job start processing
        try:
            from services.structured_logging import structured_logger
            timeout_seconds = job_data.get('timeout') or JOB_TIMEOUT_TUTOR_CHAT
            structured_logger.log_tutor_job_start_processing(
                job_id=job_id,
                correlation_id=correlation_id,
                worker_id=self.worker_id,
                timeout_seconds=timeout_seconds
            )
        except Exception:
            pass  # Non-critical
        
        try:
            # Update progress with correlation_id
            job_queue.update_job_status(
                job_id,
                'processing',
                progress=10,
                message='Initializing tutor agent',
                correlation_id=correlation_id
            )
            
            # Get timeout from job payload if available, else use job-type specific env var
            timeout_seconds = job_data.get('timeout') or JOB_TIMEOUT_TUTOR_CHAT
            logger.info(f"[WORKER] Tutor job {job_id}, correlation_id: {correlation_id}, using timeout: {timeout_seconds}s (from job payload: {job_data.get('timeout') is not None}, env default: {JOB_TIMEOUT_TUTOR_CHAT}s)")
            
            # Add timeout protection with configurable timeout
            import threading
            
            timeout_occurred = threading.Event()
            result_container = {'result': None, 'exception': None, 'started': False}
            
            def run_with_timeout():
                """Run tutor graph in a separate thread"""
                try:
                    result_container['started'] = True
                    logger.info(f"[WORKER] Tutor graph thread started for job {job_id}, correlation_id: {correlation_id}")
                    result = run_tutor_graph(
                        user_id=data['user_id'],
                        topic=str(data['topic']),
                        message=data['message'],
                        conversation_id=data.get('conversation_id'),
                        explanation_style=data.get('explanation_style', 'default'),
                        subject_id=data.get('subject_id'),
                        conversation_history=data.get('conversation_history'),
                        job_id=job_id,  # Pass job_id for logging
                        correlation_id=correlation_id  # Pass correlation_id for end-to-end tracing
                    )
                    # Always set result, even if None (graph may return fallback on timeout)
                    result_container['result'] = result
                    elapsed = time.time() - start_time
                    logger.info(f"[WORKER] Tutor graph completed for job {job_id}, correlation_id: {correlation_id}, in {elapsed:.2f}s")
                except Exception as e:
                    result_container['exception'] = e
                    elapsed = time.time() - start_time
                    logger.error(f"[WORKER] Tutor graph failed for job {job_id}, correlation_id: {correlation_id}, after {elapsed:.2f}s: {e}")
                    import traceback
                    logger.error(f"[WORKER] Traceback: {traceback.format_exc()}")
            
            # Start tutor graph in a thread
            tutor_thread = threading.Thread(target=run_with_timeout, daemon=True)
            tutor_thread.start()
            
            # Wait for thread to start (max 5 seconds)
            wait_start = time.time()
            while not result_container['started'] and (time.time() - wait_start) < 5:
                time.sleep(0.1)
            
            if not result_container['started']:
                logger.error(f"[WORKER] Tutor graph thread did not start for job {job_id}")
                raise RuntimeError(f"Tutor graph thread failed to start for job {job_id}")
            
            logger.info(f"[WORKER] Waiting for tutor graph to complete (timeout: {timeout_seconds}s)")
            tutor_thread.join(timeout=timeout_seconds)
            elapsed = time.time() - start_time
            
            # Track whether timeout fired
            timeout_fired = False
            
            # CRITICAL: Check for result first, even if thread is still alive
            # The graph may have returned a fallback response on timeout
            # Also check if thread is still alive to detect timeouts
            if result_container['result'] is not None:
                result = result_container['result']
                logger.info(f"[WORKER] Tutor job {job_id}, correlation_id: {correlation_id}, completed successfully in {elapsed:.2f}s (timeout: {timeout_seconds}s, timeout_fired: False)")
                
                # Structured logging for job completion
                try:
                    from services.structured_logging import structured_logger
                    structured_logger.log_job_complete(
                        job_id=job_id,
                        job_type='tutor_chat',
                        duration_seconds=elapsed,
                        correlation_id=correlation_id,
                        user_id=data.get('user_id'),
                        conversation_id=data.get('conversation_id')
                    )
                except Exception:
                    pass  # Non-critical
            elif tutor_thread.is_alive():
                # Thread is still running and no result - true timeout
                timeout_fired = True
                timeout_occurred.set()
                error_msg = f"Tutor job {job_id} timed out after {timeout_seconds} seconds (elapsed: {elapsed:.2f}s)"
                logger.error(f"[TIMEOUT] {error_msg}, correlation_id: {correlation_id}")
                logger.error(f"[TIMEOUT] Thread is still alive, graph may be stuck, correlation_id: {correlation_id}")
                logger.error(f"[TIMEOUT] Duration: {elapsed:.2f}s, Timeout threshold: {timeout_seconds}s, Timeout fired: True, correlation_id: {correlation_id}")
                
                # Structured logging for job timeout
                try:
                    from services.structured_logging import structured_logger
                    structured_logger.log_job_timeout(
                        job_id=job_id,
                        job_type='tutor_chat',
                        timeout_seconds=timeout_seconds,
                        elapsed_seconds=elapsed,
                        correlation_id=correlation_id,
                        user_id=data.get('user_id'),
                        conversation_id=data.get('conversation_id')
                    )
                except Exception:
                    pass  # Non-critical
                
                raise TimeoutError(error_msg)
            elif result_container['exception']:
                # Check for exceptions
                logger.error(f"[WORKER] Tutor job {job_id}, correlation_id: {correlation_id}, failed with exception after {elapsed:.2f}s (timeout: {timeout_seconds}s, timeout_fired: False)")
                raise result_container['exception']
            else:
                # No result, no exception, thread completed - unexpected state
                # This shouldn't happen, but handle gracefully
                error_msg = f"Tutor job {job_id} completed but returned no result (elapsed: {elapsed:.2f}s)"
                logger.error(f"[ERROR] {error_msg}, correlation_id: {correlation_id}")
                logger.error(f"[ERROR] Duration: {elapsed:.2f}s, Timeout threshold: {timeout_seconds}s, Timeout fired: False, correlation_id: {correlation_id}")
                raise RuntimeError(error_msg)
            
            # Serialize result
            if isinstance(result, dict):
                result_dict = json.loads(json.dumps(result, default=str))
            else:
                result_dict = {'response': str(result)}
            
            job_queue.update_job_status(
                job_id,
                'completed',
                progress=100,
                message='Tutor response generated successfully',
                result=result_dict,
                correlation_id=correlation_id
            )
            
            print(f"✅ Tutor job {job_id} completed successfully, correlation_id: {correlation_id}")
            logger.info(f"[WORKER] Tutor job {job_id}, correlation_id: {correlation_id}, completed successfully")
            return result_dict
            
        except TimeoutError as e:
            error_msg = str(e)
            elapsed = time.time() - start_time
            timeout_seconds_used = job_data.get('timeout') or JOB_TIMEOUT_TUTOR_CHAT
            print(f"⏱️ Tutor job {job_id} timed out: {error_msg}, correlation_id: {correlation_id}")
            logger.error(f"[TIMEOUT] Tutor job {job_id}, correlation_id: {correlation_id} - Duration: {elapsed:.2f}s, Timeout threshold: {timeout_seconds_used}s, Timeout fired: True")
            
            # Ensure timeout status is updated
            job_queue.update_job_status(
                job_id,
                'timeout',  # Use 'timeout' status so frontend can detect it
                error=f"Job timed out after {timeout_seconds_used} seconds: {error_msg}",
                progress=0,
                correlation_id=correlation_id
            )
            raise
        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            elapsed = time.time() - start_time
            error_type = type(e).__name__
            print(f"❌ Tutor job {job_id} failed: {error_msg}, correlation_id: {correlation_id}")
            print(f"Traceback: {error_trace}")
            logger.error(f"[WORKER] Tutor job {job_id}, correlation_id: {correlation_id}, failed with error: {error_msg}")
            
            # Structured logging for job failure
            try:
                from services.structured_logging import structured_logger
                structured_logger.log_job_failure(
                    job_id=job_id,
                    job_type='tutor_chat',
                    error=error_msg,
                    retry_count=job_data.get('retry_count', 0),
                    correlation_id=correlation_id,
                    user_id=data.get('user_id'),
                    conversation_id=data.get('conversation_id'),
                    elapsed_seconds=elapsed,
                    error_type=error_type
                )
            except Exception:
                pass  # Non-critical
            
            # CRITICAL: Always update job status, even on error
            try:
                job_queue.update_job_status(
                    job_id,
                    'failed',
                    error=error_msg,
                    progress=0,
                    correlation_id=correlation_id
                )
            except Exception as update_error:
                print(f"⚠️ Failed to update job status: {update_error}")
                logger.error(f"[WORKER] Failed to update job status for {job_id}, correlation_id: {correlation_id}: {update_error}")
            
            raise
    
    def process_tutor_enhance_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process tutor enhancement job (mastery/readiness/learning_path/analytics)
        This job runs after the main tutor response has been returned to the user.
        
        Payload structure:
        {
            'user_id': str,
            'conversation_id': str,
            'topic_id': str,
            'subject_id': int,
            'user_message_id': str (optional),
            'assistant_message_id': str (optional),
            'correlation_id': str
        }
        
        CRITICAL REQUIREMENTS:
        1. Failures in this job do NOT affect the tutor chat response (already returned)
        2. Workers should be allowed to fail without breaking chat - all errors caught
        3. Enhancements should be idempotent per assistant_message_id - check for existing processing
        """
        job_id = job_data['job_id']
        data = job_data.get('data', {})
        correlation_id = data.get('correlation_id', 'unknown')
        start_time = time.time()
        
        # Structured JSON logging for enhancement job start
        try:
            import json
            from datetime import datetime
            log_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": "INFO",
                "message": "Tutor enhancement job started",
                "event": "enhancement_start",
                "context": {
                    "job_id": job_id,
                    "job_type": "tutor_enhance",
                    "correlation_id": correlation_id,
                    "worker_id": self.worker_id,
                    "user_id": user_id,
                    "conversation_id": conversation_id,
                    "topic_id": topic_id,
                    "subject_id": subject_id,
                    "timeout_seconds": 300
                }
            }
            logger.info(json.dumps(log_data, default=str))
            
            # Also use structured_logger method for compatibility
            from services.structured_logging import structured_logger
            structured_logger.log_tutor_job_start_processing(
                job_id=job_id,
                correlation_id=correlation_id,
                worker_id=self.worker_id,
                timeout_seconds=300  # 5 minutes for enhancement job
            )
        except Exception:
            pass  # Non-critical
        
        logger.info(f"[WORKER] Starting tutor enhancement job {job_id}, correlation_id: {correlation_id}")
        
        # Extract payload fields first
        user_id = data.get('user_id')
        conversation_id = data.get('conversation_id')
        topic_id = data.get('topic_id')
        subject_id = data.get('subject_id')
        user_message_id = data.get('user_message_id')
        assistant_message_id = data.get('assistant_message_id')
        
        # CRITICAL: Check idempotency - if enhancement already processed for this assistant_message_id, skip
        # This ensures retries are safe and won't duplicate mastery/readiness updates
        if assistant_message_id:
            try:
                # Check if enhancement already completed for this message
                # Use Redis to track processed enhancements
                from services.redis_connection import get_redis_client
                redis_client = get_redis_client()
                if redis_client:
                    enhancement_key = f"enhancement:completed:{assistant_message_id}"
                    if redis_client.exists(enhancement_key):
                        logger.info(f"[WORKER] Enhancement already processed for assistant_message_id {assistant_message_id}, skipping (idempotent), correlation_id: {correlation_id}")
                        # Return existing result if available, or empty result
                        job_queue.update_job_status(
                            job_id,
                            'completed',
                            progress=100,
                            message='Enhancement already processed (idempotent)',
                            result={"idempotent": True, "assistant_message_id": assistant_message_id},
                            correlation_id=correlation_id
                        )
                        return {"idempotent": True, "assistant_message_id": assistant_message_id}
            except Exception as idempotency_check_error:
                # Non-critical - continue processing if idempotency check fails
                logger.warning(f"[WORKER] Idempotency check failed (non-fatal): {idempotency_check_error}, correlation_id: {correlation_id}")
        
        try:
            # Update progress with correlation_id
            job_queue.update_job_status(
                job_id,
                'processing',
                progress=10,
                message='Computing mastery, readiness, and learning path',
                correlation_id=correlation_id
            )
            
            if not user_id or not conversation_id or not topic_id:
                raise ValueError(f"Missing required fields: user_id={user_id}, conversation_id={conversation_id}, topic_id={topic_id}")
            
            from agents.services.tutor_enhancement_service import TutorEnhancementService
            from services.supabase_client import get_supabase_client
            
            supabase = get_supabase_client()
            if not supabase:
                raise RuntimeError("Supabase client not available")
            
            # Use the new enhancement service
            enhancement_service = TutorEnhancementService(supabase)
            
            # Process enhancement job - this reads conversation/message data independently
            # and computes all enhancements (mastery, readiness, learning_path)
            result = enhancement_service.process_enhancement_job(
                user_id=user_id,
                conversation_id=conversation_id,
                topic_id=topic_id,
                subject_id=subject_id,
                assistant_message_id=assistant_message_id,
                user_message_id=user_message_id,
                correlation_id=correlation_id
            )
            
            logger.info(f"[WORKER] Enhancement computations completed, correlation_id: {correlation_id}")
            
            elapsed = time.time() - start_time
            
            # Mark enhancement as completed for idempotency (per assistant_message_id)
            if assistant_message_id:
                try:
                    from services.redis_connection import get_redis_client
                    redis_client = get_redis_client()
                    if redis_client:
                        enhancement_key = f"enhancement:completed:{assistant_message_id}"
                        # Store for 7 days (604800 seconds) to prevent duplicate processing
                        redis_client.setex(enhancement_key, 604800, "completed")
                except Exception as mark_error:
                    # Non-critical - log but don't fail
                    logger.warning(f"[WORKER] Failed to mark enhancement as completed (non-fatal): {mark_error}, correlation_id: {correlation_id}")
            
            # Update job status in Redis with correlation_id
            job_queue.update_job_status(
                job_id,
                'completed',
                progress=100,
                message='Enhancement computations completed',
                result=result,
                correlation_id=correlation_id
            )
            
            # Structured JSON logging for enhancement job completion
            try:
                import json
                from datetime import datetime
                log_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "INFO",
                    "message": "Tutor enhancement job completed",
                    "event": "enhancement_end",
                    "context": {
                        "job_id": job_id,
                        "job_type": "tutor_enhance",
                        "correlation_id": correlation_id,
                        "worker_id": self.worker_id,
                        "user_id": user_id,
                        "conversation_id": conversation_id,
                        "topic_id": topic_id,
                        "subject_id": subject_id,
                        "duration_seconds": elapsed,
                        "mastery_updates_count": len(result.get("mastery_updates", [])),
                        "has_readiness": result.get("readiness") is not None,
                        "has_learning_path": result.get("learning_path") is not None,
                        "concepts_covered": len(result.get("concept_ids", []))
                    }
                }
                logger.info(json.dumps(log_data, default=str))
                
                # Also use structured_logger method for compatibility
                from services.structured_logging import structured_logger
                structured_logger.log_job_complete(
                    job_id=job_id,
                    job_type='tutor_enhance',
                    duration_seconds=elapsed,
                    correlation_id=correlation_id,
                    user_id=user_id,
                    conversation_id=conversation_id
                )
            except Exception:
                pass  # Non-critical
            
            logger.info(f"[WORKER] Tutor enhancement job {job_id}, correlation_id: {correlation_id}, completed in {elapsed:.2f}s")
            print(f"✅ Tutor enhancement job {job_id} completed successfully, correlation_id: {correlation_id}")
            return result
            
        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            elapsed = time.time() - start_time
            error_type = type(e).__name__
            
            # CRITICAL: Workers should be allowed to fail without breaking chat
            # The tutor response was already returned, so this failure is non-fatal
            logger.error(f"[WORKER] Tutor enhancement job {job_id}, correlation_id: {correlation_id}, failed (non-fatal, chat response already returned): {error_msg}")
            logger.error(f"[WORKER] Traceback: {error_trace}")
            print(f"❌ Tutor enhancement job {job_id} failed (non-fatal): {error_msg}, correlation_id: {correlation_id}")
            
            # Structured logging for job failure
            try:
                from services.structured_logging import structured_logger
                structured_logger.log_job_failure(
                    job_id=job_id,
                    job_type='tutor_enhance',
                    error_type=error_type,
                    error_message=error_msg,
                    duration_seconds=elapsed,
                    correlation_id=correlation_id,
                    user_id=data.get('user_id'),
                    conversation_id=data.get('conversation_id')
                )
            except Exception:
                pass  # Non-critical
            
            # Update job status in Redis with correlation_id
            try:
                job_queue.update_job_status(
                    job_id,
                    'failed',
                    error=error_msg,
                    progress=0,
                    correlation_id=correlation_id
                )
            except Exception as update_error:
                logger.error(f"[WORKER] Failed to update job status: {update_error}, correlation_id: {correlation_id}")
            
            # CRITICAL: Re-raise exception so worker can handle retry logic
            # But failures here don't affect the main tutor chat response (already returned)
            # This ensures workers can fail without breaking chat
            raise
    
    async def process_grading_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process single answer grading job"""
        try:
            data = job_data['data']
            
            # Initialize agent if needed
            self._initialize_agents()
            if self.grading_agent is None:
                raise Exception("Grading agent not available")
            
            job_queue.update_job_status(
                job_data['job_id'],
                'processing',
                progress=10,
                message='Grading answer'
            )
            
            # Grade answer
            result = self.grading_agent.grade_answer(
                question=data['question'],
                model_answer=data['model_answer'],
                student_answer=data['student_answer'],
                user_id=data.get('user_id'),
                max_marks=data.get('max_marks', 10),
                question_id=data.get('question_id'),
                topic_id=data.get('topic_id'),
                topic_name=data.get('topic_name'),
                difficulty_level=data.get('difficulty_level'),
                subject=data.get('subject')
            )
            
            # Convert result to dict
            if hasattr(result, 'model_dump'):
                result_dict = result.model_dump()
            elif hasattr(result, 'dict'):
                result_dict = result.dict()
            else:
                result_dict = result if isinstance(result, dict) else {'result': str(result)}
            
            job_queue.update_job_status(
                job_data['job_id'],
                'completed',
                progress=100,
                message='Answer graded successfully',
                result=result_dict
            )
            
            return result_dict
            
        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            print(f"❌ Grading job {job_data['job_id']} failed: {error_msg}")
            print(f"Traceback: {error_trace}")
            
            job_queue.update_job_status(
                job_data['job_id'],
                'failed',
                error=error_msg,
                progress=0
            )
            raise
    
    async def process_mock_exam_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process mock exam grading job"""
        try:
            data = job_data['data']
            
            # Initialize agent if needed
            self._initialize_agents()
            if self.mock_exam_agent is None:
                raise Exception("Mock exam grading agent not available")
            
            job_queue.update_job_status(
                job_data['job_id'],
                'processing',
                progress=5,
                message='Starting exam grading workflow'
            )
            
            # Run mock exam grading workflow
            report = await run_mock_exam_graph(
                agent=self.mock_exam_agent,
                user_id=data['user_id'],
                attempted_questions=data['attempted_questions'],
                request_id=job_data.get('request_id'),
                job_id=job_data['job_id'],
                subject=data.get('subject'),
                exam_type=data.get('exam_type')
            )
            
            # Convert report to dict
            if hasattr(report, 'model_dump'):
                report_dict = report.model_dump()
            elif hasattr(report, 'dict'):
                report_dict = report.dict()
            else:
                report_dict = report if isinstance(report, dict) else {'report': str(report)}
            
            job_queue.update_job_status(
                job_data['job_id'],
                'completed',
                progress=100,
                message='Exam graded successfully',
                result=report_dict
            )
            
            print(f"✅ Mock exam job {job_data['job_id']} completed: {report_dict.get('percentage_score', 'N/A')}%")
            return report_dict
            
        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            print(f"❌ Mock exam job {job_data['job_id']} failed: {error_msg}")
            print(f"Traceback: {error_trace}")
            
            job_queue.update_job_status(
                job_data['job_id'],
                'failed',
                error=error_msg,
                progress=0
            )
            raise
    
    async def process_helping_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process helping agent explanation job"""
        try:
            data = job_data['data']
            
            # Initialize agent if needed
            self._initialize_agents()
            if self.helping_agent is None:
                raise Exception("Helping agent not available")
            
            job_queue.update_job_status(
                job_data['job_id'],
                'processing',
                progress=10,
                message='Explaining concept'
            )
            
            # Get explanation from helping agent
            explanation = self.helping_agent.explain(
                query=data['query'],
                context=data.get('context'),
                subject=data.get('subject')
            )
            
            result_dict = {
                'explanation': explanation,
                'success': True
            }
            
            job_queue.update_job_status(
                job_data['job_id'],
                'completed',
                progress=100,
                message='Explanation generated successfully',
                result=result_dict
            )
            
            return result_dict
            
        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            print(f"❌ Helping job {job_data['job_id']} failed: {error_msg}")
            print(f"Traceback: {error_trace}")
            
            job_queue.update_job_status(
                job_data['job_id'],
                'failed',
                error=error_msg,
                progress=0
            )
            raise
    
    async def process_lesson_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process lesson creation job"""
        try:
            data = job_data['data']
            
            # Initialize tutor agent for lesson creation
            if self.tutor_agent is None:
                from agents.ai_tutor_agent import AITutorAgent
                self.tutor_agent = AITutorAgent()
            
            job_queue.update_job_status(
                job_data['job_id'],
                'processing',
                progress=10,
                message='Creating lesson'
            )
            
            # Get LLM service from agent
            services = self.tutor_agent.build_services()
            llm_service = services["llm"]
            
            # Generate lesson using LLMService
            lesson_data = llm_service.generate_lesson(
                topic=data['topic'],
                learning_objectives=data['learning_objectives'],
                difficulty_level=data.get('difficulty_level', 'intermediate')
            )
            
            result_dict = {
                'lesson_content': lesson_data.get('lesson_content', ''),
                'key_points': lesson_data.get('key_points', []),
                'practice_questions': lesson_data.get('practice_questions', []),
                'estimated_duration': lesson_data.get('estimated_duration', 30)
            }
            
            job_queue.update_job_status(
                job_data['job_id'],
                'completed',
                progress=100,
                message='Lesson created successfully',
                result=result_dict
            )
            
            return result_dict
            
        except Exception as e:
            error_msg = str(e)
            error_trace = traceback.format_exc()
            print(f"❌ Lesson job {job_data['job_id']} failed: {error_msg}")
            print(f"Traceback: {error_trace}")
            
            job_queue.update_job_status(
                job_data['job_id'],
                'failed',
                error=error_msg,
                progress=0
            )
            raise
    
    def process_job_sync(self, job_data: Dict[str, Any]):
        """Process a single job synchronously"""
        job_id = job_data['job_id']
        job_type = job_data['job_type']
        
        print(f"🔄 Processing job: {job_id} (type: {job_type})")
        start_time = time.time()
        
        try:
            import asyncio
            
            if job_type == 'tutor_chat':
                result = self.process_tutor_job(job_data)
            elif job_type == 'tutor_enhance':
                result = self.process_tutor_enhance_job(job_data)
            elif job_type == 'grade_answer':
                # Run async grading job
                result = asyncio.run(self.process_grading_job(job_data))
            elif job_type == 'grade_mock_exam':
                # Run async mock exam job
                result = asyncio.run(self.process_mock_exam_job(job_data))
            elif job_type == 'explain_concept':
                # Run async helping job
                result = asyncio.run(self.process_helping_job(job_data))
            elif job_type == 'create_lesson':
                # Run async lesson creation job
                result = asyncio.run(self.process_lesson_job(job_data))
            else:
                raise ValueError(f"Unknown job type: {job_type}")
            
            elapsed = time.time() - start_time
            self.processed_count += 1
            print(f"✅ Job {job_id} completed in {elapsed:.2f}s")
            return result
            
        except Exception as e:
            elapsed = time.time() - start_time
            self.error_count += 1
            print(f"❌ Job {job_id} failed after {elapsed:.2f}s: {e}")
            
            # Attempt retry
            queue_name_map = {
                'tutor_chat': QUEUE_TUTOR,
                'tutor_enhance': QUEUE_TUTOR,
                'grade_answer': QUEUE_GRADING,
                'grade_mock_exam': QUEUE_MOCK_EXAM,
                'explain_concept': QUEUE_HELPING,
                'create_lesson': QUEUE_LESSON
            }
            queue_name = queue_name_map.get(job_type)
            
            if queue_name and job_queue.retry_job(job_id, queue_name):
                print(f"🔄 Job {job_id} queued for retry")
            else:
                print(f"❌ Job {job_id} exceeded max retries")
            
            raise
    
    def run(self):
        """Main worker loop - continuously process jobs from queues"""
        if not AI_WORKFLOWS_AVAILABLE:
            print("❌ AI workflows not available, cannot start worker")
            return
        
        self.running = True
        self._initialize_agents()
        
        print(f"✅ Worker {self.worker_id} started")
        print(f"📊 Monitoring {len(self.queues)} queues")
        
        # Round-robin queue polling
        queue_index = 0
        
        while self.running:
            try:
                # Poll queues in round-robin fashion
                queue_name = self.queues[queue_index % len(self.queues)]
                queue_index += 1
                
                # Dequeue job (non-blocking with 1 second timeout)
                job_data = job_queue.dequeue_job(queue_name, timeout=1)
                
                if job_data:
                    job_id = job_data.get('job_id', 'unknown')
                    print(f"🔄 Worker {self.worker_id} picked up job: {job_id}")
                    # Process job (handles sync/async internally)
                    try:
                        self.process_job_sync(job_data)
                        print(f"✅ Worker {self.worker_id} completed job: {job_id}")
                        # Job completed successfully, continue to next job immediately
                    except Exception as e:
                        print(f"❌ Error processing job {job_id}: {e}")
                        traceback.print_exc()
                        # Continue processing other jobs even if one fails
                        # The job status should already be updated to 'failed' in process_job_sync
                else:
                    # No job available, small sleep to prevent CPU spinning
                    time.sleep(0.1)
                
                # Print stats every 100 iterations
                if (self.processed_count + self.error_count) % 100 == 0:
                    stats = job_queue.get_queue_stats()
                    print(f"📊 Stats: Processed={self.processed_count}, Errors={self.error_count}, "
                          f"Queues={stats}")
                
            except KeyboardInterrupt:
                print("\n🛑 Keyboard interrupt received")
                self.running = False
                break
            except Exception as e:
                print(f"❌ Worker error: {e}")
                traceback.print_exc()
                time.sleep(1)  # Brief pause before retrying
        
        print(f"🛑 Worker {self.worker_id} stopped")
        print(f"📊 Final stats: Processed={self.processed_count}, Errors={self.error_count}")


def main():
    """Entry point for worker process"""
    import argparse
    
    parser = argparse.ArgumentParser(description='AI Job Worker')
    parser.add_argument('--worker-id', type=str, help='Worker identifier')
    parser.add_argument('--queues', nargs='+', 
                       choices=['tutor', 'grading', 'mock_exam', 'helping', 'lesson'],
                       default=['tutor', 'grading', 'mock_exam', 'helping', 'lesson'],
                       help='Queues to monitor')
    
    args = parser.parse_args()
    
    # Map queue names
    queue_map = {
        'tutor': QUEUE_TUTOR,
        'grading': QUEUE_GRADING,
        'mock_exam': QUEUE_MOCK_EXAM,
        'helping': QUEUE_HELPING,
        'lesson': QUEUE_LESSON
    }
    queues = [queue_map[q] for q in args.queues]
    
    # Create and run worker
    worker = AIWorker(worker_id=args.worker_id, queues=queues)
    worker.run()


if __name__ == '__main__':
    main()
