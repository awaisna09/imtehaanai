"""
Minimal Tutor Enhance Worker

A simple worker process that processes tutor_enhance jobs from a minimal Redis queue.
No workload isolation, no processing markers, no concurrency - processes one job at a time.
"""

import time
import signal
import logging
import sys
from typing import Dict, Any, Optional
from datetime import datetime

from services.minimal_tutor_enhance_queue import MinimalTutorEnhanceQueue
from agents.services.tutor_enhancement_service import TutorEnhancementService
from services.supabase_client import get_supabase_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Worker configuration
WORKER_POLL_TIMEOUT = 5  # seconds
WORKER_SLEEP_ON_ERROR = 1  # seconds


class MinimalTutorEnhanceWorker:
    """
    Minimal worker for tutor_enhance jobs.
    
    Features:
    - Simple dequeue → process → mark completed/failed
    - No workload isolation
    - No processing markers
    - No concurrency (one job at a time)
    - No re-enqueueing due to limits
    """
    
    def __init__(self):
        self.queue = MinimalTutorEnhanceQueue()
        self.running = False
        self.processed_count = 0
        self.failed_count = 0
        
        # Initialize Supabase client
        self.supabase = get_supabase_client()
        if not self.supabase:
            raise RuntimeError("Supabase client not available")
        
        # Initialize enhancement service
        self.enhancement_service = TutorEnhancementService(self.supabase)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def _store_enhancements(
        self,
        assistant_message_id: str,
        user_id: str,
        conversation_id: str,
        topic_id: str,
        subject_id: Optional[int],
        result: Dict[str, Any],
        correlation_id: Optional[str] = None,
        job_id: Optional[str] = None
    ):
        """
        Store enhancement results in tutor_enhancements table.
        
        Args:
            assistant_message_id: Assistant message ID (primary key)
            user_id: User identifier
            conversation_id: Conversation identifier
            topic_id: Topic identifier
            subject_id: Subject identifier
            result: Enhancement results from process_enhancement_job
        """
        try:
            # Prepare enhancement data
            # Note: Supabase handles JSONB conversion automatically, so we pass Python dicts/lists directly
            enhancement_data = {
                'assistant_message_id': assistant_message_id,
                'user_id': user_id,
                'conversation_id': conversation_id,
                'topic_id': topic_id,
                'subject_id': subject_id,
                'mastery_updates': result.get('mastery_updates', []),  # JSONB - pass as list/dict
                'readiness': result.get('readiness'),  # JSONB - pass as dict
                'learning_path': result.get('learning_path'),  # JSONB - pass as dict
                'concept_ids': result.get('concept_ids', []),  # TEXT[] - pass as list
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # Remove None values (but keep empty lists/arrays)
            enhancement_data = {
                k: v for k, v in enhancement_data.items() 
                if v is not None
            }
            
            # Upsert (insert or update) enhancement record
            # Use upsert to handle case where enhancement is computed multiple times
            db_start_time = time.time()
            response = self.supabase.table('tutor_enhancements').upsert(
                enhancement_data,
                on_conflict='assistant_message_id'
            ).execute()
            db_duration_ms = int((time.time() - db_start_time) * 1000)
            
            if response.data:
                # Structured JSON logging for DB write success
                try:
                    import json
                    from services.structured_logging import structured_logger
                    log_data = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "INFO",
                        "message": "Enhancements stored in database",
                        "event": "db_write_success",
                        "context": {
                            "job_id": job_id,
                            "correlation_id": correlation_id,
                            "conversation_id": conversation_id,
                            "assistant_message_id": assistant_message_id,
                            "table": "tutor_enhancements",
                            "operation": "upsert",
                            "duration_ms": db_duration_ms,
                            "success": True
                        }
                    }
                    structured_logger.logger.info(json.dumps(log_data, default=str))
                except Exception:
                    pass  # Non-critical
                
                logger.info(f"[WORKER] Stored enhancement for assistant_message_id: {assistant_message_id}, correlation_id: {correlation_id}, duration_ms: {db_duration_ms}")
            else:
                # Structured JSON logging for DB write failure (no data returned)
                try:
                    import json
                    from services.structured_logging import structured_logger
                    log_data = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "level": "WARNING",
                        "message": "Enhancements DB write returned no data",
                        "event": "db_write_failure",
                        "context": {
                            "job_id": job_id,
                            "correlation_id": correlation_id,
                            "conversation_id": conversation_id,
                            "assistant_message_id": assistant_message_id,
                            "table": "tutor_enhancements",
                            "operation": "upsert",
                            "duration_ms": db_duration_ms,
                            "success": False,
                            "error": "No data returned from Supabase"
                        }
                    }
                    structured_logger.logger.warning(json.dumps(log_data, default=str))
                except Exception:
                    pass  # Non-critical
                
                logger.warning(f"[WORKER] No data returned when storing enhancement for {assistant_message_id}, correlation_id: {correlation_id}")
                
        except Exception as e:
            # Structured JSON logging for DB write failure (exception)
            try:
                import json
                from services.structured_logging import structured_logger
                log_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": "ERROR",
                    "message": "Enhancements DB write failed",
                    "event": "db_write_failure",
                    "context": {
                        "job_id": job_id,
                        "correlation_id": correlation_id,
                        "conversation_id": conversation_id,
                        "assistant_message_id": assistant_message_id,
                        "table": "tutor_enhancements",
                        "operation": "upsert",
                        "success": False,
                        "error": str(e)
                    }
                }
                structured_logger.logger.error(json.dumps(log_data, default=str))
            except Exception:
                pass  # Non-critical
            
            logger.error(f"[WORKER] Error storing enhancements: {e}, correlation_id: {correlation_id}")
            import traceback
            logger.error(f"[WORKER] Traceback: {traceback.format_exc()}")
            raise
    
    def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single tutor_enhance job.
        
        Args:
            job_data: Job data from queue
            
        Returns:
            Enhancement results
        """
        job_id = job_data.get('job_id')
        data = job_data.get('data', {})
        correlation_id = data.get('correlation_id', 'unknown')
        
        # Include correlation_id in all log lines
        logger.info(f"[WORKER] Processing tutor_enhance job {job_id}, correlation_id: {correlation_id}")
        start_time = time.time()
        
        try:
            # Extract job parameters
            user_id = data.get('user_id')
            conversation_id = data.get('conversation_id')
            topic_id = data.get('topic_id')
            subject_id = data.get('subject_id')
            user_message_id = data.get('user_message_id')
            assistant_message_id = data.get('assistant_message_id')
            
            if not user_id or not conversation_id or not topic_id:
                raise ValueError(
                    f"Missing required fields: user_id={user_id}, "
                    f"conversation_id={conversation_id}, topic_id={topic_id}"
                )
            
            # Process enhancement job
            result = self.enhancement_service.process_enhancement_job(
                user_id=user_id,
                conversation_id=conversation_id,
                topic_id=topic_id,
                subject_id=subject_id,
                assistant_message_id=assistant_message_id,
                user_message_id=user_message_id,
                correlation_id=correlation_id
            )
            
            elapsed = time.time() - start_time
            logger.info(
                f"[WORKER] Job {job_id} completed successfully in {elapsed:.2f}s, "
                f"correlation_id: {correlation_id}"
            )
            
            return result
            
        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)
            logger.error(
                f"[WORKER] Job {job_id} failed after {elapsed:.2f}s: {error_msg}, "
                f"correlation_id: {correlation_id}"
            )
            import traceback
            logger.error(f"[WORKER] Traceback (correlation_id: {correlation_id}): {traceback.format_exc()}")
            raise
    
    def run(self):
        """
        Main worker loop.
        
        Continuously dequeues jobs, processes them, and marks them as completed/failed.
        Runs one job at a time with no concurrency.
        """
        if not self.queue.is_available():
            logger.error("Redis queue not available, cannot start worker")
            return
        
        logger.info("Starting minimal tutor_enhance worker...")
        logger.info(f"Queue: {self.queue.queue_name}")
        logger.info(f"Poll timeout: {WORKER_POLL_TIMEOUT}s")
        logger.info("Processing one job at a time (no concurrency)")
        
        self.running = True
        
        while self.running:
            try:
                # Dequeue job (blocking)
                job_data = self.queue.dequeue_job(timeout=WORKER_POLL_TIMEOUT)
                
                if job_data:
                    job_id = job_data.get('job_id')
                    data = job_data.get('data', {})
                    correlation_id = data.get('correlation_id', 'unknown')
                    assistant_message_id = data.get('assistant_message_id')
                    conversation_id = data.get('conversation_id')
                    
                    # Structured JSON logging for dequeue start
                    try:
                        import json
                        from services.structured_logging import structured_logger
                        log_data = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "level": "INFO",
                            "message": "Worker dequeued tutor_enhance job",
                            "event": "worker_dequeue_start",
                            "context": {
                                "job_id": job_id,
                                "job_type": "tutor_enhance",
                                "correlation_id": correlation_id,
                                "conversation_id": conversation_id,
                                "assistant_message_id": assistant_message_id,
                                "user_id": data.get('user_id'),
                                "topic_id": data.get('topic_id'),
                                "subject_id": data.get('subject_id')
                            }
                        }
                        structured_logger.logger.info(json.dumps(log_data, default=str))
                    except Exception:
                        pass  # Non-critical
                    
                    logger.info(f"[WORKER] Dequeued job {job_id}, correlation_id: {correlation_id}, assistant_message_id: {assistant_message_id}")
                    
                    try:
                        # Extract job parameters
                        user_id = data.get('user_id')
                        conversation_id = data.get('conversation_id')
                        topic_id = data.get('topic_id')
                        subject_id = data.get('subject_id')
                        user_message_id = data.get('user_message_id')
                        
                        # Process job
                        result = self.process_job(job_data)
                        
                        # Structured JSON logging for enhancements computed
                        try:
                            import json
                            from services.structured_logging import structured_logger
                            log_data = {
                                "timestamp": datetime.utcnow().isoformat(),
                                "level": "INFO",
                                "message": "Enhancements computed",
                                "event": "enhancements_computed",
                                "context": {
                                    "job_id": job_id,
                                    "correlation_id": correlation_id,
                                    "conversation_id": conversation_id,
                                    "assistant_message_id": assistant_message_id,
                                    "user_id": user_id,
                                    "topic_id": topic_id,
                                    "subject_id": subject_id,
                                    "has_mastery_updates": bool(result.get('mastery_updates')),
                                    "has_readiness": bool(result.get('readiness')),
                                    "has_learning_path": bool(result.get('learning_path')),
                                    "concept_ids_count": len(result.get('concept_ids', []))
                                }
                            }
                            structured_logger.logger.info(json.dumps(log_data, default=str))
                        except Exception:
                            pass  # Non-critical
                        
                        logger.info(f"[WORKER] Enhancements computed for job {job_id}, correlation_id: {correlation_id}, assistant_message_id: {assistant_message_id}")
                        
                        # Store enhancements in database
                        if assistant_message_id and result:
                            try:
                                self._store_enhancements(
                                    assistant_message_id=assistant_message_id,
                                    user_id=user_id,
                                    conversation_id=conversation_id,
                                    topic_id=topic_id,
                                    subject_id=subject_id,
                                    result=result,
                                    correlation_id=correlation_id,
                                    job_id=job_id
                                )
                                logger.info(f"[WORKER] Stored enhancements for assistant_message_id: {assistant_message_id}, correlation_id: {correlation_id}")
                            except Exception as store_error:
                                logger.error(f"[WORKER] Failed to store enhancements (non-fatal): {store_error}, correlation_id: {correlation_id}")
                                # Continue - job result is still stored in queue
                        
                        # Mark as completed
                        self.queue.mark_job_completed(job_id, result)
                        self.processed_count += 1
                        
                        # Structured JSON logging for dequeue end (success)
                        try:
                            import json
                            from services.structured_logging import structured_logger
                            log_data = {
                                "timestamp": datetime.utcnow().isoformat(),
                                "level": "INFO",
                                "message": "Worker completed tutor_enhance job",
                                "event": "worker_dequeue_end",
                                "context": {
                                    "job_id": job_id,
                                    "correlation_id": correlation_id,
                                    "conversation_id": conversation_id,
                                    "assistant_message_id": assistant_message_id,
                                    "success": True,
                                    "processed_count": self.processed_count,
                                    "failed_count": self.failed_count
                                }
                            }
                            structured_logger.logger.info(json.dumps(log_data, default=str))
                        except Exception:
                            pass  # Non-critical
                        
                        logger.info(
                            f"[WORKER] ✅ Job {job_id} completed, correlation_id: {correlation_id}. "
                            f"Total processed: {self.processed_count}, "
                            f"Total failed: {self.failed_count}"
                        )
                        
                    except Exception as e:
                        # Mark as failed
                        self.queue.mark_job_failed(job_id, str(e))
                        self.failed_count += 1
                        
                        # Structured JSON logging for dequeue end (failure)
                        try:
                            import json
                            from services.structured_logging import structured_logger
                            log_data = {
                                "timestamp": datetime.utcnow().isoformat(),
                                "level": "ERROR",
                                "message": "Worker failed tutor_enhance job",
                                "event": "worker_dequeue_end",
                                "context": {
                                    "job_id": job_id,
                                    "correlation_id": correlation_id,
                                    "conversation_id": conversation_id,
                                    "assistant_message_id": assistant_message_id,
                                    "success": False,
                                    "error": str(e),
                                    "processed_count": self.processed_count,
                                    "failed_count": self.failed_count
                                }
                            }
                            structured_logger.logger.error(json.dumps(log_data, default=str))
                        except Exception:
                            pass  # Non-critical
                        
                        logger.error(
                            f"[WORKER] ❌ Job {job_id} failed: {e}, correlation_id: {correlation_id}. "
                            f"Total processed: {self.processed_count}, "
                            f"Total failed: {self.failed_count}"
                        )
                        
                        # Continue processing next job (don't crash on single job failure)
                        time.sleep(WORKER_SLEEP_ON_ERROR)
                
                # If no job, continue loop (will block on next dequeue)
                
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                self.running = False
                break
                
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                time.sleep(WORKER_SLEEP_ON_ERROR)
        
        logger.info("Worker stopped")
        logger.info(f"Total jobs processed: {self.processed_count}")
        logger.info(f"Total jobs failed: {self.failed_count}")


def main():
    """Entry point for worker process"""
    try:
        worker = MinimalTutorEnhanceWorker()
        worker.run()
    except Exception as e:
        logger.error(f"Failed to start worker: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
