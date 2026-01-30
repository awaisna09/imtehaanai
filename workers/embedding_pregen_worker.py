#!/usr/bin/env python3
"""
Embedding Pre-generation Background Worker
Runs as a background job to pre-generate embeddings for high-traffic topics/concepts.
"""

import os
import sys
import time
import logging
import signal
from typing import Dict, Any
from dotenv import load_dotenv

# Add parent directory to path for imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

load_dotenv('config.env')

# Import services (after path setup)
from services.embedding_pregen import get_pregen_service  # noqa: E402
from agents.concept_agent import ConceptAgent  # noqa: E402

logger = logging.getLogger(__name__)

# Configuration
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
PREGEN_INTERVAL_SECONDS = int(
    os.getenv("EMBEDDING_PREGEN_INTERVAL_SECONDS", 3600)
)  # Run every hour
PREGEN_BATCH_SIZE = int(
    os.getenv("EMBEDDING_PREGEN_BATCH_SIZE", 50)
)  # Generate 50 embeddings per run
ENABLE_PREGEN = os.getenv("ENABLE_EMBEDDING_PREGEN", "true").lower() == "true"


class EmbeddingPregenWorker:
    """
    Background worker that pre-generates embeddings for high-traffic topics.
    Runs periodically, rate-limited, and safe for incremental execution.
    """

    def __init__(self):
        """Initialize the pre-generation worker"""
        self.running = False
        self.concept_agent = None
        self.supabase = None
        self.pregen_service = None

        # Initialize Supabase client (singleton)
        try:
            from services.supabase_client import get_supabase_client
            self.supabase = get_supabase_client()
        except Exception as e:
            logger.error(f"Failed to initialize Supabase: {e}")
            self.supabase = None

        # Initialize ConceptAgent
        try:
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                self.concept_agent = ConceptAgent(api_key=api_key, supabase_client=self.supabase)
        except Exception as e:
            logger.error(f"Failed to initialize ConceptAgent: {e}")

        # Initialize pre-generation service
        if self.concept_agent:
            self.pregen_service = get_pregen_service(
                concept_agent=self.concept_agent,
                supabase_client=self.supabase
            )

    def run_once(self) -> Dict[str, Any]:
        """
        Run one iteration of pre-generation.

        Returns:
            Dict with generation statistics
        """
        # KILL SWITCH: Check if embedding pregen is disabled
        from services.job_kill_switch import job_kill_switch
        if job_kill_switch.is_job_disabled("embedding_pregen"):
            job_kill_switch.log_disabled_job("embedding_pregen")
            return {
                "status": "skipped",
                "reason": "kill_switch_active",
                "retry_delay_seconds": job_kill_switch.get_retry_delay()
            }
        
        if not self.pregen_service:
            logger.warning(
                "Pre-generation service not available - "
                "skipping pre-generation"
            )
            return {
                "status": "skipped",
                "reason": "service_not_available"
            }

        logger.info(
            "[EMBEDDING PREGEN] Starting background pre-generation run"
        )

        try:
            stats = self.pregen_service.run_background_pre_generation(
                max_embeddings=PREGEN_BATCH_SIZE
            )

            logger.info(
                f"[EMBEDDING PREGEN] Completed: generated={stats['total_generated']}, "
                f"skipped={stats['total_skipped']}, failed={stats['total_failed']}"
            )

            return {
                "status": "completed",
                **stats
            }

        except Exception as e:
            logger.error(
                f"[EMBEDDING PREGEN] Error during pre-generation: {e}",
                exc_info=True
            )
            return {
                "status": "failed",
                "error": str(e)
            }

    def run_loop(self) -> None:
        """Run the pre-generation loop continuously"""
        if not ENABLE_PREGEN:
            logger.info(
                "[EMBEDDING PREGEN] Pre-generation disabled via "
                "ENABLE_EMBEDDING_PREGEN"
            )
            return

        self.running = True
        logger.info(
            f"[EMBEDDING PREGEN] Worker started "
            f"(interval: {PREGEN_INTERVAL_SECONDS}s)"
        )

        # Run immediately on startup
        self.run_once()

        # Then run periodically
        while self.running:
            try:
                time.sleep(PREGEN_INTERVAL_SECONDS)
                if self.running:
                    self.run_once()
            except KeyboardInterrupt:
                logger.info("[EMBEDDING PREGEN] Received interrupt signal, shutting down")
                self.running = False
                break
            except Exception as e:
                logger.error(
                    f"[EMBEDDING PREGEN] Error in run loop: {e}",
                    exc_info=True
                )
                # Continue running despite errors
                time.sleep(60)  # Wait 1 min before retrying

    def stop(self) -> None:
        """Stop the worker"""
        logger.info("[EMBEDDING PREGEN] Stopping worker")
        self.running = False


def main():
    """Main entry point for the pre-generation worker"""
    # Configure logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Handle signals for graceful shutdown
    worker = EmbeddingPregenWorker()

    def signal_handler(signum, frame):
        logger.info(f"[EMBEDDING PREGEN] Received signal {signum}, shutting down")
        worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run the worker
    try:
        worker.run_loop()
    except Exception as e:
        logger.error(f"[EMBEDDING PREGEN] Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
