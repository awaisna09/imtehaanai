"""
Job Kill Switch Service
Provides env-based kill switches to disable high-I/O background jobs.
"""

import os
import logging
from typing import Optional
from dotenv import load_dotenv

load_dotenv('config.env')

logger = logging.getLogger(__name__)


class JobKillSwitch:
    """Manages kill switches for heavy background jobs"""

    # Kill switch environment variables
    KILL_SWITCH_EMBEDDING_PREGEN = os.getenv(
        "DISABLE_EMBEDDING_PREGEN", "false"
    ).lower() == "true"

    KILL_SWITCH_BULK_BACKFILLS = os.getenv(
        "DISABLE_BULK_BACKFILLS", "false"
    ).lower() == "true"

    KILL_SWITCH_ANALYTICS_REBUILD = os.getenv(
        "DISABLE_ANALYTICS_REBUILD", "false"
    ).lower() == "true"

    # Retry delay when job is disabled (seconds) - Default 1 hour
    DISABLED_JOB_RETRY_DELAY = int(
        os.getenv("DISABLED_JOB_RETRY_DELAY", "3600")
    )
    
    @classmethod
    def is_job_disabled(cls, job_type: str) -> bool:
        """
        Check if a job type is disabled via kill switch.

        Args:
            job_type: Job type identifier

        Returns:
            True if job is disabled, False otherwise
        """
        job_type_lower = job_type.lower()

        if "embedding" in job_type_lower or "pregen" in job_type_lower:
            return cls.KILL_SWITCH_EMBEDDING_PREGEN
        elif "backfill" in job_type_lower or "bulk" in job_type_lower:
            return cls.KILL_SWITCH_BULK_BACKFILLS
        elif ("analytics" in job_type_lower and
              "rebuild" in job_type_lower):
            return cls.KILL_SWITCH_ANALYTICS_REBUILD

        return False

    @classmethod
    def get_retry_delay(cls) -> int:
        """
        Get retry delay for disabled jobs.

        Returns:
            Retry delay in seconds
        """
        return cls.DISABLED_JOB_RETRY_DELAY

    @classmethod
    def log_disabled_job(cls, job_type: str, job_id: Optional[str] = None):
        """
        Log a clear warning when a job is skipped due to kill switch.

        Args:
            job_type: Job type identifier
            job_id: Optional job ID
        """
        job_id_str = job_id or 'N/A'
        env_var = f"DISABLE_{job_type.upper()}"
        logger.warning(
            f"🚫 KILL SWITCH ACTIVE: Job type '{job_type}' is disabled. "
            f"Skipping job {job_id_str}. "
            f"Set {env_var}=false to re-enable."
        )

        # Structured logging
        try:
            from services.structured_logging import structured_logger
            structured_logger._log_structured(
                "WARNING",
                f"Job disabled by kill switch: {job_type}",
                context={
                    "event": "job_kill_switch",
                    "job_type": job_type,
                    "job_id": job_id,
                    "retry_delay_seconds": cls.DISABLED_JOB_RETRY_DELAY
                }
            )
        except Exception:
            pass  # Non-blocking


# Global instance
job_kill_switch = JobKillSwitch()
