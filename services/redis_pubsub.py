"""
Redis Pub/Sub Service
Publishes events to Redis channels for real-time updates without WAL noise.
"""

import os
import json
import logging
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv

from services.redis_connection import (
    get_redis_client,
    is_redis_available
)

load_dotenv('config.env')

logger = logging.getLogger(__name__)

# Channel prefix for pub/sub
CHANNEL_PREFIX = os.getenv(
    "REDIS_PUBSUB_PREFIX", "imtehaan:pubsub:"
)


def publish_analytics_update(
    user_id: str,
    update_type: str = "daily_analytics"
):
    """
    Publish analytics update event to Redis pub/sub.

    Args:
        user_id: User ID for the update
        update_type: Type of update (default: "daily_analytics")
    """
    if not is_redis_available():
        return  # Non-blocking: continue even if Redis unavailable

    try:
        redis_client = get_redis_client()
        channel = f"{CHANNEL_PREFIX}analytics:{user_id}"

        message = {
            "type": update_type,
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat()
        }

        redis_client.publish(channel, json.dumps(message))
        logger.debug(f"📤 Published analytics update to {channel}")

    except Exception as e:
        logger.warning(
            f"⚠️ Failed to publish analytics update: {e}"
        )
        # Non-blocking: continue even if publish fails


def publish_time_tracking_update(
    user_id: str,
    page_type: Optional[str] = None
):
    """
    Publish time tracking update event to Redis pub/sub.

    Args:
        user_id: User ID for the update
        page_type: Optional page type filter
    """
    if not is_redis_available():
        return  # Non-blocking: continue even if Redis unavailable

    try:
        redis_client = get_redis_client()
        channel = f"{CHANNEL_PREFIX}time_tracking:{user_id}"

        message = {
            "type": "time_tracking",
            "user_id": user_id,
            "page_type": page_type,
            "timestamp": datetime.utcnow().isoformat()
        }

        redis_client.publish(channel, json.dumps(message))
        logger.debug(
            f"📤 Published time tracking update to {channel}"
        )

    except Exception as e:
        logger.warning(
            f"⚠️ Failed to publish time tracking update: {e}"
        )
        # Non-blocking: continue even if publish fails
