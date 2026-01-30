"""
Reusable Redis Connection Module
Provides environment-based configuration and connection management
"""

import os
import logging
from typing import Optional
from enum import Enum
from urllib.parse import urlparse
import redis
from redis.connection import ConnectionPool
from dotenv import load_dotenv

# Load environment variables only locally (skip when RAILWAY_ENVIRONMENT is set)
if not os.getenv("RAILWAY_ENVIRONMENT"):
    load_dotenv('config.env')

# Configure logging
logger = logging.getLogger(__name__)


def _safe_redis_log_target(redis_url: str) -> str:
    """
    Safely extract host:port from Redis URL for logging (no passwords).
    
    Args:
        redis_url: Redis connection URL
        
    Returns:
        Safe string with host:port only
    """
    try:
        parsed = urlparse(redis_url)
        if parsed.hostname and parsed.port:
            return f"{parsed.hostname}:{parsed.port}"
        elif parsed.hostname:
            return parsed.hostname
        else:
            return "URL-based"
    except Exception:
        # Fallback: try to extract host:port manually
        if "@" in redis_url:
            # Format: redis://user:pass@host:port/db
            after_at = redis_url.split("@")[-1]
            if ":" in after_at:
                return after_at.split("/")[0]  # host:port
            return after_at.split("/")[0] if "/" in after_at else after_at
        return "URL-based"


class Environment(str, Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


class RedisConfig:
    """Redis configuration based on environment"""
    
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "development").lower()
        
        # Base configuration
        if self.environment == Environment.PRODUCTION:
            # Production: Prioritize REDIS_URL (Railway provides this)
            # Fall back to separate vars only if REDIS_URL is not available
            redis_url = os.getenv("REDIS_URL")
            redis_user = os.getenv("REDISUSER") or os.getenv("REDIS_USER") or "default"
            redis_password = os.getenv("REDISPASSWORD") or os.getenv("REDIS_PASSWORD")
            redis_host = os.getenv("REDISHOST") or os.getenv("REDIS_HOST")
            redis_port = os.getenv("REDISPORT") or os.getenv("REDIS_PORT")
            redis_db = os.getenv("REDIS_DB", "0")
            
            # Priority 1: Use REDIS_URL if available
            # If REDIS_URL uses redis.railway.internal, rebuild using external vars
            if redis_url:
                # Check if REDIS_URL uses internal hostname that's not accessible
                if "redis.railway.internal" in redis_url:
                    # Fallback: rebuild redis_url using external variables
                    if redis_host and redis_port:
                        # Rebuild URL: redis://{REDISUSER}:{REDISPASSWORD}@{REDISHOST}:{REDISPORT}/{REDIS_DB}
                        password_part = f":{redis_password}@" if redis_password else ""
                        redis_url = f"redis://{redis_user}{password_part}{redis_host}:{redis_port}/{redis_db}"
                        logger.warning(
                            f"REDIS_URL contains redis.railway.internal (not accessible). "
                            f"Rebuilt using external vars: {_safe_redis_log_target(redis_url)}"
                        )
                    else:
                        # Try internal connection anyway (may fail)
                        logger.warning(
                            f"REDIS_URL uses redis.railway.internal but external vars not available. "
                            f"Attempting internal connection: {_safe_redis_log_target(redis_url)}"
                        )
                
                # Use the URL (either original or rebuilt)
                self.host = None
                self.port = None
                self.password = None
                self.db = 0
                self.url = redis_url
                logger.info(
                    f"Using REDIS_URL (production mode): {_safe_redis_log_target(redis_url)}"
                )
            # Priority 2: Use separate variables if REDIS_URL is not available
            elif redis_host or redis_port or redis_password:
                self.host = redis_host or "localhost"
                self.port = int(redis_port or 6379)
                self.password = redis_password or None
                self.db = int(redis_db)
                self.url = None
                logger.info(
                    f"Using separate Redis variables (production mode): {self.host}:{self.port}"
                )
                # Only enable SSL for external Railway hosts (not internal)
                if ".railway.app" in self.host or ".rlwy.net" in self.host:
                    logger.info(
                        f"Detected external Railway Redis host - SSL will be enabled"
                    )
                elif "redis.railway.internal" in self.host:
                    logger.info(
                        f"Detected internal Railway Redis host - no SSL needed"
                    )
            else:
                # Neither REDIS_URL nor separate variables are set
                raise ValueError(
                    "Redis configuration is required in production. "
                    "Please set either REDIS_URL or separate variables "
                    "(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD). "
                    "Railway provides REDIS_URL automatically when you "
                    "add a Redis service."
                )
            
            # Production settings
            self.socket_connect_timeout = 10
            self.socket_timeout = 30
            self.health_check_interval = 30
            self.max_connections = 100
            self.retry_on_timeout = True
            self.decode_responses = True
            self.socket_keepalive = True
            self.socket_keepalive_options = {
                1: 1,  # TCP_KEEPIDLE
                2: 10,  # TCP_KEEPINTVL
                3: 3,   # TCP_KEEPCNT
            }
            
        elif self.environment == Environment.STAGING:
            # Staging: Similar to production but with relaxed timeouts
            redis_url = os.getenv("REDIS_URL")
            if redis_url:
                self.host = None
                self.port = None
                self.password = None
                self.db = 0
                self.url = redis_url
            else:
                self.host = os.getenv("REDIS_HOST", "localhost")
                self.port = int(os.getenv("REDIS_PORT", 6379))
                self.password = os.getenv("REDIS_PASSWORD")
                self.db = int(os.getenv("REDIS_DB", 0))
                self.url = None
            
            self.socket_connect_timeout = 5
            self.socket_timeout = 20
            self.health_check_interval = 30
            self.max_connections = 50
            self.retry_on_timeout = True
            self.decode_responses = True
            self.socket_keepalive = True
            self.socket_keepalive_options = {
                1: 1,
                2: 10,
                3: 3,
            }
            
        else:
            # Development/Test: Prioritize REDIS_URL, fall back to separate vars
            redis_url = os.getenv("REDIS_URL")
            redis_host = os.getenv("REDIS_HOST")
            redis_port = os.getenv("REDIS_PORT")
            redis_password = os.getenv("REDIS_PASSWORD")
            
            # Priority 1: Use REDIS_URL if available
            if redis_url:
                self.host = None
                self.port = None
                self.password = None
                self.db = 0
                self.url = redis_url
                logger.info(
                    f"Using REDIS_URL (development mode): {_safe_redis_log_target(redis_url)}"
                )
            # Priority 2: Use separate variables if REDIS_URL is not available
            elif redis_host or redis_port or redis_password:
                self.host = redis_host or "localhost"
                self.port = int(redis_port or 6379)
                self.password = redis_password or None
                self.db = int(os.getenv("REDIS_DB", 0))
                self.url = None
                logger.info(
                    f"Using separate Redis variables (development mode): "
                    f"{self.host}:{self.port}"
                )
                # Only enable SSL for external Railway hosts (not internal)
                if ".railway.app" in self.host or ".rlwy.net" in self.host:
                    logger.info(
                        f"Detected external Railway Redis host - SSL will be enabled"
                    )
                elif "redis.railway.internal" in self.host:
                    logger.info(
                        f"Detected internal Railway Redis host - no SSL needed"
                    )
            else:
                # Local configuration fallback
                self.host = os.getenv("REDIS_HOST", "localhost")
                self.port = int(os.getenv("REDIS_PORT", 6379))
                self.password = os.getenv("REDIS_PASSWORD") or None
                self.db = int(os.getenv("REDIS_DB", 0))
                self.url = None
            
            self.socket_connect_timeout = 3
            self.socket_timeout = 10
            self.health_check_interval = 60
            self.max_connections = 20
            self.retry_on_timeout = True
            self.decode_responses = True
            self.socket_keepalive = True
            self.socket_keepalive_options = {}
    
    def get_connection_kwargs(self) -> dict:
        """Get connection parameters for redis client"""
        if self.url:
            # Use redis.from_url() for URL-based connections
            # This handles both redis:// and rediss:// URLs properly
            return {
                "redis_url": self.url,
                "max_connections": self.max_connections,
                "socket_connect_timeout": self.socket_connect_timeout,
                "socket_timeout": self.socket_timeout,
                "health_check_interval": self.health_check_interval,
                "retry_on_timeout": self.retry_on_timeout,
                "decode_responses": self.decode_responses,
                "socket_keepalive": self.socket_keepalive,
                "socket_keepalive_options": (
                    self.socket_keepalive_options
                    if self.socket_keepalive_options
                    else None
                ),
            }
        else:
            # For Railway Redis, try simple redis:// URL first (no SSL)
            # Railway external connections might work without SSL
            # If that fails, we'll try with SSL in _initialize_connection
            password_part = f":{self.password}@" if self.password else ""
            redis_url = f"redis://{password_part}{self.host}:{self.port}/{self.db}"
            
            # Return URL to use with redis.from_url() - try non-SSL first
            return {
                "redis_url": redis_url,
                "max_connections": self.max_connections,
                "socket_connect_timeout": self.socket_connect_timeout,
                "socket_timeout": self.socket_timeout,
                "health_check_interval": self.health_check_interval,
                "retry_on_timeout": self.retry_on_timeout,
                "decode_responses": self.decode_responses,
                "socket_keepalive": self.socket_keepalive,
                "socket_keepalive_options": (
                    self.socket_keepalive_options
                    if self.socket_keepalive_options
                    else None
                ),
            }


class RedisConnection:
    """Redis connection manager with connection pooling and health checks"""
    
    _instance: Optional['RedisConnection'] = None
    _client: Optional[redis.Redis] = None
    _pool: Optional[ConnectionPool] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisConnection, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self.config = RedisConfig()
            self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize Redis connection with connection pooling"""
        try:
            kwargs = self.config.get_connection_kwargs()
            
            # If redis_url is provided, use it directly (like Node.js examples)
            # redis-py automatically handles redis:// and rediss:// URLs
            if "redis_url" in kwargs:
                redis_url = kwargs.pop("redis_url")
                # Extract connection pool parameters
                pool_kwargs = {
                    "max_connections": kwargs.pop("max_connections", 100),
                    "socket_connect_timeout": kwargs.pop(
                        "socket_connect_timeout", 10
                    ),
                    "socket_timeout": kwargs.pop("socket_timeout", 30),
                    "health_check_interval": kwargs.pop(
                        "health_check_interval", 30
                    ),
                    "retry_on_timeout": kwargs.pop("retry_on_timeout", True),
                    "decode_responses": kwargs.pop("decode_responses", True),
                    "socket_keepalive": kwargs.pop("socket_keepalive", True),
                    "socket_keepalive_options": kwargs.pop(
                        "socket_keepalive_options", None
                    ),
                }
                # Remove None values
                pool_kwargs = {
                    k: v for k, v in pool_kwargs.items() if v is not None
                }
                
                # Use redis.from_url() directly - it handles SSL automatically
                # Similar to Node.js: redis.from_url(process.env.REDIS_URL)
                safe_target = _safe_redis_log_target(redis_url)
                logger.info(f"Connecting to Redis: {safe_target}")
                self._client = redis.from_url(redis_url, **pool_kwargs)
                self._client.ping()
                logger.info(
                    f"✅ Redis connected: {self.config.environment} environment | {safe_target}"
                )
            elif "connection_pool" not in kwargs:
                # Create connection pool for separate host/port/password connections
                self._pool = ConnectionPool(**kwargs)
                kwargs = {"connection_pool": self._pool}
                self._client = redis.Redis(**kwargs)
                # Test connection
                self._client.ping()
                logger.info(
                    f"✅ Redis connected: {self.config.environment} environment | "
                    f"{self.config.host or 'URL-based'}:"
                    f"{self.config.port or 'N/A'}"
                )
            else:
                # Use provided connection pool
                self._client = redis.Redis(**kwargs)
                # Test connection
                self._client.ping()
                logger.info(
                    f"✅ Redis connected: {self.config.environment} environment | "
                    f"{self.config.host or 'URL-based'}:"
                    f"{self.config.port or 'N/A'}"
                )
            
        except redis.ConnectionError as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error initializing Redis: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def get_client(self) -> redis.Redis:
        """Get Redis client instance (with observability)"""
        if self._client is None:
            self._initialize_connection()
        
        # Health check with observability
        try:
            self._client.ping()
        except redis.ConnectionError as e:
            logger.warning("Redis connection lost, reinitializing...")
            # Log connectivity event for observability
            try:
                from services.structured_logging import structured_logger
                from services.observability import observability
                structured_logger.log_redis_connectivity(
                    event="connection_lost",
                    available=False,
                    error=str(e)
                )
                observability.track_redis_connectivity_event(
                    event="connection_lost",
                    available=False,
                    error=str(e)
                )
            except Exception:
                pass  # Don't fail if logging fails
            try:
                self._initialize_connection()
                # Log reconnection success
                try:
                    from services.structured_logging import structured_logger
                    from services.observability import observability
                    structured_logger.log_redis_connectivity(
                        event="connection_restored",
                        available=True
                    )
                    observability.track_redis_connectivity_event(
                        event="connection_restored",
                        available=True
                    )
                except Exception:
                    pass
            except Exception as reinit_error:
                # Log reconnection failure
                try:
                    from services.structured_logging import structured_logger
                    from services.observability import observability
                    structured_logger.log_redis_connectivity(
                        event="reconnection_failed",
                        available=False,
                        error=str(reinit_error)
                    )
                    observability.track_redis_connectivity_event(
                        event="reconnection_failed",
                        available=False,
                        error=str(reinit_error)
                    )
                except Exception:
                    pass
                raise
        
        return self._client
    
    def is_connected(self) -> bool:
        """Check if Redis is connected (with observability)"""
        if self._client is None:
            return False
        try:
            self._client.ping()
            return True
        except (redis.ConnectionError, AttributeError):
            return False
    
    def close(self):
        """Close Redis connection and pool"""
        if self._client:
            try:
                self._client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
            finally:
                self._client = None
        
        if self._pool:
            try:
                self._pool.disconnect()
                logger.info("Redis connection pool closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection pool: {e}")
            finally:
                self._pool = None
    
    def get_info(self) -> dict:
        """Get Redis connection information"""
        if not self.is_connected():
            return {"connected": False}
        
        try:
            info = self._client.info()
            return {
                "connected": True,
                "environment": self.config.environment,
                "host": self.config.host or "URL-based",
                "port": self.config.port or "N/A",
                "db": self.config.db,
                "redis_version": info.get("redis_version"),
                "used_memory_human": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "total_commands_processed": info.get(
                    "total_commands_processed"
                ),
            }
        except Exception as e:
            logger.error(f"Error getting Redis info: {e}")
            return {"connected": False, "error": str(e)}


# Global Redis connection instance
redis_connection = RedisConnection()


def get_redis_client() -> redis.Redis:
    """
    Get Redis client instance (singleton pattern)
    
    Returns:
        redis.Redis: Redis client instance
        
    Raises:
        redis.ConnectionError: If Redis connection fails
    """
    return redis_connection.get_client()


def is_redis_available() -> bool:
    """
    Check if Redis is available and connected
    
    Returns:
        bool: True if Redis is connected, False otherwise
    """
    return redis_connection.is_connected()
