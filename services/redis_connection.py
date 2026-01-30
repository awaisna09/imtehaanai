"""
Reusable Redis Connection Module
Provides environment-based configuration and connection management
"""

import os
import logging
import ssl
from typing import Optional
from enum import Enum
import redis
from redis.connection import ConnectionPool
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config.env')

# Configure logging
logger = logging.getLogger(__name__)


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
            # Production: Try REDIS_URL first, fall back to separate vars if needed
            redis_url = os.getenv("REDIS_URL")
            redis_host = os.getenv("REDIS_HOST")
            redis_port = os.getenv("REDIS_PORT")
            redis_password = os.getenv("REDIS_PASSWORD")
            
            # If separate variables are set, use them
            # (allows bypassing broken REDIS_URL)
            if redis_host or redis_port or redis_password:
                self.host = redis_host or "localhost"
                self.port = int(redis_port or 6379)
                self.password = redis_password or None
                self.db = int(os.getenv("REDIS_DB", 0))
                self.url = None
                logger.info(
                    f"Using separate Redis variables (production mode): "
                    f"{self.host}:{self.port}"
                )
                # Railway Redis requires SSL - log this
                if ".railway.app" in self.host or ".rlwy.net" in self.host:
                    logger.info(
                        f"Detected Railway Redis host - SSL will be enabled"
                    )
            elif redis_url:
                # Use REDIS_URL if provided and no separate variables
                self.host = None
                self.port = None
                self.password = None
                self.db = 0
                self.url = redis_url
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
            # Development/Test: Support both URL and local configuration
            # Priority: If REDIS_HOST is explicitly set, use separate vars
            # Otherwise, try REDIS_URL first, then fall back to separate vars
            redis_host = os.getenv("REDIS_HOST")
            redis_port = os.getenv("REDIS_PORT")
            redis_password = os.getenv("REDIS_PASSWORD")
            
            # If separate variables are explicitly set, use them
            # (allows bypassing broken REDIS_URL)
            if redis_host or redis_port or redis_password:
                self.host = redis_host or "localhost"
                self.port = int(redis_port or 6379)
                self.password = redis_password or None
                self.db = int(os.getenv("REDIS_DB", 0))
                self.url = None
                logger.info(
                    f"Using separate Redis variables (development mode): "
                    f"{self.host}:{self.port}"
                )
                # Railway Redis requires SSL - log this
                if ".railway.app" in self.host or ".rlwy.net" in self.host:
                    logger.info(
                        f"Detected Railway Redis host - SSL will be enabled"
                    )
            else:
                # Try REDIS_URL if no separate variables are set
                redis_url = os.getenv("REDIS_URL")
                if redis_url:
                    # Use managed Redis URL if provided
                    self.host = None
                    self.port = None
                    self.password = None
                    self.db = 0
                    self.url = redis_url
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
            return {
                "connection_pool": ConnectionPool.from_url(
                    self.url,
                    max_connections=self.max_connections,
                    socket_connect_timeout=self.socket_connect_timeout,
                    socket_timeout=self.socket_timeout,
                    health_check_interval=self.health_check_interval,
                    retry_on_timeout=self.retry_on_timeout,
                    decode_responses=self.decode_responses,
                    socket_keepalive=self.socket_keepalive,
                    socket_keepalive_options=(
                        self.socket_keepalive_options
                        if self.socket_keepalive_options
                        else None
                    ),
                )
            }
        else:
            # For Railway Redis, check if SSL is required
            # Railway Redis typically requires SSL/TLS
            use_ssl = os.getenv("REDIS_SSL", "true").lower() == "true"
            # If host contains .railway.app or .rlwy.net, likely needs SSL
            if self.host and (".railway.app" in self.host or ".rlwy.net" in self.host):
                use_ssl = True
            
            # If SSL is needed, use ConnectionPool with SSL context
            if use_ssl:
                logger.info(
                    f"Using SSL for Redis connection to "
                    f"{self.host}:{self.port}"
                )

                # Create SSL context for Railway Redis
                # Railway Redis uses self-signed certificates,
                # so we disable verification
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                # Use ConnectionPool with SSL context for Railway Redis
                return {
                    "connection_pool": ConnectionPool(
                        host=self.host,
                        port=self.port,
                        password=self.password,
                        db=self.db,
                        max_connections=self.max_connections,
                        socket_connect_timeout=(
                            self.socket_connect_timeout
                        ),
                        socket_timeout=self.socket_timeout,
                        health_check_interval=(
                            self.health_check_interval
                        ),
                        retry_on_timeout=self.retry_on_timeout,
                        decode_responses=self.decode_responses,
                        socket_keepalive=self.socket_keepalive,
                        socket_keepalive_options=(
                            self.socket_keepalive_options
                            if self.socket_keepalive_options
                            else None
                        ),
                        ssl=True,
                        ssl_cert_reqs=ssl.CERT_NONE,
                    )
                }
            
            # Non-SSL connection using separate parameters
            connection_params = {
                "host": self.host,
                "port": self.port,
                "password": self.password,
                "db": self.db,
                "socket_connect_timeout": self.socket_connect_timeout,
                "socket_timeout": self.socket_timeout,
                "health_check_interval": self.health_check_interval,
                "max_connections": self.max_connections,
                "retry_on_timeout": self.retry_on_timeout,
                "decode_responses": self.decode_responses,
                "socket_keepalive": self.socket_keepalive,
            }
            
            # Add keepalive options if available
            if self.socket_keepalive_options:
                connection_params["socket_keepalive_options"] = (
                    self.socket_keepalive_options
                )
            
            return connection_params


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
            
            # Create connection pool if using explicit config
            if "connection_pool" not in kwargs:
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
            
        except redis.ConnectionError as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error initializing Redis: {e}")
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
