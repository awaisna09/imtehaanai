"""
Centralized Configuration Validation
Validates environment configuration at startup with fail-fast behavior.
Ensures system cannot run with unsafe or incomplete configuration.
Reusable across API and worker processes.
"""

import os
import sys
import io
from typing import List, Tuple, Dict, Any, Optional
from dotenv import load_dotenv

# Fix Windows console encoding for emoji support
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass  # If redirection fails, continue without it

# Load config.env (optional - only if file exists)
# In production (Railway), environment variables are set directly
config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.env")
if os.path.exists(config_path):
    load_dotenv(config_path)


class ConfigValidator:
    """Validates environment configuration"""
    
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.environment = os.getenv("ENVIRONMENT", "development").lower()
    
    def validate_environment_identifier(self):
        """Validate ENVIRONMENT identifier matches allowed values"""
        allowed_environments = ["development", "staging", "production"]
        env = os.getenv("ENVIRONMENT", "development").lower()
        
        if env not in allowed_environments:
            self.errors.append(
                f"❌ ENVIRONMENT: Invalid value '{env}'. "
                f"Must be one of: {', '.join(allowed_environments)}"
            )
    
    def validate_all(self) -> Tuple[bool, List[str], List[str]]:
        """Run all validation checks in order"""
        # Critical validations first (fail fast)
        self.validate_environment_identifier()
        self.validate_required()
        self.validate_redis_config()
        self.validate_worker_config()
        self.validate_queue_config()
        self.validate_job_config()
        self.validate_batching()
        self.validate_rate_limits()
        self.validate_caching()
        self.validate_security()
        self.validate_production_readiness()
        
        is_valid = len(self.errors) == 0
        return is_valid, self.errors, self.warnings
    
    def validate_queue_config(self):
        """Validate queue configuration with range checks"""
        max_queue_size = self._get_int("MAX_QUEUE_SIZE", 10000)
        queue_back_pressure_threshold = self._get_float("QUEUE_BACK_PRESSURE_THRESHOLD", 0.8)
        queue_back_pressure_delay = self._get_float("QUEUE_BACK_PRESSURE_DELAY", 1.0)
        queue_full_policy = os.getenv("QUEUE_FULL_POLICY", "reject").lower()
        
        # Range validation
        if max_queue_size < 1:
            self.errors.append(f"❌ Queue: MAX_QUEUE_SIZE must be >= 1, got {max_queue_size}")
        if max_queue_size > 100000:
            self.errors.append(
                f"❌ Queue: MAX_QUEUE_SIZE ({max_queue_size}) is dangerously high. "
                f"Risk of Redis memory exhaustion. Recommended: 5000-10000"
            )
        
        # Back-pressure threshold must be between 0 and 1
        if queue_back_pressure_threshold < 0.0 or queue_back_pressure_threshold > 1.0:
            self.errors.append(
                f"❌ Queue: QUEUE_BACK_PRESSURE_THRESHOLD must be between 0.0 and 1.0, "
                f"got {queue_back_pressure_threshold}"
            )
        
        # Back-pressure delay must be positive
        if queue_back_pressure_delay < 0:
            self.errors.append(
                f"❌ Queue: QUEUE_BACK_PRESSURE_DELAY must be >= 0, got {queue_back_pressure_delay}"
            )
        
        # Queue full policy validation
        allowed_policies = ["reject", "drop_oldest"]
        if queue_full_policy not in allowed_policies:
            self.errors.append(
                f"❌ Queue: QUEUE_FULL_POLICY must be one of {allowed_policies}, got '{queue_full_policy}'"
            )
    
    def validate_job_config(self):
        """Validate job configuration with range checks"""
        max_retries = self._get_int("MAX_RETRIES", 3)
        retry_delay = self._get_int("RETRY_DELAY", 60)
        max_retry_delay = self._get_int("MAX_RETRY_DELAY", 600)
        job_timeout = self._get_int("JOB_TIMEOUT", 3600)
        job_timeout_warning = self._get_int("JOB_TIMEOUT_WARNING", 1800)
        job_result_ttl = self._get_int("JOB_RESULT_TTL", 86400)
        
        # Range validation
        if max_retries < 0:
            self.errors.append(f"❌ Job: MAX_RETRIES must be >= 0, got {max_retries}")
        if max_retries > 10:
            self.errors.append(
                f"❌ Job: MAX_RETRIES ({max_retries}) is very high. "
                f"Recommended: 0-5 to prevent resource exhaustion"
            )
        
        if retry_delay < 1:
            self.errors.append(f"❌ Job: RETRY_DELAY must be >= 1 second, got {retry_delay}")
        if retry_delay > 3600:
            self.errors.append(
                f"❌ Job: RETRY_DELAY ({retry_delay}) is very high. Recommended: 60-600 seconds"
            )
        
        if max_retry_delay < retry_delay:
            self.errors.append(
                f"❌ Job: MAX_RETRY_DELAY ({max_retry_delay}) must be >= RETRY_DELAY ({retry_delay})"
            )
        
        if job_timeout < 1:
            self.errors.append(f"❌ Job: JOB_TIMEOUT must be >= 1 second, got {job_timeout}")
        if job_timeout > 86400:  # 24 hours
            self.errors.append(
                f"❌ Job: JOB_TIMEOUT ({job_timeout}) is very high. "
                f"Recommended: 3600-7200 seconds (1-2 hours)"
            )
        
        if job_timeout_warning >= job_timeout:
            self.errors.append(
                f"❌ Job: JOB_TIMEOUT_WARNING ({job_timeout_warning}) must be < JOB_TIMEOUT ({job_timeout})"
            )
        
        if job_result_ttl < 0:
            self.errors.append(f"❌ Job: JOB_RESULT_TTL must be >= 0, got {job_result_ttl}")
        if job_result_ttl > 604800:  # 7 days
            self.warnings.append(
                f"⚠️ Job: JOB_RESULT_TTL ({job_result_ttl}) is high. "
                f"Consider lower value to reduce Redis memory usage"
            )
    
    def _get_int(self, var: str, default: int) -> int:
        """Get integer environment variable with default"""
        try:
            return int(os.getenv(var, str(default)))
        except ValueError:
            value = os.getenv(var, str(default))
            self.errors.append(f"❌ {var}: Invalid integer value '{value}'. Must be a number.")
            return default
    
    def _get_float(self, var: str, default: float) -> float:
        """Get float environment variable with default"""
        try:
            return float(os.getenv(var, str(default)))
        except ValueError:
            value = os.getenv(var, str(default))
            self.errors.append(f"❌ {var}: Invalid float value '{value}'. Must be a number.")
            return default
    
    def validate_required(self):
        """Validate required configuration variables"""
        required_vars = {
            "OPENAI_API_KEY": "OpenAI API key is required for AI operations",
            "SUPABASE_URL": "Supabase URL is required for database operations",
            "SUPABASE_ANON_KEY": "Supabase anon key is required for database operations",
        }
        
        for var, message in required_vars.items():
            value = os.getenv(var)
            if not value or value.strip() == "":
                self.errors.append(f"❌ REQUIRED: {var} is missing or empty. {message}")
        
        # Validate format of required URLs
        supabase_url = os.getenv("SUPABASE_URL", "")
        if supabase_url and not (supabase_url.startswith("http://") or supabase_url.startswith("https://")):
            self.errors.append(
                f"❌ REQUIRED: SUPABASE_URL must start with http:// or https://, got: {supabase_url[:50]}..."
            )
    
    def validate_redis_config(self):
        """Validate Redis configuration"""
        redis_url = os.getenv("REDIS_URL", "").strip()
        redis_host = os.getenv("REDIS_HOST", "localhost").strip()
        redis_port = self._get_int("REDIS_PORT", 6379)
        redis_retry_base_delay = self._get_float("REDIS_RETRY_BASE_DELAY", 2.0)
        redis_retry_max_delay = self._get_float("REDIS_RETRY_MAX_DELAY", 60.0)
        redis_retry_max_attempts = self._get_int("REDIS_RETRY_MAX_ATTEMPTS", 0)
        redis_health_check_interval = self._get_int("REDIS_HEALTH_CHECK_INTERVAL", 10)
        
        # Either REDIS_URL or REDIS_HOST must be set
        if not redis_url and not redis_host:
            self.errors.append(
                "❌ Redis: Either REDIS_URL or REDIS_HOST must be set. "
                "Redis is required for job queue, rate limiting, and caching"
            )
        
        # Validate Redis URL format if provided
        if redis_url and not (redis_url.startswith("redis://") or redis_url.startswith("rediss://")):
            self.errors.append(
                f"❌ Redis: REDIS_URL must start with redis:// or rediss://, got: {redis_url[:50]}..."
            )
        
        # Validate port range
        if redis_port < 1 or redis_port > 65535:
            self.errors.append(
                f"❌ Redis: REDIS_PORT must be between 1 and 65535, got {redis_port}"
            )
        
        # Validate retry configuration
        if redis_retry_base_delay < 0:
            self.errors.append(
                f"❌ Redis: REDIS_RETRY_BASE_DELAY must be >= 0, got {redis_retry_base_delay}"
            )
        if redis_retry_max_delay < redis_retry_base_delay:
            self.errors.append(
                f"❌ Redis: REDIS_RETRY_MAX_DELAY ({redis_retry_max_delay}) "
                f"must be >= REDIS_RETRY_BASE_DELAY ({redis_retry_base_delay})"
            )
        if redis_retry_max_attempts < 0:
            self.errors.append(
                f"❌ Redis: REDIS_RETRY_MAX_ATTEMPTS must be >= 0 (0 = infinite), got {redis_retry_max_attempts}"
            )
        if redis_health_check_interval < 1:
            self.errors.append(
                f"❌ Redis: REDIS_HEALTH_CHECK_INTERVAL must be >= 1 second, got {redis_health_check_interval}"
            )
    
    def validate_worker_config(self):
        """Validate worker configuration with range checks and cross-field constraints"""
        worker_concurrency = self._get_int("WORKER_CONCURRENCY", 3)
        max_db_connections = self._get_int("MAX_DB_CONNECTIONS", 10)
        worker_poll_timeout = self._get_int("WORKER_POLL_TIMEOUT", 5)
        
        # Health monitoring and graceful degradation
        health_check_interval = self._get_int("HEALTH_CHECK_INTERVAL", 30)
        worker_health_ttl = self._get_int("WORKER_HEALTH_TTL", 60)
        worker_health_update_interval = self._get_int(
            "WORKER_HEALTH_UPDATE_INTERVAL", 30
        )
        max_consecutive_failures = self._get_int("MAX_CONSECUTIVE_FAILURES", 10)
        degradation_threshold = self._get_int("DEGRADATION_MODE_THRESHOLD", 5)
        enable_graceful_degradation = (
            os.getenv("ENABLE_GRACEFUL_DEGRADATION", "true").lower() == "true"
        )
        
        # Range validation
        if worker_concurrency < 1:
            self.errors.append(f"❌ Worker: WORKER_CONCURRENCY must be >= 1, got {worker_concurrency}")
        if worker_concurrency > 20:
            self.errors.append(
                f"❌ Worker: WORKER_CONCURRENCY ({worker_concurrency}) is dangerously high. "
                f"Recommended: 2-5 for production to protect database stability"
            )
        
        if max_db_connections < 1:
            self.errors.append(f"❌ Worker: MAX_DB_CONNECTIONS must be >= 1, got {max_db_connections}")
        if max_db_connections > 100:
            self.warnings.append(
                f"⚠️ Worker: MAX_DB_CONNECTIONS ({max_db_connections}) is very high. "
                f"Ensure your database plan supports this many connections"
            )
        
        if worker_poll_timeout < 1:
            self.errors.append(f"❌ Worker: WORKER_POLL_TIMEOUT must be >= 1 second, got {worker_poll_timeout}")
        if worker_poll_timeout > 60:
            self.warnings.append(
                f"⚠️ Worker: WORKER_POLL_TIMEOUT ({worker_poll_timeout}) is high. "
                f"May increase job pickup latency. Recommended: 5-10 seconds"
            )
        
        # Health monitoring validation
        if health_check_interval < 1:
            self.errors.append(
                f"❌ Worker: HEALTH_CHECK_INTERVAL must be >= 1 second, "
                f"got {health_check_interval}"
            )
        if health_check_interval > 300:
            self.warnings.append(
                f"⚠️ Worker: HEALTH_CHECK_INTERVAL ({health_check_interval}) is high. "
                f"May delay degradation detection. Recommended: 30-60 seconds"
            )
        
        if worker_health_ttl < 10:
            self.errors.append(
                f"❌ Worker: WORKER_HEALTH_TTL must be >= 10 seconds, "
                f"got {worker_health_ttl}"
            )
        if worker_health_ttl > 600:
            self.warnings.append(
                f"⚠️ Worker: WORKER_HEALTH_TTL ({worker_health_ttl}) is high. "
                f"Dead workers may be detected slowly. Recommended: 60-120 seconds"
            )
        
        if worker_health_update_interval < 5:
            self.errors.append(
                f"❌ Worker: WORKER_HEALTH_UPDATE_INTERVAL must be >= 5 seconds, "
                f"got {worker_health_update_interval}"
            )
        if worker_health_update_interval > 120:
            self.warnings.append(
                f"⚠️ Worker: WORKER_HEALTH_UPDATE_INTERVAL "
                f"({worker_health_update_interval}) is high. "
                f"Health reports may be stale. Recommended: 30-60 seconds"
            )
        
        # Graceful degradation validation
        if max_consecutive_failures < 1:
            self.errors.append(
                f"❌ Worker: MAX_CONSECUTIVE_FAILURES must be >= 1, "
                f"got {max_consecutive_failures}"
            )
        if max_consecutive_failures > 100:
            self.warnings.append(
                f"⚠️ Worker: MAX_CONSECUTIVE_FAILURES ({max_consecutive_failures}) "
                f"is very high. May delay degradation. Recommended: 5-20"
            )
        
        if degradation_threshold < 1:
            self.errors.append(
                f"❌ Worker: DEGRADATION_MODE_THRESHOLD must be >= 1, "
                f"got {degradation_threshold}"
            )
        if degradation_threshold > max_consecutive_failures:
            self.errors.append(
                f"❌ Worker: DEGRADATION_MODE_THRESHOLD ({degradation_threshold}) "
                f"must be <= MAX_CONSECUTIVE_FAILURES ({max_consecutive_failures})"
            )
        
        # Cross-field constraint: Worker concurrency must not exceed DB connections
        if worker_concurrency > max_db_connections:
            self.errors.append(
                f"❌ Worker: WORKER_CONCURRENCY ({worker_concurrency}) "
                f"must be <= MAX_DB_CONNECTIONS ({max_db_connections}) "
                f"to prevent connection pool exhaustion. "
                f"Fix: Increase MAX_DB_CONNECTIONS or decrease WORKER_CONCURRENCY"
            )
        
        # Cross-field constraint: DB connections should be at least worker_concurrency + buffer
        recommended_min_connections = worker_concurrency + 2
        if max_db_connections < recommended_min_connections:
            self.warnings.append(
                f"⚠️ Worker: MAX_DB_CONNECTIONS ({max_db_connections}) is close to "
                f"WORKER_CONCURRENCY ({worker_concurrency}). "
                f"Recommended: At least {recommended_min_connections} to handle job variations"
            )
        
        # Cross-field constraint: Worker health TTL should be >= update interval
        if worker_health_ttl < worker_health_update_interval:
            self.warnings.append(
                f"⚠️ Worker: WORKER_HEALTH_TTL ({worker_health_ttl}) should be >= "
                f"WORKER_HEALTH_UPDATE_INTERVAL ({worker_health_update_interval}) "
                f"to prevent premature expiration"
            )
    
    def validate_rate_limits(self):
        """Validate rate limiting configuration with range checks"""
        categories = [
            "TUTOR_CHAT",
            "ANSWER_GRADING",
            "MOCK_EXAM_GRADING",
            "CONCEPT_EXPLANATION",
            "LESSON_CREATION",
            "ALL_AI_WORK"
        ]
        
        for category in categories:
            requests_var = f"RATE_LIMIT_{category}_REQUESTS"
            window_var = f"RATE_LIMIT_{category}_WINDOW"
            
            requests = self._get_int(requests_var, 0)
            window = self._get_int(window_var, 3600)
            
            # Range validation
            if requests < 0:
                self.errors.append(f"❌ Rate Limit: {requests_var} must be >= 0, got {requests}")
            if requests == 0:
                self.warnings.append(
                    f"⚠️ Rate Limit: {requests_var} is 0, which disables rate limiting. "
                    f"Consider setting a limit to prevent abuse"
                )
            if requests > 100000:
                self.errors.append(
                    f"❌ Rate Limit: {requests_var} ({requests}) is dangerously high. "
                    f"Recommended: 10-1000 depending on category"
                )
            
            if window < 1:
                self.errors.append(f"❌ Rate Limit: {window_var} must be >= 1 second, got {window}")
            if window > 86400:  # 24 hours
                self.warnings.append(
                    f"⚠️ Rate Limit: {window_var} ({window}) is very long. "
                    f"Recommended: 3600 seconds (1 hour)"
                )
            
            # Cross-field constraint: requests per window should be reasonable
            if window > 0 and requests > 0:
                requests_per_second = requests / window
                if requests_per_second > 100:
                    self.warnings.append(
                        f"⚠️ Rate Limit: {requests_var} allows {requests_per_second:.2f} requests/second. "
                        f"This may be too high for production"
                    )
    
    def validate_caching(self):
        """Validate caching configuration"""
        cache_ttls = {
            "CACHE_TTL_STATIC": 86400,
            "CACHE_TTL_SEMI_STATIC": 3600,
            "CACHE_TTL_FREQUENT": 300,
            "CACHE_TTL_USER": 1800,
            "CACHE_TTL_QUERY": 600,
        }
        
        for var, default in cache_ttls.items():
            value = int(os.getenv(var, default))
            if value <= 0:
                self.warnings.append(f"⚠️ Cache: {var} is {value}, should be > 0")
            
            # Verify hierarchy: STATIC > SEMI_STATIC > FREQUENT
            if var == "CACHE_TTL_STATIC":
                semi_static = int(os.getenv("CACHE_TTL_SEMI_STATIC", 3600))
                if value <= semi_static:
                    self.warnings.append(
                        f"⚠️ Cache: CACHE_TTL_STATIC ({value}) should be > CACHE_TTL_SEMI_STATIC ({semi_static})"
                    )
            elif var == "CACHE_TTL_SEMI_STATIC":
                frequent = int(os.getenv("CACHE_TTL_FREQUENT", 300))
                if value <= frequent:
                    self.warnings.append(
                        f"⚠️ Cache: CACHE_TTL_SEMI_STATIC ({value}) should be > CACHE_TTL_FREQUENT ({frequent})"
                    )
    
    def validate_batching(self):
        """Validate batching configuration with range checks and cross-field constraints"""
        batch_size = self._get_int("DB_BATCH_SIZE", 50)
        batch_interval = self._get_float("DB_BATCH_INTERVAL", 2.0)
        max_batch_wait = self._get_float("MAX_BATCH_WAIT", 5.0)
        
        # Range validation
        if batch_size < 1:
            self.errors.append(f"❌ Batching: DB_BATCH_SIZE must be >= 1, got {batch_size}")
        if batch_size > 1000:
            self.errors.append(
                f"❌ Batching: DB_BATCH_SIZE ({batch_size}) is dangerously high. "
                f"Risk of memory exhaustion. Recommended: 50-100"
            )
        
        if batch_interval < 0.1:
            self.errors.append(
                f"❌ Batching: DB_BATCH_INTERVAL must be >= 0.1 seconds, got {batch_interval}"
            )
        if batch_interval > 60:
            self.warnings.append(
                f"⚠️ Batching: DB_BATCH_INTERVAL ({batch_interval}) is high. "
                f"May cause write latency. Recommended: 1.0-5.0 seconds"
            )
        
        if max_batch_wait < 0.1:
            self.errors.append(
                f"❌ Batching: MAX_BATCH_WAIT must be >= 0.1 seconds, got {max_batch_wait}"
            )
        if max_batch_wait > 300:  # 5 minutes
            self.warnings.append(
                f"⚠️ Batching: MAX_BATCH_WAIT ({max_batch_wait}) is very high. "
                f"May cause significant write latency. Recommended: 2.0-10.0 seconds"
            )
        
        # Cross-field constraint: Max batch wait should be >= batch interval
        if max_batch_wait < batch_interval:
            self.errors.append(
                f"❌ Batching: MAX_BATCH_WAIT ({max_batch_wait}) must be >= DB_BATCH_INTERVAL ({batch_interval})"
            )
    
    def validate_security(self):
        """Validate security configuration"""
        allowed_origins = os.getenv("ALLOWED_ORIGINS", "*")
        enable_debug = os.getenv("ENABLE_DEBUG", "false").lower() == "true"
        
        # CORS security check
        if "*" in allowed_origins and self.environment == "production":
            self.errors.append(
                "❌ Security: ALLOWED_ORIGINS contains '*' in production. "
                "This is a security risk. Set to specific domains."
            )
        elif "*" in allowed_origins and self.environment != "development":
            self.warnings.append(
                f"⚠️ Security: ALLOWED_ORIGINS contains '*' in {self.environment} environment. "
                "Consider restricting to specific domains for better security"
            )
        
        # Debug mode check
        if enable_debug and self.environment == "production":
            self.warnings.append(
                "⚠️ Security: ENABLE_DEBUG=true in production. "
                "This may expose sensitive information. Set to false."
            )
    
    def validate_production_readiness(self):
        """Validate production readiness"""
        if self.environment != "production":
            return
        
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        log_format = os.getenv("LOG_FORMAT", "json")
        enable_debug = os.getenv("ENABLE_DEBUG", "false").lower() == "true"
        
        # Production logging checks
        if log_level == "DEBUG":
            self.warnings.append(
                "⚠️ Production: LOG_LEVEL=DEBUG in production. "
                "This is verbose and may impact performance. Consider INFO or WARNING."
            )
        
        if log_format != "json":
            self.warnings.append(
                f"⚠️ Production: LOG_FORMAT={log_format} in production. "
                "JSON format is recommended for structured logging and parsing."
            )
        
        if enable_debug:
            self.warnings.append(
                "⚠️ Production: ENABLE_DEBUG=true in production. "
                "Disable for better performance and security."
            )
        
        # Redis URL check
        redis_url = os.getenv("REDIS_URL")
        redis_password = os.getenv("REDIS_PASSWORD")
        
        if not redis_url and not redis_password:
            self.warnings.append(
                "⚠️ Production: No Redis password configured. "
                "Consider using REDIS_URL with authentication or setting REDIS_PASSWORD."
            )
    
    def print_report(self):
        """Print validation report"""
        # Use plain text on Windows to avoid encoding issues
        use_emoji = sys.platform != "win32"
        check_mark = "✅" if use_emoji else "[OK]"
        cross_mark = "❌" if use_emoji else "[ERROR]"
        warning_mark = "⚠️" if use_emoji else "[WARNING]"
        
        print("\n" + "=" * 70)
        print("CONFIGURATION VALIDATION REPORT")
        print("=" * 70)
        print(f"Environment: {self.environment.upper()}\n")
        
        if self.errors:
            print(f"{cross_mark} ERRORS ({len(self.errors)}):")
            for error in self.errors:
                # Replace emojis in error messages for Windows
                error_msg = error
                if not use_emoji:
                    error_msg = error.replace("❌", "[ERROR]").replace("✅", "[OK]").replace("⚠️", "[WARNING]")
                print(f"  {error_msg}")
            print()
        else:
            print(f"{check_mark} No errors found\n")
        
        if self.warnings:
            print(f"{warning_mark} WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                # Replace emojis in warning messages for Windows
                warning_msg = warning
                if not use_emoji:
                    warning_msg = warning.replace("❌", "[ERROR]").replace("✅", "[OK]").replace("⚠️", "[WARNING]")
                print(f"  {warning_msg}")
            print()
        else:
            print(f"{check_mark} No warnings\n")
        
        print("=" * 70)
        
        if self.errors:
            print(f"{cross_mark} Configuration validation FAILED")
            print("   Fix errors before deploying to production")
            return False
        elif self.warnings:
            print(f"{warning_mark} Configuration validation PASSED with warnings")
            print("   Review warnings before deploying to production")
            return True
        else:
            print(f"{check_mark} Configuration validation PASSED")
            return True


def validate_and_exit(fail_on_warnings: bool = False):
    """
    Validate configuration and exit with appropriate code.
    This is the main entry point for startup validation.
    
    Args:
        fail_on_warnings: If True, treat warnings as errors (useful for production)
    
    Exits:
        0: Validation passed
        1: Validation failed (errors found)
        2: Validation passed with warnings (if fail_on_warnings=True, exits with 1)
    """
    validator = ConfigValidator()
    is_valid, errors, warnings = validator.validate_all()
    
    # Print report
    validator.print_report()
    
    # Use plain text on Windows to avoid encoding issues
    use_emoji = sys.platform != "win32"
    check_mark = "✅" if use_emoji else "[OK]"
    cross_mark = "❌" if use_emoji else "[ERROR]"
    warning_mark = "⚠️" if use_emoji else "[WARNING]"
    
    # Fail fast on errors
    if not is_valid:
        print("\n" + "=" * 70)
        print(f"{cross_mark} CONFIGURATION VALIDATION FAILED")
        print("=" * 70)
        print("\nThe application cannot start with invalid configuration.")
        print("Please fix the errors above and restart.\n")
        sys.exit(1)
    
    # Handle warnings
    if warnings:
        if fail_on_warnings or validator.environment == "production":
            print("\n" + "=" * 70)
            print(f"{cross_mark} CONFIGURATION VALIDATION FAILED (Warnings treated as errors)")
            print("=" * 70)
            print("\nWarnings are not allowed in production environment.")
            print("Please fix the warnings above and restart.\n")
            sys.exit(1)
        else:
            print(f"\n{warning_mark} Continuing with warnings (not in production mode)...")
            # Don't exit - just continue with warnings in development mode
    
    # Success
    print(f"\n{check_mark} Configuration validation passed - system ready to start")
    return True


def main():
    """Main validation function (CLI entry point)"""
    # In CLI mode, fail on warnings for production
    fail_on_warnings = os.getenv("ENVIRONMENT", "development").lower() == "production"
    validate_and_exit(fail_on_warnings=fail_on_warnings)


if __name__ == "__main__":
    main()
