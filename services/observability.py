"""
Observability Service
Metrics, monitoring, and visibility into queue depth, failures, and processing times
"""

import json
import time
from typing import Dict, Any, Optional
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

from services.redis_connection import get_redis_client, is_redis_available
from services.job_queue import job_queue

load_dotenv('config.env')

# Metrics storage (in-memory, can be extended to Redis for persistence)
_metrics_store: Dict[str, Any] = defaultdict(dict)
_metrics_lock = None  # Will be set to threading.Lock if needed


class ObservabilityService:
    """Service for tracking metrics and observability"""
    
    def __init__(self):
        self.redis = get_redis_client() if is_redis_available() else None
        self.metrics_prefix = "metrics:"
    
    def track_request_latency(
        self,
        endpoint: str,
        method: str,
        duration_ms: float,
        status_code: int,
        user_id: Optional[str] = None
    ):
        """
        Track API request latency (separate from job processing time)
        
        Args:
            endpoint: API endpoint path
            method: HTTP method
            duration_ms: Request duration in milliseconds
            status_code: HTTP status code
            user_id: Optional user ID
        """
        try:
            key = f"{self.metrics_prefix}api:requests"
            timestamp = int(time.time())
            
            # Store in Redis with TTL (24 hours)
            metric_data = {
                "endpoint": endpoint,
                "method": method,
                "duration_ms": duration_ms,
                "status_code": status_code,
                "user_id": user_id,
                "timestamp": timestamp
            }
            
            if self.redis:
                # Store in sorted set for time-series queries
                self.redis.zadd(
                    f"{key}:{endpoint}",
                    {json.dumps(metric_data): timestamp}
                )
                # Keep only last 24 hours
                cutoff = timestamp - 86400
                self.redis.zremrangebyscore(f"{key}:{endpoint}", 0, cutoff)
        except Exception as e:
            print(f"⚠️ Error tracking request latency: {e}")
    
    def track_job_processing_time(
        self,
        job_type: str,
        duration_seconds: float,
        job_id: str,
        success: bool = True,
        queue_wait_seconds: Optional[float] = None
    ):
        """
        Track background job processing time (separate from request latency)
        
        Args:
            job_type: Type of job
            duration_seconds: Processing duration in seconds
            job_id: Job ID
            success: Whether job succeeded
            queue_wait_seconds: Optional queue wait time in seconds
        """
        try:
            key = f"{self.metrics_prefix}jobs:processing_time"
            timestamp = int(time.time())
            
            metric_data = {
                "job_type": job_type,
                "duration_seconds": duration_seconds,
                "job_id": job_id,
                "success": success,
                "timestamp": timestamp
            }
            if queue_wait_seconds is not None:
                metric_data["queue_wait_seconds"] = queue_wait_seconds
            
            if self.redis:
                # Store in sorted set
                self.redis.zadd(
                    f"{key}:{job_type}",
                    {json.dumps(metric_data): timestamp}
                )
                # Keep only last 7 days
                cutoff = timestamp - (7 * 86400)
                self.redis.zremrangebyscore(f"{key}:{job_type}", 0, cutoff)
        except Exception as e:
            print(f"⚠️ Error tracking job processing time: {e}")
    
    def track_performance_timing(
        self,
        job_id: str,
        job_type: str,
        stage_type: str,
        duration_ms: float,
        stage_name: Optional[str] = None
    ):
        """
        Track performance timing for a specific stage.
        
        Args:
            job_id: Job ID
            job_type: Type of job
            stage_type: Type of stage (ai_provider_call, database_read, etc.)
            duration_ms: Duration in milliseconds
            stage_name: Optional stage name
        """
        try:
            if not self.redis:
                return
            
            key = f"{self.metrics_prefix}performance_timing:{job_type}"
            timestamp = int(time.time())
            
            timing_data = {
                "job_id": job_id,
                "job_type": job_type,
                "stage_type": stage_type,
                "duration_ms": duration_ms,
                "timestamp": timestamp
            }
            if stage_name:
                timing_data["stage_name"] = stage_name
            
            # Store in sorted set
            self.redis.zadd(
                f"{key}:{stage_type}",
                {json.dumps(timing_data): timestamp}
            )
            # Keep only last 7 days
            cutoff = timestamp - (7 * 86400)
            self.redis.zremrangebyscore(f"{key}:{stage_type}", 0, cutoff)
        except Exception as e:
            print(f"⚠️ Error tracking performance timing: {e}")
    
    def track_queue_depth(self, queue_name: str, depth: int):
        """Track queue depth over time"""
        try:
            if self.redis:
                timestamp = int(time.time())
                key = f"{self.metrics_prefix}queues:depth:{queue_name}"
                
                # Store depth with timestamp
                self.redis.zadd(key, {str(depth): timestamp})
                
                # Keep only last 24 hours
                cutoff = timestamp - 86400
                self.redis.zremrangebyscore(key, 0, cutoff)
        except Exception as e:
            print(f"⚠️ Error tracking queue depth: {e}")
    
    def track_queue_rejection(
        self,
        queue_name: str,
        job_id: str,
        job_type: str,
        queue_size: int,
        max_queue_size: int,
        policy: str
    ):
        """Track queue rejection events (critical for back-pressure monitoring)"""
        try:
            if self.redis:
                timestamp = int(time.time())
                key = f"{self.metrics_prefix}queues:rejections:{queue_name}"
                
                rejection_data = {
                    "queue_name": queue_name,
                    "job_id": job_id,
                    "job_type": job_type,
                    "queue_size": queue_size,
                    "max_queue_size": max_queue_size,
                    "policy": policy,
                    "timestamp": timestamp
                }
                
                # Store rejection event
                self.redis.zadd(key, {json.dumps(rejection_data): timestamp})
                
                # Keep only last 7 days
                cutoff = timestamp - (7 * 86400)
                self.redis.zremrangebyscore(key, 0, cutoff)
        except Exception as e:
            print(f"⚠️ Error tracking queue rejection: {e}")
    
    def track_job_failure(
        self,
        job_type: str,
        error: str,
        retry_count: int,
        job_id: str
    ):
        """Track job failures"""
        try:
            if self.redis:
                timestamp = int(time.time())
                key = f"{self.metrics_prefix}jobs:failures"
                
                failure_data = {
                    "job_type": job_type,
                    "error": error,
                    "retry_count": retry_count,
                    "job_id": job_id,
                    "timestamp": timestamp
                }
                
                # Store failure
                self.redis.zadd(
                    f"{key}:{job_type}",
                    {json.dumps(failure_data): timestamp}
                )
                
                # Keep only last 7 days
                cutoff = timestamp - (7 * 86400)
                self.redis.zremrangebyscore(f"{key}:{job_type}", 0, cutoff)
        except Exception as e:
            print(f"⚠️ Error tracking job failure: {e}")
    
    def get_queue_metrics(self) -> Dict[str, Any]:
        """Get comprehensive queue metrics"""
        try:
            stats = job_queue.get_queue_stats()
            
            # Get queue depths
            queue_depths = {}
            for queue_name in ['tutor_queue', 'grading_queue', 'mock_exam_queue', 'helping_queue', 'lesson_queue']:
                depth = stats.get(queue_name, 0)
                queue_depths[queue_name] = depth
                # Track depth
                self.track_queue_depth(queue_name, depth)
            
            # Get processing jobs count
            processing_count = 0
            if self.redis:
                pattern = "processing:*"
                cursor = 0
                while True:
                    cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                    processing_count += len(keys)
                    if cursor == 0:
                        break
            
            return {
                "queue_depths": queue_depths,
                "processing_jobs": processing_count,
                "redis_connected": stats.get('redis_connected', False),
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def get_queue_rejection_metrics(
        self,
        queue_name: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get queue rejection metrics (critical for back-pressure monitoring)"""
        try:
            if not self.redis:
                return {"error": "Redis not available"}
            
            cutoff = int(time.time()) - (hours * 3600)
            rejections = {}
            
            # Map internal queue names to display names
            queue_map = {
                'jobs:tutor': 'tutor_queue',
                'jobs:grading': 'grading_queue',
                'jobs:mock_exam': 'mock_exam_queue',
                'jobs:helping': 'helping_queue',
                'jobs:lesson': 'lesson_queue'
            }
            
            queues_to_check = [queue_name] if queue_name else list(queue_map.keys())
            
            for qn in queues_to_check:
                key = f"{self.metrics_prefix}queues:rejections:{qn}"
                # Get rejections in time range
                rejection_keys = self.redis.zrangebyscore(key, cutoff, int(time.time()))
                
                display_name = queue_map.get(qn, qn)
                rejections[display_name] = {
                    "count": len(rejection_keys),
                    "recent_rejections": [
                        json.loads(rk) for rk in rejection_keys[-10:]  # Last 10 rejections
                    ]
                }
            
            return {
                "rejections": rejections,
                "time_range_hours": hours,
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def get_failure_metrics(self, job_type: Optional[str] = None, hours: int = 24) -> Dict[str, Any]:
        """Get failure metrics for jobs"""
        try:
            if not self.redis:
                return {"error": "Redis not available"}
            
            cutoff = int(time.time()) - (hours * 3600)
            failures = {}
            
            job_types = [job_type] if job_type else [
                'tutor_chat', 'grade_answer', 'grade_mock_exam',
                'explain_concept', 'create_lesson'
            ]
            
            for jt in job_types:
                key = f"{self.metrics_prefix}jobs:failures:{jt}"
                # Get failures in time range
                failure_keys = self.redis.zrangebyscore(key, cutoff, int(time.time()))
                
                failures[jt] = {
                    "count": len(failure_keys),
                    "recent_failures": [
                        json.loads(fk) for fk in failure_keys[-10:]  # Last 10 failures
                    ]
                }
            
            return {
                "failures": failures,
                "time_range_hours": hours,
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def get_processing_time_metrics(
        self,
        job_type: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get processing time metrics"""
        try:
            if not self.redis:
                return {"error": "Redis not available"}
            
            cutoff = int(time.time()) - (hours * 3600)
            metrics = {}
            
            job_types = [job_type] if job_type else [
                'tutor_chat', 'grade_answer', 'grade_mock_exam',
                'explain_concept', 'create_lesson'
            ]
            
            for jt in job_types:
                key = f"{self.metrics_prefix}jobs:processing_time:{jt}"
                # Get processing times in range
                times_data = self.redis.zrangebyscore(key, cutoff, int(time.time()))
                
                if times_data:
                    times = [
                        json.loads(td).get('duration_seconds', 0)
                        for td in times_data
                    ]
                    
                    metrics[jt] = {
                        "count": len(times),
                        "avg_seconds": sum(times) / len(times) if times else 0,
                        "min_seconds": min(times) if times else 0,
                        "max_seconds": max(times) if times else 0,
                        "p50_seconds": sorted(times)[len(times) // 2] if times else 0,
                        "p95_seconds": sorted(times)[int(len(times) * 0.95)] if times else 0,
                        "p99_seconds": sorted(times)[int(len(times) * 0.99)] if times else 0
                    }
                else:
                    metrics[jt] = {
                        "count": 0,
                        "avg_seconds": 0,
                        "min_seconds": 0,
                        "max_seconds": 0
                    }
            
            return {
                "processing_times": metrics,
                "time_range_hours": hours,
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def get_request_latency_metrics(
        self,
        endpoint: Optional[str] = None,
        hours: int = 1
    ) -> Dict[str, Any]:
        """Get API request latency metrics (separate from job processing)"""
        try:
            if not self.redis:
                return {"error": "Redis not available"}
            
            cutoff = int(time.time()) - (hours * 3600)
            metrics = {}
            
            endpoints = [endpoint] if endpoint else [
                '/tutor/chat', '/grade-answer', '/grade-mock-exam',
                '/helping/explain', '/tutor/lesson'
            ]
            
            for ep in endpoints:
                key = f"{self.metrics_prefix}api:requests:{ep}"
                # Get request latencies in range
                request_data = self.redis.zrangebyscore(key, cutoff, int(time.time()))
                
                if request_data:
                    latencies = [
                        json.loads(rd).get('duration_ms', 0)
                        for rd in request_data
                    ]
                    
                    metrics[ep] = {
                        "count": len(latencies),
                        "avg_ms": sum(latencies) / len(latencies) if latencies else 0,
                        "min_ms": min(latencies) if latencies else 0,
                        "max_ms": max(latencies) if latencies else 0,
                        "p50_ms": sorted(latencies)[len(latencies) // 2] if latencies else 0,
                        "p95_ms": sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0,
                        "p99_ms": sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0
                    }
                else:
                    metrics[ep] = {
                        "count": 0,
                        "avg_ms": 0,
                        "min_ms": 0,
                        "max_ms": 0
                    }
            
            return {
                "request_latencies": metrics,
                "time_range_hours": hours,
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def get_worker_health(self) -> Dict[str, Any]:
        """Get worker health metrics (uses lightweight health reporting)"""
        try:
            if not self.redis:
                return {"error": "Redis not available"}
            
            # Get worker health from lightweight health reporting
            from services.worker_health import worker_health_reporter
            workers_summary = worker_health_reporter.get_workers_summary()
            
            # Also check for stale processing jobs (indicates worker crash)
            pattern = "processing:*"
            cursor = 0
            stale_jobs = []
            active_jobs = []
            
            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    job_id = key.replace("processing:", "")
                    # Get job data to check age
                    job_data = job_queue.get_job(job_id)
                    if job_data:
                        created_at = job_data.get('created_at')
                        if created_at:
                            try:
                                created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                age_seconds = (datetime.utcnow() - created_dt.replace(tzinfo=None)).total_seconds()
                                
                                # Consider stale if older than 1 hour
                                if age_seconds > 3600:
                                    stale_jobs.append({
                                        "job_id": job_id,
                                        "age_seconds": age_seconds,
                                        "job_type": job_data.get('job_type')
                                    })
                                else:
                                    active_jobs.append(job_id)
                            except (ValueError, TypeError):
                                active_jobs.append(job_id)
                
                if cursor == 0:
                    break
            
            return {
                "workers": workers_summary,
                "active_processing_jobs": len(active_jobs),
                "stale_processing_jobs": len(stale_jobs),
                "stale_jobs": stale_jobs[:10],  # First 10 stale jobs
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }


    def get_system_health(self) -> Dict[str, Any]:
        """
        Get comprehensive system health status (production observability)
        Includes Redis connectivity, queue status, worker health, and error rates
        """
        try:
            health_status = {
                "timestamp": datetime.utcnow().isoformat(),
                "api_status": "healthy",
                "redis_status": "unknown",
                "queue_status": {},
                "worker_status": {},
                "error_rates": {}
            }
            
            # Check Redis connectivity
            from services.redis_connection import is_redis_available, get_redis_client
            redis_available = is_redis_available()
            
            if redis_available:
                try:
                    redis_client = get_redis_client()
                    redis_client.ping()
                    health_status["redis_status"] = "connected"
                except Exception as e:
                    health_status["redis_status"] = "disconnected"
                    health_status["redis_error"] = str(e)
            else:
                health_status["redis_status"] = "unavailable"
            
            # Get queue metrics
            try:
                queue_metrics = self.get_queue_metrics()
                health_status["queue_status"] = queue_metrics
            except Exception as e:
                health_status["queue_status"] = {"error": str(e)}
            
            # Get worker health
            try:
                worker_health = self.get_worker_health()
                health_status["worker_status"] = worker_health
                
                # Check for critical issues
                if worker_health.get("stale_processing_jobs", 0) > 0:
                    health_status["api_status"] = "degraded"
                    health_status["critical_issues"] = [
                        f"Worker health: {worker_health.get('stale_processing_jobs', 0)} stale jobs detected"
                    ]
            except Exception as e:
                health_status["worker_status"] = {"error": str(e)}
            
            # Get recent error rates (last hour)
            try:
                failure_metrics = self.get_failure_metrics(hours=1)
                total_failures = sum(
                    cat.get("count", 0)
                    for cat in failure_metrics.get("failures", {}).values()
                )
                health_status["error_rates"] = {
                    "failures_last_hour": total_failures,
                    "details": failure_metrics.get("failures", {})
                }
            except Exception as e:
                health_status["error_rates"] = {"error": str(e)}
            
            # Overall status determination
            if health_status["redis_status"] != "connected":
                health_status["api_status"] = "degraded"
            if health_status.get("error_rates", {}).get("failures_last_hour", 0) > 100:
                health_status["api_status"] = "degraded"
            
            return health_status
            
        except Exception as e:
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "api_status": "error",
                "error": str(e)
            }
    
    def track_redis_connectivity_event(self, event: str, available: bool, error: Optional[str] = None):
        """
        Track Redis connectivity events (for failure isolation monitoring)
        """
        try:
            if self.redis:
                timestamp = int(time.time())
                key = f"{self.metrics_prefix}redis:connectivity"
                
                event_data = {
                    "event": event,
                    "available": available,
                    "timestamp": timestamp
                }
                if error:
                    event_data["error"] = error
                
                # Store event
                self.redis.zadd(key, {json.dumps(event_data): timestamp})
                
                # Keep only last 24 hours
                cutoff = timestamp - 86400
                self.redis.zremrangebyscore(key, 0, cutoff)
        except Exception as e:
            print(f"⚠️ Error tracking Redis connectivity: {e}")


# Global observability service instance
observability = ObservabilityService()
