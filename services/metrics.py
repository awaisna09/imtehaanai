#!/usr/bin/env python3
"""
Lightweight Production-Grade Metrics Service
Tracks p50, p95, p99 per agent, queue wait time, AI call duration, worker restarts

Features:
- Redis-backed time-series storage
- Efficient percentile calculation
- Minimal overhead
- No external SaaS required
"""

import json
import os
import time
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import defaultdict
from services.redis_connection import get_redis_client, is_redis_available

logger = logging.getLogger(__name__)


class MetricsService:
    """
    Lightweight metrics service for production monitoring
    
    Storage Strategy:
    - Redis sorted sets (ZSET) for time-series data
    - Efficient percentile calculation using sorted data
    - Automatic cleanup of old data
    - Minimal memory footprint
    """
    
    def __init__(self):
        self.redis = get_redis_client() if is_redis_available() else None
        self.metrics_prefix = "metrics:"
        
        # Retention periods (in seconds)
        self.retention_seconds = {
            'agent': int(os.getenv("METRICS_RETENTION_AGENT", "86400")),  # 24 hours
            'queue_wait': int(os.getenv("METRICS_RETENTION_QUEUE", "86400")),  # 24 hours
            'ai_call': int(os.getenv("METRICS_RETENTION_AI", "86400")),  # 24 hours
            'worker_restart': int(os.getenv("METRICS_RETENTION_WORKER", "604800")),  # 7 days
        }
    
    def _calculate_percentiles(self, values: List[float], percentiles: List[int] = [50, 95, 99]) -> Dict[int, float]:
        """
        Calculate percentiles from sorted list of values
        
        Args:
            values: Sorted list of numeric values
            percentiles: List of percentile values to calculate (default: [50, 95, 99])
        
        Returns:
            Dictionary mapping percentile to value
        """
        if not values:
            return {p: 0.0 for p in percentiles}
        
        sorted_values = sorted(values)
        result = {}
        
        for p in percentiles:
            if p < 0 or p > 100:
                continue
            
            index = int((p / 100.0) * (len(sorted_values) - 1))
            result[p] = sorted_values[index]
        
        return result
    
    def track_agent_metric(
        self,
        agent_name: str,
        metric_name: str,
        value: float,
        unit: str = "seconds",
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Track a metric for a specific agent
        
        Args:
            agent_name: Name of the agent (e.g., 'tutor', 'grading', 'mock_exam')
            metric_name: Name of the metric (e.g., 'processing_time', 'success_rate')
            value: Metric value
            unit: Unit of measurement (default: 'seconds')
            metadata: Optional metadata (job_id, trace_id, etc.)
        """
        if not self.redis:
            return
        
        try:
            timestamp = int(time.time())
            key = f"{self.metrics_prefix}agent:{agent_name}:{metric_name}"
            
            metric_data = {
                "value": value,
                "unit": unit,
                "timestamp": timestamp,
                "agent": agent_name,
                "metric": metric_name
            }
            
            if metadata:
                metric_data["metadata"] = metadata
            
            # Store in sorted set (score = timestamp, value = JSON)
            self.redis.zadd(
                key,
                {json.dumps(metric_data, default=str): timestamp}
            )
            
            # Cleanup old data
            cutoff = timestamp - self.retention_seconds['agent']
            self.redis.zremrangebyscore(key, 0, cutoff)
            
        except Exception as e:
            logger.warning(f"Failed to track agent metric {agent_name}:{metric_name}: {e}")
    
    def track_queue_wait_time(
        self,
        job_type: str,
        wait_seconds: float,
        job_id: Optional[str] = None
    ):
        """
        Track queue wait time (time between job creation and processing start)
        
        Args:
            job_type: Type of job
            wait_seconds: Wait time in seconds
            job_id: Optional job ID
        """
        if not self.redis:
            return
        
        try:
            timestamp = int(time.time())
            key = f"{self.metrics_prefix}queue_wait:{job_type}"
            
            metric_data = {
                "wait_seconds": wait_seconds,
                "job_type": job_type,
                "timestamp": timestamp
            }
            
            if job_id:
                metric_data["job_id"] = job_id
            
            # Store in sorted set
            self.redis.zadd(
                key,
                {json.dumps(metric_data, default=str): timestamp}
            )
            
            # Cleanup old data
            cutoff = timestamp - self.retention_seconds['queue_wait']
            self.redis.zremrangebyscore(key, 0, cutoff)
            
        except Exception as e:
            logger.warning(f"Failed to track queue wait time: {e}")
    
    def track_ai_call_duration(
        self,
        agent_name: str,
        duration_ms: float,
        call_type: str = "api_call",  # 'api_call' or 'prompt_construction'
        model: Optional[str] = None,
        prompt_tokens: Optional[int] = None,
        completion_tokens: Optional[int] = None,
        job_id: Optional[str] = None
    ):
        """
        Track AI call duration, separate from prompt construction
        
        Args:
            agent_name: Name of the agent
            duration_ms: Duration in milliseconds
            call_type: Type of call ('api_call' or 'prompt_construction')
            model: Optional model name
            prompt_tokens: Optional prompt token count
            completion_tokens: Optional completion token count
            job_id: Optional job ID
        """
        if not self.redis:
            return
        
        try:
            timestamp = int(time.time())
            key = f"{self.metrics_prefix}ai_call:{agent_name}:{call_type}"
            
            metric_data = {
                "duration_ms": duration_ms,
                "agent": agent_name,
                "call_type": call_type,
                "timestamp": timestamp
            }
            
            if model:
                metric_data["model"] = model
            if prompt_tokens is not None:
                metric_data["prompt_tokens"] = prompt_tokens
            if completion_tokens is not None:
                metric_data["completion_tokens"] = completion_tokens
            if job_id:
                metric_data["job_id"] = job_id
            
            # Store in sorted set
            self.redis.zadd(
                key,
                {json.dumps(metric_data, default=str): timestamp}
            )
            
            # Cleanup old data
            cutoff = timestamp - self.retention_seconds['ai_call']
            self.redis.zremrangebyscore(key, 0, cutoff)
            
        except Exception as e:
            logger.warning(f"Failed to track AI call duration: {e}")
    
    def track_worker_restart(
        self,
        worker_id: str,
        restart_reason: str = "unknown",
        uptime_seconds: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Track worker restart event
        
        Args:
            worker_id: Worker identifier
            restart_reason: Reason for restart (e.g., 'crash', 'memory_limit', 'manual')
            uptime_seconds: Uptime before restart
            metadata: Optional metadata (memory_usage, error, etc.)
        """
        if not self.redis:
            return
        
        try:
            timestamp = int(time.time())
            key = f"{self.metrics_prefix}worker:restarts"
            
            restart_data = {
                "worker_id": worker_id,
                "restart_reason": restart_reason,
                "timestamp": timestamp
            }
            
            if uptime_seconds is not None:
                restart_data["uptime_seconds"] = uptime_seconds
            
            if metadata:
                restart_data["metadata"] = metadata
            
            # Store in sorted set
            self.redis.zadd(
                key,
                {json.dumps(restart_data, default=str): timestamp}
            )
            
            # Cleanup old data
            cutoff = timestamp - self.retention_seconds['worker_restart']
            self.redis.zremrangebyscore(key, 0, cutoff)
            
        except Exception as e:
            logger.warning(f"Failed to track worker restart: {e}")
    
    def get_agent_metrics(
        self,
        agent_name: str,
        metric_name: str,
        hours: int = 24,
        percentiles: List[int] = [50, 95, 99]
    ) -> Dict[str, Any]:
        """
        Get metrics for a specific agent with percentile calculations
        
        Args:
            agent_name: Name of the agent
            metric_name: Name of the metric
            hours: Time range in hours
            percentiles: Percentiles to calculate
        
        Returns:
            Dictionary with count, min, max, avg, and percentiles
        """
        if not self.redis:
            return {"error": "Redis not available"}
        
        try:
            cutoff = int(time.time()) - (hours * 3600)
            key = f"{self.metrics_prefix}agent:{agent_name}:{metric_name}"
            
            # Get all values in time range
            metric_records = self.redis.zrangebyscore(key, cutoff, int(time.time()))
            
            if not metric_records:
                return {
                    "agent": agent_name,
                    "metric": metric_name,
                    "count": 0,
                    "time_range_hours": hours,
                    "percentiles": {p: 0.0 for p in percentiles}
                }
            
            # Extract values
            values = []
            for record_str in metric_records:
                try:
                    record = json.loads(record_str)
                    value = record.get("value", 0)
                    if isinstance(value, (int, float)):
                        values.append(float(value))
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
            
            if not values:
                return {
                    "agent": agent_name,
                    "metric": metric_name,
                    "count": 0,
                    "time_range_hours": hours,
                    "percentiles": {p: 0.0 for p in percentiles}
                }
            
            # Calculate statistics
            percentile_values = self._calculate_percentiles(values, percentiles)
            
            return {
                "agent": agent_name,
                "metric": metric_name,
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
                "percentiles": percentile_values,
                "time_range_hours": hours,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get agent metrics: {e}")
            return {"error": str(e)}
    
    def get_queue_wait_metrics(
        self,
        job_type: Optional[str] = None,
        hours: int = 24,
        percentiles: List[int] = [50, 95, 99]
    ) -> Dict[str, Any]:
        """
        Get queue wait time metrics with percentiles
        
        Args:
            job_type: Optional job type filter
            hours: Time range in hours
            percentiles: Percentiles to calculate
        
        Returns:
            Dictionary with metrics per job type
        """
        if not self.redis:
            return {"error": "Redis not available"}
        
        try:
            cutoff = int(time.time()) - (hours * 3600)
            metrics = {}
            
            job_types = [job_type] if job_type else [
                'tutor_chat', 'grade_answer', 'grade_mock_exam',
                'explain_concept', 'create_lesson'
            ]
            
            for jt in job_types:
                key = f"{self.metrics_prefix}queue_wait:{jt}"
                records = self.redis.zrangebyscore(key, cutoff, int(time.time()))
                
                if not records:
                    metrics[jt] = {
                        "count": 0,
                        "percentiles": {p: 0.0 for p in percentiles}
                    }
                    continue
                
                # Extract wait times
                wait_times = []
                for record_str in records:
                    try:
                        record = json.loads(record_str)
                        wait_time = record.get("wait_seconds", 0)
                        if isinstance(wait_time, (int, float)):
                            wait_times.append(float(wait_time))
                    except (json.JSONDecodeError, ValueError, TypeError):
                        continue
                
                if wait_times:
                    percentile_values = self._calculate_percentiles(wait_times, percentiles)
                    metrics[jt] = {
                        "count": len(wait_times),
                        "min": min(wait_times),
                        "max": max(wait_times),
                        "avg": sum(wait_times) / len(wait_times),
                        "percentiles": percentile_values
                    }
                else:
                    metrics[jt] = {
                        "count": 0,
                        "percentiles": {p: 0.0 for p in percentiles}
                    }
            
            return {
                "queue_wait_times": metrics,
                "time_range_hours": hours,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get queue wait metrics: {e}")
            return {"error": str(e)}
    
    def get_ai_call_metrics(
        self,
        agent_name: Optional[str] = None,
        call_type: str = "api_call",
        hours: int = 24,
        percentiles: List[int] = [50, 95, 99]
    ) -> Dict[str, Any]:
        """
        Get AI call duration metrics (separate from prompt construction)
        
        Args:
            agent_name: Optional agent name filter
            call_type: Type of call ('api_call' or 'prompt_construction')
            hours: Time range in hours
            percentiles: Percentiles to calculate
        
        Returns:
            Dictionary with metrics per agent
        """
        if not self.redis:
            return {"error": "Redis not available"}
        
        try:
            cutoff = int(time.time()) - (hours * 3600)
            metrics = {}
            
            agents = [agent_name] if agent_name else [
                'tutor', 'grading', 'mock_exam', 'helping', 'lesson'
            ]
            
            for agent in agents:
                key = f"{self.metrics_prefix}ai_call:{agent}:{call_type}"
                records = self.redis.zrangebyscore(key, cutoff, int(time.time()))
                
                if not records:
                    metrics[agent] = {
                        "count": 0,
                        "percentiles": {p: 0.0 for p in percentiles}
                    }
                    continue
                
                # Extract durations
                durations = []
                for record_str in records:
                    try:
                        record = json.loads(record_str)
                        duration = record.get("duration_ms", 0)
                        if isinstance(duration, (int, float)):
                            durations.append(float(duration))
                    except (json.JSONDecodeError, ValueError, TypeError):
                        continue
                
                if durations:
                    percentile_values = self._calculate_percentiles(durations, percentiles)
                    metrics[agent] = {
                        "count": len(durations),
                        "min": min(durations),
                        "max": max(durations),
                        "avg": sum(durations) / len(durations),
                        "percentiles": percentile_values,
                        "unit": "milliseconds"
                    }
                else:
                    metrics[agent] = {
                        "count": 0,
                        "percentiles": {p: 0.0 for p in percentiles}
                    }
            
            return {
                "ai_call_durations": metrics,
                "call_type": call_type,
                "time_range_hours": hours,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get AI call metrics: {e}")
            return {"error": str(e)}
    
    def get_worker_restart_metrics(
        self,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get worker restart metrics
        
        Args:
            hours: Time range in hours
        
        Returns:
            Dictionary with restart statistics
        """
        if not self.redis:
            return {"error": "Redis not available"}
        
        try:
            cutoff = int(time.time()) - (hours * 3600)
            key = f"{self.metrics_prefix}worker:restarts"
            
            records = self.redis.zrangebyscore(key, cutoff, int(time.time()))
            
            if not records:
                return {
                    "total_restarts": 0,
                    "time_range_hours": hours,
                    "restarts_by_reason": {},
                    "recent_restarts": []
                }
            
            # Parse records
            restarts = []
            restarts_by_reason = defaultdict(int)
            
            for record_str in records:
                try:
                    record = json.loads(record_str)
                    restarts.append(record)
                    reason = record.get("restart_reason", "unknown")
                    restarts_by_reason[reason] += 1
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
            
            # Sort by timestamp (most recent first)
            restarts.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            
            return {
                "total_restarts": len(restarts),
                "time_range_hours": hours,
                "restarts_by_reason": dict(restarts_by_reason),
                "recent_restarts": restarts[:20],  # Last 20 restarts
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get worker restart metrics: {e}")
            return {"error": str(e)}
    
    def get_all_metrics_summary(
        self,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get comprehensive metrics summary
        
        Args:
            hours: Time range in hours
        
        Returns:
            Dictionary with all metrics
        """
        return {
            "queue_wait": self.get_queue_wait_metrics(hours=hours),
            "ai_call_api": self.get_ai_call_metrics(call_type="api_call", hours=hours),
            "ai_call_prompt": self.get_ai_call_metrics(call_type="prompt_construction", hours=hours),
            "worker_restarts": self.get_worker_restart_metrics(hours=hours),
            "timestamp": datetime.utcnow().isoformat()
        }


# Global metrics service instance
metrics_service = MetricsService()
