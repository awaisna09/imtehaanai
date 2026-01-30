#!/usr/bin/env python3
"""
Latency Aggregation Service
Aggregates latency metrics from structured logs for background job execution.
Calculates p50/p95/p99 percentiles and time breakdowns by category.
"""

import os
import json
import time
import logging
from typing import Dict, List, Optional, Any
from collections import defaultdict
from datetime import datetime

# Import Redis for log storage/retrieval
try:
    from services.redis_connection import get_redis_client, is_redis_available
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)


class LatencyAggregator:
    """
    Aggregates latency metrics from structured logs.
    Reads from Redis (if available) or can parse log files.
    """

    def __init__(self):
        """Initialize the latency aggregator"""
        self.redis_client = None
        if REDIS_AVAILABLE and is_redis_available():
            try:
                self.redis_client = get_redis_client()
            except Exception as e:
                logger.warning(
                    f"Failed to initialize Redis for latency aggregation: {e}"
                )

        # Configuration
        self.METRICS_RETENTION_HOURS = int(
            os.getenv("METRICS_RETENTION_HOURS", 24)
        )  # 24 hours default
        self.MIN_SAMPLES_FOR_PERCENTILES = int(
            os.getenv("METRICS_MIN_SAMPLES", 10)
        )  # Minimum samples for reliable percentiles

    def _calculate_percentile(
        self,
        values: List[float],
        percentile: float
    ) -> float:
        """
        Calculate percentile from sorted list.

        Args:
            values: Sorted list of values
            percentile: Percentile (0.0-1.0, e.g., 0.95 for p95)

        Returns:
            Percentile value
        """
        if not values:
            return 0.0

        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile)
        index = min(index, len(sorted_values) - 1)
        return sorted_values[index]

    def _aggregate_job_execution_times(
        self,
        job_type: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, List[float]]:
        """
        Aggregate job execution times from observability service metrics.

        Args:
            job_type: Optional job type filter
            hours: Time window in hours

        Returns:
            Dict mapping job_type -> list of execution times (seconds)
        """
        execution_times: Dict[str, List[float]] = defaultdict(list)

        if not self.redis_client:
            logger.warning(
                "Redis not available - cannot aggregate execution times"
            )
            return execution_times

        try:
            cutoff = int(time.time()) - (hours * 3600)

            # Get processing time metrics from observability service
            # Pattern: metrics:jobs:processing_time:{job_type}
            job_types = (
                [job_type] if job_type
                else [
                    'tutor_chat', 'grade_answer', 'grade_mock_exam',
                    'explain_concept', 'create_lesson'
                ]
            )

            for jt in job_types:
                key = f"metrics:jobs:processing_time:{jt}"
                try:
                    # Get metrics in time window
                    metric_entries = self.redis_client.zrangebyscore(
                        key, cutoff, int(time.time())
                    )

                    for entry_json in metric_entries:
                        try:
                            metric_data = json.loads(entry_json)
                            duration = metric_data.get("duration_seconds")
                            if duration is not None:
                                execution_times[jt].append(float(duration))
                        except (json.JSONDecodeError, KeyError, ValueError):
                            continue
                except Exception as e:
                    logger.warning(
                        f"Error processing metrics key {key}: {e}"
                    )
                    continue

        except Exception as e:
            logger.error(f"Error aggregating execution times: {e}")

        return execution_times

    def _aggregate_performance_timings(
        self,
        job_type: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Dict[str, List[float]]]:
        """
        Aggregate performance timings by stage type from observability metrics.

        Args:
            job_type: Optional job type filter
            hours: Time window in hours

        Returns:
            Dict mapping job_type -> stage_type -> list of durations (ms)
        """
        timings: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )

        if not self.redis_client:
            return timings

        try:
            cutoff = int(time.time()) - (hours * 3600)

            # Get performance timing metrics from observability service
            job_types = (
                [job_type] if job_type
                else [
                    'tutor_chat', 'grade_answer', 'grade_mock_exam',
                    'explain_concept', 'create_lesson'
                ]
            )

            # Stage types to track
            stage_types = [
                "ai_provider_call",
                "database_read",
                "database_write",
                "cache_read",
                "cache_write"
            ]

            for jt in job_types:
                for stage_type in stage_types:
                    key = f"metrics:performance_timing:{jt}:{stage_type}"
                    try:
                        timing_entries = self.redis_client.zrangebyscore(
                            key, cutoff, int(time.time())
                        )

                        for entry_json in timing_entries:
                            try:
                                timing_data = json.loads(entry_json)
                                duration_ms = timing_data.get("duration_ms")
                                if duration_ms is not None:
                                    timings[jt][stage_type].append(
                                        float(duration_ms)
                                    )
                            except (json.JSONDecodeError, KeyError, ValueError):
                                continue
                    except Exception as e:
                        logger.debug(
                            f"Error processing timing key {key}: {e}"
                        )
                        continue

        except Exception as e:
            logger.error(f"Error aggregating performance timings: {e}")

        return timings

    def _aggregate_queue_wait_times(
        self,
        job_type: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, List[float]]:
        """
        Aggregate queue wait times from observability service metrics.

        Args:
            job_type: Optional job type filter
            hours: Time window in hours

        Returns:
            Dict mapping job_type -> list of wait times (seconds)
        """
        wait_times: Dict[str, List[float]] = defaultdict(list)

        if not self.redis_client:
            return wait_times

        try:
            cutoff = int(time.time()) - (hours * 3600)

            # Get processing time metrics (which include queue_wait_seconds)
            job_types = (
                [job_type] if job_type
                else [
                    'tutor_chat', 'grade_answer', 'grade_mock_exam',
                    'explain_concept', 'create_lesson'
                ]
            )

            for jt in job_types:
                key = f"metrics:jobs:processing_time:{jt}"
                try:
                    metric_entries = self.redis_client.zrangebyscore(
                        key, cutoff, int(time.time())
                    )

                    for entry_json in metric_entries:
                        try:
                            metric_data = json.loads(entry_json)
                            wait_time = metric_data.get("queue_wait_seconds")
                            if wait_time is not None:
                                wait_times[jt].append(float(wait_time))
                        except (json.JSONDecodeError, KeyError, ValueError):
                            continue
                except Exception as e:
                    logger.warning(f"Error processing wait time key {key}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error aggregating queue wait times: {e}")

        return wait_times

    def get_job_latency_metrics(
        self,
        job_type: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get comprehensive latency metrics for jobs.

        Args:
            job_type: Optional job type filter
            hours: Time window in hours

        Returns:
            Dict with:
                - execution_times: p50/p95/p99 per job_type
                - queue_wait_times: p50/p95/p99 per job_type
                - time_breakdown: breakdown by category per job_type
        """
        # Aggregate execution times
        execution_times = self._aggregate_job_execution_times(
            job_type=job_type,
            hours=hours
        )

        # Aggregate queue wait times
        queue_wait_times = self._aggregate_queue_wait_times(
            job_type=job_type,
            hours=hours
        )

        # Aggregate performance timings by stage type
        performance_timings = self._aggregate_performance_timings(
            job_type=job_type,
            hours=hours
        )

        # Build metrics response
        metrics: Dict[str, Any] = {
            "execution_times": {},
            "queue_wait_times": {},
            "time_breakdown": {},
            "time_range_hours": hours,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Process execution times
        job_types = (
            [job_type] if job_type
            else list(set(list(execution_times.keys()) + list(queue_wait_times.keys())))
        )

        for jt in job_types:
            exec_times = execution_times.get(jt, [])
            wait_times = queue_wait_times.get(jt, [])

            if exec_times:
                metrics["execution_times"][jt] = {
                    "count": len(exec_times),
                    "p50_seconds": self._calculate_percentile(exec_times, 0.50),
                    "p95_seconds": self._calculate_percentile(exec_times, 0.95),
                    "p99_seconds": self._calculate_percentile(exec_times, 0.99),
                    "min_seconds": min(exec_times) if exec_times else 0.0,
                    "max_seconds": max(exec_times) if exec_times else 0.0,
                    "avg_seconds": sum(exec_times) / len(exec_times) if exec_times else 0.0
                }
            else:
                metrics["execution_times"][jt] = {
                    "count": 0,
                    "p50_seconds": 0.0,
                    "p95_seconds": 0.0,
                    "p99_seconds": 0.0,
                    "min_seconds": 0.0,
                    "max_seconds": 0.0,
                    "avg_seconds": 0.0
                }

            if wait_times:
                metrics["queue_wait_times"][jt] = {
                    "count": len(wait_times),
                    "p50_seconds": self._calculate_percentile(wait_times, 0.50),
                    "p95_seconds": self._calculate_percentile(wait_times, 0.95),
                    "p99_seconds": self._calculate_percentile(wait_times, 0.99),
                    "min_seconds": min(wait_times) if wait_times else 0.0,
                    "max_seconds": max(wait_times) if wait_times else 0.0,
                    "avg_seconds": sum(wait_times) / len(wait_times) if wait_times else 0.0
                }
            else:
                metrics["queue_wait_times"][jt] = {
                    "count": 0,
                    "p50_seconds": 0.0,
                    "p95_seconds": 0.0,
                    "p99_seconds": 0.0,
                    "min_seconds": 0.0,
                    "max_seconds": 0.0,
                    "avg_seconds": 0.0
                }

            # Build time breakdown by category for this job type
            breakdown = {
                "llm_api_call_ms": [],
                "database_operations_ms": [],
                "cache_access_ms": [],
                "other_ms": []
            }

            # Aggregate timings by stage type for this job type
            job_timings = performance_timings.get(jt, {})
            for stage_type, durations in job_timings.items():
                if stage_type == "ai_provider_call":
                    breakdown["llm_api_call_ms"].extend(durations)
                elif stage_type in ["database_read", "database_write"]:
                    breakdown["database_operations_ms"].extend(durations)
                elif stage_type in ["cache_read", "cache_write"]:
                    breakdown["cache_access_ms"].extend(durations)
                else:
                    breakdown["other_ms"].extend(durations)

            # Calculate percentiles for each category
            metrics["time_breakdown"][jt] = {}
            for category, durations in breakdown.items():
                if durations:
                    metrics["time_breakdown"][jt][category] = {
                        "count": len(durations),
                        "p50_ms": self._calculate_percentile(durations, 0.50),
                        "p95_ms": self._calculate_percentile(durations, 0.95),
                        "p99_ms": self._calculate_percentile(durations, 0.99),
                        "total_ms": sum(durations),
                        "avg_ms": sum(durations) / len(durations)
                    }
                else:
                    metrics["time_breakdown"][jt][category] = {
                        "count": 0,
                        "p50_ms": 0.0,
                        "p95_ms": 0.0,
                        "p99_ms": 0.0,
                        "total_ms": 0.0,
                        "avg_ms": 0.0
                    }

        return metrics

    def get_prometheus_metrics(self, hours: int = 24) -> str:
        """
        Get metrics in Prometheus exposition format.

        Args:
            hours: Time window in hours

        Returns:
            Prometheus-formatted metrics string
        """
        metrics = self.get_job_latency_metrics(hours=hours)
        lines = []

        # Execution time metrics
        for job_type, data in metrics.get("execution_times", {}).items():
            if data.get("count", 0) > 0:
                lines.append(
                    f'job_execution_time_p50_seconds{{job_type="{job_type}"}} '
                    f'{data["p50_seconds"]:.3f}'
                )
                lines.append(
                    f'job_execution_time_p95_seconds{{job_type="{job_type}"}} '
                    f'{data["p95_seconds"]:.3f}'
                )
                lines.append(
                    f'job_execution_time_p99_seconds{{job_type="{job_type}"}} '
                    f'{data["p99_seconds"]:.3f}'
                )
                lines.append(
                    f'job_execution_time_avg_seconds{{job_type="{job_type}"}} '
                    f'{data["avg_seconds"]:.3f}'
                )
                lines.append(
                    f'job_execution_time_count{{job_type="{job_type}"}} '
                    f'{data["count"]}'
                )

        # Queue wait time metrics
        for job_type, data in metrics.get("queue_wait_times", {}).items():
            if data.get("count", 0) > 0:
                lines.append(
                    f'job_queue_wait_time_p50_seconds{{job_type="{job_type}"}} '
                    f'{data["p50_seconds"]:.3f}'
                )
                lines.append(
                    f'job_queue_wait_time_p95_seconds{{job_type="{job_type}"}} '
                    f'{data["p95_seconds"]:.3f}'
                )
                lines.append(
                    f'job_queue_wait_time_p99_seconds{{job_type="{job_type}"}} '
                    f'{data["p99_seconds"]:.3f}'
                )
                lines.append(
                    f'job_queue_wait_time_avg_seconds{{job_type="{job_type}"}} '
                    f'{data["avg_seconds"]:.3f}'
                )

        # Time breakdown metrics
        for job_type, breakdown in metrics.get("time_breakdown", {}).items():
            for category, data in breakdown.items():
                if data.get("count", 0) > 0:
                    # Convert category name to metric name
                    metric_name = category.replace("_ms", "").replace("_", "_")
                    lines.append(
                        f'job_time_{metric_name}_p50_ms{{job_type="{job_type}"}} '
                        f'{data["p50_ms"]:.3f}'
                    )
                    lines.append(
                        f'job_time_{metric_name}_p95_ms{{job_type="{job_type}"}} '
                        f'{data["p95_ms"]:.3f}'
                    )
                    lines.append(
                        f'job_time_{metric_name}_p99_ms{{job_type="{job_type}"}} '
                        f'{data["p99_ms"]:.3f}'
                    )
                    lines.append(
                        f'job_time_{metric_name}_total_ms{{job_type="{job_type}"}} '
                        f'{data["total_ms"]:.3f}'
                    )

        return "\n".join(lines) if lines else "# No metrics available"

    def get_statsd_metrics(self, hours: int = 24) -> List[str]:
        """
        Get metrics in StatsD format (for sending to StatsD daemon).

        Args:
            hours: Time window in hours

        Returns:
            List of StatsD metric strings
        """
        metrics = self.get_job_latency_metrics(hours=hours)
        lines = []

        # Execution time metrics
        for job_type, data in metrics.get("execution_times", {}).items():
            if data.get("count", 0) > 0:
                lines.append(
                    f"job.execution_time.p50.{job_type}:{data['p50_seconds']:.3f}|g"
                )
                lines.append(
                    f"job.execution_time.p95.{job_type}:{data['p95_seconds']:.3f}|g"
                )
                lines.append(
                    f"job.execution_time.p99.{job_type}:{data['p99_seconds']:.3f}|g"
                )
                lines.append(
                    f"job.execution_time.avg.{job_type}:{data['avg_seconds']:.3f}|g"
                )
                lines.append(
                    f"job.execution_time.count.{job_type}:{data['count']}|c"
                )

        # Queue wait time metrics
        for job_type, data in metrics.get("queue_wait_times", {}).items():
            if data.get("count", 0) > 0:
                lines.append(
                    f"job.queue_wait_time.p50.{job_type}:{data['p50_seconds']:.3f}|g"
                )
                lines.append(
                    f"job.queue_wait_time.p95.{job_type}:{data['p95_seconds']:.3f}|g"
                )
                lines.append(
                    f"job.queue_wait_time.p99.{job_type}:{data['p99_seconds']:.3f}|g"
                )

        # Time breakdown metrics
        for job_type, breakdown in metrics.get("time_breakdown", {}).items():
            for category, data in breakdown.items():
                if data.get("count", 0) > 0:
                    metric_name = category.replace("_ms", "").replace("_", ".")
                    lines.append(
                        f"job.time.{metric_name}.p50.{job_type}:"
                        f"{data['p50_ms']:.3f}|g"
                    )
                    lines.append(
                        f"job.time.{metric_name}.p95.{job_type}:"
                        f"{data['p95_ms']:.3f}|g"
                    )
                    lines.append(
                        f"job.time.{metric_name}.p99.{job_type}:"
                        f"{data['p99_ms']:.3f}|g"
                    )

        return lines


# Singleton instance
_latency_aggregator: Optional[LatencyAggregator] = None


def get_latency_aggregator() -> LatencyAggregator:
    """Get or create the singleton latency aggregator"""
    global _latency_aggregator
    if _latency_aggregator is None:
        _latency_aggregator = LatencyAggregator()
    return _latency_aggregator
