#!/usr/bin/env python3
"""
Memory Monitor Service
Tracks memory usage, peak memory, and logs memory on restart

Features:
- Peak memory tracking
- Memory usage logging
- Integration with metrics service
- Optional periodic monitoring
"""

import os
import sys
import json
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

# Try to import psutil
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Memory log file
MEMORY_LOG_FILE = LOGS_DIR / "memory.log"

# Peak memory tracking (in-memory, can be persisted to Redis if needed)
_peak_memory_mb: float = 0.0
_peak_memory_timestamp: Optional[float] = None


def get_memory_usage() -> Dict[str, Any]:
    """
    Get current memory usage for this process
    
    Returns:
        Dictionary with memory information
    """
    if not PSUTIL_AVAILABLE:
        return {
            "error": "psutil not available - install with: pip install psutil",
            "pid": os.getpid()
        }
    
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        
        # Get system memory info (optional)
        try:
            system_memory = psutil.virtual_memory()
            system_available_mb = round(system_memory.available / 1024 / 1024, 2)
            system_total_mb = round(system_memory.total / 1024 / 1024, 2)
            system_percent = round(system_memory.percent, 2)
        except Exception:
            system_available_mb = None
            system_total_mb = None
            system_percent = None
        
        rss_mb = round(memory_info.rss / 1024 / 1024, 2)
        vms_mb = round(memory_info.vms / 1024 / 1024, 2)
        
        # Update peak memory
        global _peak_memory_mb, _peak_memory_timestamp
        if rss_mb > _peak_memory_mb:
            _peak_memory_mb = rss_mb
            _peak_memory_timestamp = time.time()
        
        result = {
            "pid": process.pid,
            "memory_rss_mb": rss_mb,
            "memory_vms_mb": vms_mb,
            "peak_memory_mb": _peak_memory_mb,
            "peak_memory_timestamp": datetime.fromtimestamp(_peak_memory_timestamp).isoformat() if _peak_memory_timestamp else None,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if system_available_mb is not None:
            result["system_available_mb"] = system_available_mb
            result["system_total_mb"] = system_total_mb
            result["system_percent"] = system_percent
        
        return result
    except Exception as e:
        return {
            "error": f"Failed to get memory usage: {str(e)}",
            "pid": os.getpid()
        }


def log_memory_usage(
    service_name: str,
    reason: str = "periodic",
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log memory usage to file
    
    Args:
        service_name: Name of service (e.g., 'backend', 'worker')
        reason: Reason for logging (e.g., 'restart', 'periodic', 'threshold')
        context: Additional context
    """
    try:
        memory_info = get_memory_usage()
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": service_name,
            "reason": reason,
            "memory": memory_info,
            "context": context or {}
        }
        
        # Write to memory log file
        with open(MEMORY_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{json.dumps(log_entry, default=str)}\n")
            f.flush()
            os.fsync(f.fileno())  # Force write to disk
        
        # Also log to structured logger if available
        try:
            from services.structured_logging import structured_logger
            structured_logger.log_worker_event(
                event="memory_usage",
                worker_id=service_name,
                memory_rss_mb=memory_info.get("memory_rss_mb", 0),
                memory_vms_mb=memory_info.get("memory_vms_mb", 0),
                peak_memory_mb=memory_info.get("peak_memory_mb", 0),
                reason=reason
            )
        except ImportError:
            pass  # Structured logger not available
        
        # Track in metrics service if available
        try:
            from services.metrics import metrics_service
            # Track memory usage as a metric
            metrics_service.track_agent_metric(
                agent_name=service_name,
                metric_name="memory_usage_mb",
                value=memory_info.get("memory_rss_mb", 0),
                unit="mb",
                metadata={
                    "reason": reason,
                    "peak_memory_mb": memory_info.get("peak_memory_mb", 0)
                }
            )
        except ImportError:
            pass  # Metrics service not available
        
    except Exception as e:
        logger.warning(f"Failed to log memory usage: {e}")


def log_memory_restart(
    service_name: str,
    memory_mb: float,
    threshold_mb: float,
    uptime_seconds: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log memory-triggered restart
    
    Args:
        service_name: Name of service
        memory_mb: Memory usage that triggered restart
        threshold_mb: Memory threshold that was exceeded
        uptime_seconds: Uptime before restart
        metadata: Additional metadata
    """
    try:
        import json
        
        restart_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": service_name,
            "event": "memory_restart",
            "memory_mb": memory_mb,
            "threshold_mb": threshold_mb,
            "uptime_seconds": uptime_seconds,
            "metadata": metadata or {}
        }
        
        # Write to memory log file
        with open(MEMORY_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{json.dumps(restart_entry, default=str)}\n")
            f.flush()
            os.fsync(f.fileno())
        
        # Track in metrics service
        try:
            from services.metrics import metrics_service
            metrics_service.track_worker_restart(
                worker_id=service_name,
                restart_reason="memory_limit",
                uptime_seconds=uptime_seconds,
                metadata={
                    "memory_mb": memory_mb,
                    "threshold_mb": threshold_mb,
                    **(metadata or {})
                }
            )
        except ImportError:
            pass
        
        # Log to stderr (captured by PM2)
        print(
            f"\n{'='*80}",
            f"MEMORY RESTART - {service_name}",
            f"{'='*80}",
            f"Memory: {memory_mb} MB (threshold: {threshold_mb} MB)",
            f"Uptime: {uptime_seconds:.1f}s" if uptime_seconds else "Uptime: unknown",
            f"Timestamp: {restart_entry['timestamp']}",
            f"{'='*80}\n",
            sep="\n",
            file=sys.stderr
        )
        sys.stderr.flush()
        
    except Exception as e:
        logger.error(f"Failed to log memory restart: {e}")


def get_peak_memory() -> Dict[str, Any]:
    """
    Get peak memory information
    
    Returns:
        Dictionary with peak memory details
    """
    global _peak_memory_mb, _peak_memory_timestamp
    
    return {
        "peak_memory_mb": _peak_memory_mb,
        "peak_memory_timestamp": datetime.fromtimestamp(_peak_memory_timestamp).isoformat() if _peak_memory_timestamp else None,
        "current_memory_mb": get_memory_usage().get("memory_rss_mb", 0)
    }


def reset_peak_memory():
    """Reset peak memory tracking (useful after restart)"""
    global _peak_memory_mb, _peak_memory_timestamp
    _peak_memory_mb = 0.0
    _peak_memory_timestamp = None
