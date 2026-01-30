"""
Crash Logger Service
Logs fatal exceptions with full context to persistent file
"""

import os
import sys
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# Try to import psutil, fallback if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# Create logs directory if it doesn't exist
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Crash log file
CRASH_LOG_FILE = LOGS_DIR / "crashes.log"


def get_process_info() -> Dict[str, Any]:
    """Get current process information"""
    if not PSUTIL_AVAILABLE:
        return {
            "error": "psutil not available - install with: pip install psutil",
            "pid": os.getpid()
        }
    
    try:
        process = psutil.Process()
        
        # Get memory info
        memory_info = process.memory_info()
        
        # Get CPU info (non-blocking)
        try:
            cpu_percent = process.cpu_percent(interval=0.1)
        except Exception:
            cpu_percent = None
        
        # Get uptime
        create_time = process.create_time()
        uptime_seconds = datetime.utcnow().timestamp() - create_time
        
        result = {
            "pid": process.pid,
            "memory_rss_mb": round(memory_info.rss / 1024 / 1024, 2),
            "memory_vms_mb": round(memory_info.vms / 1024 / 1024, 2),
            "uptime_seconds": round(uptime_seconds, 2),
            "uptime_formatted": _format_uptime(uptime_seconds),
            "num_threads": process.num_threads()
        }
        
        if cpu_percent is not None:
            result["cpu_percent"] = round(cpu_percent, 2)
        
        # File descriptors (may not be available on Windows)
        try:
            if hasattr(process, 'num_fds'):
                result["num_fds"] = process.num_fds()
        except Exception:
            pass
        
        return result
    except Exception as e:
        return {
            "error": f"Failed to get process info: {str(e)}",
            "pid": os.getpid()
        }


def _format_uptime(seconds: float) -> str:
    """Format uptime as human-readable string"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"


def log_crash(
    service_name: str,
    exception: Exception,
    context: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log fatal exception with full context to persistent file
    
    Args:
        service_name: Name of service (e.g., 'backend', 'worker')
        exception: The exception that caused the crash
        context: Additional context (e.g., active_jobs for workers)
    """
    try:
        # Get stack trace
        exc_type = type(exception).__name__
        exc_message = str(exception)
        exc_traceback = traceback.format_exc()
        
        # Get process info
        process_info = get_process_info()
        
        # Build crash report
        crash_report = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": service_name,
            "exception": {
                "type": exc_type,
                "message": exc_message,
                "traceback": exc_traceback
            },
            "process": process_info,
            "context": context or {}
        }
        
        # Write to crash log file (append mode)
        with open(CRASH_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "=" * 80 + "\n")
            f.write(f"CRASH REPORT - {crash_report['timestamp']}\n")
            f.write("=" * 80 + "\n")
            f.write(json.dumps(crash_report, indent=2, default=str))
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())  # Force write to disk
        
        # Also print to stderr (will be captured by PM2 if running)
        print(f"\n{'='*80}", file=sys.stderr)
        print(f"FATAL CRASH in {service_name}", file=sys.stderr)
        print(f"{'='*80}", file=sys.stderr)
        print(f"Exception: {exc_type}: {exc_message}", file=sys.stderr)
        print(f"Uptime: {process_info.get('uptime_formatted', 'unknown')}", file=sys.stderr)
        print(f"Memory: {process_info.get('memory_rss_mb', 'unknown')} MB RSS", file=sys.stderr)
        if context and 'active_jobs' in context:
            print(f"Active Jobs: {context['active_jobs']}", file=sys.stderr)
        print(f"Full crash report saved to: {CRASH_LOG_FILE}", file=sys.stderr)
        print(f"\nStack Trace:\n{exc_traceback}", file=sys.stderr)
        print(f"{'='*80}\n", file=sys.stderr)
        sys.stderr.flush()
        
    except Exception as log_error:
        # If crash logging itself fails, write minimal info to stderr
        exc_type = type(exception).__name__
        exc_message = str(exception)
        print(f"\nCRITICAL: Failed to log crash: {log_error}", file=sys.stderr)
        print(f"Original exception: {exc_type}: {exc_message}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()


def get_active_jobs_count(worker_instance: Optional[Any] = None) -> int:
    """
    Get active jobs count from worker instance
    
    Args:
        worker_instance: EnhancedAIWorker instance (if available)
    
    Returns:
        Active jobs count or 0 if unavailable
    """
    try:
        if worker_instance and hasattr(worker_instance, 'active_jobs'):
            return worker_instance.active_jobs
    except Exception:
        pass
    return 0
