"""
Custom Exceptions for Queue and Worker Operations
"""


class QueueFullException(Exception):
    """Raised when queue is full and cannot accept more jobs"""
    pass


class WorkerUnavailableException(Exception):
    """Raised when worker is unavailable"""
    pass
