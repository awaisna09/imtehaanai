#!/usr/bin/env python3
"""
Batch Parallelization Utility

Provides centralized utilities for parallelizing independent batch operations
with configurable concurrency limits, error handling, and result aggregation.
"""

import asyncio
import logging
import os
from typing import (
    Callable, List, Any, Optional, TypeVar, Tuple
)
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('config.env')

# Type variable for generic function return types
T = TypeVar('T')


class BatchParallelizationConfig:
    """Configuration for batch parallelization limits"""

    # Base concurrency limits from environment
    WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", 3))
    MAX_DB_CONNECTIONS = int(os.getenv("MAX_DB_CONNECTIONS", 10))

    # Job-specific concurrency multipliers
    # These allow different job types to have different concurrency limits
    MASTERY_UPDATE_CONCURRENCY_MULTIPLIER = float(
        os.getenv("BATCH_MASTERY_UPDATE_CONCURRENCY_MULTIPLIER", "2.0")
    )
    CONCEPT_PROCESSING_CONCURRENCY_MULTIPLIER = float(
        os.getenv(
            "BATCH_CONCEPT_PROCESSING_CONCURRENCY_MULTIPLIER", "2.0"
        )
    )
    DEFAULT_BATCH_CONCURRENCY_MULTIPLIER = float(
        os.getenv("BATCH_DEFAULT_CONCURRENCY_MULTIPLIER", "1.5")
    )

    @classmethod
    def get_max_concurrency(
        cls,
        job_type: str,
        item_count: int,
        base_limit: Optional[int] = None
    ) -> int:
        """
        Calculate maximum concurrency for a batch operation.

        Args:
            job_type: Type of batch job (e.g., 'mastery_update',
                     'concept_processing')
            item_count: Number of items to process
            base_limit: Optional base limit (defaults to MAX_DB_CONNECTIONS)

        Returns:
            Maximum concurrent operations (capped by item_count)
        """
        if base_limit is None:
            base_limit = cls.MAX_DB_CONNECTIONS

        # Get multiplier for job type
        multiplier_map = {
            'mastery_update': cls.MASTERY_UPDATE_CONCURRENCY_MULTIPLIER,
            'concept_processing': (
                cls.CONCEPT_PROCESSING_CONCURRENCY_MULTIPLIER
            ),
        }
        multiplier = multiplier_map.get(
            job_type, cls.DEFAULT_BATCH_CONCURRENCY_MULTIPLIER
        )

        # Calculate max concurrency
        max_concurrent = int(base_limit * multiplier)

        # Cap at item count (no point in exceeding)
        max_concurrent = min(max_concurrent, item_count)

        # Ensure at least 1
        max_concurrent = max(1, max_concurrent)

        return max_concurrent


async def process_batch_parallel(
    items: List[Any],
    process_func: Callable[[Any], T],
    job_type: str = "default",
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    base_limit: Optional[int] = None,
    error_handler: Optional[
        Callable[[Any, Exception], Optional[T]]
    ] = None
) -> List[Tuple[int, T, Optional[Exception]]]:
    """
    Process a batch of items in parallel with concurrency limiting.

    Each item is processed independently, and results are aggregated
    deterministically (preserving input order).

    Args:
        items: List of items to process
        process_func: Function to process each item (synchronous)
        job_type: Type of batch job (for concurrency limit calculation)
        job_id: Optional job ID for instrumentation
        trace_id: Optional trace ID for instrumentation
        base_limit: Optional base concurrency limit
        error_handler: Optional function to handle errors per item

    Returns:
        List of tuples: (index, result, exception)
        - index: Original index in items list
        - result: Processed result (None if error)
        - exception: Exception if processing failed (None if success)
    """
    if not items:
        return []

    # Calculate max concurrency
    max_concurrent = BatchParallelizationConfig.get_max_concurrency(
        job_type, len(items), base_limit
    )

    logger.info(
        f"[BATCH_PARALLEL] Processing {len(items)} items with "
        f"max_concurrency={max_concurrent} (job_type={job_type})"
    )

    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_item_async(
        index: int, item: Any
    ) -> Tuple[int, T, Optional[Exception]]:
        """Process a single item with concurrency limit"""
        async with semaphore:
            try:
                # Run synchronous function in thread pool
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, process_func, item
                )
                return (index, result, None)
            except Exception as e:
                logger.warning(
                    f"[BATCH_PARALLEL] Item {index} failed: {e}",
                    exc_info=True
                )

                # Try error handler if provided
                if error_handler:
                    try:
                        fallback_result = error_handler(item, e)
                        return (index, fallback_result, e)
                    except Exception as handler_error:
                        logger.error(
                            f"[BATCH_PARALLEL] Error handler failed: "
                            f"{handler_error}",
                            exc_info=True
                        )

                return (index, None, e)

    # Create tasks for all items
    tasks = [
        process_item_async(i, item)
        for i, item in enumerate(items)
    ]

    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results and handle exceptions from gather itself
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(
                f"[BATCH_PARALLEL] Task {i} raised exception: {result}",
                exc_info=True
            )
            processed_results.append((i, None, result))
        else:
            processed_results.append(result)

    # Sort by index to preserve input order
    processed_results.sort(key=lambda x: x[0])

    # Log summary
    success_count = sum(
        1 for _, _, exc in processed_results if exc is None
    )
    failure_count = len(processed_results) - success_count

    logger.info(
        f"[BATCH_PARALLEL] Completed: {success_count} success, "
        f"{failure_count} failures"
    )

    return processed_results


def run_batch_parallel_sync(
    items: List[Any],
    process_func: Callable[[Any], T],
    job_type: str = "default",
    job_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    base_limit: Optional[int] = None,
    error_handler: Optional[
        Callable[[Any, Exception], Optional[T]]
    ] = None
) -> List[Tuple[int, T, Optional[Exception]]]:
    """
    Synchronous wrapper for process_batch_parallel.

    Use this when calling from synchronous contexts (e.g., LangGraph nodes).

    Args:
        items: List of items to process
        process_func: Function to process each item (synchronous)
        job_type: Type of batch job (for concurrency limit calculation)
        job_id: Optional job ID for instrumentation
        trace_id: Optional trace ID for instrumentation
        base_limit: Optional base concurrency limit
        error_handler: Optional function to handle errors per item

    Returns:
        List of tuples: (index, result, exception)
    """
    try:
        # Try to get existing event loop
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is running, we can't use asyncio.run()
            # Fall back to sequential processing
            logger.warning(
                "[BATCH_PARALLEL] Event loop is running, "
                "falling back to sequential processing"
            )
            return _process_sequential_fallback(
                items, process_func, error_handler
            )
    except RuntimeError:
        # No event loop exists, create one
        pass

    # Run async code in new event loop
    return asyncio.run(
        process_batch_parallel(
            items, process_func, job_type, job_id, trace_id,
            base_limit, error_handler
        )
    )


def _process_sequential_fallback(
    items: List[Any],
    process_func: Callable[[Any], T],
    error_handler: Optional[
        Callable[[Any, Exception], Optional[T]]
    ] = None
) -> List[Tuple[int, T, Optional[Exception]]]:
    """Fallback to sequential processing if async is not available"""
    results = []
    for i, item in enumerate(items):
        try:
            result = process_func(item)
            results.append((i, result, None))
        except Exception as e:
            logger.warning(
                f"[BATCH_PARALLEL] Sequential fallback: Item {i} "
                f"failed: {e}",
                exc_info=True
            )
            if error_handler:
                try:
                    fallback_result = error_handler(item, e)
                    results.append((i, fallback_result, e))
                except Exception:
                    results.append((i, None, e))
            else:
                results.append((i, None, e))
    return results
