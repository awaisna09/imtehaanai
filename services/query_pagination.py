"""
Query Pagination Utilities
Enforces pagination and explicit column selection for all database queries.
"""

import os
from typing import Any, Optional, List

# Default max page size
DEFAULT_MAX_PAGE_SIZE = int(os.getenv("DB_MAX_PAGE_SIZE", "50"))
MAX_PAGE_SIZE = int(os.getenv("DB_ABSOLUTE_MAX_PAGE_SIZE", "1000"))


def enforce_pagination(
    query_builder: Any,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    max_page_size: Optional[int] = None
) -> Any:
    """
    Enforce pagination on a Supabase query builder.
    
    Args:
        query_builder: Supabase query builder (e.g., from .from().select())
        page: Page number (1-indexed, default: 1)
        page_size: Items per page (default: DEFAULT_MAX_PAGE_SIZE, max: MAX_PAGE_SIZE)
        max_page_size: Override max page size (default: MAX_PAGE_SIZE)
    
    Returns:
        Query builder with pagination applied
    """
    max_page_size = max_page_size or MAX_PAGE_SIZE
    page_size = page_size or DEFAULT_MAX_PAGE_SIZE
    
    # Enforce max page size
    if page_size > max_page_size:
        page_size = max_page_size
    
    # Default to first page if not specified
    page = page or 1
    if page < 1:
        page = 1
    
    # Calculate range (Supabase uses 0-indexed range)
    from_range = (page - 1) * page_size
    to_range = from_range + page_size - 1
    
    # Apply range pagination
    return query_builder.range(from_range, to_range)


def validate_column_selection(columns: str) -> bool:
    """
    Validate that column selection is explicit (not SELECT *).
    
    Args:
        columns: Column selection string (e.g., "id, name, created_at" or "*")
    
    Returns:
        True if explicit columns, False if SELECT *
    """
    if not columns or columns.strip() == "*":
        return False
    return True


def enforce_explicit_columns(
    query_builder: Any,
    columns: str,
    table_name: str = "unknown"
) -> Any:
    """
    Enforce explicit column selection (no SELECT *).
    
    Args:
        query_builder: Supabase query builder
        columns: Column list (comma-separated string)
        table_name: Table name for error messages
    
    Returns:
        Query builder with explicit columns
    
    Raises:
        ValueError: If columns is "*" or empty
    """
    if not validate_column_selection(columns):
        raise ValueError(
            f"SELECT * is not allowed. Table '{table_name}' requires explicit column selection. "
            f"Example: .select('id, name, created_at')"
        )
    
    return query_builder.select(columns)
