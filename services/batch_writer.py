"""
Batched Database Write Service
Batches database writes to avoid IO spikes
"""

import os
import time
import threading
from typing import Dict, List, Any, Optional, Callable
from collections import defaultdict
from queue import Queue
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Import Supabase operations helper for concurrency limiting
from services.supabase_ops import sb_execute

load_dotenv('config.env')

# Batch configuration
BATCH_SIZE = int(os.getenv("DB_BATCH_SIZE", 50))  # Batch size
BATCH_INTERVAL = float(os.getenv("DB_BATCH_INTERVAL", 2.0))  # Seconds between batches
MAX_BATCH_WAIT = float(os.getenv("MAX_BATCH_WAIT", 5.0))  # Maximum wait time before flushing


class BatchWriter:
    """Batched database write service to avoid IO spikes"""
    
    def __init__(self, batch_size: int = BATCH_SIZE, batch_interval: float = BATCH_INTERVAL):
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self.max_wait = MAX_BATCH_WAIT
        
        # Queue for pending writes (table -> list of writes)
        self.pending_writes: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.last_flush: Dict[str, float] = defaultdict(float)
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Background thread for periodic flushing
        self.flush_thread: Optional[threading.Thread] = None
        self.running = False
        
        # Write handler function (set by worker)
        self.write_handler: Optional[Callable[[str, List[Dict[str, Any]]], None]] = None
        
        # Rollup job queue (for async rollup processing)
        self.rollup_queue: List[Dict[str, Any]] = []
        self.rollup_dedupe: Dict[str, float] = {}  # dedupe_key -> timestamp
        self.rollup_lock = threading.Lock()
    
    def set_write_handler(self, handler: Callable[[str, List[Dict[str, Any]]], None]):
        """Set the function that will execute batched writes"""
        self.write_handler = handler
    
    def enqueue_write(
        self,
        table: str,
        operation: str,  # 'insert', 'update', 'upsert', 'delete'
        data: Optional[Dict[str, Any]] = None,
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Enqueue a database write operation for batching
        
        Args:
            table: Table name
            operation: Operation type ('insert', 'update', 'upsert', 'delete')
            data: Data to write (required for insert/update/upsert, optional for delete)
            filters: Optional filters for update/delete operations
        """
        with self.lock:
            write_op = {
                'operation': operation,
                'data': data,
                'filters': filters,
                'timestamp': time.time()
            }
            # Validate operation
            if operation not in ['insert', 'update', 'upsert', 'delete']:
                raise ValueError(f"Invalid operation: {operation}. Must be 'insert', 'update', 'upsert', or 'delete'")
            # Validate data for non-delete operations
            if operation != 'delete' and data is None:
                raise ValueError(f"Data is required for {operation} operation")
            
            self.pending_writes[table].append(write_op)
            current_count = len(self.pending_writes[table])
            
            # Flush if batch size reached
            if current_count >= self.batch_size:
                self._flush_table(table)
            else:
                # Check if max wait time exceeded
                if table not in self.last_flush:
                    self.last_flush[table] = time.time()
                else:
                    elapsed = time.time() - self.last_flush[table]
                    if elapsed >= self.max_wait:
                        self._flush_table(table)
    
    def _flush_table(self, table: str):
        """Flush pending writes for a specific table"""
        if not self.pending_writes[table]:
            return
        
        # Get pending writes
        writes = self.pending_writes[table].copy()
        self.pending_writes[table].clear()
        self.last_flush[table] = time.time()
        
        # Execute batched write
        if self.write_handler:
            try:
                self.write_handler(table, writes)
                print(f"✅ Batched write: {table} ({len(writes)} operations)")
            except Exception as e:
                print(f"❌ Batch write error for {table}: {e}")
                # Re-enqueue failed writes (simple retry)
                with self.lock:
                    self.pending_writes[table].extend(writes)
        else:
            print(f"⚠️ No write handler set, writes lost for {table}")
    
    def flush_all(self):
        """Flush all pending writes for all tables"""
        with self.lock:
            tables = list(self.pending_writes.keys())
            for table in tables:
                if self.pending_writes[table]:
                    self._flush_table(table)
    
    def start_periodic_flush(self):
        """Start background thread for periodic flushing"""
        if self.running:
            return
        
        self.running = True
        
        def flush_loop():
            while self.running:
                time.sleep(self.batch_interval)
                with self.lock:
                    # Flush tables that have pending writes and exceeded interval
                    current_time = time.time()
                    tables_to_flush = []
                    
                    for table, last_flush_time in self.last_flush.items():
                        if self.pending_writes[table]:
                            elapsed = current_time - last_flush_time
                            if elapsed >= self.batch_interval:
                                tables_to_flush.append(table)
                    
                    for table in tables_to_flush:
                        self._flush_table(table)
        
        self.flush_thread = threading.Thread(target=flush_loop, daemon=True)
        self.flush_thread.start()
        print(f"✅ Batch writer periodic flush started (interval: {self.batch_interval}s)")
    
    def stop_periodic_flush(self):
        """Stop background thread"""
        self.running = False
        if self.flush_thread:
            self.flush_thread.join(timeout=2)
        # Flush remaining writes
        self.flush_all()
        print("🛑 Batch writer stopped, flushed remaining writes")
    
    def enqueue_rollup(self, user_id: str, dedupe_key: str):
        """
        Enqueue a rollup job for async processing.
        
        Args:
            user_id: User ID to process rollup for
            dedupe_key: Deduplication key (format: "rollup:{user_id}:{date}")
        """
        with self.rollup_lock:
            # Check deduplication (in-memory fallback)
            if dedupe_key in self.rollup_dedupe:
                # Check if dedupe entry is still valid (within 1 hour)
                if time.time() - self.rollup_dedupe[dedupe_key] < 3600:
                    print(f"♻️ Rollup job already queued: {dedupe_key}")
                    return
            
            # Add to rollup queue
            self.rollup_queue.append({
                "user_id": user_id,
                "dedupe_key": dedupe_key,
                "timestamp": time.time()
            })
            self.rollup_dedupe[dedupe_key] = time.time()
            print(f"✅ Rollup job queued: {user_id} (dedupe: {dedupe_key})")
    
    def get_pending_rollups(self) -> List[Dict[str, Any]]:
        """
        Get pending rollup jobs and clear the queue.
        Returns list of rollup jobs to process.
        """
        with self.rollup_lock:
            jobs = self.rollup_queue.copy()
            self.rollup_queue.clear()
            # Clean old dedupe entries (older than 1 hour)
            current_time = time.time()
            self.rollup_dedupe = {
                k: v for k, v in self.rollup_dedupe.items()
                if current_time - v < 3600
            }
            return jobs


# Global batch writer instance
batch_writer = BatchWriter()


def execute_batched_write(table: str, writes: List[Dict[str, Any]], supabase_client):
    """
    Execute batched writes for a table using Supabase client
    
    Args:
        table: Table name
        writes: List of write operations
        supabase_client: Supabase client instance
    """
    if not writes:
        return
    
    try:
        # Group writes by operation type
        inserts = []
        updates = []
        upserts = []
        deletes = []
        
        for write in writes:
            op = write.get('operation', 'insert')
            data = write.get('data', {})
            filters = write.get('filters')
            
            if op == 'insert':
                inserts.append(data)
            elif op == 'update':
                updates.append((data, filters))
            elif op == 'upsert':
                upserts.append(data)
            elif op == 'delete':
                deletes.append(filters)
        
        # Execute inserts in batch
        if inserts:
            sb_execute(supabase_client.table(table).insert(inserts))
        
        # Execute upserts in batch
        if upserts:
            # For daily_analytics, specify conflict resolution on (date, user_id)
            if table == "daily_analytics":
                # Use upsert with on_conflict to handle duplicate key errors
                for upsert_data in upserts:
                    user_id = upsert_data.get("user_id")
                    try:
                        sb_execute(
                            supabase_client.table(table).upsert(
                                upsert_data,
                                on_conflict="date,user_id"
                            )
                        )
                        # Publish Redis pub/sub event (non-blocking, no WAL)
                        if user_id:
                            try:
                                from services.redis_pubsub import (
                                    publish_analytics_update
                                )
                                publish_analytics_update(
                                    user_id, "daily_analytics"
                                )
                            except Exception:
                                pass  # Non-blocking: continue
                    except Exception:
                        # If upsert fails, try update first, then insert
                        try:
                            sb_execute(
                                supabase_client.table(table)
                                .update(upsert_data)
                                .eq("date", upsert_data.get("date"))
                                .eq("user_id", user_id)
                            )
                            # Publish Redis pub/sub event (non-blocking)
                            if user_id:
                                try:
                                    from services.redis_pubsub import (
                                        publish_analytics_update
                                    )
                                    publish_analytics_update(
                                        user_id, "daily_analytics"
                                    )
                                except Exception:
                                    pass
                        except Exception:
                            # If update fails (record doesn't exist), insert
                            sb_execute(
                                supabase_client.table(table)
                                .insert(upsert_data)
                            )
                            # Publish Redis pub/sub event (non-blocking)
                            if user_id:
                                try:
                                    from services.redis_pubsub import (
                                        publish_analytics_update
                                    )
                                    publish_analytics_update(
                                        user_id, "daily_analytics"
                                    )
                                except Exception:
                                    pass
            else:
                # For other tables, use standard upsert
                sb_execute(supabase_client.table(table).upsert(upserts))
        
        # Execute updates (one by one with filters)
        for data, filters in updates:
            query = supabase_client.table(table).update(data)
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            sb_execute(query)
        
        # Execute deletes (one by one with filters)
        for filters in deletes:
            if not filters:
                raise ValueError("Delete operation requires filters")
            query = supabase_client.table(table).delete()
            for key, value in filters.items():
                query = query.eq(key, value)
            sb_execute(query)
        
    except Exception as e:
        print(f"❌ Error executing batched writes for {table}: {e}")
        raise
