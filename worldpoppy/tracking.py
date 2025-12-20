"""
This module provides a simple, thread-safe log for tracking
API calls made by the `manifest_builder` module.

"""

import threading

__all__ = ["api_query_log"]


class ApiQueryLog:
    """
    A simple log that uses a lock to ensure thread-safe appends.

    This prevents race conditions when multiple pqdm threads
    try to log their queries at the same time.
    """
    def __init__(self):
        self._queries = []
        self._lock = threading.Lock()

    def log_request(self, url):
        """Atomically logs a URL to the internal list."""
        with self._lock:
            self._queries.append(url)

    def get_count(self):
        """Atomically retrieves the current number of logged queries."""
        with self._lock:
            return len(self._queries)

    def get_queries(self):
        """
        Atomically retrieves a *copy* of the logged query list.

        A copy is returned so the internal list is not
        modified by other parts of the application.
        """
        with self._lock:
            return self._queries.copy()

    def reset(self):
        """Resets the log to zero. (Useful for testing)"""
        with self._lock:
            self._queries = []

# This is the single, global, shared instance that the
# rest of the library should import and use.
api_query_log = ApiQueryLog()
