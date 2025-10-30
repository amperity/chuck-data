"""
Job ID caching for quick status lookups.

This module provides caching for Chuck job IDs and their corresponding Databricks
run IDs. The cache maintains the last 20 job launches to enable quick status checks
without requiring the user to specify job IDs.
"""

import json
import logging
import os
from typing import Optional, List, Dict, Tuple
from collections import deque


# Cache file location
def _get_cache_file_path() -> str:
    """Get the path to the job cache file."""
    return os.path.join(os.path.expanduser("~"), ".chuck_job_cache.json")


# Maximum number of job entries to cache
MAX_CACHE_SIZE = 20


class JobCache:
    """Cache for job IDs with LRU eviction policy."""

    def __init__(self, cache_file: Optional[str] = None):
        """Initialize job cache.

        Args:
            cache_file: Optional path to cache file (for testing)
        """
        self.cache_file = cache_file or _get_cache_file_path()
        self._cache: deque = deque(maxlen=MAX_CACHE_SIZE)
        self._load()

    def _load(self):
        """Load cache from file."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r") as f:
                    data = json.load(f)
                    # Load as deque with maxlen
                    self._cache = deque(data.get("jobs", []), maxlen=MAX_CACHE_SIZE)
                    logging.debug(f"Loaded {len(self._cache)} jobs from cache")
            except (json.JSONDecodeError, Exception) as e:
                logging.warning(f"Failed to load job cache: {e}")
                self._cache = deque(maxlen=MAX_CACHE_SIZE)

    def _save(self):
        """Save cache to file."""
        try:
            # Ensure directory exists
            directory = os.path.dirname(self.cache_file)
            if directory:
                os.makedirs(directory, exist_ok=True)

            with open(self.cache_file, "w") as f:
                json.dump({"jobs": list(self._cache)}, f, indent=2)
            logging.debug(f"Saved {len(self._cache)} jobs to cache")
        except Exception as e:
            logging.error(f"Failed to save job cache: {e}")

    def add_job(self, job_id: str, run_id: Optional[str] = None):
        """Add or update a job in the cache.

        If the job already exists, it's moved to the front (most recent).
        If the cache is full, the oldest job is evicted.

        Args:
            job_id: Chuck job identifier
            run_id: Optional Databricks run identifier
        """
        # Remove existing entry for this job_id if present
        self._cache = deque(
            [job for job in self._cache if job.get("job_id") != job_id],
            maxlen=MAX_CACHE_SIZE,
        )

        # Add new entry at the front (most recent)
        entry = {"job_id": job_id}
        if run_id:
            entry["run_id"] = run_id

        self._cache.appendleft(entry)
        self._save()
        logging.debug(f"Cached job: {job_id}, run_id: {run_id}")

    def get_last_job(self) -> Optional[Dict[str, str]]:
        """Get the most recent job from cache.

        Returns:
            Dictionary with 'job_id' and optional 'run_id', or None if cache is empty
        """
        if self._cache:
            return dict(self._cache[0])
        return None

    def get_all_jobs(self) -> List[Dict[str, str]]:
        """Get all cached jobs (most recent first).

        Returns:
            List of job dictionaries with 'job_id' and optional 'run_id'
        """
        return [dict(job) for job in self._cache]

    def find_run_id(self, job_id: str) -> Optional[str]:
        """Find Databricks run ID for a given Chuck job ID.

        Args:
            job_id: Chuck job identifier

        Returns:
            Databricks run ID if found, None otherwise
        """
        for job in self._cache:
            if job.get("job_id") == job_id:
                return job.get("run_id")
        return None

    def clear(self):
        """Clear all cached jobs."""
        self._cache.clear()
        self._save()
        logging.debug("Cleared job cache")


# Global cache instance
_job_cache = JobCache()


# Public API functions


def cache_job(job_id: str, run_id: Optional[str] = None):
    """Cache a job ID and optionally its Databricks run ID.

    Args:
        job_id: Chuck job identifier
        run_id: Optional Databricks run identifier
    """
    _job_cache.add_job(job_id, run_id)


def get_last_job_id() -> Optional[str]:
    """Get the most recent Chuck job ID from cache.

    Returns:
        The most recent job ID, or None if cache is empty
    """
    last_job = _job_cache.get_last_job()
    return last_job.get("job_id") if last_job else None


def get_last_job_with_run_id() -> Optional[Tuple[str, Optional[str]]]:
    """Get the most recent job with its run ID from cache.

    Returns:
        Tuple of (job_id, run_id) or None if cache is empty
    """
    last_job = _job_cache.get_last_job()
    if last_job:
        job_id = last_job.get("job_id")
        if job_id is not None:
            return (job_id, last_job.get("run_id"))
    return None


def get_all_cached_jobs() -> List[Dict[str, str]]:
    """Get all cached jobs (most recent first).

    Returns:
        List of job dictionaries
    """
    return _job_cache.get_all_jobs()


def find_run_id_for_job(job_id: str) -> Optional[str]:
    """Find Databricks run ID for a Chuck job ID.

    Args:
        job_id: Chuck job identifier

    Returns:
        Databricks run ID if found, None otherwise
    """
    return _job_cache.find_run_id(job_id)


def clear_cache():
    """Clear the job cache."""
    _job_cache.clear()
