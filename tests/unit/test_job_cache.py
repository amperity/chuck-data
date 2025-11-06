"""Tests for job_cache module."""

import os
import tempfile
from chuck_data.job_cache import (
    JobCache,
)


def test_job_cache_add_and_retrieve():
    """Test adding and retrieving jobs from cache."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        cache = JobCache(cache_file)
        cache.add_job("chk-001", "run-001")

        last_job = cache.get_last_job()
        assert last_job["job_id"] == "chk-001"
        assert last_job["run_id"] == "run-001"
    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)


def test_job_cache_maintains_last_5():
    """Test that cache only keeps last 20 jobs."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        cache = JobCache(cache_file)

        # Add 25 jobs
        for i in range(1, 26):
            cache.add_job(f"chk-{i:03d}", f"run-{i:03d}")

        all_jobs = cache.get_all_jobs()

        # Should only have 20 jobs
        assert len(all_jobs) == 20

        # Most recent should be chk-025
        assert all_jobs[0]["job_id"] == "chk-025"

        # Oldest should be chk-006 (001-005 were evicted)
        assert all_jobs[19]["job_id"] == "chk-006"
    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)


def test_job_cache_updates_existing():
    """Test that updating an existing job moves it to front."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        cache = JobCache(cache_file)

        cache.add_job("chk-001", "run-001")
        cache.add_job("chk-002", "run-002")
        cache.add_job("chk-003", "run-003")

        # Update chk-001 with new run_id
        cache.add_job("chk-001", "run-001-updated")

        all_jobs = cache.get_all_jobs()

        # Should still have 3 jobs
        assert len(all_jobs) == 3

        # chk-001 should be at front now
        assert all_jobs[0]["job_id"] == "chk-001"
        assert all_jobs[0]["run_id"] == "run-001-updated"
    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)


def test_job_cache_find_run_id():
    """Test finding run ID for a job ID."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        cache = JobCache(cache_file)

        cache.add_job("chk-001", "run-001")
        cache.add_job("chk-002", "run-002")
        cache.add_job("chk-003", "run-003")

        # Find existing
        assert cache.find_run_id("chk-002") == "run-002"

        # Find non-existing
        assert cache.find_run_id("chk-999") is None
    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)


def test_job_cache_find_job_id():
    """Test finding job ID for a run ID."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        cache = JobCache(cache_file)

        cache.add_job("chk-001", "run-001")
        cache.add_job("chk-002", "run-002")
        cache.add_job("chk-003", "run-003")

        # Find existing
        assert cache.find_job_id("run-002") == "chk-002"

        # Find non-existing
        assert cache.find_job_id("run-999") is None
    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)


def test_job_cache_persistence():
    """Test that cache persists across instances."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        # Create first instance and add jobs
        cache1 = JobCache(cache_file)
        cache1.add_job("chk-001", "run-001")
        cache1.add_job("chk-002", "run-002")

        # Create new instance - should load from file
        cache2 = JobCache(cache_file)
        all_jobs = cache2.get_all_jobs()

        assert len(all_jobs) == 2
        assert all_jobs[0]["job_id"] == "chk-002"
        assert all_jobs[1]["job_id"] == "chk-001"
    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)


def test_job_cache_without_run_id():
    """Test caching job without run ID."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        cache = JobCache(cache_file)
        cache.add_job("chk-001")  # No run_id

        last_job = cache.get_last_job()
        assert last_job["job_id"] == "chk-001"
        assert "run_id" not in last_job
    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)


def test_job_cache_clear():
    """Test clearing the cache."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        cache = JobCache(cache_file)
        cache.add_job("chk-001", "run-001")
        cache.add_job("chk-002", "run-002")

        assert len(cache.get_all_jobs()) == 2

        cache.clear()

        assert len(cache.get_all_jobs()) == 0
        assert cache.get_last_job() is None
    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)


def test_job_cache_with_timestamp():
    """Test that cached_at timestamp is added when caching job data."""
    from datetime import datetime

    with tempfile.NamedTemporaryFile(delete=False) as f:
        cache_file = f.name

    try:
        cache = JobCache(cache_file)

        # Cache job with job_data (should add timestamp)
        job_data = {
            "job-id": "chk-123",
            "state": "succeeded",
            "record-count": 1000,
            "credits": 50,
        }
        cache.add_job("chk-123", "run-456", job_data)

        # Retrieve and verify timestamp exists
        cached_job = cache.get_last_job()
        assert cached_job["job_id"] == "chk-123"
        assert "cached_at" in cached_job

        # Verify timestamp is valid ISO format
        cached_at = cached_job["cached_at"]
        assert isinstance(cached_at, str)
        # Should be parseable as datetime
        parsed_time = datetime.fromisoformat(cached_at.replace("Z", "+00:00"))
        assert parsed_time is not None

        # Cache job without job_data (should NOT add timestamp)
        cache.add_job("chk-456", "run-789")
        all_jobs = cache.get_all_jobs()

        # First job (chk-456) should not have timestamp
        assert all_jobs[0]["job_id"] == "chk-456"
        assert "cached_at" not in all_jobs[0]

        # Second job (chk-123) should still have timestamp
        assert all_jobs[1]["job_id"] == "chk-123"
        assert "cached_at" in all_jobs[1]

    finally:
        if os.path.exists(cache_file):
            os.remove(cache_file)
