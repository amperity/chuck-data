"""Tests for job_cache module."""

import os
import tempfile
from chuck_data.job_cache import (
    JobCache,
    cache_job,
    get_last_job_id,
    find_run_id_for_job,
    clear_cache,
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
