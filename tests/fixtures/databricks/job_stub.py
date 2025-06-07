"""Job operations mixin for DatabricksClientStub."""


class JobStubMixin:
    """Mixin providing job operations for DatabricksClientStub."""

    def __init__(self):
        self.create_stitch_notebook_calls = []

    def list_jobs(self, **kwargs):
        """List jobs."""
        return {"jobs": []}

    def get_job(self, job_id):
        """Get job by ID."""
        return {
            "job_id": job_id,
            "settings": {"name": f"test_job_{job_id}"},
            "state": "TERMINATED",
        }

    def run_job(self, job_id):
        """Run a job."""
        return {"run_id": f"run_{job_id}_001", "job_id": job_id, "state": "RUNNING"}

    def submit_job_run(self, config_path, init_script_path, run_name=None):
        """Submit a job run and return run_id."""
        from datetime import datetime

        if not run_name:
            run_name = (
                f"Chuck AI One-Time Run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        # Return a successful job submission
        return {"run_id": 123456}

    def get_job_run_status(self, run_id):
        """Get job run status."""
        return {
            "state": {"life_cycle_state": "RUNNING"},
            "run_id": int(run_id),
            "run_name": "Test Run",
            "creator_user_name": "test@example.com",
        }

    def create_stitch_notebook(self, *args, **kwargs):
        """Create a stitch notebook (simulate successful creation)."""
        # Track the call
        self.create_stitch_notebook_calls.append((args, kwargs))

        if hasattr(self, "_create_stitch_notebook_result"):
            return self._create_stitch_notebook_result
        if hasattr(self, "_create_stitch_notebook_error"):
            raise self._create_stitch_notebook_error
        return {
            "notebook_id": "test-notebook-123",
            "path": "/Workspace/Stitch/test_notebook.py",
        }

    def set_create_stitch_notebook_result(self, result):
        """Configure create_stitch_notebook return value."""
        self._create_stitch_notebook_result = result

    def set_create_stitch_notebook_error(self, error):
        """Configure create_stitch_notebook to raise error."""
        self._create_stitch_notebook_error = error
