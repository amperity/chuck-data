"""Unit tests for EMRAPIClient."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from chuck_data.clients.emr import EMRAPIClient


@pytest.fixture
def mock_emr_client():
    """Create a mock boto3 EMR client."""
    with patch("chuck_data.clients.emr.boto3") as mock_boto3:
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        yield mock_client


@pytest.fixture
def emr_client(mock_emr_client):
    """Create an EMRAPIClient instance with mocked boto3."""
    return EMRAPIClient(region="us-west-2", cluster_id="j-TESTCLUSTER123")


class TestEMRAPIClientInit:
    """Test EMRAPIClient initialization."""

    def test_init_with_cluster_id(self, mock_emr_client):
        """Test initialization with cluster ID."""
        client = EMRAPIClient(region="us-west-2", cluster_id="j-TESTCLUSTER123")
        assert client.region == "us-west-2"
        assert client.cluster_id == "j-TESTCLUSTER123"
        assert client.emr is not None

    def test_init_with_credentials(self, mock_emr_client):
        """Test initialization with explicit AWS credentials."""
        client = EMRAPIClient(
            region="us-west-2",
            aws_access_key_id="AKIATEST",
            aws_secret_access_key="secretkey",
        )
        assert client.aws_access_key_id == "AKIATEST"
        assert client.aws_secret_access_key == "secretkey"

    def test_init_with_profile(self):
        """Test initialization with AWS profile."""
        with patch("chuck_data.clients.emr.boto3.Session") as mock_session:
            mock_session_instance = Mock()
            mock_session.return_value = mock_session_instance
            mock_session_instance.client.return_value = Mock()

            client = EMRAPIClient(region="us-west-2", aws_profile="my-profile")
            assert client.aws_profile == "my-profile"
            mock_session.assert_called_once_with(profile_name="my-profile")


class TestClusterManagement:
    """Test cluster management methods."""

    def test_list_clusters(self, emr_client, mock_emr_client):
        """Test listing EMR clusters."""
        mock_emr_client.list_clusters.return_value = {
            "Clusters": [
                {
                    "Id": "j-CLUSTER1",
                    "Name": "Cluster 1",
                    "Status": {"State": "WAITING"},
                },
                {
                    "Id": "j-CLUSTER2",
                    "Name": "Cluster 2",
                    "Status": {"State": "RUNNING"},
                },
            ]
        }

        clusters = emr_client.list_clusters()
        assert len(clusters) == 2
        assert clusters[0]["Id"] == "j-CLUSTER1"
        assert clusters[1]["Id"] == "j-CLUSTER2"

    def test_list_clusters_with_filters(self, emr_client, mock_emr_client):
        """Test listing clusters with state filters."""
        mock_emr_client.list_clusters.return_value = {"Clusters": []}

        emr_client.list_clusters(cluster_states=["WAITING", "RUNNING"])
        mock_emr_client.list_clusters.assert_called_once_with(
            ClusterStates=["WAITING", "RUNNING"]
        )

    def test_describe_cluster(self, emr_client, mock_emr_client):
        """Test describing a cluster."""
        mock_emr_client.describe_cluster.return_value = {
            "Cluster": {
                "Id": "j-TESTCLUSTER123",
                "Name": "Test Cluster",
                "Status": {"State": "WAITING"},
            }
        }

        cluster = emr_client.describe_cluster()
        assert cluster["Id"] == "j-TESTCLUSTER123"
        assert cluster["Status"]["State"] == "WAITING"

    def test_describe_cluster_with_explicit_id(self, emr_client, mock_emr_client):
        """Test describing a cluster with explicit cluster ID."""
        mock_emr_client.describe_cluster.return_value = {
            "Cluster": {"Id": "j-OTHER123", "Name": "Other Cluster"}
        }

        cluster = emr_client.describe_cluster("j-OTHER123")
        assert cluster["Id"] == "j-OTHER123"
        mock_emr_client.describe_cluster.assert_called_once_with(ClusterId="j-OTHER123")

    def test_get_cluster_status(self, emr_client, mock_emr_client):
        """Test getting cluster status."""
        mock_emr_client.describe_cluster.return_value = {
            "Cluster": {"Status": {"State": "RUNNING"}}
        }

        status = emr_client.get_cluster_status()
        assert status == "RUNNING"

    def test_validate_connection_success(self, emr_client, mock_emr_client):
        """Test successful connection validation."""
        mock_emr_client.describe_cluster.return_value = {
            "Cluster": {"Id": "j-TESTCLUSTER123"}
        }

        assert emr_client.validate_connection() is True

    def test_validate_connection_failure(self, emr_client, mock_emr_client):
        """Test failed connection validation."""
        from botocore.exceptions import ClientError

        mock_emr_client.describe_cluster.side_effect = ClientError(
            {"Error": {"Code": "ClusterNotFound", "Message": "Cluster not found"}},
            "DescribeCluster",
        )

        assert emr_client.validate_connection() is False

    def test_terminate_cluster(self, emr_client, mock_emr_client):
        """Test terminating a cluster."""
        result = emr_client.terminate_cluster()
        assert result is True
        mock_emr_client.terminate_job_flows.assert_called_once_with(
            JobFlowIds=["j-TESTCLUSTER123"]
        )


class TestJobExecution:
    """Test job execution methods (Steps API)."""

    def test_add_job_flow_step(self, emr_client, mock_emr_client):
        """Test adding a job flow step."""
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-STEP123"]}

        step_id = emr_client.add_job_flow_step(
            name="Test Step",
            jar="s3://bucket/app.jar",
            main_class="com.example.Main",
            args=["arg1", "arg2"],
            action_on_failure="CONTINUE",
        )

        assert step_id == "s-STEP123"
        mock_emr_client.add_job_flow_steps.assert_called_once()
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        assert call_args["JobFlowId"] == "j-TESTCLUSTER123"
        assert len(call_args["Steps"]) == 1
        assert call_args["Steps"][0]["Name"] == "Test Step"

    def test_submit_bash_script_inline(self, emr_client, mock_emr_client):
        """Test submitting a bash script with inline content."""
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-BASH123"]}

        step_id = emr_client.submit_bash_script(
            name="Setup Environment",
            script_content='mkdir -p /opt/amperity && echo "Setup complete"',
        )

        assert step_id == "s-BASH123"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]
        assert hadoop_jar_step["Jar"] == "command-runner.jar"
        assert "bash" in hadoop_jar_step["Args"]
        assert "-c" in hadoop_jar_step["Args"]
        assert (
            'mkdir -p /opt/amperity && echo "Setup complete"' in hadoop_jar_step["Args"]
        )

    def test_submit_bash_script_s3(self, emr_client, mock_emr_client):
        """Test submitting a bash script from S3."""
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-BASH456"]}

        step_id = emr_client.submit_bash_script(
            name="Run Init Script",
            script_s3_path="s3://bucket/chuck/init-scripts/chuck-init-123.sh",
        )

        assert step_id == "s-BASH456"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]
        assert hadoop_jar_step["Jar"] == "command-runner.jar"
        assert "bash" in hadoop_jar_step["Args"]
        assert "-c" in hadoop_jar_step["Args"]
        # Should download and pipe to bash
        assert any(
            "aws s3 cp" in arg and "| bash" in arg for arg in hadoop_jar_step["Args"]
        )

    def test_submit_bash_script_no_params(self, emr_client):
        """Test that submitting bash script without content or path fails."""
        with pytest.raises(
            ValueError, match="Either script_content or script_s3_path must be provided"
        ):
            emr_client.submit_bash_script(name="Test")

    def test_submit_bash_script_both_params(self, emr_client):
        """Test that submitting bash script with both content and path fails."""
        with pytest.raises(
            ValueError,
            match="Only one of script_content or script_s3_path should be provided",
        ):
            emr_client.submit_bash_script(
                name="Test",
                script_content="echo test",
                script_s3_path="s3://bucket/script.sh",
            )

    def test_submit_bash_script_with_env_vars_inline(self, emr_client, mock_emr_client):
        """Test submitting bash script with environment variables (inline)."""
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-BASH789"]}

        step_id = emr_client.submit_bash_script(
            name="Setup with Env",
            script_content='echo "API URL: $CHUCK_API_URL"',
            env_vars={
                "CHUCK_API_URL": "https://api.amperity.com",
                "DEBUG": "true",
            },
        )

        assert step_id == "s-BASH789"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]
        assert hadoop_jar_step["Jar"] == "command-runner.jar"

        # Check that environment variables are exported in the command
        command = hadoop_jar_step["Args"][2]  # The -c argument
        assert "export CHUCK_API_URL='https://api.amperity.com'" in command
        assert "export DEBUG='true'" in command
        assert 'echo "API URL: $CHUCK_API_URL"' in command

    def test_submit_bash_script_with_env_vars_s3(self, emr_client, mock_emr_client):
        """Test submitting S3 bash script with environment variables."""
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-BASH999"]}

        step_id = emr_client.submit_bash_script(
            name="Init Script with Env",
            script_s3_path="s3://bucket/init.sh",
            env_vars={
                "CHUCK_API_URL": "https://api.amperity.com",
                "JNAME": "zulu17-ca-amd64",
            },
        )

        assert step_id == "s-BASH999"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]

        # Check that environment variables are exported before downloading script
        command = hadoop_jar_step["Args"][2]
        assert "export CHUCK_API_URL='https://api.amperity.com'" in command
        assert "export JNAME='zulu17-ca-amd64'" in command
        assert "aws s3 cp s3://bucket/init.sh - | bash" in command

    def test_submit_spark_job(self, emr_client, mock_emr_client):
        """Test submitting a Spark job."""
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-SPARK123"]}

        step_id = emr_client.submit_spark_job(
            name="Spark Job",
            jar_path="s3://bucket/app.jar",
            main_class="com.example.SparkApp",
            args=["input.txt"],
            spark_args=["--executor-memory", "4g"],
        )

        assert step_id == "s-SPARK123"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]
        assert hadoop_jar_step["Jar"] == "command-runner.jar"
        assert "spark-submit" in hadoop_jar_step["Args"]
        assert "--executor-memory" in hadoop_jar_step["Args"]
        assert "4g" in hadoop_jar_step["Args"]

    def test_submit_spark_redshift_job(self, emr_client, mock_emr_client):
        """Test submitting a Spark job with Redshift connector."""
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-REDSHIFT123"]}

        step_id = emr_client.submit_spark_redshift_job(
            name="Stitch with Redshift",
            jar_path="s3://bucket/stitch.jar",
            main_class="amperity.stitch_standalone.chuck_main",
            args=["", "s3://bucket/config.json"],
            s3_temp_dir="s3://bucket/temp/",
            redshift_jdbc_url="jdbc:redshift://cluster.redshift.amazonaws.com:5439/dev",
            aws_iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
            spark_args=["--executor-memory", "8g"],
        )

        assert step_id == "s-REDSHIFT123"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]
        assert hadoop_jar_step["Jar"] == "command-runner.jar"

        args = hadoop_jar_step["Args"]
        # The command is wrapped in bash -c for environment variable export
        assert args[0] == "bash"
        assert args[1] == "-c"
        # The actual command is in the third element
        command = args[2]
        assert "spark-submit" in command
        # Check Redshift connector packages
        assert "--packages" in command
        assert "spark-redshift_2.12:6.5.1-spark_3.5" in command
        assert "redshift-jdbc42" in command
        # Check S3 temp dir configuration
        assert "spark.hadoop.fs.s3a.tempdir" in command
        assert "s3://bucket/temp/" in command
        # Check Redshift JDBC URL configuration
        assert "spark.redshift.jdbc.url" in command
        assert "jdbc:redshift://cluster.redshift.amazonaws.com:5439/dev" in command
        # Check IAM role configuration
        assert "spark.redshift.aws_iam_role" in command
        assert "arn:aws:iam::123456789012:role/RedshiftRole" in command
        # Check custom spark args are included
        assert "--executor-memory" in command
        assert "8g" in command
        # Check main class and jar
        assert "--class" in command
        assert "amperity.stitch_standalone.chuck_main" in command
        assert "s3://bucket/stitch.jar" in command
        # Check environment variable export
        assert "export CHUCK_API_URL=" in command

    def test_submit_spark_redshift_job_minimal(self, emr_client, mock_emr_client):
        """Test submitting Redshift job with minimal parameters."""
        mock_emr_client.add_job_flow_steps.return_value = {"StepIds": ["s-REDSHIFT456"]}

        step_id = emr_client.submit_spark_redshift_job(
            name="Minimal Redshift Job",
            jar_path="s3://bucket/app.jar",
            main_class="com.example.Main",
        )

        assert step_id == "s-REDSHIFT456"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]

        args = hadoop_jar_step["Args"]
        # The command is wrapped in bash -c for environment variable export
        assert args[0] == "bash"
        assert args[1] == "-c"
        # The actual command is in the third element
        command = args[2]
        # Should still have packages even without optional params
        assert "--packages" in command
        # Should have S3A configuration
        assert "spark.hadoop.fs.s3a.impl" in command

    def test_submit_spark_databricks_job(self, emr_client, mock_emr_client):
        """Test submitting a Spark job with Databricks Unity Catalog connector."""
        mock_emr_client.add_job_flow_steps.return_value = {
            "StepIds": ["s-DATABRICKS123"]
        }

        step_id = emr_client.submit_spark_databricks_job(
            name="Stitch with Databricks",
            jar_path="s3://bucket/stitch.jar",
            main_class="amperity.stitch_standalone.chuck_main",
            args=["", "s3://bucket/config.json"],
            databricks_jdbc_url="jdbc:databricks://workspace.cloud.databricks.com:443/default;httpPath=/sql/1.0/warehouses/abc123;AuthMech=3;UID=token;PWD=dapi123",
            databricks_catalog="prod",
            databricks_schema="customers",
            spark_args=["--executor-memory", "8g"],
        )

        assert step_id == "s-DATABRICKS123"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]
        assert hadoop_jar_step["Jar"] == "command-runner.jar"

        args = hadoop_jar_step["Args"]
        assert "spark-submit" in args
        # Check Databricks connector packages
        assert "--packages" in args
        packages_idx = args.index("--packages")
        packages_str = args[packages_idx + 1]
        assert "databricks-jdbc:2.6.36" in packages_str
        assert "spark-databricks_2.12:0.2.0" in packages_str
        # Check Databricks JDBC URL configuration
        assert any("spark.databricks.jdbc.url" in arg for arg in args)
        # Check catalog configuration
        assert any("spark.databricks.catalog" in arg for arg in args)
        # Check schema configuration
        assert any("spark.databricks.schema" in arg for arg in args)
        # Check custom spark args are included
        assert "--executor-memory" in args
        assert "8g" in args
        # Check main class and jar
        assert "--class" in args
        assert "amperity.stitch_standalone.chuck_main" in args
        assert "s3://bucket/stitch.jar" in args

    def test_submit_spark_databricks_job_minimal(self, emr_client, mock_emr_client):
        """Test submitting Databricks job with minimal parameters."""
        mock_emr_client.add_job_flow_steps.return_value = {
            "StepIds": ["s-DATABRICKS456"]
        }

        step_id = emr_client.submit_spark_databricks_job(
            name="Minimal Databricks Job",
            jar_path="s3://bucket/app.jar",
            main_class="com.example.Main",
        )

        assert step_id == "s-DATABRICKS456"
        call_args = mock_emr_client.add_job_flow_steps.call_args[1]
        hadoop_jar_step = call_args["Steps"][0]["HadoopJarStep"]

        args = hadoop_jar_step["Args"]
        # Should still have packages even without optional params
        assert "--packages" in args
        # Should have Databricks configuration
        assert any(
            "spark.databricks.delta.optimizeWrite.enabled" in arg for arg in args
        )

    def test_describe_step(self, emr_client, mock_emr_client):
        """Test describing a step."""
        mock_emr_client.describe_step.return_value = {
            "Step": {
                "Id": "s-STEP123",
                "Name": "Test Step",
                "Status": {"State": "RUNNING"},
            }
        }

        step = emr_client.describe_step("s-STEP123")
        assert step["Id"] == "s-STEP123"
        assert step["Status"]["State"] == "RUNNING"

    def test_get_step_status_running(self, emr_client, mock_emr_client):
        """Test getting status of a running step."""
        mock_emr_client.describe_step.return_value = {
            "Step": {
                "Status": {
                    "State": "RUNNING",
                    "StateChangeReason": {"Message": "Step is running"},
                    "Timeline": {"StartDateTime": datetime(2024, 1, 15, 10, 30, 0)},
                }
            }
        }

        status = emr_client.get_step_status("s-STEP123")
        assert status["status"] == "RUNNING"
        assert "start_time" in status
        assert "end_time" not in status

    def test_get_step_status_completed(self, emr_client, mock_emr_client):
        """Test getting status of a completed step."""
        mock_emr_client.describe_step.return_value = {
            "Step": {
                "Status": {
                    "State": "COMPLETED",
                    "StateChangeReason": {"Message": "Step completed successfully"},
                    "Timeline": {
                        "StartDateTime": datetime(2024, 1, 15, 10, 30, 0),
                        "EndDateTime": datetime(2024, 1, 15, 11, 30, 0),
                    },
                }
            }
        }

        status = emr_client.get_step_status("s-STEP123")
        assert status["status"] == "COMPLETED"
        assert "start_time" in status
        assert "end_time" in status

    def test_get_step_status_failed(self, emr_client, mock_emr_client):
        """Test getting status of a failed step."""
        mock_emr_client.describe_step.return_value = {
            "Step": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": {"Message": "Step failed"},
                    "Timeline": {
                        "StartDateTime": datetime(2024, 1, 15, 10, 30, 0),
                        "EndDateTime": datetime(2024, 1, 15, 10, 45, 0),
                    },
                    "FailureDetails": {
                        "Reason": "Application error",
                        "Message": "Exception in main class",
                        "LogFile": "s3://bucket/logs/error.log",
                    },
                }
            }
        }

        status = emr_client.get_step_status("s-STEP123")
        assert status["status"] == "FAILED"
        assert status["failure_reason"] == "Application error"
        assert "Exception in main class" in status["failure_message"]

    def test_list_steps(self, emr_client, mock_emr_client):
        """Test listing steps."""
        mock_emr_client.list_steps.return_value = {
            "Steps": [
                {"Id": "s-STEP1", "Name": "Step 1", "Status": {"State": "COMPLETED"}},
                {"Id": "s-STEP2", "Name": "Step 2", "Status": {"State": "RUNNING"}},
            ]
        }

        steps = emr_client.list_steps()
        assert len(steps) == 2
        assert steps[0]["Id"] == "s-STEP1"

    def test_cancel_step(self, emr_client, mock_emr_client):
        """Test cancelling a step."""
        result = emr_client.cancel_step("s-STEP123")
        assert result is True
        mock_emr_client.cancel_steps.assert_called_once_with(
            ClusterId="j-TESTCLUSTER123", StepIds=["s-STEP123"]
        )

    def test_wait_for_step_completed(self, emr_client, mock_emr_client):
        """Test waiting for a step to complete."""
        mock_emr_client.describe_step.return_value = {
            "Step": {
                "Status": {
                    "State": "COMPLETED",
                    "StateChangeReason": {"Message": "Completed"},
                    "Timeline": {},
                }
            }
        }

        status = emr_client.wait_for_step("s-STEP123", timeout=5)
        assert status["status"] == "COMPLETED"

    def test_wait_for_step_failed(self, emr_client, mock_emr_client):
        """Test waiting for a step that fails."""
        mock_emr_client.describe_step.return_value = {
            "Step": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": {"Message": "Failed"},
                    "Timeline": {},
                    "FailureDetails": {"Reason": "Error"},
                }
            }
        }

        with pytest.raises(ValueError, match="failed"):
            emr_client.wait_for_step("s-STEP123", timeout=5)


class TestUtilityMethods:
    """Test utility methods."""

    def test_get_cluster_dns(self, emr_client, mock_emr_client):
        """Test getting cluster DNS."""
        mock_emr_client.describe_cluster.return_value = {
            "Cluster": {"MasterPublicDnsName": "ec2-1-2-3-4.compute-1.amazonaws.com"}
        }

        dns = emr_client.get_cluster_dns()
        assert dns == "ec2-1-2-3-4.compute-1.amazonaws.com"

    def test_get_monitoring_url(self, emr_client):
        """Test getting EMR console monitoring URL."""
        url = emr_client.get_monitoring_url()
        assert "us-west-2.console.aws.amazon.com/emr" in url
        assert "j-TESTCLUSTER123" in url


class TestErrorHandling:
    """Test error handling."""

    def test_describe_cluster_no_id(self):
        """Test describing cluster without cluster ID."""
        with patch("chuck_data.clients.emr.boto3") as mock_boto3:
            mock_boto3.client.return_value = Mock()
            client = EMRAPIClient(region="us-west-2")  # No cluster_id

            with pytest.raises(ValueError, match="cluster_id must be provided"):
                client.describe_cluster()

    def test_client_error_handling(self, emr_client, mock_emr_client):
        """Test handling of AWS ClientError."""
        from botocore.exceptions import ClientError

        mock_emr_client.describe_cluster.side_effect = ClientError(
            {"Error": {"Code": "InvalidInput", "Message": "Invalid cluster ID"}},
            "DescribeCluster",
        )

        with pytest.raises(ValueError, match="Error describing cluster"):
            emr_client.describe_cluster()

    def test_connection_error_handling(self, emr_client, mock_emr_client):
        """Test handling of connection errors."""
        from botocore.exceptions import BotoCoreError

        mock_emr_client.describe_cluster.side_effect = BotoCoreError()

        with pytest.raises(ConnectionError, match="Connection error occurred"):
            emr_client.describe_cluster()
