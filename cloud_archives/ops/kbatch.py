"""Dagster operations for running kbatch jobs.

Define operations and helper functions for running the nwp-consumer
as a kbatch job on a kubernetes cluster. The operations are designed
to be run as part of a dagster pipeline.

The key method is `kbatch_consumer_graph`, which combines a selection
of operations into a graph that configures, runs, and tracks a kbatch
nwp-consumer job, streaming logs back to stdout and cleaning up
resources on error or success.
"""

import datetime as dt
import time

import dagster as dg
import httpx
import kbatch._core as kbc
from kbatch._types import Job
from pydantic import Field

# --- CONSTANTS --- #

# Set the kbatch url and token arguments to none in all calls to kbatch
# * Don't ask me why, but setting them as one would expect manually
#   (through env vars) in these parameters doesn't work. Instead, force
#   the kbatch core to find them from the environment by setting them
#   to None.
KBATCH_DICT = {
    "kbatch_url": None,
    "token": None,
}


# --- CLASSES AND METHODS --- #


class KbatchJobException(Exception):
    """Exception raised when a kbatch job fails.

    Contains the name of the job that failed alongside the message.
    Useful for enabling further handling of the job failure, e.g.
    cleaning up of resources.
    """

    def __init__(self, message: str, job_name: str):
        super().__init__(message)
        self.job_name = job_name


@dg.failure_hook
def kbatch_job_failure_hook(context: dg.HookContext) -> None:
    """Failure hook that deletes a kbatch job on exception.

    Can be applied to individual ops via
        some_kbatch_op.with_failure_hook(kbatch_job_failure_hook)()
    or to all ops in a job via
        @dg.job(hooks={kbatch_job_failure_hook})

    Args:
        context: The dagster context within which the hook is operating.
    """
    op_exception = context.op_exception

    if isinstance(op_exception, KbatchJobException):
        job_name = op_exception.job_name
        dg.get_dagster_logger().info(f"Deleting kbatch job {job_name}.")
        kbc.delete_job(resource_name=job_name, **KBATCH_DICT)


def wait_for_status_change(old_status: str, job_name: str, timeout: int = 60 * 10) -> str:
    """Wait for the status of a kbatch job to change from old_status.

    The amount of time to wait is modified by the timeout parameter.

    Args:
        old_status: The status to wait for the job to change from.
        job_name: The name of the job to check.
        timeout: The maximum time to wait for the status to change.

    Returns:
        The new status of the job.
    """
    time_spent: int = 0
    while time_spent < timeout:
        increment_secs: int = 30
        time.sleep(increment_secs)
        time_spent += increment_secs

        # Get the status of the pod in the job
        # * This can fail and be retried within the timeout limit so
        #   catch a number of recoverable errors.
        try:
            pods_info: list[dict] = kbc.list_pods(job_name=job_name, **KBATCH_DICT)["items"]
        except httpx.ConnectError as e:
            if "Temporary failure in name resolution" in str(e):
                dg.get_dagster_logger().debug(f"Name resolution error, retrying: {e}")
                continue
            else:
                raise e
        except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            dg.get_dagster_logger().debug(f"Timed out listing pods, retrying: {e}")
            continue
        except httpx.HTTPStatusError as e:
            if "503" in str(e):
                dg.get_dagster_logger().debug(f"Service unavailable, retrying: {e}")
                continue
            else:
                raise e
        except Exception as e:
            raise e

        if len(pods_info) == 0:
            continue

        new_status: str = pods_info[0]["status"]["phase"]

        # Exit if status has changed
        if new_status != old_status:
            dg.get_dagster_logger().info(
                f"Job {job_name} is no longer {old_status}, status: {new_status}.",
            )
            if new_status == "Failed":
                condition: str = pods_info[0]["status"]["container_statuses"][0]["state"]
                dg.get_dagster_logger().error(f"Condition: {condition}")
            return new_status

        # Log if still waiting every 10 minutes
        if time_spent % (10 * 60) == 0:
            dg.get_dagster_logger().debug(
                f"Kbatch job {job_name} still {old_status} after {int(time_spent / 60)} mins.",
            )

    # Raise exception if timed out
    if time_spent >= timeout:
        dg.get_dagster_logger().info(pods_info[0]["status"])
        raise KbatchJobException(
            message=f"Timed out waiting for status '{old_status}' to change.",
            job_name=job_name,
        )

    return new_status


# --- OPS --- #


class NWPConsumerConfig(dg.Config):
    """Configuration object for the nwp consumer.

    Defines the configuration for the running of the nwp-consumer docker image.
    Builds upon the dagster Config type, allowing for the configuration to be
    passed to an Op in a dagster pipeline.

    Default values of an ellipsis (...) are used to indicate that the value
    must be provided when the configuration object is instantiated.
    """

    docker_tag: str = Field(
        description="The tag of the nwp-consumer docker image to use.",
        default="0.2.1",
    )
    source: str = Field(
        description="The source of the data to consume.",
        default=...,
    )
    sink: str = Field(
        description="The sink to write the data to.",
        default=...,
    )
    env: dict[str, str] = Field(
        description="Environment variables to pass to the nwp-consumer.",
        default_factory=lambda: {},
    )
    inittime: str = Field(
        description="The initialisation time of the nwp data to consume.",
        default=dt.datetime.now(dt.UTC)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .strftime("%Y-%m-%d|%H:%M"),
        regex=r"^\d{4}-\d{2}-\d{2}\|\d{2}:\d{2}$",
    )


@dg.op(
    ins={"depends_on": dg.In(dg.Nothing)},
)
def define_kbatch_consumer_job(
    context: dg.OpExecutionContext,
    config: NWPConsumerConfig,
) -> Job:
    """Define a kbatch job object to run the nwp-consumer.

    Builds a kbatch job object specifying the parameters required
    to run the nwp-consumer docker image according to the
    input configuration object.

    Args:
        context: The dagster context.
        config: Configuration for the nwp-consumer.

    Returns:
        The kbatch job definition object.
    """
    # Get the init time either from config or partition key
    itstring = config.inittime
    if context.has_partition_key:
        itstring = context.partition_key
    it = dt.datetime.strptime(itstring, "%Y-%m-%d|%H:%M").replace(tzinfo=dt.UTC)

    job = Job(
        name=f"{config.source}-{config.sink}-backfill",
        image=f"ghcr.io/openclimatefix/nwp-consumer:{config.docker_tag}",
        args=[
            "consume",
            f"--source={config.source}",
            f"--sink={config.sink}",
            "--rsink=local",
            "--rdir=raw",
            "--zdir=data",
            f"--from={it.strftime('%Y-%m-%dT%H:%M')}",
        ],
        env=config.env,
    )

    return job


@dg.op
def submit_kbatch_job(context: dg.OpExecutionContext, job: Job) -> str:
    """Submit a kbatch job object to the kbatch server.

    Requires one of the two following configurations set:

        - the appropriate kbatch token and url set in the environment variables
          KBATCH_URL and JUPYTERHUB_API_TOKEN
        - a `~/.config/kbatch/config.json file containing a dictionary with the
          keys "kbatch_url" and "token".

    This can be generated using the kbatch CLI with the command:
        kbatch configure --kbatch_url <kbatch_url> --token <token>

    Defines a "Nothing" input to allow for the op to have upstream dependencies
    in a graph without the passing of data.

    Args:
        context: The dagster context.
        job: A kbatch Job object defining the job to submit.

    Returns:
        The name of the created job.
    """
    # Request large pod sizes
    profile: dict = {
        "resources": {
            "limits": {
                "cpu": "8",
                "memory": "72G",
            },
            "requests": {
                "cpu": "7.0",
                "memory": "54G",
            },
        },
    }
    # Submit the job using kbatch core
    result = kbc.submit_job(job=job, profile=profile, **KBATCH_DICT)
    # Extract the job name from the result
    job_name: str = result["metadata"]["name"]
    context.log.info(f"Kbatch job {job_name} requested.")

    return job_name


@dg.op
def follow_kbatch_job(
    context: dg.OpExecutionContext,
    job_name: str,
) -> str:
    """Blocking function that follows the status of a kbatch job.

    Waits for a job to start running, then follows the logs, streaming
    back to stdout. Checks for failures within the logs and raises an
    exception if the job fails.

    This function assumes the job is only running on a single pod.
    On a partial read error the function will re-try the read.

    Args:
        context: The dagster context.
        job_name: The name of the job.

    Returns:
        The name of the job.
    """
    context.log.info("Assessing status of kbatch job.")

    # Pods take a short while to be provisioned
    status: str = wait_for_status_change(old_status="Pending", job_name=job_name)
    # If the pod fails to be provisioned there will be no logs to view.
    # The condition will be printed to the logs (e.g. ImagePullBackoff)
    if status == "Failed":
        raise KbatchJobException(
            message=f"Job {job_name} failed, see logs.",
            job_name=job_name,
        )

    # Otherwise, wait up to timout for the pod to finish running
    pod_name: str = kbc.list_pods(job_name=job_name, **KBATCH_DICT)["items"][0]["metadata"]["name"]
    status = wait_for_status_change(old_status="Running", job_name=job_name, timeout=60 * 60 * 24)

    # Get the logs from the pod

    total_attempts: int = 0
    while total_attempts < 3:
        try:
            logs: str = kbc._logs(
                pod_name=pod_name,
                stream=False,
                read_timeout=60 * 6,
                **KBATCH_DICT,
            )
            for line in logs.split("\n"):
                print(line)  # noqa: T201
            break
        except httpx.RemoteProtocolError as e:
            time.sleep(20)
            total_attempts += 1
            continue

        context.log().warn("Failed to read logs after 3 attempts.")

    pods_info: list[dict] = kbc.list_pods(job_name=job_name, **KBATCH_DICT)["items"]
    pod_status = pods_info[0]["status"]["phase"]
    context.log.info(f"Captured all logs for job {job_name}; status '{pod_status}'.")

    if status == "Failed":
        raise KbatchJobException(
            message=f"Job {job_name} failed, see logs.",
            job_name=job_name,
        )

    return job_name


@dg.op(
    out={"job_name": dg.Out(str)},
)
def delete_kbatch_job(job_name: str) -> str:
    """Deletes a kbatch job.

    Args:
        job_name: The name of the job. Must be a dagster op output.
    """
    dg.get_dagster_logger().info(f"Deleting kbatch job {job_name}.")
    kbc.delete_job(resource_name=job_name, **KBATCH_DICT)
    return job_name


# --- GRAPHS --- #


@dg.graph
def kbatch_consumer_graph(depends_on: dg.Nothing) -> str:
    """Graph for running the nwp-consumer as a kbatch job.

    Defines the set of operations that configure, run, and track a kbatch
    nwp-consumer job, streaming logs back to stdout and deleting the job
    upon completion. Any ops that manage or interact with a running kbatch
    job also include a hook that deletes the job on exceptions in the graph.

    Implements a Nothing input to allow for the graph to have upstream
    dependencies in a pipeline without the passing of data.
    """
    job: Job = define_kbatch_consumer_job(depends_on=depends_on)
    job_name: str = submit_kbatch_job.with_hooks({kbatch_job_failure_hook})(job=job)
    job_name = follow_kbatch_job.with_hooks({kbatch_job_failure_hook})(job_name=job_name)
    job_name = delete_kbatch_job(job_name=job_name)

    return job_name
