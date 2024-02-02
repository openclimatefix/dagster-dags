import dagster as dg
from typing import Callable
import datetime as dt
import time

from kbatch._types import Job
import kbatch._core as kbc

# --- CONSTANTS --- #

# Set the kbatch url and token arguments to none in all calls to kbatch
# * Don't ask me why, but setting them their values using the environment
#   variables doesn't work, so let kbatch do that instead
KBATCH_DICT = {
    "kbatch_url": None,
    "token": None,
}


# --- CLASSES AND METHODS --- #

class KbatchJobException(Exception):
    """Exception raised when a kbatch job fails.

    Contains the name of the job that failed.
    """

    def __init__(self, message: str, job_name: str):
        super().__init__(message)
        self.job_name = job_name


@dg.op
def delete_kbatch_job(job_name: str) -> None:
    """Deletes a kbatch job.

    Args:
        job_name: The name of the job.
    """
    dg.get_dagster_logger().info(f"Deleting kbatch job {job_name}.")
    kbc.delete_job(resource_name=job_name, **KBATCH_DICT)


@dg.failure_hook
def kbatch_job_failure_hook(context: dg.HookContext) -> None:
    """Failure hook that deletes a kbatch job on exception."""
    op_exception = context.op_exception

    if isinstance(op_exception, KbatchJobException):
        job_name = op_exception.job_name
        dg.get_dagster_logger().info(f"Deleting kbatch job {job_name}.")
        kbc.delete_job(resource_name=job_name, **KBATCH_DICT)


# --- OPS --- #

class KbatchConsumerConfig(dg.Config):
    """Configuration for a kbatch consumer job.

    Defines the configuration for the running of the nwp-consumer docker image
    using kbatch on a kubernetes cluster.
    """

    # The tag of the nwp-consumer docker image to use
    docker_tag: str
    # The source of the data to consume
    source: str
    # The sink to write the data to
    sink: str
    # Environment variables to pass to the consumer
    env: dict[str, str]


@dg.op
def define_kbatch_consumer_job(
        context: dg.OpExecutionContext,
        config: KbatchConsumerConfig,
) -> Job:
    """Define a kbatch job object to run the nwp-consumer.

    Args:
        context: The dagster context.
        config: The configuration for the job and consumer.
    Returns:
        The kbatch job definition object.
    """
    # Get the inittime as a datetime object from the partition key
    inittime = dt.datetime.strptime(context.partition_key, "%Y-%m-%d|%H:%M").replace(
        tzinfo=dt.UTC,
    )

    job = Job(
        name=f"icon-backfill",
        image=f"ghcr.io/openclimatefix/nwp-consumer:{config.docker_tag}",
        args=[
            "consume",
            f"--source={config.source}",
            f"--sink={config.sink}",
            "--rdir=raw",
            "--zdir=data",
            f"--from={inittime.strftime('%Y-%m-%dT%H:%M')}",
        ],
        env=config.env,
    )

    return job


@dg.op
def submit_kbatch_job(context: dg.OpExecutionContext, job: Job) -> str:
    """Submit a kbatch job object to the kbatch server.

    Args:
        context: The dagster context.
        job: The job to submit.
    Returns:
        The name of the job.
    """

    # Submit the job
    result = kbc.submit_job(job=job, **KBATCH_DICT)
    # Extract the job name from the result
    job_name: str = result["metadata"]["name"]

    context.log.info(f"Kbatch job {job_name} requested.")

    return job_name


@dg.op
def follow_kbatch_job(
        context: dg.OpExecutionContext,
        job_name: str,
) -> str:
    """Wait for a kbatch job not to be pending.

    Args:
        context: The dagster context.
        job_name: The name of the job.
    Returns:
        The name of the job.
    """
    context.log.info("Assessing status of kbatch job.")
    # Wait for job not to be pending
    timeout: int = 60 * 2  # 2 minutes
    time_spent: int = 0
    pods_info: list[dict] = [{"status": {"phase": "Pending"}}]
    while time_spent < timeout:
        time.sleep(10)
        time_spent += 10
        pods_info = kbc.list_pods(job_name=job_name, **KBATCH_DICT)["items"]
        pod_status = pods_info[0]["status"]["phase"]
        if pod_status != "Pending":
            context.log.info(f"Job {job_name} is no longer Pending, status: {pod_status}.")
            break
        if time_spent % (1 * 60) == 0:
            context.log.info(f"Kbatch job {job_name} still pending after {int(time_spent / 60)} minutes.")
        if time_spent >= timeout:
            condition: str = pods_info[0]["status"]["container_statuses"][0]["state"]
            context.log.error(condition)
            raise KbatchJobException(
                message=f"Timed out waiting for kbatch job not to be pending.",
                job_name=job_name,
            )

    # Capture the logs
    # * Allows one hour for pod to run
    for log in kbc._logs(
            pod_name=kbc.list_pods(job_name=job_name, **KBATCH_DICT)["items"][0]["metadata"]["name"],
            stream=True,
            read_timeout=60 * 60,
            **KBATCH_DICT
    ):
        print(log)
        # Check whether the pod encountered an error
        if "encountered error running nwp-consumer" in log:
            raise KbatchJobException(
                message=f"Job {job_name} failed, see logs.",
                job_name=job_name,
            )

    # Get the pod status
    pods_info: list[dict] = kbc.list_pods(job_name=job_name, **KBATCH_DICT)["items"]
    pod_status = pods_info[0]["status"]["phase"]

    if pod_status == "Failed":
        raise KbatchJobException(
            message=f"Job {job_name} failed, see logs.",
            job_name=job_name,
        )

    return job_name


# --- GRAPHS --- #

@dg.graph
def kbatch_consumer_graph() -> str:
    job_name: str = submit_kbatch_job(job=define_kbatch_consumer_job())
    job_name = follow_kbatch_job(job_name=job_name)
    delete_kbatch_job(job_name=job_name)

    return job_name
