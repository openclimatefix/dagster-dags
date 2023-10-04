import nwp_consumer.cmd.main as consumer
import contextlib
import os
from dagster import Config, OpExecutionContext, op
from dagster_docker import execute_docker_container

@contextlib.contextmanager
def modify_env(vars: dict[str, str]):
    """Temporarily modify the environment."""
    for var in vars:
        oldval = os.environ.get(var)
        newval = vars[var]
        os.environ[var] = newval
        vars[var] = oldval
    try:
        yield
    finally:
        for var in vars:
            os.environ[var] = oldval

class NWPConsumerConfig(Config):
    """Configuration for the NWP consumer."""

    date_from: str
    date_to: str
    source: str
    raw_dir: str
    zarr_dir: str
    env_overrides: dict[str, str]


@op
def nwp_consumer_download_op(context: OpExecutionContext, config: NWPConsumerConfig) \
        -> NWPConsumerConfig:
    """Download the data from the source."""
    with modify_env(config.env_overrides):
        consumer.run({
            "download": True,
            "convert": False,
            "consume": False,
            "check": False,
            "--source": config.source,
            "--sink": "local",
            "--from": config.date_from,
            "--to": config.date_to,
            "--rdir": config.raw_dir,
            "--zdir": config.zarr_dir,
            "--create-latest": False,
        })

    return config

@op
def nwp_consumer_convert_op(context: OpExecutionContext, downloadedConfig: NWPConsumerConfig):
    """Convert the downloaded data to zarr format."""
    with modify_env(downloadedConfig.env_overrides):
        consumer.run({
            "download": False,
            "convert": True,
            "consume": False,
            "check": False,
            "--source": downloadedConfig.source,
            "--sink": "local",
            "--from": downloadedConfig.date_from,
            "--to": downloadedConfig.date_to,
            "--rdir": downloadedConfig.raw_dir,
            "--zdir": downloadedConfig.zarr_dir,
            "--create-latest": False,
        })
