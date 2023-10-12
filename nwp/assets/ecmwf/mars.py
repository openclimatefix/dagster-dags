import contextlib
import os

import nwp_consumer.cmd.main as consumer
from dagster import Config, OpExecutionContext, op


@contextlib.contextmanager
def modify_env(newvars: dict[str, str]):
    """Temporarily modify the environment."""
    oldvars: dict[str, str] = os.environ.copy()
    for key in newvars:
        os.environ[key] = newvars[key]
    try:
        yield
    finally:
        for key in newvars:
            if key in oldvars:
                os.environ[key] = oldvars[key]
            else:
                del os.environ[key]

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
            "env": False,
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
            "env": False,
            "--source": downloadedConfig.source,
            "--sink": "local",
            "--from": downloadedConfig.date_from,
            "--to": downloadedConfig.date_to,
            "--rdir": downloadedConfig.raw_dir,
            "--zdir": downloadedConfig.zarr_dir,
            "--create-latest": False,
        })
