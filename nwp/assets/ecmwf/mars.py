import os
import subprocess
import sys

from dagster import Config, OpExecutionContext, op
from dagster_docker import execute_docker_container


class NWPConsumerConfig(Config):
    date_from: str
    date_to: str
    source: str
    docker_volumes: list[str]
    raw_dir: str
    zarr_dir: str
    env_vars: list[str]

@op
def nwp_consumer_docker_op(context: OpExecutionContext, config: NWPConsumerConfig):
    execute_docker_container(
        context=context,
        image="ghcr.io/openclimatefix/nwp-consumer:latest",
        command=[
            "consume", f'--source={config.source}',
            f'--from={config.date_from}',
            f'--to={config.date_to}'
        ],
        env_vars=config.env_vars,
        container_kwargs={
            "volumes": config.docker_volumes
        }
    )

    pass

@op
def nwp_consumer_download_op(context: OpExecutionContext, config: NWPConsumerConfig):
    process = subprocess.run(
        [f"{os.path.dirname(sys.executable)}/nwp-consumer", "download",
         f'--source={config.source}', f'--from={config.date_from}', f'--to={config.date_to}',
         f'--rdir={config.raw_dir}', f'--zdir={config.zarr_dir}'],
        capture_output=True,
        text=True
    )
    print(process.stdout)
    print(process.stderr)
    process.check_returncode()
    return config

@op
def nwp_consumer_convert_op(context: OpExecutionContext, downloadedConfig: NWPConsumerConfig):
    process = subprocess.run(
        [f"{os.path.dirname(sys.executable)}/nwp-consumer", "convert",
         f'--source={downloadedConfig.source}', f'--from={downloadedConfig.date_from}',
         f'--to={downloadedConfig.date_to}',
         f'--rdir={downloadedConfig.raw_dir}', f'--zdir={downloadedConfig.zarr_dir}'],
        capture_output=True,
        text=True
    )
    print(process.stdout)
    print(process.stderr)

