import nwp_consumer.cmd.main as consumer
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
    """Download the data from the source."""
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
