from dagster import Config, OpExecutionContext, op
from dagster_docker import execute_docker_container


class NWPConsumerConfig(Config):
    date_from: str
    date_to: str
    source: str
    docker_volumes: list[str]
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

