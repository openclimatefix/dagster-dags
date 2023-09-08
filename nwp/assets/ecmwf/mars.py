from dagster_docker import execute_docker_container
from dagster import Config, OpExecutionContext, op
import datetime as dt


class NWPConsumerConfig(Config):
    date_from: str
    date_to: str
    source: str
    docker_volumes: list[str]

@op
def nwp_consumer_docker_op(context: OpExecutionContext, config: NWPConsumerConfig):
    execute_docker_container(
            context=context,
            image="ghcr.io/openclimatefix/nwp-consumer",
            command=[
                "consume", f'--source={config.source}', 
                f'--from={config.date_from}', 
                f'--to={config.date_to}'
            ],
            env_vars=[
                "ECMWF_API_KEY", "ECMWF_API_URL", "ECMWF_API_EMAIL",
                "CEDA_FTP_USER", "CEDA_FTP_PASS",
                "METOFFICE_ORDER_ID", "METOFFICE_CLIENT_ID", "METOFFICE_CLIENT_SECRET",
                "AWS_S3_BUCKET", "AWS_REGION", "AWS_ACCESS_KEY", "AWS_ACCESS_SECRET",
            ],
            container_kwargs={
                "volumes": docker_volumes
            }
    )

    pass

