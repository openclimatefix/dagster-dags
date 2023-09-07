from dagster_docker import execute_docker_container
from dagster import Config, OpExecutionContext, op
import datetime as dt


class NWPConsumerConfig(Config):
    date_from: str
    date_to: str
    source: str

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
            env_vars=["ECMWF_API_KEY", "ECMWF_API_URL", "ECMWF_API_EMAIL"],
            container_kwargs={
                "volumes": [
                    '/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/raw:/tmp/raw',
                    '/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/zarr:/tmp/zarr',
                    '/tmp/nwpc:/tmp/nwpc'
                ]
            }
    )

    pass

