import dagster as dg
from dagster_docker import PipesDockerClient

from .assets import nwp, pv, sat


class BaseStorageResource(dg.ConfigurableResource):
    """A resource defining where to store external data.

    Consists of a location on disk accessible to the Dagster instance.
    Where possible all data not handled by Dagster's IO managers (e.g.
    data downloaded via dagster pipes docker instances) will use this
    path as a base location.
    """

    location: str

nwp_assets = dg.load_assets_from_package_module(
    package_module=nwp,
    group_name="nwp",
    key_prefix="nwp",
)

sat_assets = dg.load_assets_from_package_module(
    package_module=sat,
    group_name="sat",
    key_prefix="sat",
)

pv_assets = dg.load_assets_from_package_module(
    package_module=pv,
    group_name="pv",
    key_prefix="pv",
)

defs = dg.Definitions(
    assets=[*nwp_assets, *sat_assets, *pv_assets],
    resources={
        "pipes_subprocess_client": dg.PipesSubprocessClient(),
        "pipes_docker_client": PipesDockerClient(),
        "base_storage_resource": BaseStorageResource(
            location=dg.EnvVar("BASE_STORAGE_PATH"),
        ),
    },
    jobs=[],
    schedules=[],
)

