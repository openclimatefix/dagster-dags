
from nwp_consumer.internal.inputs.ecmwf import mars

from constants import LOCATIONS_BY_ENVIRONMENT
from ._factories import make_asset_definitions, MakeAssetDefinitionsOptions


RAW_FOLDER = LOCATIONS_BY_ENVIRONMENT["local"].RAW_FOLDER

fetcher = mars.Client(
    area="nw-india",
    param_group="basic",
)

ecmwf_uk_source_archive, ecmwf_uk_raw_archive, ecmwf_uk_zarr_archive = make_asset_definitions(
    opts=MakeAssetDefinitionsOptions(
        area="nw_india",
        fetcher=fetcher,
    )
)



