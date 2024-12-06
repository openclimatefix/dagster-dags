import sys

from local_archives.nwp import all_assets


def test_nwp_asset_key_prefixes() -> None:
    """Test asset keys for all nwp assets have the correct key structure."""
    for asset in all_assets:
        assert len(asset.key.path) == 4

        # Ensure that the prefix is as expected
        # The first element should be the flavor:
        assert asset.key.path[0] in ["nwp", "sat"]
        # The second element should be the provider
        assert asset.key.path[1] in ["ecmwf", "metoffice", "eumetsat", "cams", "ceda", "meteomatics", "gfs", "ecmwf-eps"]
        # The third element should be the region
        assert asset.key.path[2] in ["uk", "eu", "global", "nw_india", "malta", "india", "india-stat"]
