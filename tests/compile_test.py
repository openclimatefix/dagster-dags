
from local_archives import all_assets


def test_nwp_asset_key_prefixes() -> None:
    """Test asset keys for all nwp assets have the correct key structure."""
    for asset in all_assets:
        # Ensure that the prefix is one of the expected flavours
        assert asset.key.path[0] in ["nwp", "sat", "air"]

