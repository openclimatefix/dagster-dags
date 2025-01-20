import unittest

from local_archives import nwp_assets, sat_assets


class TestAssetKeyPrefixes(unittest.TestCase):
    def test_nwp_asset_key_prefixes(self) -> None:
        """Test asset keys for all nwp assets have the correct key structure."""
        for asset in [*nwp_assets, *sat_assets]:
            # Ensure that the prefix is one of the expected flavours
            self.assertIn( asset.key.path[0], ["nwp", "sat", "air"])

