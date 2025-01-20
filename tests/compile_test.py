import unittest

import dagster as dg

from src.local_archives import nwp_assets, sat_assets


class TestAssetKeyPrefixes(unittest.TestCase):
    def test_nwp_asset_key_prefixes(self) -> None:
        """Test asset keys for all nwp assets have the correct key structure."""
        for asset in [*nwp_assets, *sat_assets]:
            if isinstance(asset, dg.AssetsDefinition):
                # Ensure that the prefix is one of the expected flavours
                self.assertIn( asset.key.path[0], ["nwp", "sat", "air"])

