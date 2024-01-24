"""Defines creation functions for icon assets."""


import dataclasses as dc

import dagster as dg


@dc.dataclass
class MakeDefinitionsOptions:
    """Typesafe options for the _make_defintions function."""

    area: str
    partitions: dg.PartitionsDefinition

    def key_prefix(self) -> list[str]:
        """Generate an asset key prefix based on the area."""
        return ["nwp", "icon", self.area]


@dc.dataclass
class MakeDefinitionsOutputs:
    """Typesafe outputs for the _make_definitions function."""

    zarr_asset: dg.AssetDefinition


def _make_definitions(opts: MakeDefinitionsOptions) -> MakeDefinitionsOutputs:
    @dg.asset(
        name="zarr_archive",
        key_prefix=opts.key_prefix(),
        partitions_def=opts.partitions,
        auto_materialize_policy=dg.AutoMaterializationPolicy.eager(),
        compute_kind="external",
        op_tags={
            "MAX_RUNTIME_SECONDS_TAG": 20 * 60 * 60,
        },
    )
    def zarr_asset() -> dg.AssetsDefinition:
        """A zarr archive asset."""
        pass

    return MakeDefinitionsOutputs(
        zarr_asset=zarr_asset,
    )
