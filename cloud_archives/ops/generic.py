"""Defines implementation-agnostic ops for generic graph-building."""


import dagster as dg
from pydantic import Field


class AssetMaterializationConfig(dg.Config):
    """Configuration for asset materialisation.

    Builds upon the dagster Config type, allowing for the configuration to be
    passed to an Op in a dagster pipeline. Default values of an ellipsis (...)
    are used to indicate that the value must be provided.
    """

    asset_key: list[str] = Field(
        description="The key of the asset to materialise.",
        default=...,
    )
    asset_description: str | None = Field(
        description="A description of the asset.",
        default=None,
    )


@dg.op
def log_asset_materialization(
    context: dg.OpExecutionContext,
    config: AssetMaterializationConfig,
    metadata: dict[str, dg.MetadataValue],
) -> None:
    """Materialises an asset according to the config."""
    context.log_event(
        dg.AssetMaterialization(
            asset_key=config.asset_key,
            description=config.asset_description,
            partition=context.partition_key if context.has_partition_key else None,
            metadata=metadata,
        ),
    )


@dg.op(
    ins={"depends_on": dg.In(dg.Nothing)},
)
def raise_exception() -> None:
    """Dagster Op that raises an exception.

    This Op is used to mark a branch in a graph as being undesirable.
    Defines a "Nothing" input to allow for the op to have upstream dependencies
    in a graph without the passing of data.
    """
    raise Exception("Reached exception Op.")
