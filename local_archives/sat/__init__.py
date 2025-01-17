"""Definitions for the sat dagster code location."""

import dagster as dg

sat_assets: list[dg.AssetsDefinition] = dg.load_assets_from_current_module(
    group_name="sat",
    key_prefix=["sat"],
)

