"""Definitions for the sat dagster code location."""

import dagster as dg

from . import eumetsat

all_assets: list[dg.AssetsDefinition] = [
    *eumetsat.all_assets,
]

all_jobs: list[dg.JobDefinition] = []

all_schedules: list[dg.ScheduleDefinition] = []

