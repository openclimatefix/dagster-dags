"""Dagster IOManager resource for saving parquet data."""

import datetime as dt
import os
import re
from typing import override

import dagster as dg
import pandas as pd
from dagster._seven.temp_dir import get_system_temp_directory


class PartitionedParquetIOManager(dg.ConfigurableIOManager):
    """IOManager for storing Pandas DataFrames in parquet at a specified path.

    It stores outputs for different partitions in different filepaths.

    Downstream ops can either load this DataFrame or simply retrieve a path
    to where the data is stored.
    """

    @property
    def _base_path(self) -> str:
        """The base path to the parquet files."""
        raise NotImplementedError()

    @override
    def handle_output(self, context: dg.OutputContext, df: pd.DataFrame) -> None:
        path: str = self._get_path(
            asset_key=context.asset_key,
            partition_time_window=context.asset_partitions_time_window,
        )
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        row_count: int = len(df)
        try:
            df.to_parquet(path=path, index=False)
            context.log.debug(f"Data saved to {path}")
        except FileExistsError:
            context.log.info(f"Not overwriting existing file at {path}")
        context.add_output_metadata({"row_count": row_count, "path": path})

    @override
    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        path: str = self._get_path(
            asset_key=context.asset_key,
            partition_time_window=context.asset_partitions_time_window,
        )
        context.log.debug(f"Loading data from {path}")
        return pd.read_parquet(path)

    def manual_load(
        self,
        asset_key: dg.AssetKey,
        partition_time_window: dg.TimeWindow,
    ) -> pd.DataFrame:
        """Load data for a given asset key and partition time window.

        Useful if you need to manually call data stored by this IOManager.
        Not used automatically by dagster assets.
        """
        path: str = self._get_path(
            asset_key=asset_key,
            partition_time_window=partition_time_window,
        )
        dg.get_dagster_logger().debug(f"Loading data from {path}")
        return pd.read_parquet(path)

    def _get_path(
        self,
        asset_key: dg.AssetKey,
        partition_time_window: dg.TimeWindow | None = None,
    ) -> str:
        """Get the parquet file path for the given asset key and time window.

        The path is based on the asset key and partitions, if any.
        If it is partitioned data, the path will include the partition window
        as part of the filename, and the partitions will be stored in
        subfolders matching the cadence of the partitions.
        """
        key = asset_key.path[-1]

        if partition_time_window is None:
            return os.path.join(self._base_path, f"{key}.pq")
        else:
            # For different partition cadences, name the files accordingly
            start, end = partition_time_window
            partition_str: str = f"{start:%Y%m%dT%H%M%s}_{end:%Y%m%dT%H%M%s}"
            partition_folder: str = f"{start:%Y/%m/%d}"

            if dt.timedelta(days=27) < end - start < dt.timedelta(days=33):
                # Probably a monthly partition
                partition_str = f"{start:%Y%m}"
                partition_folder = f"{start:%Y/%m}"
            elif dt.timedelta(days=364) < end - start < dt.timedelta(days=366):
                # Probably a yearly partition
                partition_str = f"{start:%Y}"
                partition_folder = f"{start:%Y}"
            else:
                filename: str = f"{partition_str}_{key}.pq"

            # To match legacy filenaming of passiv, put a special case in
            match: re.Match | None = re.match(r"^passiv_(\d+min)_\w+$", key)
            if match:
                filename = f"{partition_str}_{match.group(1)}.parquet"

            return os.path.join(self._base_path, partition_folder, filename)


class LocalPartitionedParquetIOManager(PartitionedParquetIOManager):
    """IOManager for storing Pandas DataFrames in parquet locally."""

    base_path: str = get_system_temp_directory()

    @property
    def _base_path(self) -> str:
        return self.base_path


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):
    """IOManager for storing Pandas DataFrames in parquet on S3."""

    s3_bucket: str
    prefix: str | None

    @property
    def _base_path(self) -> str:
        if self.prefix:
            return f"s3://{self.s3_bucket}/{self.prefix}"
        else:
            return f"s3://{self.s3_bucket}"

class HuggingfacePartitionedParquetIOManager(PartitionedParquetIOManager):
    """IOManager for storing Pandas DataFrames in parquet on Huggingface."""

    token: str
    hf_dataset: str
    hf_organisation: str
    prefix: str | None

    @override
    def setup_for_execution(self, _: dg.InitResourceContext) -> None:
        os.environ["HF_TOKEN"] = self.token

    @property
    def _base_path(self) -> str:
        if self.prefix:
            return f"hf://datasets/{self.hf_organisation}/{self.hf_dataset}/{self.prefix}"
        else:
            return f"hf://datasets/{self.hf_organisation}/{self.hf_dataset}"


