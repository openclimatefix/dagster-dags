"""Dagster IOManager resource for saving parquet data."""

import datetime as dt
import os
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
        path: str = self._get_path(context)
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        row_count: int = len(df)
        context.log.info(f"Row count: {row_count}")
        df.to_parquet(path=path, index=False)
        context.add_output_metadata({"row_count": row_count, "path": path})

    @override
    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        path: str = self._get_path(context)
        return pd.read_parquet(path)


    def _get_path(self, context: dg.InputContext | dg.OutputContext) -> str:
        """Get the parquet file path for the given context.

        The path is based on the asset key and partitions, if any.
        If it is partitioned data, the path will include the partition window
        as part of the filename, and the partitions will be stored in
        subfolders matching the cadence of the partitions.
        """
        key = context.asset_key.path[-1]

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
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
            return os.path.join(self._base_path, partition_folder, f"{key}_{partition_str}.pq")

        else:
            return os.path.join(self._base_path, f"{key}.pq")


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


