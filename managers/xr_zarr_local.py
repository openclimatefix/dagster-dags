import dagster as dg
import zarr
import xarray as xr
import datetime as dt
from ocf_blosc2 import Blosc2
import pathlib

from nwp.ecmwf.ecmwf_uk import map_partition_to_time

class LocalFilesystemXarrayZarrManager(dg.ConfigurableIOManager):
    """IOManager for reading and writing xarray datasets to the local filesystem.

    Datasets are stored in zipped zarr format. It is expected to be used with an asset
    continaing a MultiPartitionDefinition with two keys: "date" and "inittime" from which
    the full initialisation time of the dataset can be inferred.

    The dataset is stored in a folder structure using the assets key prefixes and the
    base path. The full path to the dataset is:

    {base_path}/{slash_joined_asset_key_prefixes}/{date}{inittime}.zarr.zip
    """

    base_path: str = ""
    filename_formatstr: str = "%Y%m%dT%H%M.zarr.zip"

    def _get_path(self, context: dg.InputContext | dg.OutputContext) -> pathlib.Path:
        """Get the path to the zarr file."""
        if context.has_partition_key:
            if type (context.asset_key.path) is not list or len(context.asset_key.path) <= 1:
                raise ValueError(
                    "AssetKey is not a list of strings with at least two elements."
                    "Ensure the you have setkey_prefix on the asset."
                )

            asset_prefixes: str = '/'.join(context.asset_key.path[:-1])
            it = map_partition_to_time(context)
            return pathlib.Path(self.base_path) / asset_prefixes / it.strftime(self.filename_formatstr)
        else:
            # Not yet implemented
            raise NotImplementedError("No partition key found")

    def handle_output(self, context: dg.OutputContext, obj: xr.Dataset) -> None:
        """Save an xarray dataset to a zarr file."""
        dst = self._get_path(context)
        if dst.exists():
            dst.unlink()
        dst.parent.mkdir(parents=True, exist_ok=True)
        dataVar: str = next(iter(obj.data_vars.keys()))
        with zarr.ZipStore(path=dst.as_posix(), mode="w") as store:
            obj.to_zarr(
                store=store,
                encoding={
                    "init_time": {"units": "nanoseconds since 1970-01-01"},
                    dataVar: {
                        "compressor": Blosc2(cname="zstd", clevel=5),
                    },
                },
            )
        context.add_output_metadata({
            "path": dg.MetadataValue.path(dst.as_posix()),
            "size": dg.MetadataValue.int(dst.stat().st_size),
            "modified": dg.MetadataValue.text(
                dt.datetime.fromtimestamp(dst.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            ),
        })

    def load_input(self, context: dg.InputContext) -> xr.Dataset:
        """Load an xarray dataset from a zarr file."""
        src = self._get_path(context)
        return xr.open_zarr(f"zip::{src.as_posix()}")
