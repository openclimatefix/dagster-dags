from dagster import asset  # import the `dagster` library
from nwp.assets.dwd.common import IconConfig

import os
from glob import glob

import xarray as xr
import zarr
from huggingface_hub import HfApi
from ocf_blosc2 import Blosc2
import shutil

from nwp.assets.dwd.consts import (
    EU_PRESSURE_LEVELS,
    EU_VAR2D_LIST,
    EU_VAR3D_LIST,
    GLOBAL_INVARIENT_LIST,
    GLOBAL_PRESSURE_LEVELS,
    GLOBAL_VAR2D_LIST,
    GLOBAL_VAR3D_LIST,
)
from nwp.assets.dwd.utils import get_dset, get_run


def does_files_exist(config, now_datetime):
    model = config.model
    api = HfApi()
    existing_files = api.list_repo_files(
        repo_id="openclimatefix/dwd-icon-global" if model == "global" else "openclimatefix/dwd-icon-eu",
        repo_type="dataset", )
    path_in_repo = f"data/{now_datetime.year}/"
    f"{now_datetime.month}/"
    f"{now_datetime.day}/"
    f"{now_datetime.year}{str(now_datetime.month).zfill(2)}{str(now_datetime.day).zfill(2)}"
    f"_{str(now_datetime.hour).zfill(2)}.zarr.zip"
    # Check if the current run exists or not
    if path_in_repo in existing_files:
        return True
    else:
        return False


@asset
def download_model_files(config: IconConfig):
    model = config.model
    run = config.run
    delay = config.delay
    folder = config.folder
    _, _, now = get_run(run, delay=delay)
    if does_files_exist(config, now):
        return
    if model == "global":
        var_3d_list = GLOBAL_VAR3D_LIST
        var_2d_list = GLOBAL_VAR2D_LIST
        invariant = GLOBAL_INVARIENT_LIST
        pressure_levels = GLOBAL_PRESSURE_LEVELS
        f_steps = list(range(0, 79)) + list(range(81, 99, 3))  # 4 days
    else:
        var_3d_list = EU_VAR3D_LIST
        var_2d_list = EU_VAR2D_LIST
        invariant = None
        pressure_levels = EU_PRESSURE_LEVELS
        f_steps = list(range(0, 79)) + list(range(81, 123, 3))  # 5 days
    if not os.path.exists(folder):
        os.mkdir(folder)

    vars_3d = [v + "@" + str(p) for v in var_3d_list for p in pressure_levels]
    vars_2d = var_2d_list
    get_dset(
        vars_2d=vars_2d,
        vars_3d=vars_3d,
        invarient=invariant,
        folder=folder,
        run=run,
        f_times=f_steps,
        model=model,
        delay=delay
    )


@asset(deps=[download_model_files])
def process_model_files(
        config: IconConfig
):
    model = config.model
    run = config.run
    delay = config.delay
    folder = config.folder
    zarr_path = config.folder
    date_string, _, now = get_run(run, delay=delay)
    if does_files_exist(config, now):
        return
    if model == "global":
        var_base = "icon_global_icosahedral"
        var_3d_list = GLOBAL_VAR3D_LIST
        var_2d_list = GLOBAL_VAR2D_LIST
        lon_ds = xr.open_dataset(
            list(glob(os.path.join(folder, run, f"{var_base}_time-invariant_{date_string}_CLON.grib2")))[0],
            engine="cfgrib",
            backend_kwargs={"errors": "ignore"},
        )
        lat_ds = xr.open_dataset(
            list(glob(os.path.join(folder, run, f"{var_base}_time-invariant_{date_string}_CLAT.grib2")))[0],
            engine="cfgrib",
            backend_kwargs={"errors": "ignore"},
        )
        lons = lon_ds.tlon.values
        lats = lat_ds.tlat.values
        f_steps = list(range(0, 79)) + list(range(81, 99, 3))  # 4 days
    else:
        var_base = "icon-eu_europe_regular-lat-lon"
        var_3d_list = EU_VAR3D_LIST
        var_2d_list = EU_VAR2D_LIST
        lons = None
        lats = None
        f_steps = list(range(0, 79)) + list(range(81, 123, 3))
    datasets = []
    for var_3d in var_3d_list:
        paths = [
            list(
                glob(
                    os.path.join(
                        folder,
                        run,
                        f"{var_base}_pressure-level_{date_string}_{str(s).zfill(3)}_*_{var_3d.upper()}.grib2",
                    )
                )
            )
            for s in f_steps
        ]
        paths = [p for p in paths if len(p) > 0]
        if not paths:
            raise Exception("No Pressure Level Paths")
        ds = xr.concat(
            [
                xr.open_mfdataset(
                    p,
                    engine="cfgrib",
                    backend_kwargs={"errors": "ignore"},
                    combine="nested",
                    concat_dim="isobaricInhPa",
                ).sortby("isobaricInhPa")
                for p in paths
            ],
            dim="step",
        ).sortby("step")
        ds = ds.rename({v: var_3d for v in ds.data_vars})
        coords_to_remove = []
        for coord in ds.coords:
            if coord not in ds.dims and coord != "time" and coord != "valid_time":
                coords_to_remove.append(coord)
        if len(coords_to_remove) > 0:
            ds = ds.drop_vars(coords_to_remove)
        datasets.append(ds)
    ds_atmos = xr.merge(datasets)
    total_dataset = []
    for var_2d in var_2d_list:
        vard_2d_files = list(
            glob(os.path.join(folder, run, f"{var_base}_single-level_{date_string}_*_{var_2d.upper()}.grib2")))
        if not vard_2d_files:
            continue
        ds = xr.open_mfdataset(
            vard_2d_files,
            engine="cfgrib",
            combine="nested",
            concat_dim="step",
            backend_kwargs={"errors": "ignore"},
        ).sortby("step")
        # Rename data variable to name in list, so no conflicts
        ds = ds.rename({v: var_2d for v in ds.data_vars})
        # Remove extra coordinates that are not dimensions or time
        coords_to_remove = []
        for coord in ds.coords:
            if coord not in ds.dims and coord != "time":
                coords_to_remove.append(coord)
        if len(coords_to_remove) > 0:
            ds = ds.drop_vars(coords_to_remove)
        total_dataset.append(ds)
    ds = xr.merge(total_dataset)
    # Merge both
    ds = xr.merge([ds, ds_atmos])
    if lats is not None and lons is not None:
        ds = ds.assign_coords({"latitude": lats, "longitude": lons})
    if model == "global":
        chunking = {
            "step": 37,
            "values": 122500,
            "isobaricInhPa": -1,
        }
    else:
        chunking = {
            "step": 37,
            "latitude": 326,
            "longitude": 350,
            "isobaricInhPa": -1,
        }
    encoding = {var: {"compressor": Blosc2("zstd", clevel=9)} for var in ds.data_vars}
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    with zarr.ZipStore(
            zarr_path,
            mode="w",
    ) as store:
        ds.chunk(chunking).to_zarr(store, encoding=encoding, compute=True)


@asset(deps=[process_model_files])
def upload_model_files_to_hf(config: IconConfig):
    _, _, now = get_run(config.run, delay=config.delay)
    if does_files_exist(config, now):
        return
    api = HfApi()
    model = config.model
    zarr_path = config.zarr_path
    dataset_xr = xr.open_zarr(zarr_path)
    api.upload_file(
        path_or_fileobj=zarr_path,
        path_in_repo=f"data/{dataset_xr.time.dt.year.values}/"
                     f"{dataset_xr.time.dt.month.values}/"
                     f"{dataset_xr.time.dt.day.values}/"
                     f"{dataset_xr.time.dt.year.values}{str(dataset_xr.time.dt.month.values).zfill(2)}{str(dataset_xr.time.dt.day.values).zfill(2)}"
                     f"_{str(dataset_xr.time.dt.hour.values).zfill(2)}.zarr.zip",
        repo_id="openclimatefix/dwd-icon-global"
        if model == "global"
        else "openclimatefix/dwd-icon-eu",
        repo_type="dataset",
    )
    shutil.rmtree(config.folder)
