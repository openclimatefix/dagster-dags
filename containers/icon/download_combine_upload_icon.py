"""Jacob's ICON global processing script."""

import argparse
import bz2
import dataclasses
import datetime as dt
import logging
import os
import pathlib
import shutil
import sys
from glob import glob
from itertools import repeat
from multiprocessing import Pool, cpu_count

import requests
import xarray as xr
import zarr
from huggingface_hub import HfApi
from ocf_blosc2 import Blosc2

api = HfApi(token=os.environ["HF_TOKEN"])
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
log = logging.getLogger("icon-etl")

"""
# CDO grid description file for global regular grid of ICON.
gridtype  = lonlat
xsize     = 2399
ysize     = 1199
xfirst    = -180
xinc      = 0.15
yfirst    = -90
yinc      = 0.15
"""



var_2d_list_europe = [
    "alb_rad",
    "alhfl_s",
    "ashfl_s",
    "asob_s",
    "asob_t",
    "aswdifd_s",
    "aswdifu_s",
    "aswdir_s",
    "athb_s",
    "athb_t",
    "aumfl_s",
    "avmfl_s",
    "cape_con",
    "cape_ml",
    "clch",
    "clcl",
    "clcm",
    "clct",
    "clct_mod",
    "cldepth",
    "h_snow",
    "hbas_con",
    "htop_con",
    "htop_dc",
    "hzerocl",
    "pmsl",
    "ps",
    "qv_2m",
    "qv_s",
    "rain_con",
    "rain_gsp",
    "relhum_2m",
    "rho_snow",
    "runoff_g",
    "runoff_s",
    "snow_con",
    "snow_gsp",
    "snowlmt",
    "synmsg_bt_cl_ir10.8",
    "t_2m",
    "t_g",
    "t_snow",
    "tch",
    "tcm",
    "td_2m",
    "tmax_2m",
    "tmin_2m",
    "tot_prec",
    "tqc",
    "tqi",
    "u_10m",
    "v_10m",
    "vmax_10m",
    "w_snow",
    "w_so",
    "ww",
    "z0",
]

var_2d_list_global = [
    "alb_rad",
    "alhfl_s",
    "ashfl_s",
    "asob_s",
    "asob_t",
    "aswdifd_s",
    "aswdifu_s",
    "aswdir_s",
    "athb_s",
    "athb_t",
    "aumfl_s",
    "avmfl_s",
    "cape_con",
    "cape_ml",
    "clch",
    "clcl",
    "clcm",
    "clct",
    "clct_mod",
    "cldepth",
    "c_t_lk",
    "freshsnw",
    "fr_ice",
    "h_snow",
    "h_ice",
    "h_ml_lk",
    "hbas_con",
    "htop_con",
    "htop_dc",
    "hzerocl",
    "pmsl",
    "ps",
    "qv_2m",
    "qv_s",
    "rain_con",
    "rain_gsp",
    "relhum_2m",
    "rho_snow",
    "runoff_g",
    "runoff_s",
    "snow_con",
    "snow_gsp",
    "snowlmt",
    "synmsg_bt_cl_ir10.8",
    "t_2m",
    "t_g",
    "t_snow",
    "t_ice",
    "t_s",
    "tch",
    "tcm",
    "td_2m",
    "tmax_2m",
    "tmin_2m",
    "tot_prec",
    "tqc",
    "tqi",
    "tqr",
    "tqs",
    "tqv",
    "u_10m",
    "v_10m",
    "vmax_10m",
    "w_snow",
    "w_so",
    "ww",
    "z0",
]

var_3d_list_global = ["clc", "fi", "p", "qv", "relhum", "t", "tke", "u", "v", "w"]
var_3d_list_europe = ["clc", "fi", "omega", "p", "qv", "relhum", "t", "tke", "u", "v", "w"]

invarient_list = ["clat", "clon"]

pressure_levels_global = [
    1000,
    950,
    925,
    900,
    850,
    800,
    700,
    600,
    500,
    400,
    300,
    250,
    200,
    150,
    100,
    70,
    50,
    30,
]

pressure_levels_europe = [
    1000,
    950,
    925,
    900,
    875,
    850,
    825,
    800,
    775,
    700,
    600,
    500,
    400,
    300,
    250,
    200,
    150,
    100,
    70,
    50,
    30,
]


@dataclasses.dataclass
class Config:
    """Details differing elements for each icon instance."""

    vars_2d: list[str]
    vars_3d: list[str]
    vars_invarient: list[str]
    base_url: str
    model_url: str
    var_url: str
    chunking: dict[str, int]
    f_steps: list[int]
    repo_id: str

GLOBAL_CONFIG = Config(
    vars_2d=var_2d_list_global,
    vars_3d=[
        v + "@" + str(p)
        for v in var_3d_list_global
        for p in pressure_levels_global
    ],
    vars_invarient=invarient_list,
    base_url="https://opendata.dwd.de/weather/nwp",
    model_url="icon/grib",
    var_url="icon_global_icosahedral",
    f_steps=list(range(0, 73)),
    repo_id="openclimatefix/dwd-icon-global",
    chunking={
        "step": 37,
        "values": 122500,
        "isobaricInhPa": -1,
    },
)

EUROPE_CONFIG = Config(
    vars_2d=var_2d_list_europe,
    vars_3d=[
        v + "@" + str(p)
        for v in var_3d_list_europe
        for p in pressure_levels_europe
    ],
    vars_invarient=[],
    base_url="https://opendata.dwd.de/weather/nwp",
    model_url="icon-eu/grib",
    var_url="icon-eu_europe_regular-lat-lon",
    f_steps=list(range(0, 73)),
    repo_id="openclimatefix/dwd-icon-eu",
    chunking={
        "step": 37,
        "latitude": 326,
        "longitude": 350,
        "isobaricInhPa": -1,
    },
)

def find_file_name(
    config: Config,
    run_string: str,
) -> list[str]:
    """Find file names to be downloaded.

    - vars_2d, a list of 2d variables to download, e.g. ['t_2m']
    - vars_3d, a list of 3d variables to download with pressure
      level, e.g. ['t@850','fi@500']
    - f_times, forecast steps, e.g. 0 or list(np.arange(1, 79))
    Note that this function WILL NOT check if the files exist on
    the server to avoid wasting time. When they're passed
    to the download_extract_files function if the file does not
    exist it will simply not be downloaded.
    """
    date_string = dt.datetime.now(tz=dt.UTC).strftime("%Y%m%d") + run_string
    if (len(config.vars_2d) == 0) and (len(config.vars_3d) == 0):
        raise ValueError("You need to specify at least one 2D or one 3D variable")

    urls = []
    for f_time in config.f_steps:
        for var in config.vars_2d:
            var_url = config.var_url + "_single-level"
            urls.append(
                f"{config.base_url}/{config.model_url}/{run_string}/{var}/"
                f"{var_url}_{date_string}_{f_time:03d}_{var.upper()}.grib2.bz2",
            )
        for var in config.vars_3d:
            var_t, plev = var.split("@")
            var_url = config.var_url + "_pressure-level"
            urls.append(
                f"{config.base_url}/{config.model_url}/{run_string}/{var_t}/"
                f"{var_url}_{date_string}_{f_time:03d}_{plev}_{var_t.upper()}.grib2.bz2",
            )
        for var in config.vars_invarient:
            var_url = config.var_url + "_time-invariant"
            urls.append(
                f"{config.base_url}/{config.model_url}/{run_string}/{var}/"
                f"{var_url}_{date_string}_{var.upper()}.grib2.bz2",
            )
    return urls


def download_extract_url(url: str, folder: str) -> str | None:
    """Download and extract a file from a given url."""
    filename = folder + os.path.basename(url).replace(".bz2", "")

    if os.path.exists(filename):
        return filename
    else:
        r = requests.get(url, stream=True)
        if r.status_code == requests.codes.ok:
            with r.raw as source, open(filename, "wb") as dest:
                dest.write(bz2.decompress(source.read()))
            return filename
        else:
            return None


def run(path: str, config: Config) -> None:
    """Download ICON data, combine and upload to Hugging Face Hub."""
    # Download files first
    for run in ["00", "06", "12", "18"]:
        if not pathlib.Path(f"{path}/{run}/").exists():
            pathlib.Path(f"{path}/{run}/").mkdir(parents=True, exist_ok=True)

        not_done = True
        while not_done:
            try:
                urls = find_file_name(
                    config=config,
                    run_string=run,
                )
                log.info(f"Downloading {len(urls)} files")

                results: list[str] = []
                # We only parallelize if we have a number of files
                # larger than the cpu count
                if len(urls) > cpu_count():
                    pool = Pool(cpu_count())
                    results = pool.starmap(
                        download_extract_url,
                        zip(urls, repeat(f"{path}/{run}/")),
                    )
                    pool.close()
                    pool.join()
                else:
                    results = []
                    for url in urls:
                        result = download_extract_url(url, f"{path}/{run}/")
                        if result is not None:
                            results.append(result)

                not_done = False
            except Exception as e:
                log.error(e)
                continue

    # Write files to zarr
    log.info("Converting files")
    for run in ["00", "06", "12", "18"]:
        if config.model_url == "icon/grib":
            lon_ds = xr.open_mfdataset(
                f"{path}/{run}/icon_global_icosahedral_time-invariant_*_CLON.grib2", engine="cfgrib",
            )
            lat_ds = xr.open_mfdataset(
                f"{path}/{run}/icon_global_icosahedral_time-invariant_*_CLAT.grib2", engine="cfgrib",
            )
            lons = lon_ds.tlon.values
            lats = lat_ds.tlat.values

        datasets = []
        for var_3d in config.vars_3d:
            paths = [
                list(
                    glob(
                        f"{path}/{run}/{config.var_url}_pressure-level_*_"
                        f"{str(s).zfill(3)}_*_{var_3d.upper()}.grib2",
                    ),
                )
                for s in range(len(config.f_steps))
            ]
            try:
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
            except Exception as e:
                log.error(e)
                continue
            ds = ds.rename({v: var_3d for v in ds.data_vars})
            coords_to_remove = []
            for coord in ds.coords:
                if coord not in ds.dims and coord != "time":
                    coords_to_remove.append(coord)
            if len(coords_to_remove) > 0:
                ds = ds.drop_vars(coords_to_remove)
            datasets.append(ds)
            log.debug(ds)
        ds_atmos = xr.merge(datasets)

        total_dataset = []
        for var_2d in config.vars_2d:
            datasets = []
            log.debug(var_2d)
            try:
                ds = (
                    xr.open_mfdataset(
                        f"{path}/{run}/{config.var_url}_single-level_*_*_{var_2d.upper()}.grib2",
                        engine="cfgrib",
                        backend_kwargs={"errors": "ignore"},
                        combine="nested",
                        concat_dim="step",
                    )
                    .sortby("step")
                    .drop_vars("valid_time")
                )
            except Exception as e:
                log.error(e)
                continue
            # Rename data variable to name in list, so no conflicts
            ds = ds.rename({v: var_2d for v in ds.data_vars})
            # Remove extra coordinates that are not dimensions or time
            coords_to_remove = []
            for coord in ds.coords:
                if coord not in ds.dims and coord != "time":
                    coords_to_remove.append(coord)
            if len(coords_to_remove) > 0:
                ds = ds.drop_vars(coords_to_remove)
            log.debug(ds)
            total_dataset.append(ds)
        ds = xr.merge(total_dataset)
        log.debug(ds)
        # Merge both
        ds = xr.merge([ds, ds_atmos])
        # Add lats and lons manually for icon global
        if config.model_url == "icon/grib":
            ds = ds.assign_coords({"latitude": lats, "longitude": lons})
        log.debug(ds)
        encoding = {var: {"compressor": Blosc2("zstd", clevel=9)} for var in ds.data_vars}
        encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
        with zarr.ZipStore(
            f"{path}/{run}.zarr.zip",
            mode="w",
        ) as store:
            ds.chunk(config.chunking).to_zarr(
                store, encoding=encoding, compute=True,
            )
        done = False
        while not done:
            try:
                api.upload_file(
                    path_or_fileobj=f"{path}/{run}.zarr.zip",
                    path_in_repo=f"data/{ds.time.dt.year.values}/" \
                      + f"{ds.time.dt.month.values}/{ds.time.dt.day.values}/" \
                      + f"{ds.time.dt.year.values}{ds.time.dt.month.values}" \
                      + f"{ds.time.dt.day.values}_{ds.time.dt.hour.values}.zarr.zip",
                    repo_id=config.repo_id,
                    repo_type="dataset",
                )
                done = True
                shutil.rmtree(f"{path}/{run}/")
                os.remove(f"{path}/{run}.zarr.zip")
            except Exception as e:
                log.error(e)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("area", choices=["eu", "global"])
    parser.add_argument("--path", default="/tmp/nwp")

    log.info("Starting ICON download script")
    args = parser.parse_args()

    path: str = f"{args.path}/{args.area}"
    if args.area == "eu":
        run(path=path, config=EUROPE_CONFIG)
    elif args.area == "global":
        run(path=path, config=GLOBAL_CONFIG)
