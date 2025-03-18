"""Jacob's ICON global processing script, modified a little bit.

Usage
=====

For usage see:

    $ python download_combine_upload_icon.py --help

For ease the script is also packaged as a docker container:

    $ docker run \
        -e HF_TOKEN=<SOME_TOKEN> \
        -v /some/path:/tmp/nwp \
        ghcr.io/openclimatefix/icon-etl:main --help

Datasets
========

Example ICON-EU dataset (~20Gb):

     <xarray.Dataset> Dimensions: (step: 93, latitude: 657, longitude: 1377, isobaricInhPa: 20)

     Coordinates:
         * isobaricInhPa (isobaricInhPa) float64 50.0 70.0 100.0 ... 950.0 1e+03
         * latitude (latitude) float64 29.5 29.56 29.62 ... 70.44 70.5
         * longitude (longitude) float64 -23.5 -23.44 -23.38 ... 62.44 62.5
         * step (step) timedelta64[ns] 00:00:00 ... 5 days 00:00:00 time datetime64[ns] ...
         valid_time (step) datetime64[ns] dask.array<chunksize=(93,), meta=np.ndarray>

     Data variables: (3/60)
         alb_rad (step, latitude, longitude)
            float32 dask.array<chunksize=(37, 326, 350), meta=np.ndarray>
         ... ...
         v (step, isobaricInhPa, latitude, longitude)
            float32 dask.array<chunksize=(37, 20, 326, 350), meta=np.ndarray>
         z0 (step, latitude, longitude)
            float32 dask.array<chunksize=(37, 326, 350), meta=np.ndarray>
"""

import argparse
import bz2
import dataclasses
import datetime as dt
import logging
import os
import pathlib
import shutil
import sys
from multiprocessing import Pool, cpu_count

import requests
import xarray as xr
import zarr
from huggingface_hub import HfApi
from ocf_blosc2 import Blosc2

# Set up logging
handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="".join((
        "{",
        '"message": "%(message)s", ',
        '"severity": "%(levelname)s", "timestamp": "%(asctime)s.%(msecs)03dZ", ',
        '"logging.googleapis.com/labels": {"python_logger": "%(name)s"}, ',
        '"logging.googleapis.com/sourceLocation": ',
        '{"file": "%(filename)s", "line": %(lineno)d, "function": "%(funcName)s"}',
        "}",
    )),
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("cfgrib.dataset").setLevel(logging.WARNING)
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

# "p", "omega", "clc", "qv", "tke", "w" are model-level only in global so have been removed
var_3d_list_global = ["fi", "relhum", "t", "u", "v"]
# "p", "omega", "qv", "tke", "w" are model-level only in europe so have been removed
var_3d_list_europe = ["clc", "fi", "omega", "relhum", "t", "u", "v"]

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
    f_steps=list(range(0, 79)),
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
    date: dt.date,
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
    # New data comes in 3 ish hours after the run time,
    # ensure the script is running with a decent buffer
    date_string = date.strftime("%Y%m%d") + run_string
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

    # If the file already exists, do nothing
    if os.path.exists(filename):
        return filename
    # If the file exists as a .bz2, convert it
    elif os.path.exists(filename + ".bz2"):
        with open(filename + ".bz2", "rb") as source, open(filename, "wb") as dest:
            try:
                dest.write(bz2.decompress(source.read()))
            except Exception as e:
                log.error(f"Failed to decompress {filename}.bz2: {e}")
                return None
            os.remove(filename + ".bz2")
        return filename
    # If the file does not exist, attempt to download and extract it
    else:
        r = requests.get(url, stream=True, timeout=60*60)
        if r.status_code == requests.codes.ok:
            with r.raw as source, open(filename, "wb") as dest:
                dest.write(bz2.decompress(source.read()))
            return filename
        else:
            log.debug(f"Failed to download {url}")
            return None


def run(path: str, config: Config, run: str, date: dt.date) -> None:
    """Download ICON data, combine and upload to Hugging Face Hub."""
    # Download files first for run
    if not pathlib.Path(f"{path}/{run}/").exists():
        pathlib.Path(f"{path}/{run}/").mkdir(parents=True, exist_ok=True)

    results: list[str | None] = []
    not_done = True
    while not_done:
        try:
            urls = find_file_name(
                config=config,
                run_string=run,
                date=date,
            )
            log.info(f"Downloading {len(urls)} files for {date} run {run}")

            # We only parallelize if we have a number of files
            # larger than the cpu count
            if len(urls) > cpu_count():
                pool = Pool(cpu_count())
                results = pool.starmap(
                    download_extract_url,
                    [(url, f"{path}/{run}/") for url in urls],
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
        except Exception:
            log.error("Error downloading files for run {run}: {e}")

    filepaths: list[str] = list(filter(None, results))
    if len(filepaths) == 0:
        log.info(f"No files downloaded for run {run}: Data not yet available")
        return
    nbytes: int = sum([os.path.getsize(f) for f in filepaths])
    log.info(
        f"Downloaded {len(filepaths)} files "
        f"with {len(results) - len(filepaths)} failed downloads "
        f"for run {run}: {nbytes} bytes",
    )

    # Write files to zarr
    log.info(f"Converting {len(filepaths)} files for run {run}")
    if config == GLOBAL_CONFIG:
        lon_ds = xr.open_mfdataset(
            f"{path}/{run}/icon_global_icosahedral_time-invariant_*_CLON.grib2", engine="cfgrib",
        )
        lat_ds = xr.open_mfdataset(
            f"{path}/{run}/icon_global_icosahedral_time-invariant_*_CLAT.grib2", engine="cfgrib",
        )
        lons = lon_ds.tlon.values
        lats = lat_ds.tlat.values

    datasets = []
    for var_3d in [v.split("@")[0] for v in config.vars_3d]:
        var_paths: list[list[pathlib.Path]] = []
        for step in config.f_steps:
            step_paths: list[pathlib.Path] = list(pathlib.Path(f"{path}/{run}/").glob(
                f"{config.var_url}_pressure-level_*_{str(step).zfill(3)}_*_{var_3d.upper()}.grib2",
            ))
            if len(step_paths) == 0:
                log.debug(f"No files found for 3D var {var_3d} for run {run} and step {step}")
                log.debug(list(pathlib.Path(f"{path}/{run}/").glob(
                    f"{config.var_url}_pressure-level_*_{var_3d.upper()}.grib2",
                )))
                continue
            else:
                var_paths.append(step_paths)
        if len(var_paths) == 0:
            log.warning(f"No files found for 3D var {var_3d} for run {run}")
            continue
        try:
            ds = xr.concat(
                [
                    xr.open_mfdataset(
                        p,
                        engine="cfgrib",
                        backend_kwargs={"errors": "ignore"} if config == GLOBAL_CONFIG else {},
                        combine="nested",
                        concat_dim="isobaricInhPa",
                    ).sortby("isobaricInhPa")
                    for p in var_paths
                ],
                dim="step",
            ).sortby("step")
        except Exception as e:
            log.error(e)
            continue
        ds = ds.rename({v: var_3d for v in ds.data_vars}) # noqa: C420
        coords_to_remove = []
        for coord in ds.coords:
            if coord not in ds.dims and coord != "time":
                coords_to_remove.append(coord)
        if len(coords_to_remove) > 0:
            ds = ds.drop_vars(coords_to_remove)
        datasets.append(ds)
    ds_atmos = xr.merge(datasets)
    log.debug(f"Merged 3D datasets: {ds_atmos}")

    total_dataset = []
    for var_2d in config.vars_2d:
        paths = list(
            pathlib.Path(f"{path}/{run}").glob(
                f"{config.var_url}_single-level_*_*_{var_2d.upper()}.grib2",
            ),
        )
        if len(paths) == 0:
            log.warning(f"No files found for 2D var {var_2d} at {run}")
            continue
        try:
            ds = (
                xr.open_mfdataset(
                    paths,
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
        ds = ds.rename({v: var_2d for v in ds.data_vars}) # noqa: C420
        # Remove extra coordinates that are not dimensions or time
        coords_to_remove = []
        for coord in ds.coords:
            if coord not in ds.dims and coord != "time":
                coords_to_remove.append(coord)
        if len(coords_to_remove) > 0:
            ds = ds.drop_vars(coords_to_remove)
        total_dataset.append(ds)
    ds = xr.merge(total_dataset)
    log.debug("Merged 2D datasets: {ds}")
    # Merge both
    ds = xr.merge([ds, ds_atmos])
    # Add lats and lons manually for icon global
    if config == GLOBAL_CONFIG:
        ds = ds.assign_coords({"latitude": lats, "longitude": lons})
    log.debug(f"Created final dataset for run {run}: {ds}")
    encoding = {var: {"compressor": Blosc2("zstd", clevel=9)} for var in ds.data_vars}
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    with zarr.storage.ZipStore(
        f"{path}/{run}.zarr.zip",
        mode="w",
    ) as store:
        log.debug(f"Compressing and storing dataset for run {run}")
        ds.chunk(config.chunking).to_zarr(
            store, encoding=encoding, compute=True, zarr_version=2, zarr_format=2,
        )

    # Upload to huggingface
    log.info(f"Uploading {run} to Hugging Face Hub")
    done = False
    hf_path = str(ds.coords["time"].dt.strftime("data/%Y/%-m/%-d/%Y%m%d_%H.zarr.zip").values)
    attempts: int = 0
    while not done:
        try:
            # Authenticate with huggingface
            api = HfApi(token=os.environ["HF_TOKEN"])
            attempts += 1
            api.upload_file(
                path_or_fileobj=f"{path}/{run}.zarr.zip",
                path_in_repo=hf_path,
                repo_id=config.repo_id,
                repo_type="dataset",
            )
            done = True
            os.remove(f"{path}/{run}.zarr.zip")
        except Exception as e:
            log.error(f"Encountered error uploading to huggingface after {attempts} attempts: {e}")
            if attempts > 50:
                shutil.move(f"{path}/{run}.zarr.zip", f"{path}/failed/{os.path.basename(hf_path)}")
                return
    log.info(f"Uploaded {run} to Hugging Face Hub at {hf_path}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("area", choices=["eu", "global"], help="Area to download data for")
    parser.add_argument("--path", default="/tmp/nwp", help="Folder in which to save files") # noqa: S108
    parser.add_argument(
        "--run",
        default="all",
        choices=["00", "06", "12", "18", "all"],
        help="Run time to download",
    )
    parser.add_argument("--rm", action="store_true", help="Remove files on completion")
    parser.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        default=dt.datetime.now(tz=dt.UTC).date(),
        help="Date to download data for (YYY-MM-DD)",
    )

    # Check HF_TOKEN env var is present
    _ = os.environ["HF_TOKEN"]
    log.info("Starting ICON download script")
    args = parser.parse_args()

    if args.date < dt.datetime.now(tz=dt.UTC).date() and args.rm:
        log.warning(
            "The script is set to remove downloaded files. "
            "If all your files are in the same 'run' folder, "
            "you will lose data before it has a chance to be processed. "
            "Consider running the script without the --rm flag.",
        )

    path: str = f"{args.path}/{args.area}"
    if args.run == "all":
        runs: list[str] = ["00", "06", "12", "18"]
    else:
        runs = [args.run]
    # Cleanup any leftover files in path
    for hour in runs:
        if args.rm:
            shutil.rmtree(path, ignore_errors=True)
        if args.area == "eu":
            run(path=path, config=EUROPE_CONFIG, run=hour, date=args.date)
        elif args.area == "global":
            run(path=path, config=GLOBAL_CONFIG, run=hour, date=args.date)
        # Remove files
        if args.rm:
            shutil.rmtree(path, ignore_errors=True)
