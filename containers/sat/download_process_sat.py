"""Pipeline for downloading, processing, and saving archival satellite data.

Consolidates the old cli_downloader, backfill_hrv and backfill_nonhrv scripts.
"""
import argparse
import dataclasses
import datetime as dt
import json
import logging
import multiprocessing
import os
import pathlib
import shutil
import subprocess
import sys
from itertools import repeat
from typing import Literal

import diskcache as dc
import eumdac
import eumdac.cli
import numpy as np
import pandas as pd
import pyproj
import pyresample
import xarray as xr
import yaml
from ocf_blosc2 import Blosc2
from requests.exceptions import HTTPError
from satpy import Scene

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger("eumdac").setLevel(logging.WARN)
logging.getLogger("urllib3").setLevel(logging.ERROR)
log = logging.getLogger("sat-etl")

# OSGB is also called "OSGB 1936 / British National Grid -- United
# Kingdom Ordnance Survey".  OSGB is used in many UK electricity
# system maps, and is used by the UK Met Office UKV model.  OSGB is a
# Transverse Mercator projection, using 'easting' and 'northing'
# coordinates which are in meters.  See https://epsg.io/27700
OSGB = 27700
# WGS84 is short for "World Geodetic System 1984", used in GPS. Uses
# latitude and longitude.
WGS84 = 4326
# Geographic bounds for various regions of interest, in order of min_lon, min_lat, max_lon, max_lat
# (see https://satpy.readthedocs.io/en/stable/_modules/satpy/scene.html)
GEOGRAPHIC_BOUNDS = {"UK": (-16, 45, 10, 62), "RSS": (-64, 16, 83, 69)}

transformer = pyproj.Transformer.from_crs(crs_from=WGS84, crs_to=OSGB)


@dataclasses.dataclass
class Config:
    """Configuration that differs between satellite types."""

    region: str
    cadence: str
    product_id: str
    native_path: str
    zarr_path: dict[str, str]

CONFIGS: dict[str, Config] = {
    "iodc": Config(
        region="india",
        cadence="15min",
        product_id="EO:EUM:DAT:MSG:HRSEVIRI-IODC",
        native_path="/mnt/disks/sat/native_files_india/",
        zarr_path={
            "hrv": "/mnt/disks/sat/%Y_hrv_iodc.zarr",
            "nonhrv": "/mnt/disks/sat/%Y_nonhrv_iodc.zarr",
        },
    ),
    "severi": Config(
        region="europe",
        cadence="5min",
        product_id="EO:EUM:DAT:MSG:MSG15-RSS",
        native_path="/mnt/disks/sat/native_files/",
        zarr_path={
            "hrv": "/mnt/disks/sat/%Y_hrv.zarr",
            "nonhrv": "/mnt/disks/sat/%Y_nonhrv.zarr",
        },
    ),
    # Optional
    "odegree": Config(
        region="europe, africa",
        cadence="15min",
        product_id="EO:EUM:DAT:MSG:HRSEVIRI",
        native_path="/mnt/disks/sat/native_files_odegree/",
        zarr_path={
            "hrv": "/mnt/disks/sat/%Y_hrv_odegree.zarr",
            "nonhrv": "/mnt/disks/sat/%Y_nonhrv_odegree.zarr",
        },
    ),
}

@dataclasses.dataclass
class Channel:
    minimum: float
    maximum: float
    variable: str

# Approximate minimum and maximum pixel values per channel, for normalization
# * Caclulated by Jacob via the min and max of a snapshot of the data
CHANNELS: dict[str, list[Channel]] = {
    # Non-HRV is 11 channels of different filters
    "nonhrv": [
        Channel(-2.5118103, 69.60857, "IR_016"),
        Channel(-64.83977, 339.15588, "IR_039"),
        Channel(63.404694, 340.26526, "IR_087"),
        Channel(2.844452, 317.86752, "IR_097"),
        Channel(199.10002, 313.2767, "IR_108"),
        Channel(-17.254883, 315.99194, "IR_120"),
        Channel(-26.29155, 274.82297, "IR_134"),
        Channel(-1.1009827, 93.786545, "VIS006"),
        Channel(-2.4184198, 101.34922, "VIS008"),
        Channel(199.57048, 249.91806, "WV_062"),
        Channel(198.95093, 286.96323, "WV_073"),
    ],
    # HRV is one greyscale wideband filter channel
    "hrv": [
        Channel(-1.2278595, 103.90016, "HRV"),
    ],
}

def download_scans(
        sat_config: Config,
        scan_times: list[pd.Timestamp],
    ) -> list[pd.Timestamp]:
    """Download satellite scans for a given config over the given scan times.

    Returns:
        List of scan times that failed to download.
    """
    failed_scan_times: list[pd.Timestamp] = []

    # Get credentials from environment
    consumer_key: str = os.environ["EUMETSAT_CONSUMER_KEY"]
    consumer_secret: str = os.environ["EUMETSAT_CONSUMER_SECRET"]

    log.info
    direction: str = "forward" if scan_times[0] < scan_times[-1] else "backward"
    i: int
    scan_time: pd.Timestamp
    for i, scan_time in enumerate(scan_times):
        # Authenticate
        # * Generates a new access token with short expiry
        # * Equivalent to `eumdac --set-credentials <key> <secret>`
        credentials_path = pathlib.Path.home() / ".eumdac" / "credentials"
        credentials_path.parent.mkdir(exist_ok=True, parents=True)
        token = eumdac.AccessToken(credentials=(consumer_key, consumer_secret))
        try:
            log.debug("Credentials are correct. Token generated: {token}")
            credentials_path.write_text(f"{consumer_key},{consumer_secret}")
            log.debug(f"Credentials written to file {credentials_path.as_posix()}")
        except HTTPError as e:
            if e.response.status_code == 401:
                log.error("Invalid credentials. Please check your EUMETSAT credentials.")
                break
            else:
                log.error(f"Error getting credentials: {e}")
                failed_scan_times.append(scan_time)
                continue

        # Download
        # * Equivalent to `eumdac download -c <product_id> -s <start_time> -e <end_time> -o <output_dir> --entry *.nat -y`
        window_start: pd.Timestamp = scan_time - pd.Timedelta(sat_config.cadence)
        window_end: pd.Timestamp = scan_time + pd.Timedelta(sat_config.cadence)
        try:
            eumdac.cli.download(
                argparse.Namespace(
                    entry=["*.nat"],
                    output_dir=sat_config.native_path,
                    collection=[sat_config.product_id],
                    start=window_start,
                    end=window_end,
                    token=token,
                    yes=True,
                    test=False,
                    query=None,
                    product=None,
                    publication_after=None,
                    publication_before=None,
                    sort=None,
                    dtstart=None,
                    dtend=None,
                    bbox=None,
                    geo=None,
                    sat=None,
                    cycle=None,
                    orbit=None,
                    relorbit=None,
                    filename=None,
                    timeliness=None,
                    product_type=None,
                    limit=None,
                    integrity=None,
                    download_coverage=None,
                    tailor=None,
                    chain=None,
                    dirs=None,
                    onedir=None,
                    no_warning_logs=None,
                    keep_order=None,
                ),
            )
        except eumdac.errors.EumdacError as e:
            log.error(f"Error downloading files: {e}")
            failed_scan_times.append(scan_time)

        log.info(f"Download [{direction}]: {i}/{len(scan_times)}")

    return failed_scan_times

def process_scans(
        sat_config: Config,
        start: dt.date,
        end: dt.date,
        dstype: Literal["hrv", "nonhrv"],
    ) -> str:
    """Process the downloaded scans into a zarr store."""
    # Check zarr file exists for the year
    zarr_path = start.strftime(sat_config.zarr_path[dstype])
    if os.path.exists(zarr_path):
        zarr_times = xr.open_zarr(zarr_path).sortby("time").time.values
        last_zarr_time = zarr_times[-1]
    else:
        # Set dummy values for times already in zarr
        last_zarr_time = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
        zarr_times = [last_zarr_time, last_zarr_time]

    # Get native files in order
    native_files = list(pathlib.Path(sat_config.native_path).glob("*/*.nat"))
    log.info(f"Found {len(native_files)} native files at {sat_config.native_path}")
    native_files.sort()

    # Convert native files to xarray datasets
    # * Append to the yearly zarr in hourly chunks
    datasets: list[xr.Dataset] = []
    i: int
    f: pathlib.Path
    for i, f in enumerate(native_files):
        try:
            dataset: xr.Dataset | None = _open_and_scale_data(zarr_times, f.as_posix(), dstype)
        except Exception as e:
            log.error(f"Exception: {e}")
            continue
        if dataset is not None:
            dataset = _preprocess_function(dataset)
            datasets.append(dataset)
        # Append to zarrs in hourly chunks (12 sets of 5 minute datasets)
        # * This is so zarr doesn't complain about mismatching chunk sizes
        if len(datasets) == 12:
            if os.path.exists(zarr_path):
                mode = "a"
            else:
                mode = "w"
                log.debug(f"No zarr store found for year. Creating new store at {zarr_path}.")
            _write_to_zarr(
                xr.concat(datasets, dim="time"),
                zarr_path,
                mode,
                chunks={"time": 12},
            )
            datasets = []

        log.info(f"Process loop [{dstype}]: {i}/{len(native_files)}")

    # Consolidate zarr metadata
    _rewrite_zarr_times(zarr_path)

    return dstype

def _convert_scene_to_dataarray(
    scene: Scene, band: str, area: str, calculate_osgb: bool = True,
) -> xr.DataArray:
    """Convertes a Scene with satellite data into a data array.

    Args:
        scene: The satpy.Scene containing the satellite data
        band: The name of the band
        area: Name of the geographic area to use, such as 'UK'
        calculate_osgb: Whether to calculate OSGB x and y coordinates,
                        only needed for first data array

    Returns:
        Returns Xarray DataArray
    """
    if area not in GEOGRAPHIC_BOUNDS:
        raise ValueError(f"`area` must be one of {GEOGRAPHIC_BOUNDS.keys()}, not '{area}'")
    log.debug("Starting scene conversion")
    if area != "RSS":
        try:
            scene = scene.crop(ll_bbox=GEOGRAPHIC_BOUNDS[area])
        except NotImplementedError:
            # 15 minutely data by default doesn't work for some reason, have to resample it
            scene = scene.resample("msg_seviri_rss_1km" if band == "HRV" else "msg_seviri_rss_3km")
            log.debug("Finished resample")
            scene = scene.crop(ll_bbox=GEOGRAPHIC_BOUNDS[area])
    log.debug("Finished crop")
    # Remove acq time from all bands because it is not useful, and can actually
    # get in the way of combining multiple Zarr datasets.
    data_attrs = {}
    for channel in scene.wishlist:
        scene[channel] = scene[channel].drop_vars("acq_time", errors="ignore")
        for attr in scene[channel].attrs:
            new_name = channel["name"] + "_" + attr
            data_attrs[new_name] = scene[channel].attrs[attr]
    dataset: xr.Dataset = scene.to_xarray_dataset()
    dataarray = dataset.to_array()
    log.debug("Converted to dataarray")

    # Lat and Lon are the same for all the channels now
    if calculate_osgb:
        lon, lat = scene[band].attrs["area"].get_lonlats()
        osgb_x, osgb_y = transformer.transform(lat, lon)
        # Assign x_osgb and y_osgb and set some attributes
        dataarray = dataarray.assign_coords(
            x_osgb=(("y", "x"), np.float32(osgb_x)),
            y_osgb=(("y", "x"), np.float32(osgb_y)),
        )
        for name in ["x_osgb", "y_osgb"]:
            dataarray[name].attrs = {
                "units": "meter",
                "coordinate_reference_system": "OSGB",
            }

        dataarray.x_osgb.attrs["name"] = "Easting"
        dataarray.y_osgb.attrs["name"] = "Northing"

    for name in ["x", "y"]:
        dataarray[name].attrs["coordinate_reference_system"] = "geostationary"
    log.debug("Calculated OSGB")
    # Round to the nearest 5 minutes
    dataarray.attrs.update(data_attrs)
    dataarray.attrs["end_time"] = pd.Timestamp(dataarray.attrs["end_time"]).round("5 min")

    # Rename x and y to make clear the coordinate system they are in
    dataarray = dataarray.rename({"x": "x_geostationary", "y": "y_geostationary"})
    if "time" not in dataarray.dims:
        time = pd.to_datetime(pd.Timestamp(dataarray.attrs["end_time"]).round("5 min"))
        dataarray = dataarray.assign_coords({"time": time}).expand_dims("time")

    del dataarray["crs"]
    del scene
    log.debug("Finished conversion")
    return dataarray


def _serialize_attrs(attrs: dict) -> dict:
    """Ensure each value of dict can be serialized.

    This is required before saving to Zarr because Zarr represents attrs values in a
    JSON file (.zmetadata).

    The `area` field (which is a `pyresample.geometry.AreaDefinition` object gets turned
    into a YAML string, which can be loaded again using
    `area_definition = pyresample.area_config.load_area_from_string(data_array.attrs['area'])`

    Returns attrs dict where every value has been made serializable.
    """
    for key, value in attrs.items():
        # Convert Dicts
        if isinstance(value, dict):
            # Convert np.float32 to Python floats (otherwise yaml.dump complains)
            for inner_key in value:
                inner_value = value[inner_key]
                if isinstance(inner_value, np.floating):
                    value[inner_key] = float(inner_value)
            attrs[key] = yaml.dump(value)
        # Convert Numpy bools
        if isinstance(value, bool | np.bool_):
            attrs[key] = str(value)
        # Convert area
        if isinstance(value, pyresample.geometry.AreaDefinition):
            attrs[key] = value.dump()
        # Convert datetimes
        if isinstance(value, dt.datetime):
            attrs[key] = value.isoformat()

    return attrs

def _rescale(dataarray: xr.DataArray, channels: list[Channel]) -> xr.DataArray | None:
        """Rescale Xarray DataArray so all values lie in the range [0, 1].

        Warning: The original `dataarray` will be modified in-place.

        Args:
            dataarray: DataArray to rescale.
                Dims MUST be named ('time', 'x_geostationary', 'y_geostationary', 'variable')!
            channels: List of Channel objects with minimum and maximum values for each channel.

        Returns:
            The DataArray rescaled to [0, 1]. NaNs in the original `dataarray` will still
            be present in the returned dataarray. The returned DataArray will be float32.
        """
        dataarray = dataarray.reindex(
                {"variable": [c.variable for c in channels]},
        ).transpose(
            "time", "y_geostationary", "x_geostationary", "variable",
        )

        dataarray -= [c.minimum for c in channels]
        dataarray /= [c.maximum - c.minimum for c in channels]
        dataarray = dataarray.clip(min=0, max=1)
        dataarray = dataarray.astype(np.float32)
        dataarray.attrs = _serialize_attrs(dataarray.attrs)  # Must be serializable
        return dataarray

def _open_and_scale_data(
        zarr_times: list[dt.datetime],
        f: str,
        dstype: Literal["hrv", "nonhrv"],
    ) -> xr.Dataset | None:
    """Opens a raw file and converts it to a normalised xarray dataset."""
    # The reader is the same for each satellite as the sensor is the same
    # * Hence "severi" in all cases
    scene = Scene(filenames={"seviri_l1b_native": [f]})
    scene.load([c.variable for c in CHANNELS[dstype]])
    da: xr.DataArray = _convert_scene_to_dataarray(
        scene, band=CHANNELS[dstype][0].variable, area="RSS", calculate_osgb=False,
    )

    # Rescale the data, update the attributes, save as dataset
    attrs = _serialize_attrs(da.attrs)
    da = _rescale(da, CHANNELS[dstype])
    da.attrs.update(attrs)
    da = da.transpose("time", "y_geostationary", "x_geostationary", "variable")
    ds: xr.Dataset = da.to_dataset(name="data")
    ds["data"] = ds.data.astype(np.float16)

    if ds.time.values[0] in zarr_times:
        log.debug(f"Skipping: {ds.time.values[0]}")
        return None

    return ds

def _preprocess_function(xr_data: xr.Dataset) -> xr.Dataset:
    """Updates the coordinates for the given dataset."""
    attrs = xr_data.attrs
    y_coords = xr_data.coords["y_geostationary"].values
    x_coords = xr_data.coords["x_geostationary"].values
    x_dataarray = xr.DataArray(
        data=np.expand_dims(xr_data.coords["x_geostationary"].values, axis=0),
        dims=["time", "x_geostationary"],
        coords={"time": xr_data.coords["time"].values, "x_geostationary": x_coords},
    )
    y_dataarray = xr.DataArray(
        data=np.expand_dims(xr_data.coords["y_geostationary"].values, axis=0),
        dims=["time", "y_geostationary"],
        coords={"time": xr_data.coords["time"].values, "y_geostationary": y_coords},
    )
    xr_data["x_geostationary_coordinates"] = x_dataarray
    xr_data["y_geostationary_coordinates"] = y_dataarray
    xr_data.attrs = attrs
    return xr_data


def _write_to_zarr(dataset: xr.Dataset, zarr_name: str, mode: str, chunks: dict) -> None:
    """Writes the given dataset to the given zarr store."""
    mode_extra_kwargs = {
        "a": {"append_dim": "time"},
        "w": {
            "encoding": {
                "data": {
                    "compressor": Blosc2("zstd", clevel=5),
                },
                "time": {"units": "nanoseconds since 1970-01-01"},
            },
        },
    }
    extra_kwargs = mode_extra_kwargs[mode]
    dataset.isel(x_geostationary=slice(0,5548)).chunk(chunks).to_zarr(
        store=zarr_name,
        compute=True,
        consolidated=True,
        mode=mode,
        **extra_kwargs,
    )


def _rewrite_zarr_times(output_name: str) -> None:
    """Rewrites the time coordinates in the given zarr store."""
    # Combine time coords
    ds = xr.open_zarr(output_name)

  # Prevent numcodecs string error
  # See https://github.com/pydata/xarray/issues/3476#issuecomment-1205346130
    for v in list(ds.coords.keys()):
        if ds.coords[v].dtype == object:
            ds[v].encoding.clear()

    for v in list(ds.variables.keys()):
        if ds[v].dtype == object:
            ds[v].encoding.clear()

    del ds["data"]
    # Need to remove these encodings to avoid chunking
    del ds.time.encoding["chunks"]
    del ds.time.encoding["preferred_chunks"]
    ds.to_zarr(f"{output_name.split('.zarr')[0]}_coord.zarr", consolidated=True)
    # Remove current time ones
    shutil.rmtree(f"{output_name}/time/")
    # Add new time ones
    shutil.copytree(f"{output_name.split('.zarr')[0]}_coord.zarr/time", f"{output_name}/time")

    # Now replace the part of the .zmetadata with the part of the .zmetadata from the new coord one
    with open(f"{output_name}/.zmetadata") as f:
        data = json.load(f)
        with open(f"{output_name.split('.zarr')[0]}_coord.zarr/.zmetadata") as f2:
            coord_data = json.load(f2)
        data["metadata"]["time/.zarray"] = coord_data["metadata"]["time/.zarray"]
    with open(f"{output_name}/.zmetadata", "w") as f:
        json.dump(data, f)


parser = argparse.ArgumentParser(
    prog="EUMETSTAT Pipeline",
    description="Downloads and processes data from EUMETSTAT",
)
parser.add_argument(
    "sat",
    help="Which satellite to download data from",
    type=str,
    choices=list(CONFIGS.keys()),
)
parser.add_argument(
    "--start_date",
    help="Date to download from (YYYY-MM-DD)",
    type=dt.date.fromisoformat,
    required=False,
)
parser.add_argument(
    "--end_date",
    help="Date to download to (YYYY-MM-DD)",
    type=dt.date.fromisoformat,
    required=False,
    default=str(dt.datetime.now(tz=dt.UTC).date()),
)

if __name__ == "__main__":
    prog_start = dt.datetime.now(tz=dt.UTC)

    # Parse running args
    args = parser.parse_args()
    # Create a reusable cache
    cache = dc.Cache(f"/mnt/disks/sat/.cache/{args.sat}")

    if args.start_date is None:
        # Try to get the start date from the last cached datetime
        try:
            args.start_date = dt.date.fromisoformat(cache.get("latest_time"))
        except Exception as e:
            raise Exception("Can't get last runtime from cache. Pass start_date in manually.") from e

    log.info(f"{prog_start!s}: Running with args: {args}")

    # Get config for desired satellite
    sat_config = CONFIGS[args.sat]

    # Get start and end times for run
    start: dt.date = args.start_date
    end: dt.date = args.end_date
    scan_times: list[pd.Timestamp] = pd.date_range(start=start, end=end, freq=sat_config.cadence).tolist()

    # Get average runtime from cache
    secs_per_scan = cache.get("secs_per_scan", default=90)
    expected_runtime = pd.Timedelta(secs_per_scan * len(scan_times), "seconds")
    log.info(f"Downloading {len(scan_times)} scans. Expected runtime: {expected_runtime!s}")

    # Perform data download passes
    # * Each pass has two simultaneous forward and backward download streams
    for pass_num in [0, 1]:
        pool = multiprocessing.Pool()
        log.info(f"Performing pass {pass_num}")
        results = pool.starmap(
                download_scans,
                [
                    (sat_config, scan_times),
                    (sat_config, list(reversed(scan_times))),
                ],
        )
        pool.close()
        pool.join()
        for result in results:
            log.info(f"Completed download with {len(result)} failed scan times.")

    log.info("Converting raw data to HRV and non-HRV Zarr Stores.")

    # Process the HRV and non-HRV data
    pool = multiprocessing.Pool()
    results = pool.starmap(
        process_scans,
        zip(repeat(sat_config), repeat(start), repeat(end), ["hrv", "nonhrv"]),
    )
    pool.close()
    pool.join()
    for result in results:
        log.info(f"Processed {result} data.")

    # Save the last processed time to cache
    cache.set("latest_time", end.isoformat())

    # Calculate the new average time per timestamp
    runtime: dt.timedelta = dt.datetime.now(tz=dt.datetime.utc) - prog_start
    new_average_secs_per_scan: int = int((secs_per_scan + (runtime.total_seconds() / len(scan_times))) / 2)
    cache.set("secs_per_scan", new_average_secs_per_scan)
    log.info(f"Completed archive for args: {args}. ({new_average_secs_per_scan} seconds per scan).")


