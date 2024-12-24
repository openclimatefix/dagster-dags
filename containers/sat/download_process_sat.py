"""Pipeline for downloading, processing, and saving archival satellite data.

Consolidates the old cli_downloader, backfill_hrv and backfill_nonhrv scripts.
"""

import argparse
import dataclasses
import datetime as dt
import logging
import os
import pathlib
import shutil
import sys
import traceback
from collections.abc import Iterator
from typing import Literal

import dask.delayed
import dask.diagnostics
import dask.distributed
import eumdac
import eumdac.cli
import eumdac.product
import numpy as np
import pandas as pd
import pyproj
import xarray as xr
from ocf_blosc2 import Blosc2
from satpy import Scene
from tqdm import tqdm

if sys.stdout.isatty():
    # Simple logging for terminals
    _formatstr="%(levelname)s | %(message)s"
else:
    # JSON logging for containers
    _formatstr="".join((
        "{",
        '"message": "%(message)s", ',
        '"severity": "%(levelname)s", "timestamp": "%(asctime)s.%(msecs)03dZ", ',
        '"logging.googleapis.com/labels": {"python_logger": "%(name)s"}, ',
        '"logging.googleapis.com/sourceLocation": ',
        '{"file": "%(filename)s", "line": %(lineno)d, "function": "%(funcName)s"}',
        "}",
    ))

_loglevel: int | str = logging.getLevelName(os.getenv("LOGLEVEL", "INFO").upper())
logging.basicConfig(
    level=logging.INFO if isinstance(_loglevel, str) else _loglevel,
    stream=sys.stdout,
    format=_formatstr,
    datefmt="%Y-%m-%dT%H:%M:%S",
)

for logger in [
    "cfgrib",
    "charset_normalizer",
    "eumdac", # If you want to know about throttling, set this to WARNING
    "native_msg",
    "pyorbital",
    "pyresample",
    "requests",
    "satpy",
    "urllib3",
]:
    logging.getLogger(logger).setLevel(logging.ERROR)

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
    zarr_fmtstr: dict[str, str]


CONFIGS: dict[str, Config] = {
    "iodc": Config(
        region="india",
        cadence="15min",
        product_id="EO:EUM:DAT:MSG:HRSEVIRI-IODC",
        zarr_fmtstr={
            "hrv": "%Y-%m_hrv_iodc.zarr",
            "nonhrv": "%Y-%m_nonhrv_iodc.zarr",
        },
    ),
    "seviri": Config(
        region="europe",
        cadence="5min",
        product_id="EO:EUM:DAT:MSG:MSG15-RSS",
        zarr_fmtstr={
            "hrv": "%Y-%m_hrv.zarr",
            "nonhrv": "%Y-%m_nonhrv.zarr",
        },
    ),
    # Optional
    "odegree": Config(
        region="europe, africa",
        cadence="15min",
        product_id="EO:EUM:DAT:MSG:HRSEVIRI",
        zarr_fmtstr={
            "hrv": "%Y-%m_hrv_odegree.zarr",
            "nonhrv": "%Y-%m_nonhrv_odegree.zarr",
        },
    ),
}


@dataclasses.dataclass
class Channel:
    """Container for channel metadata."""

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

def get_products_iterator(
    sat_config: Config,
    start: dt.datetime,
    end: dt.datetime,
    token: eumdac.AccessToken,
) -> tuple[Iterator[eumdac.product.Product], int]:
    """Get an iterator over the products for a given satellite in a given time range.

    Checks that the number of products returned matches the expected number of products.
    """
    log.info(
        f"Searching for products between {start!s} and {end!s} "
        f"for {sat_config.product_id} ",
    )
    expected_products_count = int((end - start) / pd.Timedelta(sat_config.cadence))
    datastore = eumdac.DataStore(token)
    collection = datastore.get_collection(sat_config.product_id)
    search_results: eumdac.SearchResults = collection.search(
        dtstart=start,
        dtend=end,
        sort="start,time,1", # Sort by ascending start time
    )
    log.info(
        f"Found {search_results.total_results}/{expected_products_count} products "
        f"for {sat_config.product_id} ",
    )
    return search_results.__iter__(), search_results.total_results


def download_nat(
    product: eumdac.product.Product,
    folder: pathlib.Path,
    retries: int = 6,
) -> pathlib.Path | None:
    """Download a product to a folder.

    EUMDAC products are collections of files, with a `.nat` file containing the data,
    and with `.xml` files containing metadata. This function only downloads the `.nat` files,
    skipping any files that are already present in the folder.

    Args:
        product: Product to download.
        folder: Folder to download the product to.
        retries: Number of times to retry downloading the product.

    Returns:
        Path to the downloaded file, or None if the download failed.
    """
    folder.mkdir(parents=True, exist_ok=True)
    nat_files: list[str] = [p for p in product.entries if p.endswith(".nat")]
    if len(nat_files) != 1:
        log.warning(
            f"Product '{product}' contains {len(nat_files)} .nat files. "
            "Expected 1. Skipping download.",
        )
        return None
    nat_filename: str = nat_files[0]

    filepath: pathlib.Path = folder / nat_filename
    if filepath.exists():
        log.debug(f"Skipping existing file: {filepath}")
        return filepath

    for i in range(retries):
        try:
            with (product.open(nat_filename) as fsrc, filepath.open("wb") as fdst):
                shutil.copyfileobj(fsrc, fdst, length=16 * 1024)
            return filepath
        except Exception as e:
            log.warning(
                f"Error downloading product '{product}' (attempt {i}/{retries}): '{e}'",
            )

    log.error(f"Failed to download product '{product}' after {retries} attempts.")
    return None

def process_nat(
    sat_config: Config,
    path: pathlib.Path,
    dstype: Literal["hrv", "nonhrv"],
) -> xr.DataArray | None:
    """Process a `.nat` file into an xarray dataset.

    Args:
        path: Path to the `.nat` file to open.
        dstype: Type of data to process (hrv or nonhrv).
    """
    # The reader is the same for each satellite as the sensor is the same
    # * Hence "seviri" in all cases
    try:
        scene = Scene(filenames={"seviri_l1b_native": [path.as_posix()]})
        scene.load([c.variable for c in CHANNELS[dstype]])
    except Exception as e:
        raise OSError(f"Error reading '{path!s}' as satpy Scene: {e}") from e

    try:
        da: xr.DataArray = _convert_scene_to_dataarray(
            scene,
            band=CHANNELS[dstype][0].variable,
            area="RSS",
            calculate_osgb=False,
        )
    except Exception as e:
        raise ValueError(f"Error converting '{path!s}' to DataArray: {e}") from e

    # Rescale the data, save as dataset
    # TODO: Left over from Jacob, probbaly don't want this
    try:
        da = _rescale(da, CHANNELS[dstype])
    except Exception as e:
        raise ValueError(f"Error rescaling dataarray: {e}") from e

    # Reorder the coordinates, and set the data type
    da = da.transpose("time", "y_geostationary", "x_geostationary", "variable")
    da = da.astype(np.float16)

    return da

def write_to_zarr(
    da: xr.DataArray,
    zarr_path: pathlib.Path,
) -> None:
    """Write the given data array to the given zarr store.

    If a Zarr store already exists at the given path, the DataArray will be appended to it.
    """
    mode = "a" if zarr_path.exists() else "w"
    extra_kwargs = {
        "append_dim": "time",
    } if mode == "a" else {
        "encoding": {
            "data": {"compressor": Blosc2("zstd", clevel=5)},
            "time": {"units": "nanoseconds since 1970-01-01"},
        },
    }
    try:
        write_job = da.chunk({
            "time": 1,
            "x_geostationary": -1,
            "y_geostationary": -1,
            "variable": 1,
        }).to_dataset(
            name="data",
            promote_attrs=True,
        ).to_zarr(
            store=zarr_path,
            compute=True,
            consolidated=True,
            mode=mode,
            **extra_kwargs,
        )
    except Exception as e:
        log.error(f"Error writing dataset to zarr store {zarr_path} with mode {mode}: {e}")
        traceback.print_tb(e.__traceback__)

    return None

#def download_scans(
#    sat_config: Config,
#    folder: pathlib.Path,
#    scan_time: pd.Timestamp,
#    token: eumdac.AccessToken,
#) -> list[pathlib.Path]:
#    """Download satellite scans for a satellite at a given time.
#
#    Args:
#        sat_config: Configuration for the satellite.
#        folder: Folder to download the files to.
#        scan_time: Time to download the files for.
#        token: EUMETSTAT access token.
#
#    Returns:
#        List of downloaded files.
#    """
#    files: list[pathlib.Path] = []
#
#    # Download
#    window_start: pd.Timestamp = scan_time - pd.Timedelta(sat_config.cadence)
#    window_end: pd.Timestamp = scan_time + pd.Timedelta(sat_config.cadence)
#
#    try:
#        datastore = eumdac.DataStore(token)
#        collection = datastore.get_collection(sat_config.product_id)
#        search_results = collection.search(
#            dtstart=window_start.to_pydatetime(),
#            dtend=window_end.to_pydatetime(),
#        )
#    except Exception as e:
#        log.error(f"Error finding products: {e}")
#        return []
#
#    products_count: int = 0
#    for product in search_results:
#        for entry in list(filter(lambda p: p.endswith(".nat"), product.entries)):
#            filepath: pathlib.Path = folder / entry
#            # Prevent downloading existing files
#            if filepath.exists():
#                log.debug("Skipping existing file: {filepath}")
#                files.append(filepath)
#                continue
#            # Try download a few times
#            attempts: int = 1
#            while attempts < 6:
#                try:
#                    folder.mkdir(parents=True, exist_ok=True)
#                    with (
#                        product.open(entry) as fsrc,
#                        filepath.open("wb") as fdst,
#                    ):
#                        shutil.copyfileobj(fsrc, fdst)
#                    files.append(filepath)
#                    attempts = 1000
#                except Exception as e:
#                    log.warning(
#                        f"Error downloading product '{product}' (attempt {attempts}): '{e}'",
#                    )
#                    attempts += 1
#        products_count += 1
#
#    if products_count == 0:
#        log.warning(f"No products found for {scan_time}")
#
#    return files

def _fname_to_scantime(fname: str) -> dt.datetime:
    """Converts a filename to a datetime.

    Files are of the form:
    `MSGX-SEVI-MSG15-0100-NA-20230910221240.874000000Z-NA.nat`
    So determine the time from the first element split by '.'.
    """
    return dt.datetime.strptime(fname.split(".")[0][-14:], "%Y%m%d%H%M%S")

#def process_scans(
#    sat_config: Config,
#    folder: pathlib.Path,
#    start: dt.date,
#    end: dt.date,
#    dstype: Literal["hrv", "nonhrv"],
#) -> str:
#    """Process the downloaded scans into a zarr store.
#
#    Args:
#        sat_config: Configuration for the satellite.
#        folder: Folder to download the files to.
#        start: Start date for the processing.
#        end: End date for the processing.
#        dstype: Type of data to process (hrv or nonhrv).
#    """
#    # Check zarr file exists for the month
#    zarr_path: pathlib.Path = folder.parent / start.strftime(sat_config.zarr_fmtstr[dstype])
#    zarr_times: list[dt.datetime] = []
#    if zarr_path.exists():
#        zarr_times = xr.open_zarr(zarr_path, consolidated=True).sortby("time").time.values.tolist()
#        log.debug(f"Zarr store already exists at {zarr_path} for {zarr_times[0]}-{zarr_times[-1]}")
#    else:
#        log.debug(f"Zarr store does not exist at {zarr_path}")
#
#    # Get native files in order
#    native_files: list[pathlib.Path] = list(folder.glob("*.nat"))
#    native_files.sort()
#    wanted_files = [f for f in native_files if start <= _fname_to_scantime(f.name) < end]
#    log.info(f"Found {len(wanted_files)} native files within date range at {folder.as_posix()}")
#
#    # Convert native files to xarray datasets
#    # * Append to the monthly zarr in hourly chunks
#    datasets: list[xr.Dataset] = []
#    i: int
#    f: pathlib.Path
#    for i, f in enumerate(wanted_files):
#        try:
#            # TODO: This method of passing the zarr times to the open function leaves a lot to be desired
#            # Firstly, if the times are not passed in sorted order then the created 12-dataset chunks
#            # may have missed times in them. Secondly, determining the time still requires opening and
#            # converting the file which is probably slow. Better to skip search for files whose times
#            # are already in the Zarr store in the first place and bypass the entire pipeline.
#            dataset: xr.Dataset | None = _open_and_scale_data(zarr_times, f.as_posix(), dstype)
#        except Exception as e:
#            log.error(f"Error opening/scaling data for file {f}: {e}")
#            continue
#        if dataset is not None:
#            dataset = _preprocess_function(dataset)
#            datasets.append(dataset)
#        # Append to zarrs in hourly chunks
#        # * This is so zarr doesn't complain about mismatching chunk sizes
#        if len(datasets) == int(pd.Timedelta("1h") / pd.Timedelta(sat_config.cadence)):
#            if pathlib.Path(zarr_path).exists():
#                log.debug(f"Appending to existing zarr store at {zarr_path}")
#                mode = "a"
#            else:
#                log.debug(f"Creating new zarr store at {zarr_path}")
#                mode = "w"
#            concat_ds: xr.Dataset = xr.concat(datasets, dim="time")
#            _write_to_zarr(
#                concat_ds,
#                zarr_path.as_posix(),
#                mode,
#                chunks={
#                    "time": len(datasets),
#                    "x_geostationary": -1,
#                    "y_geostationary": -1,
#                    "variable": 1,
#                },
#            )
#            datasets = []
#
#        log.info(f"Process loop [{dstype}]: {i+1}/{len(wanted_files)}")
#
#    # Consolidate zarr metadata
#    if pathlib.Path(zarr_path).exists():
#        _rewrite_zarr_times(zarr_path.as_posix())
#
#    check_data_quality(xr.open_zarr(zarr_path, consolidated=True))
#
#    return dstype


def _gen_token() -> eumdac.AccessToken:
    """Generated an aces token from environment variables."""
    consumer_key: str = os.environ["EUMETSAT_CONSUMER_KEY"]
    consumer_secret: str = os.environ["EUMETSAT_CONSUMER_SECRET"]
    token = eumdac.AccessToken(credentials=(consumer_key, consumer_secret))

    return token


def _convert_scene_to_dataarray(
    scene: Scene,
    band: str,
    area: str,
    calculate_osgb: bool = True,
) -> xr.DataArray:
    """Converts a Scene with satellite data into a data array.

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

    # Update the dataarray attributes based off of the satpy scene attributes
    data_attrs = {}
    for channel in scene.wishlist:
        # Remove acq time from all bands because it is not useful, and can actually
        # get in the way of combining multiple Zarr datasets.
        scene[channel] = scene[channel].drop_vars("acq_time", errors="ignore")
        for attr in scene[channel].attrs:
            new_name = channel["name"] + "_" + attr
            # Ignore the "area" and "_satpy_id" scene attributes as they are not serializable
            # and their data is already present in other scene attrs anyway.
            if attr not in ["area", "_satpy_id"]:
                data_attrs[new_name] = scene[channel].attrs[attr].__repr__()

    ds: xr.Dataset = scene.to_xarray_dataset()
    da = ds.to_array()

    # Lat and Lon are the same for all the channels now
    if calculate_osgb:
        lon, lat = scene[band].attrs["area"].get_lonlats()
        osgb_x, osgb_y = transformer.transform(lat, lon)
        # Assign x_osgb and y_osgb and set some attributes
        da = da.assign_coords(
            x_osgb=(("y", "x"), np.float32(osgb_x)),
            y_osgb=(("y", "x"), np.float32(osgb_y)),
        )
        for name in ["x_osgb", "y_osgb"]:
            da[name].attrs = {
                "units": "meter",
                "coordinate_reference_system": "OSGB",
            }

        da.x_osgb.attrs["name"] = "Easting"
        da.y_osgb.attrs["name"] = "Northing"

    for name in ["x", "y"]:
        da[name].attrs["coordinate_reference_system"] = "geostationary"
    log.debug("Calculated OSGB")
    # Round to the nearest 5 minutes
    data_attrs["end_time"] = pd.Timestamp(da.attrs["end_time"]).round("5 min").__str__()
    da.attrs = data_attrs

    # Rename x and y to make clear the coordinate system they are in
    da = da.rename({"x": "x_geostationary", "y": "y_geostationary"})
    if "time" not in da.dims:
        time = pd.to_datetime(pd.Timestamp(da.attrs["end_time"]).round("5 min"))
        da = da.assign_coords({"time": time}).expand_dims("time")

    del da["crs"]
    del scene
    log.debug("Finished conversion")
    return da


def _rescale(da: xr.DataArray, channels: list[Channel]) -> xr.DataArray:
    """Rescale Xarray DataArray so all values lie in the range [0, 1].

    Warning: The original `dataarray` will be modified in-place.

    Args:
        da: DataArray to rescale.
            Dims MUST be named ('time', 'x_geostationary', 'y_geostationary', 'variable')!
        channels: List of Channel objects with minimum and maximum values for each channel.

    Returns:
        The DataArray rescaled to [0, 1]. NaNs in the original `dataarray` will still
        be present in the returned dataarray. The returned DataArray will be float32.
    """
    da = da.reindex(
        {"variable": [c.variable for c in channels]},
    ).transpose(
        "time",
        "y_geostationary",
        "x_geostationary",
        "variable",
    )

    # For each channel, subtract the minimum and divide by the range
    da -= [c.minimum for c in channels]
    da /= [c.maximum - c.minimum for c in channels]
    # Since the mins and maxes are approximations, clip the values to [0, 1]
    da = da.clip(min=0, max=1)
    da = da.astype(np.float32)
    return da


#def _open_and_scale_data(
#    zarr_times: list[dt.datetime],
#    f: str,
#    dstype: Literal["hrv", "nonhrv"],
#) -> xr.Dataset | None:
#    """Opens a raw file and converts it to a normalised xarray dataset.
#
#    Args:
#        zarr_times: List of times already in the zarr store.
#        f: Path to the file to open.
#        dstype: Type of data to process (hrv or nonhrv).
#    """
#    # The reader is the same for each satellite as the sensor is the same
#    # * Hence "seviri" in all cases
#    try:
#        scene = Scene(filenames={"seviri_l1b_native": [f]})
#        scene.load([c.variable for c in CHANNELS[dstype]])
#    except Exception as e:
#        raise OSError(f"Error loading scene from file {f}: {e}") from e
#
#    try:
#        da: xr.DataArray = _convert_scene_to_dataarray(
#            scene,
#            band=CHANNELS[dstype][0].variable,
#            area="RSS",
#            calculate_osgb=False,
#        )
#    except Exception as e:
#        log.error(f"Error converting scene to dataarray: {e}")
#        return None
#
#    # Don't proceed if the dataarray time is already present in the zarr store
#    if da.time.values[0] in zarr_times:
#        log.debug(f"Skipping: {da.time.values[0]}")
#        return None
#
#    # Rescale the data, save as dataset
#    try:
#        da = _rescale(da, CHANNELS[dstype])
#    except Exception as e:
#        log.error(f"Error rescaling dataarray: {e}")
#        return None
#
#    da = da.transpose("time", "y_geostationary", "x_geostationary", "variable")
#    ds: xr.Dataset = da.to_dataset(name="data", promote_attrs=True)
#    ds["data"] = ds["data"].astype(np.float16)
#
#    return ds


#def _preprocess_function(xr_data: xr.Dataset) -> xr.Dataset:
#    """Updates the coordinates for the given dataset."""
#    # TODO: Understand why this is necessary!
#    attrs = xr_data.attrs
#    y_coords = xr_data.coords["y_geostationary"].values
#    x_coords = xr_data.coords["x_geostationary"].values
#    x_dataarray: xr.DataArray  = xr.DataArray(
#        data=np.expand_dims(xr_data.coords["x_geostationary"].values, axis=0),
#        dims=["time", "x_geostationary"],
#        coords={"time": xr_data.coords["time"].values, "x_geostationary": x_coords},
#    )
#    y_dataarray: xr.DataArray = xr.DataArray(
#        data=np.expand_dims(xr_data.coords["y_geostationary"].values, axis=0),
#        dims=["time", "y_geostationary"],
#        coords={"time": xr_data.coords["time"].values, "y_geostationary": y_coords},
#    )
#    xr_data["x_geostationary_coordinates"] = x_dataarray
#    xr_data["y_geostationary_coordinates"] = y_dataarray
#    xr_data.attrs = attrs
#    return xr_data


#def _write_to_zarr(dataset: xr.Dataset, zarr_name: str, mode: str, chunks: dict) -> None:
#    """Writes the given dataset to the given zarr store."""
#    log.info("Writing to Zarr")
#    mode_extra_kwargs: dict[str, dict] = {
#        "a": {"append_dim": "time"},
#        "w": {
#            "encoding": {
#                "data": {
#                    "compressor": Blosc2("zstd", clevel=5),
#                },
#                "time": {"units": "nanoseconds since 1970-01-01"},
#            },
#        },
#    }
#    extra_kwargs = mode_extra_kwargs[mode]
#    sliced_ds: xr.Dataset = dataset.isel(x_geostationary=slice(0, 5548)).chunk(chunks)
#    try:
#        write_job = sliced_ds.to_zarr(
#            store=zarr_name,
#            compute=False,
#            consolidated=True,
#            mode=mode,
#            **extra_kwargs,
#        )
#        with dask.diagnostics.ProgressBar():
#            write_job.compute()
#    except Exception as e:
#        log.error(f"Error writing dataset to zarr store {zarr_name} with mode {mode}: {e}")
#        traceback.print_tb(e.__traceback__)
#        return None

#def _rewrite_zarr_times(output_name: str) -> None:
#    """Rewrites the time coordinates in the given zarr store."""
#    # Combine time coords
#    ds = xr.open_zarr(output_name, consolidated=True)
#
#    # Prevent numcodecs string error
#    # See https://github.com/pydata/xarray/issues/3476#issuecomment-1205346130
#    for v in list(ds.coords.keys()):
#        if ds.coords[v].dtype == object:
#            ds[v].encoding.clear()
#    for v in list(ds.variables.keys()):
#        if ds[v].dtype == object:
#            ds[v].encoding.clear()
#
#    del ds["data"]
#    if "x_geostationary_coordinates" in ds:
#        del ds["x_geostationary_coordinates"]
#    if "y_geostationary_coordinates" in ds:
#        del ds["y_geostationary_coordinates"]
#    # Need to remove these encodings to avoid chunking
#    del ds.time.encoding["chunks"]
#    del ds.time.encoding["preferred_chunks"]
#    ds.to_zarr(f"{output_name.split('.zarr')[0]}_coord.zarr", consolidated=True, mode="w")
#    # Remove current time ones
#    shutil.rmtree(f"{output_name}/time/")
#    # Add new time ones
#    shutil.copytree(f"{output_name.split('.zarr')[0]}_coord.zarr/time", f"{output_name}/time")
#
#    # Now replace the part of the .zmetadata with the part of the .zmetadata from the new coord one
#    with open(f"{output_name}/.zmetadata") as f:
#        data = json.load(f)
#        with open(f"{output_name.split('.zarr')[0]}_coord.zarr/.zmetadata") as f2:
#            coord_data = json.load(f2)
#        data["metadata"]["time/.zarray"] = coord_data["metadata"]["time/.zarray"]
#    with open(f"{output_name}/.zmetadata", "w") as f:
#        json.dump(data, f)
#    zarr.consolidate_metadata(output_name)


parser = argparse.ArgumentParser(
    prog="EUMETSTAT Pipeline",
    description="Downloads and processes data from EUMETSTAT",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "sat",
    help="Which satellite to download data from",
    type=str,
    choices=list(CONFIGS.keys()),
)
parser.add_argument(
    "--hrv",
    help="Download HRV instead of non-HRV data",
    action="store_true",
    default=False,
)
parser.add_argument(
    "--path", "-p",
    help="Path to store the downloaded data",
    default="/mnt/disks/sat",
    type=pathlib.Path,
)
parser.add_argument(
    "--month", "-m",
    help="Month to download data for (YYYY-MM)",
    type=str,
    required=True,
    default=str(dt.datetime.now(tz=dt.UTC).strftime("%Y-%m")),
)
parser.add_argument(
    "--delete_raw", "--rm",
    help="Delete raw files after processing",
    action="store_true",
    default=False,
)
parser.add_argument(
    "--validate",
    help="Check the quality of the data",
    action="store_true",
    default=False,
)



def check_data_quality(ds: xr.Dataset) -> None:
    """Check the quality of the data in the given dataset."""

    def _calc_null_percentage(data: np.ndarray):
        nulls = np.isnan(data)
        return nulls.sum() / len(nulls)

    result = xr.apply_ufunc(
        _calc_null_percentage,
        ds.data_vars["data"],
        input_core_dims=[["x_geostationary", "y_geostationary"]],
        vectorize=True,
        dask="parallelized",
    )

    num_images_failing_nulls_threshold = (result > 0.05).sum().item()
    num_images = result.size
    log.info(
        f"{num_images_failing_nulls_threshold}/{num_images} "
        f"({num_images_failing_nulls_threshold/num_images:.2%}) "
        "of images have greater than 5% null values",
    )

def run(args: argparse.Namespace) -> None:
    """Run the download and processing pipeline."""
    prog_start = dt.datetime.now(tz=dt.UTC)
    log.info(f"{prog_start!s}: Running with args: {args}")

    # Get values from args
    folder: pathlib.Path = args.path
    sat_config = CONFIGS[args.sat]
    start: dt.datetime = dt.datetime.strptime(args.month, "%Y-%m")
    end: dt.datetime = (start + pd.DateOffset(months=1, minutes=-1)).to_pydatetime()
    dstype: str = "hrv" if args.hrv else "nonhrv"

    product_iter, total = get_products_iterator(
        sat_config=sat_config,
        start=start,
        end=end,
        token=_gen_token(),
    )

    for product in tqdm(product_iter, total=total):
        nat_filepath = download_nat(
            product=product,
            folder=folder / args.sat,
        )
        if nat_filepath is None:
            raise OSError(f"Failed to download product '{product}'")
        da = process_nat(sat_config, nat_filepath, dstype)
        write_to_zarr(
            da=da,
            zarr_path=folder / start.strftime(sat_config.zarr_fmtstr[dstype]),
        )

    runtime = dt.datetime.now(tz=dt.UTC) - prog_start
    log.info("Completed archive for args: {args} in {runtime!s}.")

    # Download data
    # We only parallelize if we have a number of files larger than the cpu count
    # token = _gen_token()
    # raw_paths: list[pathlib.Path] = []
    # if len(scan_times) > cpu_count():
    #     log.debug(f"Concurrency: {cpu_count()}")
    #     pool = Pool(max(cpu_count(), 10))  # EUMDAC only allows for 10 concurrent requests
    #     results: list[list[pathlib.Path]] = pool.starmap(
    #         download_scans,
    #         [(sat_config, folder, scan_time, token) for scan_time in scan_times],
    #     )
    #     pool.close()
    #     pool.join()
    #     raw_paths.extend(list(itertools.chain(*results)))
    # else:
    #     for scan_time in scan_times:
    #         result: list[pathlib.Path] = download_scans(sat_config, folder, scan_time, token)
    #         if len(result) > 0:
    #             raw_paths.extend(result)

    # log.info(f"Downloaded {len(raw_paths)} files.")
    # log.info("Converting raw data to HRV and non-HRV Zarr Stores.")

    # Process the HRV and non-HRV data concurrently if possible
    #completed_types: list[str] = []
    #for t in ["hrv", "nonhrv"]:
    #    log.info(f"Processing {t} data.")
    #    completed_type = process_scans(sat_config, folder, start, end, t)
    #    completed_types.append(completed_type)
    #for completed_type in completed_types:
    #    log.info(f"Processed {completed_type} data.")

    ## Calculate the new average time per timestamp
    #runtime: dt.timedelta = dt.datetime.now(tz=dt.UTC) - prog_start
    #new_average_secs_per_scan: int = int(
    #    (secs_per_scan + (runtime.total_seconds() / len(scan_times))) / 2,
    #)
    #log.info(f"Completed archive for args: {args}. ({new_average_secs_per_scan} seconds per scan).")

    if args.validate:
        zarr_path: pathlib.Path = folder.parent / start.strftime(sat_config.zarr_fmtstr[dstype])
        ds = xr.open_zarr(zarr_path, consolidated=True)
        check_data_quality(ds)

    # Delete raw files, if desired
    if args.delete_raw:
        log.info(f"Deleting {len(raw_paths)} raw files in {folder.as_posix()}.")
        for f in raw_paths:
            f.unlink()


if __name__ == "__main__":
    # Parse running args
    args = parser.parse_args()
    run(args)

