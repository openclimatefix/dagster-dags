import argparse
import dataclasses
import datetime as dt
import functools
import logging
import os
import pathlib
import sys
import tempfile
import uuid
from glob import glob
from multiprocessing import Pool, cpu_count

import cfgrib
import requests
import xarray as xr
import zarr
from ocf_blosc2 import Blosc2

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format='{"time": "%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}',
)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("cfgrib.dataset").setLevel(logging.WARNING)
log = logging.getLogger("gfs-etl")

@dataclasses.dataclass
class Config:
    """Configuration for the script."""

    steps: list[int]

DEFAULT_CONFIG = Config(
    steps=list(range(0, 55, 3)),
)

def download_url(url: str, folder: str) -> str | None:
    """Download file from URL and save it to folder."""
    filename = folder + os.path.basename(url)

    if os.path.exists(filename):
        return filename
    else:
        log.debug(f"Downloading {url} to {filename}")
        r = requests.get(url.strip(), allow_redirects=True, stream=True)
        if r.status_code == requests.codes.ok:
            with r.raw as source, open(filename, "wb") as dest:
                dest.write(source.read())
            return filename
        else:
            log.debug(f"Failed to download {url}: {r.content}")
            return None

def find_file_names(it: dt.datetime, config: Config) -> list[str]:
    """Find file names for the given init time."""
    base_url: str = "https://data.rda.ucar.edu/ds084.1"

    urls: list[str] = []
    for step in config.steps:
        url: str = f"{base_url}/{it:%Y/%Y%m%d}/gfs.0p25.{it:%Y%m%d%H}.f{step:03d}.grib2"
        urls.append(url)

    return urls

def convert_file(file: str, outfolder: str) -> str | None:
    """Convert GRIB2 file to xarray dataset stored as a zipped zarr.

    Storing the dile and passing that instead of an xaray dataset object saves memory.
    """
    # Convert the file
    try:
        ds: list[xr.Dataset] = cfgrib.open_datasets(file, backend_kwargs={"indexpath": ""})
    except Exception as e:
        log.error(f"Error opening dataset for file {file}: {e}")
        return None

    # Process all the parameters into a single file
    ds = [
        d for d in ds
        if any(x in d.coords for x in ["surface", "heightAboveGround", "isobaricInhPa"])
    ]

    # Split into surface, heightAboveGround, and isobaricInhPa lists
    surface: list[xr.Dataset] = [d for d in ds if "surface" in d.coords]
    heightAboveGround: list[xr.Dataset] = [d for d in ds if "heightAboveGround" in d.coords]
    isobaricInhPa: list[xr.Dataset] = [d for d in ds if "isobaricInhPa" in d.coords]
    del ds

    # Update name of each data variable based off the attribute GRIB_stepType
    for i, d in enumerate(surface):
        for variable in d.data_vars:
            d = d.rename({variable: f"{variable}_surface_{d[f'{variable}'].attrs['GRIB_stepType']}"})
        surface[i] = d
    for i, d in enumerate(heightAboveGround):
        for variable in d.data_vars:
            d = d.rename({variable: f"{variable}_{d[f'{variable}'].attrs['GRIB_stepType']}"})
        heightAboveGround[i] = d

    surface_merged: xr.Dataset = xr.merge(surface).drop_vars("unknown_surface_instant", errors="ignore")
    del surface
    heightAboveGround_merged: xr.Dataset = xr.merge(heightAboveGround)
    del heightAboveGround
    isobaricInhPa_merged: xr.Dataset = xr.merge(isobaricInhPa)
    del isobaricInhPa

    total_ds: xr.Dataset = (
        xr.merge([surface_merged, heightAboveGround_merged, isobaricInhPa_merged])
        .rename({"time": "init_time"})
        .expand_dims("init_time")
        .expand_dims("step")
        .transpose("init_time", "step", ...)
        .sortby("step")
        .chunk({"init_time": 1, "step": -1})
    )
    del surface_merged, heightAboveGround_merged, isobaricInhPa_merged

    outfile = f"{outfolder}/{uuid.uuid4().hex}.zarr.zip"
    with zarr.ZipStore(path=outfile, mode="w") as store:
        total_ds.to_zarr(store, compute=True)
    del total_ds
    return outfile

def _combine_same_it_datasets(dsp1: str, dsp2: str) -> str:
    """Combine two datasets with the same init time."""
    with zarr.ZipStore(dsp1, mode="r") as s1, zarr.ZipStore(dsp2, mode="r") as s2:
        ds1 = xr.open_zarr(s1)
        ds2 = xr.open_zarr(s2)
        ds = xr.concat([ds1, ds2], dim="step")
        del ds1, ds2
    os.remove(dsp2)
    with zarr.ZipStore(dsp2, mode="w") as s3:
        ds.to_zarr(s3, compute=True)
    return dsp2

def run(path: str, config: Config, date: dt.date) -> None:
    """Download GFS data, combine, and save."""
    # Dowload files first
    for hour in ["00", "06", "12", "18"]:
        if not pathlib.Path(f"{path}/{date:%Y%m%d}/{hour}/").exists():
            pathlib.Path(f"{path}/{date:%Y%m%d}/{hour}/").mkdir(parents=True, exist_ok=True)
        results: list[str] = []
        not_done = True
        while not_done:
            try:
                urls = find_file_names(
                    it=dt.datetime.combine(date, dt.time(int(hour))),
                    config=config,
                )

                # Only paralellize if there are more files than cpus
                if len(urls) > cpu_count():
                    pool = Pool(cpu_count())
                    results = pool.starmap(
                        download_url,
                        [(url, f"{path}/{date:%Y%m%d}/{hour}/") for url in urls],
                    )
                    pool.close()
                    pool.join()
                else:
                    results: list[str] = []
                    for url in urls:
                        result = download_url(url, f"{path}/{date:%Y%m%d}/{hour}/")
                        if result is not None:
                            results.append(result)

                not_done = False
            except Exception as e:
                log.error(e)
                continue

        log.info(f"Downloaded {len(results)} files for {date}:{hour}")

    # Write files to zarr
    log.info("Converting files")
    for hour in ["00", "06", "12", "18"]:

        dataset_paths: list[str] = []
        with tempfile.TemporaryDirectory() as tmpdir:
            for file in list(glob(f"{path}/{date:%Y%m%d}/{hour}/*{hour}.*.grib2")):
                ds_path = convert_file(file=file, outfolder=tmpdir)
                if ds_path is not None:
                    dataset_paths.append(ds_path)

            hour_ds_path: str = functools.reduce(_combine_same_it_datasets, dataset_paths)

            ds = xr.open_zarr(zarr.ZipStore(hour_ds_path, mode="r"))
            log.info(f"Dataset for {date}:{hour} created: {ds.to_dict(data=False)}")
            encoding = {var: {"compressor": Blosc2("zstd", clevel=9)} for var in ds.data_vars}
            encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
            with zarr.ZipStore(
                f"{path}/{date:%Y%m%d}/{date:%Y%m%d}T{hour}.zarr.zip",
                mode="w",
            ) as store:
                ds.to_zarr(
                    store,
                    encoding=encoding,
                    compute=True,
                )

if __name__ == "__main__":
    prog_start = dt.datetime.now(tz=dt.UTC)

    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default="/tmp/gfs", help="Path to save the data")
    parser.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        default=str(dt.datetime.now(tz=dt.UTC).date()),
        help="Date to download (YYYY-MM-DD)",
    )

    args = parser.parse_args()
    log.info(f"{prog_start!s}: Running with args: {args}")
    run(path=args.path, config=DEFAULT_CONFIG, date=args.date)

