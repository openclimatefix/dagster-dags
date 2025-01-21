"""Download and combine GFS data for a given date and run."""

import argparse
import dataclasses
import datetime as dt
import logging
import os
import pathlib
import shutil
import sys
import uuid
from glob import glob
from multiprocessing import Pool, cpu_count

import cfgrib
import dagster_pipes
import requests
import urllib3
import xarray as xr
import zarr
from ocf_blosc2 import Blosc2

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format=" ".join((
        '{"time": "%(asctime)s", ',
        '"name": "%(name)s", ',
        '"level": "%(levelname)s", ',
        '"message": "%(message)s"}',
    )),
)
for logger in ["requests", "urllib3", "cfgrib.dataset"]:
    logging.getLogger(logger).setLevel(logging.WARNING)
log = logging.getLogger("gfs-etl")

@dataclasses.dataclass
class Config:
    """Configuration for the script."""

    steps: list[int]

DEFAULT_CONFIG = Config(
    steps=list(range(0, 84, 3)),
)

def download_url(url: str, folder: str) -> str | None:
    """Download file from URL and save it to folder."""
    filename = folder + os.path.basename(url)

    if os.path.exists(filename):
        return filename
    else:
        log.debug(f"Downloading {url} to {filename}")
        attempts: int = 1
        while attempts < 6:
            try:
                r = requests.get(url.strip(), allow_redirects=True, stream=True, timeout=60*60)
                if r.status_code == requests.codes.ok:
                    with open(filename, "wb") as dest:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                dest.write(chunk)
                    return filename
            except urllib3.exceptions.IncompleteRead:
                log.warning(f"Encountered IncompleteRead error: retrying ({attempts})")
                attempts += 1
            except Exception as e:
                log.error(f"Failed to download {url}: {e}")
                return None
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
    """Convert GRIB2 file to xarray dataset stored as a zarr.

    Storing the dile and passing that instead of an xaray dataset object saves memory.
    """
    log.debug(f"Converting {file} to zarr")
    # Convert the file
    try:
        ds: list[xr.Dataset] = cfgrib.open_datasets(
            file,
            backend_kwargs={
                "indexpath": "",
                "errors": "ignore",
            },
        )
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
            d = d.rename({
                variable: f"{variable}_surface_{d[f'{variable}'].attrs['GRIB_stepType']}",
            })
        surface[i] = d
    for i, d in enumerate(heightAboveGround):
        for variable in d.data_vars:
            d = d.rename({variable: f"{variable}_{d[f'{variable}'].attrs['GRIB_stepType']}"})
        heightAboveGround[i] = d

    surface_merged: xr.Dataset = xr.merge(surface).drop_vars(
        ["unknown_surface_instant", "valid_time"],
        errors="ignore",
    )
    del surface
    heightAboveGround_merged: xr.Dataset = xr.merge(heightAboveGround).drop_vars(
        "valid_time",
        errors="ignore",
    )
    del heightAboveGround
    isobaricInhPa_merged: xr.Dataset = xr.merge(isobaricInhPa).drop_vars(
        "valid_time",
        errors="ignore",
    )
    del isobaricInhPa

    total_ds: xr.Dataset = (
        xr.merge([surface_merged, heightAboveGround_merged, isobaricInhPa_merged])
        .rename({"time": "init_time"})
        .expand_dims("init_time")
        .expand_dims("step")
        .transpose("init_time", "step", ...)
        .sortby("step")
        .chunk({"init_time": 1, "step": 1})
    )
    del surface_merged, heightAboveGround_merged, isobaricInhPa_merged

    outfile = f"{outfolder}/{uuid.uuid4().hex}.zarr"
    with zarr.DirectoryStore(path=outfile) as store:
        total_ds.to_zarr(store, compute=True)
    del total_ds
    # Delete the original raw file
    os.remove(file)
    return outfile

def run(path: str, config: Config, date: dt.date, run: str) -> str:
    """Download GFS data, combine, and save for a single run."""
    # Dowload files first
    if not pathlib.Path(f"{path}/{date:%Y%m%d}/{run}/").exists():
        pathlib.Path(f"{path}/{date:%Y%m%d}/{run}/").mkdir(parents=True, exist_ok=True)
    results: list[str] = []
    not_done = True
    while not_done:
        try:
            urls = find_file_names(
                it=dt.datetime.combine(date, dt.time(int(run))),
                config=config,
            )

            # Only paralellize if there are more files than cpus
            if len(urls) > cpu_count():
                pool = Pool(cpu_count())
                results = pool.starmap(
                    download_url,  # type: ignore
                    [(url, f"{path}/{date:%Y%m%d}/{run}/") for url in urls],
                )
                pool.close()
                pool.join()
                results = [r for r in results if r is not None]
            else:
                for url in urls:
                    result = download_url(url, f"{path}/{date:%Y%m%d}/{run}/")
                    if result is not None:
                        results.append(result)
            not_done = False
        except Exception as e:
            log.error(e)
            continue

        log.info(f"Downloaded {len(results)} files for {date}:{run}")

    # Write files to zarr
    log.info("Converting files")

    run_files: list[str] = list(glob(f"{path}/{date:%Y%m%d}/{run}/*{run}.*.grib2"))
    dataset_paths: list[str] = []
    # Only paralellize if there are more files than cpus
    if len(run_files) > cpu_count():
        pool = Pool(cpu_count())
        dataset_paths = pool.starmap(
                convert_file,  # type: ignore
            [(file, path + "/.work") for file in run_files],
        )
        pool.close()
        pool.join()
        dataset_paths = [dp for dp in dataset_paths if dp is not None]
    else:
        for file in run_files:
            ds_path = convert_file(file=file, outfolder=path + "/.work")
            if ds_path is not None:
                dataset_paths.append(ds_path)
    log.debug(f"Converted {len(dataset_paths)} files for {date}:{run}")


    log.info("Combining run datasets and applying compression")
    run_ds: xr.Dataset = xr.open_mfdataset(dataset_paths)
    encoding = {var: {"compressor": Blosc2("zstd", clevel=9)} for var in run_ds.data_vars}
    encoding["init_time"] = {"units": "nanoseconds since 1970-01-01"}
    outpath = f"{path}/{date:%Y%m%d}/{date:%Y%m%d}{run}.zarr.zip"
    try:
        with zarr.ZipStore(path=outpath, mode="w") as store:
            run_ds.to_zarr(
                store,
                encoding=encoding,
                compute=True,
            )
        log.info(f"Saved dataset for {date:%Y%m%d}{run} to {outpath}")
        shutil.rmtree(path + "/.work", ignore_errors=True)
    except Exception as e:
        log.error(f"Error saving dataset for {date:%Y%m%d}{run}: {e}")

    return outpath

if __name__ == "__main__":
    prog_start = dt.datetime.now(tz=dt.UTC)

    parser = argparse.ArgumentParser(
        prog="GFS ETL",
        description="Download and combine GFS data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--path",
        default="/tmp/gfs",  # noqa: S108
        help="Path to save the data",
    )
    parser.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        default=str(dt.datetime.now(tz=dt.UTC).date()),
        help="Date to download data for (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--run",
        type=str,
        default="00",
        choices=["00", "06", "12", "18"],
        help="Run to download data for (HH)",
    )

    args = parser.parse_args()
    log.info(f"{prog_start!s}: Running with args: {args}")
    out = run(path=args.path, config=DEFAULT_CONFIG, date=args.date, run=args.run)
    ds = xr.open_zarr(out)
    prog_end = dt.datetime.now(tz=dt.UTC)

    with dagster_pipes.open_dagster_pipes():
        context = dagster_pipes.PipesContext.get()
        context.report_asset_materialization(
            metadata={
                "path": out,
                "dataset": str(ds),
                "size_bytes": os.path.getsize(out),
                "elapsed_time_seconds": (prog_end - prog_start).total_seconds(),
            },
        )

