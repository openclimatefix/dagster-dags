import datetime as dt
import xarray as xr
import urllib.request
import urllib.parse
import requests
import pathlib
from typing import TypedDict

class NSDict(TypedDict):
    north: list[str]
    south: list[str]

username = os.environ["CEDA_FTP_USER"]
password = os.environ["CEDA_FTP_PASSWORD"]
base_url = "ftp://".join(
    f"{urllib.parse.quote(username)}:{urllib.parse.quote(password)}",
    "@ftp.ceda.ac.uk/badc/ukmo-nwp/data/global-grib",
)

parameter_filestrings = [
    "Total_Downward_Surface_SW_Flux",
    "high_cloud_amaount",
    "low_cloud_amount",
    "medium_cloud_amount",
    "relative_humidity_1_5m",
    "snow_depth",
    "temperature_1_5m",
    "total_cloud",
    "total_precipitation_rate",
    "visibility",
    "wind_u_10m",
    "wind_v_10m",
]

def create_parameter_dataset(parameter: str, path: pathlib.Path) -> xr.Dataset:
    """Creates a dataset for a given parameter.

    MetOffice global model data is stored on CEDA in segments:
        - 4 areas for the northern hemisphere
        - 4 areas for the southern hemisphere

    This function downloads the data for each area and concatenates it into a single dataset.

    See also:
        - https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/data/global-atmospheric-model-17-km-resolution.pdf
        - https://catalogue.ceda.ac.uk/uuid/86df725b793b4b4cb0ca0646686bd783
    """
    written_paths = NSDict(north=[], south=[]}
    areas = NSDict(
        north=["AreaA", "AreaB", "AreaC", "AreaD"],
        south=["AreaE", "AreaF", "AreaG", "AreaH"],
    )
    for area in areas["north"] + areas["south"]:
        saved_path = pathlib.Path(f"{path.as_posix()}/{parameter}_{it:%Y%m%d%H%M}_{area}_144.grib")
        # Don't download the data if it already exists
        if not saved_path.exists():
            url = f"{base_url}/{it:%Y/%m/%d}/" + \
                  f"{it:%Y%m%d%H}_WSGlobal17km_{parameter}_{area}_000144.grib"
            response = urllib.request.urlopen(url)
            with saved_path.open("wb") as f:
                for chunk in iter(lambda: response.read(16 * 1024), b""):
                    f.write(chunk)
                    f.flush()
        if area in areas["north"]:
            written_paths["north"].append(saved_path.as_posix())
        else:
            written_paths["south"].append(saved_path.as_posix())

    # Separately combine the north and south areas
    northern_ds = xr.concat([xr.open_dataset(p, engine="cfgrib") for p in written_paths["north"]], dim="longitude")
    southern_ds = xr.concat([xr.open_dataset(p, engine="cfgrib") for p in written_paths["south"]], dim="longitude")
    # Then combine the northern and southern combinations
    ds = xr.concat([northern_ds, southern_ds], dim="latitude")
    del northern_ds, southern_ds

    # Some postprocessing on the dataset
    ds = (
        ds.expand_dims(dim={"init_time": [np.datetime64(it.replace(tzinfo=None), "ns")]})
            .drop_vars(["valid_time", "time", "surface"], errors="ignore")
            .rename({"swavr": "downward_shortwave_radiation_flux"})
    )

    return ds

def download(it: dt.datetime, path: pathlib.Path = pathlib.Path("/tmp/mo-global"):
    """Downloads the MetOffice global model data for a given initialisation time."""
    datasets: list[xr.Dataset] = []
    for parameter in parameter_filestrings:
        datasets.append(create_parameter_dataset(parameter))

    # Combine all the datasets into a single dataset
    ds = xr.merge(datasets)
    del datasets
    ds.to_zarr(f"{path.as_posix()}/{it:%Y%m%dT%H%M}.zarr", mode="w")

if __name__ == "__main__":
    download(dt.datetime(2021, 1, 1, 0))
