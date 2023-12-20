"""Defines constant values for the nwp deployment."""
import dataclasses as dc


@dc.dataclass
class StorageLocations:
    """Defines the storage locations for a given environment."""

    RAW_FOLDER: str
    NWP_ZARR_FOLDER: str
    STATIC_ZARR_FOLDER: str
    POINT_ZARR_FOLDER: str
    SAT_ZARR_FOLDER: str
    EPHEMERAL_FOLDER: str

# Defines the storage locations for each environment
LOCATIONS_BY_ENVIRONMENT: dict[str, StorageLocations] = {
    "leo": StorageLocations(
        RAW_FOLDER="/mnt/storage_c/raw",
        NWP_ZARR_FOLDER="/mnt/storage_b",
        STATIC_ZARR_FOLDER="/mnt/storage_a",
        POINT_ZARR_FOLDER="/mnt/storage_a",
        SAT_ZARR_FOLDER="/mnt/storage_a",
        EPHEMERAL_FOLDER="/mnt/storage_c/ephemeral",
    ),
    "local": StorageLocations(
        RAW_FOLDER="/tmp/raw",
        NWP_ZARR_FOLDER="/tmp/zarr",
        STATIC_ZARR_FOLDER="/tmp/zarr",
        POINT_ZARR_FOLDER="/tmp/zarr",
        SAT_ZARR_FOLDER="/tmp/zarr",
        EPHEMERAL_FOLDER="/tmp/ephemeral",
    ),
}
