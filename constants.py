"""Defines constant values for the nwp deployment."""
import dataclasses as dc

@dc.dataclass
class StorageLocations:
    RAW_FOLDER: str
    PROCESSED_FOLDER: str
    EPHEMERAL_FOLDER: str

# Defines the storage locations for each environment
LOCATIONS_BY_ENVIRONMENT: dict[str, StorageLocations] = {
    "leo": StorageLocations(
        RAW_FOLDER="/mnt/storage_c/raw",
        PROCESSED_FOLDER="/mnt/storage_b",
        EPHEMERAL_FOLDER="/mnt/storage_c/ephemeral",
    ),
    "local": StorageLocations(
        RAW_FOLDER="/tmp/raw",
        PROCESSED_FOLDER="/tmp/processed",
        EPHEMERAL_FOLDER="/tmp/ephemeral",
    ),
}
