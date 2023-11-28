import dagster


class CAMSConfig(dagster.PermissiveConfig):
    date: str
    raw_dir: str
