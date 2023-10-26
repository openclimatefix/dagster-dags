import dagster


class CAMSConfig(dagster.Config):
    date: str
    raw_dir: str
