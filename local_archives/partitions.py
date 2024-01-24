import datetime as dt

import dagster as dg

# TODO: Maybe move this out to the top level?


class InitTimePartitionsDefinition(dg.MultiPartitionsDefinition):
    """Defines a multi-dimensional partition set based on init times.

    The first dimension of the partition set is the date part of the init
    time, and the second is the hour/minute part of the init time.
    The partition key therefore is of type MultiPartitionsKey, who's
    `keys_by_dimension` property is a dict with keys `date` and `inittime`.
    """

    def __init__(
        self,
        *,
        start: str,
        end: str | None = None,
        init_times: list[str],
        end_offset: int = 0,
    ) -> None:
        """Create a multipartitions definition for the given init times.

        Args:
            start: The start date of the partition set. Must be of the form
                YYYY-MM-DD.
            end: The end date of the partition set. Must be of the form
                YYYY-MM-DD, if included. Defaults to None.
            end_offset: The offset from the end date to pass to the daily partitions defintition
            init_times: A list of init times, each of the form HH:MM.
        """
        # Attepmt to parse the start date, raise an error if it is invalid
        try:
            self._start = dt.datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=dt.UTC)
        except ValueError as e:
            raise ValueError(f"Invalid start date: {start}") from e

        # Attempt to parse the end date, raise an error if it is invalid
        self._end = None
        if end is not None:
            try:
                self._end = dt.datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=dt.UTC)
                if self._end < self._start:
                    raise ValueError("End date must be after start date")
            except ValueError as e:
                raise ValueError(f"Invalid end date: {end}") from e

        # Ensure the inittimes are correctly formatted
        for t in init_times:
            try:
                dt.datetime.strptime(t, "%H:%M").replace(tzinfo=dt.UTC)
            except ValueError as e:
                raise ValueError(f"Invalid init time: {t}") from e
        self._init_times = init_times

        return super().__init__(
            {
                "date": dg.DailyPartitionsDefinition(
                    start_date=self._start, end_date=self._end, end_offset=end_offset
                ),
                "inittime": dg.StaticPartitionsDefinition(self._init_times),
            },
        )

    def parse_key(self, *, key: str) -> dt.datetime:
        """Parse a partition key into a datetime object."""
        if not isinstance(key, dg.MultiPartitionKey):
            raise TypeError(f"Key {key} is not a MultiPartitionKey")
        try:
            key_dict = key.keys_by_dimension
            it: dt.datetime = dt.datetime.strptime(
                f"{key_dict['date']}|{key_dict['inittime']}",
                "%Y-%m-%d|%H:%M",
            ).replace(tzinfo=dt.UTC)
        except ValueError as e:
            raise ValueError(f"Invalid key: {key}") from e

        return it
