
from dagster import Output, asset
from ecmwfapi import ECMWFService

server = ECMWFService("mars")

@asset
def download_mars_file():
    server.execute(
        req={
            "class": "od",
            "date": "20230815/to/20230816",
            "expver": "1",
            "levtype": "sfc",
            "param": "28.228/49.128/123.128/165.128/166.128/239.228/246.228/247.228",
            "step": "0/t0/48/by/1",
            "stream": "oper",
            "time": "00:00:00,12:00:00",
            "type": "fc",
        },
        target="20230815.grib"
    )

    return Output(None, metadata={"filepath": "20230815.grib"})
