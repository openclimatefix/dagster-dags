from dagster import Definitions, load_assets_from_modules 
from nwp import defs

def test_compiles():
    job_names = [d.name for d in list(defs.get_all_job_defs())]
    assert "get_ecmwf_data" in job_names
    assert len(job_names) == 18
