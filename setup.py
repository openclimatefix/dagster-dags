from setuptools import find_packages, setup

setup(
    name="dags",
    packages=find_packages(exclude=["*_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "xarray",
        "huggingface_hub",
        "zarr",
        "ocf_blosc2",
        "requests"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
