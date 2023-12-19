<h2 align="center">
Dagster Dags
<br>
<br>
Dagster defintions for OCF's archival datasets
</h2>

<div align="center">

<a href="https://github.com/openclimatefix/dagster-dags/graphs/contributors" alt="Contributors">
    <img src="https://img.shields.io/github/contributors/openclimatefix/dagster-dags?style=for-the-badge&color=FFFFFF" /></a>
<a href="https://github.com/openclimatefix/dagster-dags/actions/workflows/ci.yml" alt="Workflows">
    <img alt="GitHub Workflow Status (with branch)" src="https://img.shields.io/github/actions/workflow/status/openclimatefix/dagster-dags/ci.yml?branch=main&style=for-the-badge&color=FFD053"></a>
<a href="https://github.com/openclimatefix/dagster-dags/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc" alt="Issues">
    <img src="https://img.shields.io/github/issues/openclimatefix/dagster-dags?style=for-the-badge&color=FFAC5F"></a>
</div>

<br>


## Ubiquitous language

The following terms are used throughout the codebase and documentation. They are defined here to avoid ambiguity.

 - *InitTime* - The time at which a forecast is initialised. For example, a forecast initialised at 12:00 on 1st January.
 - *TargetTime* - The time at which a predicted value is valid. For example, a forecast with InitTime 12:00 on 1st January predicts that the temperature at TargetTime 12:00 on 2nd January at position x will be 10 degrees.


## Repository structure

Produced by `eza`:
```sh
eza --tree --git-ignore -F -I "*init*|test*.*|build"
```

```sh
./
├── cloud_archives/ # Dagster definitions for cloud-stored archival datasets
│  └── nwp/ # Specifications for Numerical Weather Predication data sources
│     └── icon/ 
├── constants.py # Values used across the project
├── dags_tests/ # Tests for the project
├── local_archives/ # Dagster defintions for locally-stored archival datasets
│  ├── nwp/ # Specifications for Numerical Weather Prediction data source
│  │  ├── cams/
│  │  └── ecmwf/
│  └── sat/ # Specifications for Satellite image data sources
├── managers/ # IO Managers for use across the project
├── pyproject.toml # The build configuration for the service
└── README.md
```

## Conventions

The storage of data is handled automatically into locations defined by features of the data in question. The only configurable
part of the storage is the *Base Path* - the root point from which dagster will then handle the subpaths. The full storage paths
then take into account the following features:
 - The *flavor* of the data (NWP, Satellite etc)
 - The *Provider* of the data (CEDA, ECMWF etc)
 - The *Region* the data covers (UK, EU etc)
 - The *InitTime* the data refers to

Paths are then generated via`base/flavour/provider/region/inittime`. See managers for an example implementation.
For this to work, each asset must have an asset key prefix conforming to this structure `[flavor, provider, region]`.
The *Base Paths* are defined in `constants.py`.


## Local Development

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev --module-name=local_archives
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the project.

Add your assets to the relevant code location. See [Repository Structure](#repository-structure) for details.


## Useful links

- (Detecting existing assets)[https://github.com/dagster-io/dagster/discussions/17847]

