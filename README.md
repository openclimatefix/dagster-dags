# dags

Dagster project containing code for updating, managing, and viewing OCF's server-stored datasets.

## Ubiquitous language

The following terms are used throughout the codebase and documentation. They are defined here to avoid ambiguity.

 - *InitTime* - The time at which a forecast is initialised. For example, a forecast initialised at 12:00 on 1st January.
 - *TargetTime* - The time at which a predicted value is valid. For example, a forecast with InitTime 12:00 on 1st January predicts that the temperature at TargetTime 12:00 on 2nd January at position x will be 10 degrees.

## Conventions

The storage of data is handled automatically into locations defined by features of the data in question. The only configurable
part of the storage is the *Base Path* - the root point from which dagster will then handle the subpaths. These paths are generated
via the following features:
 - The *flavour* of the data (NWP, Satellite etc)
 - The *Provider* of the data (CEDA, ECMWF etc)
 - The *Region* the data covers (UK, EU etc)
 - The *InitTime* the data refers to

Paths are then generated via`base/flavour/provider/region/inittime`. See managers for an example implementation.

For this to work, each asset must have an asset key prefix conforming to this structure `[flavour, provider, region]`.

## Local Development


First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `dags/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Useful links

- (Detecting existing assets)[https://github.com/dagster-io/dagster/discussions/17847]

