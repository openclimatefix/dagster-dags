# Dagster Dags

**Orchestrate data pipelines for ML dataset creation**

[![tags badge](https://img.shields.io/github/v/tag/openclimatefix/dagster-dags?include_prereleases&sort=semver&color=7BCDF3)](https://github.com/openclimatefix/dagster-dags/tags)
[![contributors badge](https://img.shields.io/github/contributors/openclimatefix/dagster-dags?color=FFFFFF)](https://github.com/openclimatefix/dagster-dags/graphs/contributors)
[![workflows badge](https://img.shields.io/github/actions/workflow/status/openclimatefix/dagster-dags/branch_ci.yml?branch=main&color=FFD053)](https://github.com/openclimatefix/dagster-dags/actions/workflows/branch_ci.yml)
[![ease of contribution: easy](https://img.shields.io/badge/ease%20of%20contribution:%20easy-32bd50)](https://github.com/openclimatefix/ocf-meta-repo?tab=readme-ov-file#overview-of-ocfs-nowcasting-repositories)

In order to train and evaluate an ML model, datasets must be created consistently and reproducibly.

Forecasting renewable energy generation depends on large-timescale weather data:
Numerical Weather Prediction (NWP) data; satellite imagery;
atmospheric quality data. Dagster helps to these datasets organised and up to date.

This repository contains the Dagster definitions that orchestrate the creation of these datasets.

## Installation

The repository is packaged as a Docker image that can be used as a Dagster
[code server](https://docs.dagster.io/concepts/code-locations/workspace-files#running-your-own-grpc-server)

```bash
$ docker pull ghcr.io/openclimatefix/dagster-dags
```

## Example Usage

**To add as a code location in an existing Dagster setup:**

```bash
$ docker run -d \
    -p 4000:4000 \
    -e DAGSTER_CURRENT_IMAGE=ghcr.io/openclimatefix/dagster-dags \
    ghcr.io/openclimatefix/dagster-dags
```

```yaml
# $DAGSTER_HOME/workspace.yaml

load_from:
  - grpc_server:
      host: localhost
      port: 4000
      location_name: "dagster-dags" # Name of the module
```

> [!Note]
> Setting `DAGSTER_CURRENT_IMAGE` environment variable is necessary to tell Dagster
> to spawn jobs using the set container image. Since the Containerfile has all the
> required dependencies for the user code, it makes sense to set it to itself.

**To deploy the entire Dagster multi-container stack:**

```bash
$ docker compose up -f infrastructure/docker-compose.yml
```

> [!Note]
> This will start a full Dagster setup with a web UI, a Postgres database,
> and a QueuedRunCoordinator. This might be overkill for some setups.

## Documentation

The repository is split into folders covering the basic concepts of Dagster:

- Top-level [Definitions](https://docs.dagster.io/concepts/code-locations) defining the code location are defined in `src/dagster_dags/definitions.py`
- [Assets](https://docs.dagster.io/concepts/assets/software-defined-assets) are in `src/dagster_dags/assets`
- [Resources](https://docs.dagster.io/concepts/resources#resources) are in `src/dagster_dags/resources`

They are then subdivided by module into data-type-specific folders.

## Development

To run a development Dagster server, install the required dependencies in a virtual environment,
activate it, and run the server:

```bash
$ cd scr && dagster dev --module-name=dagster_dags
```

This should spawn a UI at `localhost:3000` where you can interact with the Dagster webserver.

### Linting and static type checking

This project uses [MyPy](https://mypy.readthedocs.io/en/stable/) for static type checking
and [Ruff](https://docs.astral.sh/ruff/) for linting.
Installing the development dependencies makes them available in your virtual environment.

Use them via:

```bash
$ python -m mypy .
$ python -m ruff check .
```

Be sure to do this periodically while developing to catch any errors early
and prevent headaches with the CI pipeline. It may seem like a hassle at first,
but it prevents accidental creation of a whole suite of bugs.

### Running the test suite

Run the unittests with:

```bash
$ python -m unittest discover -s tests
```

## Further Reading

On running your own GRPC code server as a code location in Dagster:
- Dagster guide on [running a GRPC server](https://docs.dagster.io/concepts/code-locations/workspace-files#running-your-own-grpc-server).
- Creating a GRPC code server container as part of a [multi-container Dagster stack](https://docs.dagster.io/deployment/guides/docker#multi-container-docker-deployment).


---

## Contributing and community

[![issues badge](https://img.shields.io/github/issues/openclimatefix/dagster-dags?color=FFAC5F)](https://github.com/openclimatefixdagster-dags/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc)

- PR's are welcome! See the [Organisation Profile](https://github.com/openclimatefix) for details on contributing
- Find out about our other projects in the [OCF Meta Repo](https://github.com/openclimatefix/ocf-meta-repo)
- Check out the [OCF blog](https://openclimatefix.org/blog) for updates
- Follow OCF on [LinkedIn](https://uk.linkedin.com/company/open-climate-fix)

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

---

*Part of the [Open Climate Fix](https://github.com/orgs/openclimatefix/people) community.*

[![OCF Logo](https://cdn.prod.website-files.com/62d92550f6774db58d441cca/6324a2038936ecda71599a8b_OCF_Logo_black_trans.png)](https://openclimatefix.org)

