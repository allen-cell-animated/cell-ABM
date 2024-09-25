"""
Workflow for parsing PhysiCell simulations into tidy data.

Working location structure:

.. code-block:: bash

    (name)
    ├── data
    │   └── (name)_(key)_(seed).tar.xz
    └── results
        └── (name)_(key)_(seed).csv

Data from **data** are parsed into **results**.
"""

from dataclasses import dataclass, field

from container_collection.manifest import filter_manifest_files
from io_collection.keys import make_key
from io_collection.load import load_dataframe, load_tar
from io_collection.save import save_dataframe
from prefect import flow

from cell_abm_pipeline.tasks.physicell import parse_mcds_file


@dataclass
class ParametersConfig:
    """Parameter configuration for parse physicell simulations flow."""

    include_filters: list[str] = field(default_factory=lambda: ["*"])
    """List of Unix filename patterns for files to include in parsing."""

    exclude_filters: list[str] = field(default_factory=list)
    """List of Unix filename patterns for files to exclude from parsing."""


@dataclass
class ContextConfig:
    """Context configuration for parse physicell simulations flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""

    manifest_location: str
    """Location of manifest file (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for parse physicell simulations flow."""

    name: str
    """Name of the simulation series."""

    manifest_key: str
    """Key for manifest file."""

    extensions: list[str]
    """List of file extensions in complete run."""


@flow(name="parse-physicell-simulations")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main parse physicell simulations flow."""

    manifest = load_dataframe(context.manifest_location, series.manifest_key)
    filtered_files = filter_manifest_files(
        manifest, series.extensions, parameters.include_filters, parameters.exclude_filters
    )

    for key, files in filtered_files.items():
        tar_file = load_tar(**files["tar.xz"])
        results = parse_mcds_file(tar_file)

        results_key = make_key(series.name, "{{timestamp}}", "results", f"{key}.csv")
        save_dataframe(context.working_location, results_key, results, index=False)
