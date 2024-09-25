"""
Workflow for parsing ARCADE simulations into tidy data.

Working location structure:

.. code-block:: bash

    (name)
    ├── data
    │   ├── data.CELLS
    │   │   └── (name)_(key)_(seed).CELLS.tar.xz
    │   └── data.LOCATIONS
    │       └── (name)_(key)_(seed).LOCATIONS.tar.xz
    └── results
        └── (name)_(key)_(seed).csv

Data from **data.CELLS** and **data.LOCATIONS** are parsed into **results**. If
the results file already exists, additional parsing will merge results based on
cell id and tick.
"""

from dataclasses import dataclass, field

import pandas as pd
from arcade_collection.output import parse_cells_file, parse_locations_file
from container_collection.manifest import filter_manifest_files
from io_collection.keys import check_key, make_key
from io_collection.load import load_dataframe, load_tar
from io_collection.save import save_dataframe
from prefect import flow


@dataclass
class ParametersConfig:
    """Parameter configuration for parse arcade simulations flow."""

    regions: list[str] = field(default_factory=list)
    """List of subcellular regions to parse."""

    include_filters: list[str] = field(default_factory=lambda: ["*"])
    """List of Unix filename patterns for files to include in parsing."""

    exclude_filters: list[str] = field(default_factory=list)
    """List of Unix filename patterns for files to exclude from parsing."""


@dataclass
class ContextConfig:
    """Context configuration for parse arcade simulations flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""

    manifest_location: str
    """Location of manifest file (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for parse arcade simulations flow."""

    name: str
    """Name of the simulation series."""

    manifest_key: str
    """Key for manifest file."""

    extensions: list[str]
    """List of file extensions in complete run."""


@flow(name="parse-arcade-simulations")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main parse arcade simulations flow."""

    manifest = load_dataframe(context.manifest_location, series.manifest_key)
    filtered_files = filter_manifest_files(
        manifest, series.extensions, parameters.include_filters, parameters.exclude_filters
    )

    for key, files in filtered_files.items():
        results_key = make_key(series.name, "{{timestamp}}", "results", f"{key}.csv")

        if check_key(context.working_location, results_key):
            continue

        cells_tar = load_tar(**files["CELLS.tar.xz"])
        cells = parse_cells_file(cells_tar, parameters.regions)
        cells = cells.set_index(["ID", "TICK"])

        locs_tar = load_tar(**files["LOCATIONS.tar.xz"])
        locs = parse_locations_file(locs_tar, parameters.regions)
        locs = locs.set_index(["ID", "TICK"])

        results = pd.concat([cells, locs], axis=1).reset_index()
        save_dataframe(context.working_location, results_key, results, index=False)
