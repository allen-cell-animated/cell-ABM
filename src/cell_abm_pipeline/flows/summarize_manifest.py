"""
Workflow for summarizing files in the manifest.

.. code-block:: bash

    (name)
    └── YYYY-MM-DD
        └── (name).SUMMARY.txt

For each search location, flow will attempt to find all files matching the
specified series name. After applying include and exclude filters, the manifest
is updated and a summary of files, grouped by extension, is printed and saved to
a dated directory.
"""

from dataclasses import dataclass, field
from fnmatch import fnmatch

from container_collection.manifest import summarize_manifest_files, update_manifest_contents
from io_collection.keys import get_keys, make_key
from io_collection.load import load_dataframe
from io_collection.save import save_dataframe, save_text
from prefect import flow, get_run_logger


@dataclass
class ParametersConfig:
    """Parameter configuration for summarize manifest flow."""

    update_manifest: bool = True
    """True if the manifest file should be updated, False otherwise."""

    search_locations: list[str] = field(default_factory=list)
    """List of locations to search for files (local path or S3 bucket)."""

    include_filters: list[str] = field(default_factory=lambda: ["*"])
    """List of Unix filename patterns for files to include in summary."""

    exclude_filters: list[str] = field(default_factory=list)
    """List of Unix filename patterns for files to exclude from summary."""


@dataclass
class ContextConfig:
    """Context configuration for summarize manifest flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""

    manifest_location: str
    """Location of manifest file (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for summarize manifest flow."""

    name: str
    """Name of the simulation series."""

    manifest_key: str
    """Key for manifest file."""

    seeds: list[int]
    """List of series random seeds."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="summarize-manifest")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main summarize manifest flow."""

    if parameters.update_manifest:
        location_keys = {}

        for location in parameters.search_locations:
            all_keys = get_keys(location, series.name)

            selected_keys = set()
            unselected_keys = set()

            # Filter files for matches to include filters.
            for include in parameters.include_filters:
                selected_keys.update([key for key in all_keys if fnmatch(key, include)])

            # Filter files for matches to exclude filters.
            for exclude in parameters.exclude_filters:
                unselected_keys.update([key for key in all_keys if fnmatch(key, exclude)])

            location_keys[location] = list(selected_keys - unselected_keys)

        manifest = update_manifest_contents(location_keys)
        save_dataframe(context.manifest_location, series.manifest_key, manifest, index=False)
    else:
        manifest = load_dataframe(context.manifest_location, series.manifest_key)

    summary = summarize_manifest_files(manifest, series.name, series.conditions, series.seeds)
    summary_key = make_key(series.name, "{{timestamp}}", f"{series.name}.SUMMARY.txt")
    save_text(context.working_location, summary_key, summary)

    logger = get_run_logger()
    logger.info("\n%s", summary)
