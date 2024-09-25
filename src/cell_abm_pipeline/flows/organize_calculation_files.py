"""
Workflow for organizing calculation files.

Calculation files for each specified tick are merged into a single csv. The
individual tick calculation files are also compressed into a tar.xz archive.
After verifying that the file exists in the archive, the individual tick
calculation file is removed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd
from io_collection.keys import check_key, make_key, remove_key
from io_collection.load import load_dataframe, load_tar
from io_collection.save import save_dataframe, save_tar
from prefect import flow


@dataclass
class ParametersConfig:
    """Parameter configuration for organize calculation files flow."""

    suffix: str
    """Calculation type suffix."""

    ticks: list[int]
    """List of ticks to run flow on."""

    region: str | None = None
    """Subcellular region name."""


@dataclass
class ContextConfig:
    """Context configuration for organize calculation files flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for organize calculation files flow."""

    name: str
    """Name of the simulation series."""

    seeds: list[int]
    """List of series random seeds."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="organize-calculation-files")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main organize calculation files flow.

    Calls the following subflows, in order:

    1. :py:func:`run_flow_merge_files`
    2. :py:func:`run_flow_compress_files`
    3. :py:func:`run_flow_remove_files`
    """

    run_flow_merge_files(context, series, parameters)

    run_flow_compress_files(context, series, parameters)

    run_flow_remove_files(context, series, parameters)


@flow(name="organize-calculation-files_merge-files")
def run_flow_merge_files(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Organize calculation files subflow for merging files.

    Iterate through conditions and seeds to merge contents of individual ticks
    into a single csv. If merged csv exists and the specified tick does not
    exist in the csv, the tick is appended. If the merged csv exists and
    specified tick exists in the csv, the tick is skipped.
    """

    suffix = parameters.suffix
    calc_key = make_key(series.name, "calculations", f"calculations.{suffix}")
    region = f"_{parameters.region}" if parameters.region is not None else ""

    for condition in series.conditions:
        for seed in series.seeds:
            series_key = f"{series.name}_{condition['key']}_{seed:04d}"
            file_key = make_key(calc_key, f"{series_key}{region}.{suffix}.csv")
            file_key_exists = check_key(context.working_location, file_key)

            existing_ticks = []
            if file_key_exists:
                existing_contents = load_dataframe(context.working_location, file_key)
                existing_ticks = list(existing_contents["TICK"].unique())

            contents = []

            for tick in parameters.ticks:
                if tick in existing_ticks:
                    continue

                tick_key = make_key(calc_key, f"{series_key}_{tick:06d}{region}.{suffix}.csv")
                contents.append(load_dataframe(context.working_location, tick_key))

            if not contents:
                continue

            contents_dataframe = pd.concat(contents, ignore_index=True)

            if file_key_exists:
                contents_dataframe = pd.concat(
                    [existing_contents, contents_dataframe], ignore_index=True
                )

            save_dataframe(context.working_location, file_key, contents_dataframe, index=False)


@flow(name="organize-calculation-files_compress-files")
def run_flow_compress_files(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Organize calculation files subflow for compressing files.

    Iterate through conditions and seeds to combine and compress individual
    ticks into a .tar.xz archive. If the archive exists and the specified tick
    is not in the archive, the tick is appended. If the archive exists and
    specified tick exists in the archive, the tick is skipped.
    """

    suffix = parameters.suffix
    calc_key = make_key(series.name, "calculations", f"calculations.{suffix}")
    region = f"_{parameters.region}" if parameters.region is not None else ""

    for condition in series.conditions:
        for seed in series.seeds:
            series_key = f"{series.name}_{condition['key']}_{seed:04d}"
            file_key = make_key(calc_key, f"{series_key}{region}.{suffix}.tar.xz")
            file_key_exists = check_key(context.working_location, file_key)

            existing_ticks = []
            if file_key_exists:
                existing_contents = load_tar(context.working_location, file_key)
                existing_ticks = [
                    int(re.findall(r"[0-9]{6}", member.name)[0])
                    for member in existing_contents.getmembers()
                ]

            contents = []

            for tick in parameters.ticks:
                if tick in existing_ticks:
                    continue

                tick_key = make_key(calc_key, f"{series_key}_{tick:06d}{region}.{suffix}.csv")
                contents.append(tick_key)

            if not contents:
                continue

            save_tar(context.working_location, file_key, contents)


@flow(name="organize-calculation-files_remove-files")
def run_flow_remove_files(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Organize calculation files subflow for removing files.

    Iterate through conditions and seeds to remove individual ticks if the
    tick exists in the corresponding .tar.xz archive.
    """

    suffix = parameters.suffix
    calc_key = make_key(series.name, "calculations", f"calculations.{suffix}")
    region = f"_{parameters.region}" if parameters.region is not None else ""

    for condition in series.conditions:
        for seed in series.seeds:
            series_key = f"{series.name}_{condition['key']}_{seed:04d}"
            file_key = make_key(calc_key, f"{series_key}{region}.{suffix}.tar.xz")
            file_key_exists = check_key(context.working_location, file_key)

            if not file_key_exists:
                continue

            existing_contents = load_tar(context.working_location, file_key)

            for member in existing_contents.getmembers():
                tick_key = make_key(calc_key, member.name)

                if check_key(context.working_location, tick_key):
                    remove_key(context.working_location, tick_key)
