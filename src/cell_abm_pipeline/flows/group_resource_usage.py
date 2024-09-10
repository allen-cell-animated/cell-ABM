"""
Workflow for grouping resource usage.

Working location structure:

.. code-block:: bash

    (name)
    ├── data
    │   └── data.(category)
    │       └── (name)_(key)_(seed).(category).tar.xz
    ├── groups
    │   └── groups.RESOURCE_USAGE
    │       ├── (name).object_storage.csv
    │       └── (name).wall_clock.csv
    └── logs
        └── (job_id).log

Different groups use inputs from **data** and **logs**. Grouped data are saved
to **groups.RESOURCE_USAGE**.
"""

import os
import re
from dataclasses import dataclass, field

import boto3
import pandas as pd
from io_collection.keys import get_keys, make_key
from io_collection.load import load_text
from io_collection.save import save_dataframe
from prefect import flow

GROUPS: list[str] = [
    "object_storage",
    "wall_clock",
]

OBJECT_CATEGORIES = ["CELLS", "LOCATIONS"]

OBJECT_PATTERN = r"[_]*([A-z0-9\s\_]*)_([0-9]{4})\."

LOG_PATTERN = r"simulation \[ ([A-z0-9\s\_]+) \| ([0-9]{4}) \] finished in ([0-9\.]+) minutes"


@dataclass
class ParametersConfigObjectStorage:
    """Parameter configuration for group resource usage subflow - object storage."""

    search_locations: list[str] = field(default_factory=lambda: [])
    """List of locations (local paths or S3 buckets) to search for files."""

    categories: list[str] = field(default_factory=lambda: OBJECT_CATEGORIES)
    """List of object storage categories."""

    pattern: str = OBJECT_PATTERN
    """Pattern to match for object key and seed."""


@dataclass
class ParametersConfigWallClock:
    """Parameter configuration for group resource usage subflow - wall clock."""

    search_locations: list[str] = field(default_factory=lambda: [])
    """List of locations (local paths or S3 buckets) to search for files."""

    pattern: str = LOG_PATTERN
    """Pattern to match for object key, seed, and time."""

    exceptions: list[str] = field(default_factory=lambda: [])
    """List of exception strings used to filter log files."""


@dataclass
class ParametersConfig:
    """Parameter configuration for group resource usage flow."""

    groups: list[str] = field(default_factory=lambda: GROUPS)
    """List of resource usages groups."""

    object_storage: ParametersConfigObjectStorage = field(default_factory=lambda: ParametersConfigObjectStorage)
    """Parameters for group object storage subflow."""

    wall_clock: ParametersConfigWallClock = field(default_factory=lambda: ParametersConfigWallClock)
    """Parameters for group wall clock subflow."""


@dataclass
class ContextConfig:
    """Context configuration for group resource usage flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for group resource usage flow."""

    name: str
    """Name of the simulation series."""


@flow(name="group-resource-usage")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main group resource usage flow.

    Calls the following subflows, if the group is specified:

    - :py:func:`run_flow_group_object_storage`
    - :py:func:`run_flow_group_wall_clock`
    """

    if "object_storage" in parameters.groups:
        run_flow_group_object_storage(context, series, parameters.object_storage)

    if "wall_clock" in parameters.groups:
        run_flow_group_wall_clock(context, series, parameters.wall_clock)


@flow(name="group-resource-usage_group-object-storage")
def run_flow_group_object_storage(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigObjectStorage
) -> None:
    """Group resource usage subflow for object storage size."""

    group_key = make_key(series.name, "groups", "groups.RESOURCE_USAGE")

    all_sizes = []

    for category in parameters.categories:
        for location in parameters.search_locations:
            file_keys = get_keys(location, make_key(series.name, "data", f"data.{category}"))

            for file_key in file_keys:
                key, seed = re.findall(parameters.pattern, file_key.split(series.name)[-1])[0]

                if location.startswith("s3://"):
                    summary = boto3.resource("s3").ObjectSummary(location[5:], file_key)
                    size = summary.size
                else:
                    size = os.path.getsize(f"{location}{file_key}")

                all_sizes.append({"key": key, "seed": seed, "category": category, "size": size})

    sizes_df = pd.DataFrame(all_sizes)
    sizes_df.sort_values(by=["key", "category", "seed"], ignore_index=True, inplace=True)

    save_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.object_storage.csv"),
        sizes_df,
        index=False,
    )


@flow(name="group-resource-usage_group-wall-clock")
def run_flow_group_wall_clock(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigWallClock
) -> None:
    """Group resource usage subflow for wall clock time."""

    group_key = make_key(series.name, "groups", "groups.RESOURCE_USAGE")

    all_times = []

    for location in parameters.search_locations:
        file_keys = get_keys(location, make_key(series.name, "logs"))

        for file_key in file_keys:
            contents = load_text(location, file_key)

            if any(exception in contents for exception in parameters.exceptions):
                continue

            matches = re.findall(parameters.pattern, contents)

            for key, seed, time in matches:
                all_times.append({"key": key, "seed": seed, "time": time})

    times_df = pd.DataFrame(all_times)
    times_df.sort_values(by=["key", "seed"], ignore_index=True, inplace=True)

    save_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.wall_clock.csv"),
        times_df,
        index=False,
    )
