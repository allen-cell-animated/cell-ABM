"""
Workflow for plotting resource usage.

Working location structure:

.. code-block:: bash

    (name)
    ├── groups
    │   └── groups.RESOURCE_USAGE
    │       ├── (name).object_storage.csv
    │       └── (name).wall_clock.csv
    └── plots
        └── plots.RESOURCE_USAGE
            ├── (name).object_storage.(category).png
            └── (name).wall_clock.png

Plots use grouped data from **groups.RESOURCE_USAGE**. Plots are saved to
**plots.RESOURCE_USAGE**.
"""

from dataclasses import dataclass, field

from io_collection.keys import make_key
from io_collection.load import load_dataframe
from io_collection.save import save_figure
from prefect import flow

from cell_abm_pipeline.flows.group_resource_usage import OBJECT_CATEGORIES
from cell_abm_pipeline.tasks import make_box_figure

PLOTS: list[str] = [
    "object_storage",
    "wall_clock",
]


@dataclass
class ParametersConfigObjectStorage:
    """Parameter configuration for plot resouce usage subflow - object storage."""

    categories: list[str] = field(default_factory=lambda: OBJECT_CATEGORIES)
    """List of object storage categories."""


@dataclass
class ParametersConfigWallClock:
    """Parameter configuration for plot resouce usage subflow - object storage."""


@dataclass
class ParametersConfig:
    """Parameter configuration for plot resource usage flow."""

    plots: list[str] = field(default_factory=lambda: PLOTS)
    """List of resource usage plots."""

    object_storage: ParametersConfigObjectStorage = field(
        default_factory=lambda: ParametersConfigObjectStorage
    )
    """Parameters for plot object storage subflow."""

    wall_clock: ParametersConfigWallClock = field(default_factory=lambda: ParametersConfigWallClock)
    """Parameters for plot wall clock subflow."""


@dataclass
class ContextConfig:
    """Context configuration for plot resource usage flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for plot resource usage flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="plot-resource-usage")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main plot resource usage flow.

    Calls the following subflows, if the plot is specified:

    - :py:func:`run_flow_plot_object_storage`
    - :py:func:`run_flow_plot_wall_clock`
    """

    if "object_storage" in parameters.plots:
        run_flow_plot_object_storage(context, series, parameters.object_storage)

    if "wall_clock" in parameters.plots:
        run_flow_plot_wall_clock(context, series, parameters.wall_clock)


@flow(name="plot-resource-usage_plot-object-storage")
def run_flow_plot_object_storage(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigObjectStorage
) -> None:
    """Plot resource usage subflow for object storage."""

    group_key = make_key(series.name, "groups", "groups.RESOURCE_USAGE")
    plot_key = make_key(series.name, "plots", "plots.RESOURCE_USAGE")
    keys = [condition["key"] for condition in series.conditions]

    group = load_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.object_storage.csv"),
    )

    group["value"] = group["size"] / 1024**2

    for category in parameters.categories:
        category_group = group[group["category"] == category]

        save_figure(
            context.working_location,
            make_key(plot_key, f"{series.name}.object_storage.{category}.png"),
            make_box_figure(keys, category_group, ylabel="Object storage size (MiB)"),
        )


@flow(name="plot-resource-usage_plot-wall-clock")
def run_flow_plot_wall_clock(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigWallClock
) -> None:
    """Plot resource usage subflow for wall clock."""

    group_key = make_key(series.name, "groups", "groups.RESOURCE_USAGE")
    plot_key = make_key(series.name, "plots", "plots.RESOURCE_USAGE")
    keys = [condition["key"] for condition in series.conditions]

    group = load_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.wall_clock.csv"),
    )

    group["value"] = group["time"] / 60

    save_figure(
        context.working_location,
        make_key(plot_key, f"{series.name}.wall_clock.png"),
        make_box_figure(keys, group, ylabel="Wall clock time (hr)"),
    )
