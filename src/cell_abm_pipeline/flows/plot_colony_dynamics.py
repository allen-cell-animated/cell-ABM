"""
Workflow for plotting colony dynamics.

Working location structure:

.. code-block:: bash

    (name)
    ├── groups
    │   └── groups.COLONIES
    │       ├── (name).feature_distributions.(feature).json
    │       ├── (name).feature_temporal.(key).(feature).json
    │       ├── (name).neighbor_positions.(key).(seed).(tick).csv
    │       └── (name).neighbor_positions.(key).(seed).(tick).(feature).csv
    └── plots
        └── plots.COLONIES
            ├── (name).feature_distributions.(feature).png
            ├── (name).feature_temporal.(key).(feature).json
            └── (name).neighbor_positions.(key).(seed).(tick).(feature).png

Plots use grouped data from **groups.COLONIES**. Plots are saved to
**plots.COLONIES**.
"""

from dataclasses import dataclass, field

from io_collection.keys import make_key
from io_collection.load import load_dataframe, load_json
from io_collection.save import save_figure
from prefect import flow

from cell_abm_pipeline.flows.group_colony_dynamics import (
    DISTRIBUTION_FEATURES,
    POSITION_FEATURES,
    TEMPORAL_FEATURES,
)
from cell_abm_pipeline.tasks import make_graph_figure, make_histogram_figure, make_range_figure

PLOTS: list[str] = [
    "feature_distributions",
    "feature_temporal",
    "neighbor_positions",
]


FEATURE_COLORMAPS = {"depth": "magma_r", "group": "tab10"}


@dataclass
class ParametersConfigFeatureDistributions:
    """Parameter configuration for plot colony dynamics subflow - feature distributions."""

    features: list[str] = field(default_factory=lambda: DISTRIBUTION_FEATURES)
    """List of colony features."""


@dataclass
class ParametersConfigFeatureTemporal:
    """Parameter configuration for plot colony dynamics subflow - feature temporal."""

    features: list[str] = field(default_factory=lambda: TEMPORAL_FEATURES)
    """List of temporal features."""


@dataclass
class ParametersConfigNeighborPositions:
    """Parameter configuration for plot colony dynamics subflow - neighbor positions."""

    features: list[str] = field(default_factory=lambda: POSITION_FEATURES)
    """List of position features."""

    seed: int = 0
    """Simulation seed to use for plotting neighbor positions."""

    ticks: list[int] = field(default_factory=lambda: [0])
    """Simulation ticks to use for plotting neighbor positions."""

    colormaps: dict[str, str] = field(default_factory=lambda: FEATURE_COLORMAPS)
    """Colormaps for each feature."""


@dataclass
class ParametersConfig:
    """Parameter configuration for plot colony dynamics flow."""

    plots: list[str] = field(default_factory=lambda: PLOTS)
    """List of colony dynamics plots."""

    feature_distributions: ParametersConfigFeatureDistributions = field(default_factory=lambda: ParametersConfigFeatureDistributions)
    """Parameters for plot feature distributions subflow."""

    feature_temporal: ParametersConfigFeatureTemporal = field(default_factory=lambda: ParametersConfigFeatureTemporal)
    """Parameters for plot feature temporal subflow."""

    neighbor_positions: ParametersConfigNeighborPositions = field(default_factory=lambda: ParametersConfigNeighborPositions)
    """Parameters for plot neighbor positions subflow."""


@dataclass
class ContextConfig:
    """Context configuration for plot colony dynamics flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for plot colony dynamics flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="plot-colony-dynamics")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main plot colony dynamics flow.

    Calls the following subflows, if the plot is specified:

    - :py:func:`run_flow_plot_feature_distributions`
    - :py:func:`run_flow_plot_feature_temporal`
    - :py:func:`run_flow_plot_neighbor_positions`
    """

    if "feature_distributions" in parameters.plots:
        run_flow_plot_feature_distributions(context, series, parameters.feature_distributions)

    if "feature_temporal" in parameters.plots:
        run_flow_plot_feature_temporal(context, series, parameters.feature_temporal)

    if "neighbor_positions" in parameters.plots:
        run_flow_plot_neighbor_positions(context, series, parameters.neighbor_positions)


@flow(name="plot-colony-dynamics_plot-feature-distributions")
def run_flow_plot_feature_distributions(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureDistributions
) -> None:
    """Plot colony dynamics subflow for feature distributions."""

    group_key = make_key(series.name, "groups", "groups.COLONIES")
    plot_key = make_key(series.name, "plots", "plots.COLONIES")
    keys = [condition["key"] for condition in series.conditions]

    for feature in parameters.features:
        feature_key = feature.upper()

        group = load_json(
            context.working_location,
            make_key(group_key, f"{series.name}.feature_distributions.{feature_key}.json"),
        )

        assert isinstance(group, dict)

        save_figure(
            context.working_location,
            make_key(plot_key, f"{series.name}.feature_distributions.{feature_key}.png"),
            make_histogram_figure(keys, group),
        )


@flow(name="plot-colony-dynamics_plot-feature-temporal")
def run_flow_plot_feature_temporal(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureTemporal
) -> None:
    """Plot colony dynamics subflow for temporal features."""

    group_key = make_key(series.name, "groups", "groups.COLONIES")
    plot_key = make_key(series.name, "plots", "plots.COLONIES")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for feature in parameters.features:
            feature_key = f"{key}.{feature.upper()}"

            group = load_json(
                context.working_location,
                make_key(group_key, f"{series.name}.feature_temporal.{feature_key}.json"),
            )

            assert isinstance(group, dict)

            save_figure(
                context.working_location,
                make_key(plot_key, f"{series.name}.feature_temporal.{feature_key}.png"),
                make_range_figure(group),
            )


@flow(name="plot-colony-dynamics_plot-neighbor-positions")
def run_flow_plot_neighbor_positions(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigNeighborPositions
) -> None:
    """Plot colony dynamics subflow for neighbor positions."""

    group_key = make_key(series.name, "groups", "groups.COLONIES")
    plot_key = make_key(series.name, "plots", "plots.COLONIES")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for tick in parameters.ticks:
            for feature in parameters.features:
                edge_key = f"{key}.{parameters.seed:04d}.{tick:06d}"
                node_key = f"{key}.{parameters.seed:04d}.{tick:06d}.{feature.upper()}"

                edge_group = load_dataframe(
                    context.working_location,
                    make_key(group_key, f"{series.name}.neighbor_positions.{edge_key}.csv"),
                )

                node_group = load_dataframe(
                    context.working_location,
                    make_key(group_key, f"{series.name}.neighbor_positions.{node_key}.csv"),
                )

                save_figure(
                    context.working_location,
                    make_key(plot_key, f"{series.name}.neighbor_positions.{node_key}.png"),
                    make_graph_figure(node_group, edge_group, parameters.colormaps[feature]),
                )
