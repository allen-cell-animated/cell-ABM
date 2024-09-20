"""
Workflow for plotting basic metrics.

Working location structure:

.. code-block:: bash

    (name)
    ├── groups
    │   └── groups.BASIC_METRICS
    │       ├── (name).metrics_bins.(key).(seed).(tick).(metric).csv
    │       ├── (name).metrics_distributions.(metric).json
    │       ├── (name).metrics_individuals.(key).(seed).(metric).json
    │       ├── (name).metrics_spatial.(key).(seed).(tick).(metric).csv
    │       ├── (name).metrics_temporal.(key).(metric).json
    │       └── (name).population_counts.(tick).csv
    └── plots
        └── plots.BASIC_METRICS
            ├── (name).metrics_bins.(key).(seed).(tick).(metric).png
            ├── (name).metrics_distributions.(metric).png
            ├── (name).metrics_individuals.(key).(seed).(metric).png
            ├── (name).metrics_spatial.(key).(seed).(tick).(metric).png
            ├── (name).metrics_temporal.(key).(metric).png
            └── (name).population_counts.(tick).png

Plots use grouped data from **groups.BASIC_METRICS**. Plots are saved to
**plots.BASIC_METRICS**.
"""

from dataclasses import dataclass, field
from typing import Optional

from io_collection.keys import make_key
from io_collection.load import load_dataframe, load_json
from io_collection.save import save_figure
from prefect import flow

from cell_abm_pipeline.flows.group_basic_metrics import (
    BIN_METRICS,
    CELL_PHASES,
    DISTRIBUTION_METRICS,
    INDIVIDUAL_METRICS,
    SPATIAL_METRICS,
    TEMPORAL_METRICS,
)
from cell_abm_pipeline.tasks import (
    make_bar_figure,
    make_density_figure,
    make_histogram_figure,
    make_line_figure,
    make_range_figure,
    make_scatter_figure,
)

PLOTS: list[str] = [
    "metrics_bins",
    "metrics_distributions",
    "metrics_individuals",
    "metrics_spatial",
    "metrics_temporal",
    "population_counts",
]

PHASE_COLORS: dict[str, str] = {
    "PROLIFERATIVE_G1": "#7F3C8D",
    "PROLIFERATIVE_S": "#11A579",
    "PROLIFERATIVE_G2": "#3969AC",
    "PROLIFERATIVE_M": "#F2B701",
    "APOPTOTIC_EARLY": "#E73F74",
    "APOPTOTIC_LATE": "#80BA5A",
}

POPULATION_COLORS: dict[int, str] = {
    1: "#7F3C8D",
}


@dataclass
class ParametersConfigMetricsBins:
    """Parameter configuration for plot basic metrics subflow - metrics bins."""

    metrics: list[str] = field(default_factory=lambda: BIN_METRICS)
    """List of bin metrics."""

    seed: int = 0
    """Simulation seed to use for plotting bin metrics."""

    ticks: list[int] = field(default_factory=lambda: [0])
    """Simulation ticks to use for plotting bin metrics."""

    scale: float = 1
    """Metric bin scaling."""


@dataclass
class ParametersConfigMetricsDistributions:
    """Parameter configuration for plot basic metrics subflow - metrics distributions."""

    metrics: list[str] = field(default_factory=lambda: DISTRIBUTION_METRICS)
    """List of distribution metrics."""

    phases: list[str] = field(default_factory=lambda: CELL_PHASES)
    """List of cell cycle phases."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""


@dataclass
class ParametersConfigMetricsIndividuals:
    """Parameter configuration for plot basic metrics subflow - metrics individuals."""

    metrics: list[str] = field(default_factory=lambda: INDIVIDUAL_METRICS)
    """List of individual metrics."""

    seed: int = 0
    """Simulation seed to use for plotting individual metrics."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    phase_colors: dict[str, str] = field(default_factory=lambda: PHASE_COLORS)
    """Colors for each cell cycle phase."""


@dataclass
class ParametersConfigMetricsSpatial:
    """Parameter configuration for plot basic metrics subflow - metrics spatial."""

    metrics: list[str] = field(default_factory=lambda: SPATIAL_METRICS)
    """List of spatial metrics."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for plotting spatial metrics."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    ticks: list[int] = field(default_factory=lambda: [0])
    """Simulation ticks to use for plotting spatial metrics."""

    phase_colors: dict[str, str] = field(default_factory=lambda: PHASE_COLORS)
    """Colors for each cell cycle phase."""

    population_colors: dict[int, str] = field(default_factory=lambda: POPULATION_COLORS)
    """Colors for each cell population."""


@dataclass
class ParametersConfigMetricsTemporal:
    """Parameter configuration for plot basic metrics subflow - metrics temporal."""

    metrics: list[str] = field(default_factory=lambda: TEMPORAL_METRICS)
    """List of temporal metrics."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    populations: list[int] = field(default_factory=lambda: [1])
    """List of cell populations."""

    phases: list[str] = field(default_factory=lambda: CELL_PHASES)
    """List of cell cycle phases."""


@dataclass
class ParametersConfigPopulationCounts:
    """Parameter configuration for plot basic metrics subflow - population counts."""

    tick: int = 0
    """Simulation tick to use for plotting population counts."""


@dataclass
class ParametersConfig:
    """Parameter configuration for plot basic metrics flow."""

    plots: list[str] = field(default_factory=lambda: PLOTS)
    """List of basic metric plots."""

    metrics_bins: ParametersConfigMetricsBins = field(
        default_factory=lambda: ParametersConfigMetricsBins()
    )
    """Parameters for plot metrics bins subflow."""

    metrics_distributions: ParametersConfigMetricsDistributions = field(
        default_factory=lambda: ParametersConfigMetricsDistributions()
    )
    """Parameters for plot metrics distributions subflow."""

    metrics_individuals: ParametersConfigMetricsIndividuals = field(
        default_factory=lambda: ParametersConfigMetricsIndividuals()
    )
    """Parameters for plot metrics individuals subflow."""

    metrics_spatial: ParametersConfigMetricsSpatial = field(
        default_factory=lambda: ParametersConfigMetricsSpatial()
    )
    """Parameters for plot metrics spatial subflow."""

    metrics_temporal: ParametersConfigMetricsTemporal = field(
        default_factory=lambda: ParametersConfigMetricsTemporal()
    )
    """Parameters for plot metrics temporal subflow."""

    population_counts: ParametersConfigPopulationCounts = field(
        default_factory=lambda: ParametersConfigPopulationCounts()
    )
    """Parameters for plot population counts subflow."""


@dataclass
class ContextConfig:
    """Context configuration for plot basic metrics flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for plot basic metrics flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="plot-basic-metrics")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main plot basic metrics flow.

    Calls the following subflows, if the plot is specified:

    - :py:func:`run_flow_plot_metrics_bins`
    - :py:func:`run_flow_plot_metrics_distributions`
    - :py:func:`run_flow_plot_metrics_individuals`
    - :py:func:`run_flow_plot_metrics_spatial`
    - :py:func:`run_flow_plot_metrics_temporal`
    - :py:func:`run_flow_plot_population_stats`
    """

    if "metrics_bins" in parameters.plots:
        run_flow_plot_metrics_bins(context, series, parameters.metrics_bins)

    if "metrics_distributions" in parameters.plots:
        run_flow_plot_metrics_distributions(context, series, parameters.metrics_distributions)

    if "metrics_individuals" in parameters.plots:
        run_flow_plot_metrics_individuals(context, series, parameters.metrics_individuals)

    if "metrics_spatial" in parameters.plots:
        run_flow_plot_metrics_spatial(context, series, parameters.metrics_spatial)

    if "metrics_temporal" in parameters.plots:
        run_flow_plot_metrics_temporal(context, series, parameters.metrics_temporal)

    if "population_counts" in parameters.plots:
        run_flow_plot_population_counts(context, series, parameters.population_counts)


@flow(name="plot-basic-metrics_plot-metrics-bins")
def run_flow_plot_metrics_bins(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsBins
) -> None:
    """Plot basic metrics subflow for binned metrics."""

    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")
    plot_key = make_key(series.name, "plots", "plots.BASIC_METRICS")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for tick in parameters.ticks:
            for metric in parameters.metrics:
                metric_key = f"{key}.{parameters.seed:04d}.{tick:06d}.{metric.upper()}"

                group = load_dataframe(
                    context.working_location,
                    make_key(group_key, f"{series.name}.metrics_bins.{metric_key}.csv"),
                )

                save_figure(
                    context.working_location,
                    make_key(plot_key, f"{series.name}.metrics_bins.{metric_key}.png"),
                    make_density_figure(group, parameters.scale),
                )


@flow(name="plot-basic-metrics_plot-metrics-distributions")
def run_flow_plot_metrics_distributions(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsDistributions
) -> None:
    """Plot basic metrics subflow for metrics distributions."""

    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")
    plot_key = make_key(series.name, "plots", "plots.BASIC_METRICS")
    keys = [condition["key"] for condition in series.conditions]

    metrics: list[str] = []
    for metric in parameters.metrics:
        if metric in ["volume", "height"]:
            metrics = metrics + [f"{metric}.{region}" for region in parameters.regions]
        elif metric == "phase":
            metrics = metrics + [f"{metric}.{phase}" for phase in parameters.phases]
        else:
            continue

    for metric in metrics:
        metric_key = metric.upper()

        group = load_json(
            context.working_location,
            make_key(group_key, f"{series.name}.metrics_distributions.{metric_key}.json"),
        )

        assert isinstance(group, dict)

        save_figure(
            context.working_location,
            make_key(plot_key, f"{series.name}.metrics_distributions.{metric_key}.png"),
            make_histogram_figure(keys, group),
        )


@flow(name="plot-basic-metrics_plot-metrics-individuals")
def run_flow_plot_metrics_individuals(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsIndividuals
) -> None:
    """Plot basic metrics subflow for individual metrics."""

    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")
    plot_key = make_key(series.name, "plots", "plots.BASIC_METRICS")
    keys = [condition["key"] for condition in series.conditions]

    metrics: list[str] = [
        f"{metric}.{region}" for metric in parameters.metrics for region in parameters.regions
    ]

    for key in keys:
        for metric in metrics:
            metric_key = f"{key}.{parameters.seed:04d}.{metric.upper()}"

            group = load_json(
                context.working_location,
                make_key(group_key, f"{series.name}.metrics_individuals.{metric_key}.json"),
            )

            group_flat = [
                {
                    "x": line["time"],
                    "y": line["value"],
                    "color": parameters.phase_colors[line["phase"]],
                }
                for item in group
                for line in item
            ]

            save_figure(
                context.working_location,
                make_key(plot_key, f"{series.name}.metrics_individuals.{metric_key}.png"),
                make_line_figure(group_flat),
            )


@flow(name="plot-basic-metrics_plot-metrics-spatial")
def run_flow_plot_metrics_spatial(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsSpatial
) -> None:
    """Plot basic metrics subflow for spatial metrics."""

    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")
    plot_key = make_key(series.name, "plots", "plots.BASIC_METRICS")
    keys = [condition["key"] for condition in series.conditions]

    metrics: list[str] = []
    for metric in parameters.metrics:
        if metric in ["volume", "height"]:
            metrics = metrics + [f"{metric}.{region}" for region in parameters.regions]
        else:
            metrics.append(metric)

    for key in keys:
        for seed in parameters.seeds:
            for tick in parameters.ticks:
                for metric in metrics:
                    metric_key = f"{key}.{seed:04d}.{tick:06d}.{metric.upper()}"

                    colormap: Optional[dict] = None

                    if metric == "phase":
                        colormap = parameters.phase_colors
                    elif metric == "population":
                        colormap = parameters.population_colors

                    group = load_dataframe(
                        context.working_location,
                        make_key(group_key, f"{series.name}.metrics_spatial.{metric_key}.csv"),
                    )

                    save_figure(
                        context.working_location,
                        make_key(plot_key, f"{series.name}.metrics_spatial.{metric_key}.png"),
                        make_scatter_figure(group, colormap),
                    )


@flow(name="plot-basic-metrics_plot-metrics-temporal")
def run_flow_plot_metrics_temporal(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsTemporal
) -> None:
    """Plot basic metrics subflow for temporal metrics."""

    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")
    plot_key = make_key(series.name, "plots", "plots.BASIC_METRICS")
    keys = [condition["key"] for condition in series.conditions]

    metrics: list[str] = []
    for metric in parameters.metrics:
        if metric in ["volume", "height"]:
            metrics = metrics + [f"{metric}.{region}" for region in parameters.regions]
        elif metric == "population":
            metrics = metrics + [f"{metric}.{population}" for population in parameters.populations]
        elif metric == "phase":
            metrics = metrics + [f"{metric}.{phase}" for phase in parameters.phases]
        else:
            metrics.append(metric)

    for key in keys:
        for metric in metrics:
            metric_key = f"{key}.{metric.upper()}"

            group = load_json(
                context.working_location,
                make_key(group_key, f"{series.name}.metrics_temporal.{metric_key}.json"),
            )

            assert isinstance(group, dict)

            save_figure(
                context.working_location,
                make_key(plot_key, f"{series.name}.metrics_temporal.{metric_key}.png"),
                make_range_figure(group),
            )


@flow(name="plot-basic-metrics_plot-population-counts")
def run_flow_plot_population_counts(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigPopulationCounts
) -> None:
    """Plot basic metrics subflow for population counts."""

    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")
    plot_key = make_key(series.name, "plots", "plots.BASIC_METRICS")
    keys = [condition["key"] for condition in series.conditions]

    group = load_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.population_counts.{parameters.tick:06d}.csv"),
    )

    key_group = {
        key: {
            "COUNT": {
                "mean": group[group["key"] == key]["count"].mean(),
                "std": group[group["key"] == key]["count"].std(ddof=1),
            }
        }
        for key in keys
    }

    save_figure(
        context.working_location,
        make_key(plot_key, f"{series.name}.population_counts.{parameters.tick:06d}.png"),
        make_bar_figure(keys, key_group),
    )
