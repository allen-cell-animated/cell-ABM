"""
Workflow for grouping basic metrics.

Working location structure:

.. code-block:: bash

    (name)
    ├── analysis
    │   ├── analysis.BASIC_METRICS
    │   │   └── (name)_(key).BASIC_METRICS.csv
    │   └── analysis.POSITIONS
    │       ├── (name)_(key)_(seed).POSITIONS.csv
    │       └── (name)_(key)_(seed).POSITIONS.tar.xz
    └── groups
        └── groups.BASIC_METRICS
            ├── (name).metrics_bins.(key).(time).(metric).csv
            ├── (name).metrics_distributions.(metric).json
            ├── (name).metrics_individuals.(key).(seed).(metric).json
            ├── (name).metrics_spatial.(key).(seed).(time).(metric).csv
            ├── (name).metrics_temporal.(key).(metric).json
            └── (name).population_counts.(time).csv

Different groups use inputs from **results** and **analysis.POSITIONS**. Grouped
data are saved to **groups.BASIC_METRICS**.

Different groups can be visualized using the corresponding plotting workflow or
loaded into alternative tools.
"""

import ast
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import groupby

import numpy as np
import pandas as pd
from io_collection.keys import make_key
from io_collection.load import load_dataframe
from io_collection.save import save_dataframe, save_json
from prefect import flow
from prefect.tasks import task_input_hash

from cell_abm_pipeline.tasks import (
    bin_to_hex,
    calculate_category_durations,
    calculate_data_bins,
    check_data_bounds,
)

OPTIONS = {
    "cache_result_in_memory": False,
    "cache_key_fn": task_input_hash,
    "cache_expiration": timedelta(hours=12),
}

GROUPS: list[str] = [
    "metrics_bins",
    "metrics_distributions",
    "metrics_individuals",
    "metrics_spatial",
    "metrics_temporal",
    "population_counts",
]

CELL_PHASES: list[str] = [
    "PROLIFERATIVE_G1",
    "PROLIFERATIVE_S",
    "PROLIFERATIVE_G2",
    "PROLIFERATIVE_M",
    "APOPTOTIC_EARLY",
    "APOPTOTIC_LATE",
]

BIN_METRICS: list[str] = [
    "count",
    "volume",
    "height",
]

DISTRIBUTION_METRICS: list[str] = [
    "phase",
    "volume",
    "height",
]

INDIVIDUAL_METRICS: list[str] = [
    "volume",
    "height",
]

SPATIAL_METRICS: list[str] = [
    "population",
    "phase",
    "volume",
    "height",
]

TEMPORAL_METRICS: list[str] = [
    "count",
    "population",
    "phase",
    "volume",
    "height",
]

BOUNDS: dict[str, list] = {
    "volume.DEFAULT": [0, 6000],
    "volume.NUCLEUS": [0, 2000],
    "height.DEFAULT": [0, 21],
    "height.NUCLEUS": [0, 21],
    "phase.PROLIFERATIVE_G1": [0, 5],
    "phase.PROLIFERATIVE_S": [0, 20],
    "phase.PROLIFERATIVE_G2": [0, 40],
    "phase.PROLIFERATIVE_M": [0, 2],
    "phase.APOPTOTIC_EARLY": [0, 6],
    "phase.APOPTOTIC_LATE": [0, 12],
}

BANDWIDTH: dict[str, float] = {
    "volume.DEFAULT": 100,
    "volume.NUCLEUS": 50,
    "height.DEFAULT": 1,
    "height.NUCLEUS": 1,
    "phase.PROLIFERATIVE_G1": 0.25,
    "phase.PROLIFERATIVE_S": 0.25,
    "phase.PROLIFERATIVE_G2": 0.25,
    "phase.PROLIFERATIVE_M": 0.25,
    "phase.APOPTOTIC_EARLY": 0.25,
    "phase.APOPTOTIC_LATE": 0.25,
}


@dataclass
class ParametersConfigMetricsBins:
    """Parameter configuration for group basic metrics subflow - metrics bins."""

    metrics: list[str] = field(default_factory=lambda: BIN_METRICS)
    """List of bin metrics."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seed(s) to use for grouping metric bins."""

    time: int = 0
    """Simulation time (in hours) to use for grouping metric bins."""

    scale: float = 1
    """Metric bin scaling."""


@dataclass
class ParametersConfigMetricsDistributions:
    """Parameter configuration for group basic metrics subflow - metrics distributions."""

    metrics: list[str] = field(default_factory=lambda: DISTRIBUTION_METRICS)
    """List of distribution metrics."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seed(s) to use for grouping metric distributions."""

    phases: list[str] = field(default_factory=lambda: CELL_PHASES)
    """List of cell cycle phases."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    bounds: dict[str, list] = field(default_factory=lambda: BOUNDS)
    """Bounds for metric distributions."""

    bandwidth: dict[str, float] = field(default_factory=lambda: BANDWIDTH)
    """Bandwidths for metric distributions."""

    threshold: float = 0.2
    """Threshold for separating phase durations (in hours)."""


@dataclass
class ParametersConfigMetricsIndividuals:
    """Parameter configuration for group basic metrics subflow - metrics individuals."""

    metrics: list[str] = field(default_factory=lambda: INDIVIDUAL_METRICS)
    """List of individual metrics."""

    seed: int = 0
    """Simulation seed to use for grouping individual metrics."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""


@dataclass
class ParametersConfigMetricsSpatial:
    """Parameter configuration for group basic metrics subflow - metrics spatial."""

    metrics: list[str] = field(default_factory=lambda: SPATIAL_METRICS)
    """List of spatial metrics."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seed(s) to use for grouping spatial metrics."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    times: list[int] = field(default_factory=lambda: [0])
    """Simulation time(s) (in hours) to use for grouping spatial metrics."""


@dataclass
class ParametersConfigMetricsTemporal:
    """Parameter configuration for group basic metrics subflow - metrics temporal."""

    metrics: list[str] = field(default_factory=lambda: TEMPORAL_METRICS)
    """List of temporal metrics."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seed(s) to use for grouping temporal metrics."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    populations: list[int] = field(default_factory=lambda: [1])
    """List of cell populations."""

    phases: list[str] = field(default_factory=lambda: CELL_PHASES)
    """List of cell cycle phases."""


@dataclass
class ParametersConfigPopulationCounts:
    """Parameter configuration for group basic metrics subflow - population counts."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seed(s) to use for grouping population counts."""

    time: int = 0
    """Simulation time (in hours) to use for grouping population counts."""


@dataclass
class ParametersConfig:
    """Parameter configuration for group basic metrics flow."""

    groups: list[str] = field(default_factory=lambda: GROUPS)
    """List of basic metrics groups."""

    metrics_bins: ParametersConfigMetricsBins = field(
        default_factory=lambda: ParametersConfigMetricsBins
    )
    """Parameters for group metrics bins subflow."""

    metrics_distributions: ParametersConfigMetricsDistributions = field(
        default_factory=lambda: ParametersConfigMetricsDistributions
    )
    """Parameters for group metrics distributions subflow."""

    metrics_individuals: ParametersConfigMetricsIndividuals = field(
        default_factory=lambda: ParametersConfigMetricsIndividuals
    )
    """Parameters for group metrics individuals subflow."""

    metrics_spatial: ParametersConfigMetricsSpatial = field(
        default_factory=lambda: ParametersConfigMetricsSpatial
    )
    """Parameters for group metrics spatial subflow."""

    metrics_temporal: ParametersConfigMetricsTemporal = field(
        default_factory=lambda: ParametersConfigMetricsTemporal
    )
    """Parameters for group metrics temporal subflow."""

    population_counts: ParametersConfigPopulationCounts = field(
        default_factory=lambda: ParametersConfigPopulationCounts
    )
    """Parameters for group population counts subflow."""


@dataclass
class ContextConfig:
    """Context configuration for group basic metrics flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for group basic metrics flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="group-basic-metrics")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main group basic metrics flow.

    Calls the following subflows, if the group is specified:

    - :py:func:`run_flow_group_metrics_bins`
    - :py:func:`run_flow_group_metrics_distributions`
    - :py:func:`run_flow_group_metrics_individuals`
    - :py:func:`run_flow_group_metrics_spatial`
    - :py:func:`run_flow_group_metrics_temporal`
    - :py:func:`run_flow_group_population_stats`
    """

    if "metrics_bins" in parameters.groups:
        run_flow_group_metrics_bins(context, series, parameters.metrics_bins)

    if "metrics_distributions" in parameters.groups:
        run_flow_group_metrics_distributions(context, series, parameters.metrics_distributions)

    if "metrics_individuals" in parameters.groups:
        run_flow_group_metrics_individuals(context, series, parameters.metrics_individuals)

    if "metrics_spatial" in parameters.groups:
        run_flow_group_metrics_spatial(context, series, parameters.metrics_spatial)

    if "metrics_temporal" in parameters.groups:
        run_flow_group_metrics_temporal(context, series, parameters.metrics_temporal)

    if "population_counts" in parameters.groups:
        run_flow_group_population_counts(context, series, parameters.population_counts)


@flow(name="group-basic-metrics_group-metrics-bins")
def run_flow_group_metrics_bins(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsBins
) -> None:
    """Group basic metrics subflow for binned metrics."""

    analysis_metrics_key = make_key(series.name, "analysis", "analysis.BASIC_METRICS")
    analysis_positions_key = make_key(series.name, "analysis", "analysis.POSITIONS")
    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    for superkey in superkeys:
        metrics_key = make_key(analysis_metrics_key, f"{series.name}_{superkey}.BASIC_METRICS.csv")
        metrics_df = load_dataframe.with_options(**OPTIONS)(context.working_location, metrics_key)
        metrics_df = metrics_df[
            metrics_df["SEED"].isin(parameters.seeds) & (metrics_df["time"] == parameters.time)
        ]

        x = []
        y = []
        v: dict[str, list] = {metric: [] for metric in parameters.metrics}

        for (key, seed), group in metrics_df.groupby(["KEY", "SEED"]):
            group.set_index("ID", inplace=True)

            series_key = f"{series.name}_{key}_{seed:04d}"
            positions_key = make_key(analysis_positions_key, f"{series_key}.POSITIONS.csv")
            positions = load_dataframe.with_options(**OPTIONS)(
                context.working_location, positions_key, converters={"ids": ast.literal_eval}
            )
            positions = positions[positions["TICK"] == group["TICK"].unique()[0]]

            x.extend(positions["x"])
            y.extend(positions["y"])

            for metric in parameters.metrics:
                if metric == "count":
                    v[metric].extend(positions["ids"].map(len))
                else:
                    v[metric].extend(
                        [np.mean([group.loc[i][metric] for i in ids]) for ids in positions["ids"]]
                    )

        for metric in parameters.metrics:
            bins = bin_to_hex(np.array(x), np.array(y), np.array(v[metric]), parameters.scale)
            bins_df = pd.DataFrame(
                [[x, y, np.mean(v[metric])] for (x, y), v[metric] in bins.items()],
                columns=["x", "y", "v"],
            )

            metric_key = f"{superkey}.{parameters.time:03d}.{metric.upper()}"
            save_dataframe(
                context.working_location,
                make_key(group_key, f"{series.name}.metrics_bins.{metric_key}.csv"),
                bins_df,
                index=False,
            )


@flow(name="group-basic-metrics_group-metrics-distributions")
def run_flow_group_metrics_distributions(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsDistributions
) -> None:
    """Group basic metrics subflow for metrics distributions."""

    analysis_key = make_key(series.name, "analysis", "analysis.BASIC_METRICS")
    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    metrics: list[str] = []
    for metric in parameters.metrics:
        if metric in ["volume", "height"]:
            metrics = metrics + [f"{metric}.{region}" for region in parameters.regions]
        elif metric == "phase":
            metrics = metrics + [f"{metric}.{phase}" for phase in parameters.phases]
        else:
            continue

    distribution_bins: dict[str, dict] = {metric: {} for metric in metrics}
    distribution_means: dict[str, dict] = {metric: {} for metric in metrics}
    distribution_stdevs: dict[str, dict] = {metric: {} for metric in metrics}

    for key in superkeys:
        metrics_key = make_key(analysis_key, f"{series.name}_{key}.BASIC_METRICS.csv")
        metrics_df = load_dataframe.with_options(**OPTIONS)(context.working_location, metrics_key)
        metrics_df = metrics_df[metrics_df["SEED"].isin(parameters.seeds)]

        for metric in metrics:
            if "phase" in metric:
                phase = metric.split(".")[1]
                values = np.array(
                    calculate_category_durations(metrics_df, "PHASE", phase, parameters.threshold)
                )
            else:
                column = metric.replace(".DEFAULT", "")
                values = metrics_df[column].values

            bounds = (parameters.bounds[metric][0], parameters.bounds[metric][1])
            bandwidth = parameters.bandwidth[metric]

            valid = check_data_bounds(values, bounds, f"[ {key} ] metric [ {metric} ]")

            if not valid:
                continue

            distribution_means[metric][key] = np.mean(values)
            distribution_stdevs[metric][key] = np.std(values, ddof=1)
            distribution_bins[metric][key] = calculate_data_bins(values, bounds, bandwidth)

    for metric, distribution in distribution_bins.items():
        distribution["*"] = {
            "bandwidth": parameters.bandwidth[metric],
            "means": distribution_means[metric],
            "stdevs": distribution_stdevs[metric],
        }

        save_json(
            context.working_location,
            make_key(group_key, f"{series.name}.metrics_distributions.{metric.upper()}.json"),
            distribution,
        )


@flow(name="group-basic-metrics_group-metrics-individuals")
def run_flow_group_metrics_individuals(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsIndividuals
) -> None:
    """Group basic metrics subflow for individual metrics."""

    analysis_key = make_key(series.name, "analysis", "analysis.BASIC_METRICS")
    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    metrics: list[str] = [
        f"{metric}.{region}" for metric in parameters.metrics for region in parameters.regions
    ]

    for key in superkeys:
        metrics_key = make_key(analysis_key, f"{series.name}_{key}.BASIC_METRICS.csv")
        metrics_df = load_dataframe.with_options(**OPTIONS)(context.working_location, metrics_key)
        metrics_df = metrics_df[metrics_df["SEED"] == parameters.seed]

        for metric in metrics:
            times = metrics_df.groupby(["KEY", "ID"])["time"].apply(np.hstack)
            values = metrics_df.groupby(["KEY", "ID"])[metric.replace(".DEFAULT", "")]
            phases = metrics_df.groupby(["KEY", "ID"])["PHASE"].apply(np.hstack)

            entries = [
                [
                    {"time_and_value": np.array([x[:2] for x in group]), "phase": phase}
                    for phase, group in groupby(zip(time, value, phase), key=lambda x: x[2])
                ]
                for time, value, phase in zip(times, values.apply(np.hstack), phases)
            ]

            individuals = [
                [
                    {
                        "time": item["time_and_value"][:, 0].tolist(),
                        "value": item["time_and_value"][:, 1].tolist(),
                        "phase": item["phase"],
                    }
                    for item in entry
                ]
                for entry in entries
            ]

            metric_key = f"{key}.{parameters.seed:04d}.{metric.upper()}"
            save_json(
                context.working_location,
                make_key(group_key, f"{series.name}.metrics_individuals.{metric_key}.json"),
                individuals,
            )


@flow(name="group-basic-metrics_group-metrics-spatial")
def run_flow_group_metrics_spatial(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsSpatial
) -> None:
    """Group basic metrics subflow for spatial metrics."""

    analysis_key = make_key(series.name, "analysis", "analysis.BASIC_METRICS")
    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    metrics: list[str] = []
    for metric in parameters.metrics:
        if metric in ["volume", "height"]:
            metrics = metrics + [f"{metric}.{region}" for region in parameters.regions]
        else:
            metrics.append(metric)

    for key in superkeys:
        metrics_key = make_key(analysis_key, f"{series.name}_{key}.BASIC_METRICS.csv")
        metrics_df = load_dataframe.with_options(**OPTIONS)(context.working_location, metrics_key)

        for seed in parameters.seeds:
            seed_df = metrics_df[metrics_df["SEED"] == seed]

            for time in parameters.times:
                data = seed_df[seed_df["time"] == time]

                for metric in metrics:
                    column = metric.replace(".DEFAULT", "") if "." in metric else metric.upper()
                    spatial = data[["cx", "cy", "cz", column]].rename(
                        columns={"cx": "x", "cy": "y", "cz": "z", column: "v"}
                    )

                    metric_key = f"{key}.{seed:04d}.{time:03d}.{metric.upper()}"
                    save_dataframe(
                        context.working_location,
                        make_key(group_key, f"{series.name}.metrics_spatial.{metric_key}.csv"),
                        spatial,
                        index=False,
                    )


@flow(name="group-basic-metrics_group-metrics-temporal")
def run_flow_group_metrics_temporal(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMetricsTemporal
) -> None:
    """Group basic metrics subflow for temporal metrics."""

    analysis_key = make_key(series.name, "analysis", "analysis.BASIC_METRICS")
    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

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

    for key in superkeys:
        metrics_key = make_key(analysis_key, f"{series.name}_{key}.BASIC_METRICS.csv")
        metrics_df = load_dataframe.with_options(**OPTIONS)(context.working_location, metrics_key)

        for metric in metrics:
            if metric == "count":
                values = metrics_df.groupby(["SEED", "time"]).size().groupby(["time"])
            elif "phase" in metric:
                phase_subset = metrics_df[metrics_df["PHASE"] == metric.split(".")[1]]
                phase_counts = phase_subset.groupby(["SEED", "time"]).size()
                total_counts = metrics_df.groupby(["SEED", "time"]).size()
                values = (phase_counts / total_counts).groupby("time")
            elif "population" in metric:
                pop_subset = metrics_df[metrics_df["POPULATION"] == int(metric.split(".")[1])]
                pop_counts = pop_subset.groupby(["SEED", "time"]).size()
                total_counts = metrics_df.groupby(["SEED", "time"]).size()
                values = (pop_counts / total_counts).groupby("time")
            else:
                column = metric.replace(".DEFAULT", "")
                values = metrics_df.groupby(["SEED", "time"])[column].mean().groupby(["time"])

            temporal = {
                "time": list(values.groups.keys()),
                "mean": [v if not np.isnan(v) else "nan" for v in values.mean()],
                "std": [v if not np.isnan(v) else "nan" for v in values.std(ddof=1)],
                "min": [v if not np.isnan(v) else "nan" for v in values.min()],
                "max": [v if not np.isnan(v) else "nan" for v in values.max()],
            }

            save_json(
                context.working_location,
                make_key(group_key, f"{series.name}.metrics_temporal.{key}.{metric.upper()}.json"),
                temporal,
            )


@flow(name="group-basic-metrics_group-population-counts")
def run_flow_group_population_counts(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigPopulationCounts
) -> None:
    """Group basic metrics subflow for population counts."""

    analysis_key = make_key(series.name, "analysis", "analysis.BASIC_METRICS")
    group_key = make_key(series.name, "groups", "groups.BASIC_METRICS")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    counts: list[dict] = []

    for key in superkeys:
        metrics_key = make_key(analysis_key, f"{series.name}_{key}.BASIC_METRICS.csv")
        metrics_df = load_dataframe.with_options(**OPTIONS)(
            context.working_location, metrics_key, usecols=["KEY", "SEED", "time"]
        )
        metrics_df = metrics_df[
            metrics_df["SEED"].isin(parameters.seeds) & (metrics_df["time"] == parameters.time)
        ]

        counts.extend(
            [
                {
                    "key": record["KEY"],
                    "seed": record["SEED"],
                    "count": record[0],
                }
                for record in metrics_df.groupby(["KEY", "SEED"])
                .size()
                .reset_index()
                .to_dict("records")
            ]
        )

    save_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.population_counts.{parameters.time:03d}.csv"),
        pd.DataFrame(counts).drop_duplicates(),
        index=False,
    )
