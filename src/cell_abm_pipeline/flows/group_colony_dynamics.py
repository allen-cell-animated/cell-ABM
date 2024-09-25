"""
Workflow for grouping colony dynamics.

Working location structure:

.. code-block:: bash

    (name)
    ├── analysis
    │   ├── analysis.MEASURES
    │   │   └── (name)_(key).MEASURES.csv
    │   └── analysis.COLONIES
    │       └── (name)_(key).COLONIES.csv
    └── groups
        └── groups.COLONIES
            ├── (name).feature_distributions.(feature).json
            ├── (name).feature_temporal.(key).(feature).json
            ├── (name).neighbor_positions.(key).(seed).(tick).csv
            └── (name).neighbor_positions.(key).(seed).(tick).(feature).csv

Different groups use inputs from **analysis.COLONIES** and
**analysis.MEASURES**. Grouped data are saved to **groups.COLONIES**.

Different groups can be visualized using the corresponding plotting workflow or
loaded into alternative tools.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from datetime import timedelta

import numpy as np
import pandas as pd
from abm_shape_collection import extract_voxel_contours
from arcade_collection.output import extract_tick_json, get_location_voxels
from arcade_collection.output.convert_model_units import (
    estimate_spatial_resolution,
    estimate_temporal_resolution,
)
from io_collection.keys import make_key
from io_collection.load import load_dataframe, load_tar
from io_collection.save import save_dataframe, save_json
from prefect import flow
from prefect.tasks import task_input_hash

from cell_abm_pipeline.tasks import calculate_data_bins, check_data_bounds

OPTIONS = {
    "cache_result_in_memory": False,
    "cache_key_fn": task_input_hash,
    "cache_expiration": timedelta(hours=12),
}

GROUPS: list[str] = [
    "colony_contours",
    "feature_distributions",
    "feature_temporal",
    "neighbor_positions",
]

PROJECTIONS: list[str] = [
    "top",
    "side1",
    "side2",
]

DISTRIBUTION_FEATURES: list[str] = [
    "degree",
    "eccentricity",
    "degree_centrality",
    "closeness_centrality",
    "betweenness_centrality",
]

TEMPORAL_FEATURES: list[str] = [
    "degree",
    "eccentricity",
    "degree_centrality",
    "closeness_centrality",
    "betweenness_centrality",
    "radius",
    "diameter",
]

POSITION_FEATURES: list[str] = [
    "depth",
    "group",
]

BOUNDS: dict[str, list] = {
    "degree": [-1, 30],
    "eccentricity": [-1, 15],
    "degree_centrality": [-0.1, 1],
    "closeness_centrality": [-0.1, 1],
    "betweenness_centrality": [-0.1, 1],
}

BANDWIDTH: dict[str, float] = {
    "degree": 1,
    "eccentricity": 1,
    "degree_centrality": 0.05,
    "closeness_centrality": 0.05,
    "betweenness_centrality": 0.05,
}


@dataclass
class ParametersConfigColonyContours:
    """Parameter configuration for group colony dynamics subflow - colony contours."""

    regions: list[str | None] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    seed: int = 0
    """Simulation random seed to use for grouping colony contours."""

    time: int = 0
    """Simulation time (in hours) to use for grouping colony contours."""

    ds: float | None = None
    """Spatial scaling in um/voxel."""

    dt: float | None = None
    """Temporal scaling in hours/tick."""

    projection: str = "top"
    """Selected colony projection."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of projection bounding box."""

    slice_index: int | None = None
    """Slice index along the colony projection axis."""


@dataclass
class ParametersConfigFeatureDistributions:
    """Parameter configuration for group colony dynamics subflow - feature distributions."""

    features: list[str] = field(default_factory=lambda: DISTRIBUTION_FEATURES)
    """List of colony features."""

    bounds: dict[str, list] = field(default_factory=lambda: BOUNDS)
    """Bounds for feature distributions."""

    bandwidth: dict[str, float] = field(default_factory=lambda: BANDWIDTH)
    """Bandwidths for feature distributions."""


@dataclass
class ParametersConfigFeatureTemporal:
    """Parameter configuration for group colony dynamics subflow - feature temporal."""

    features: list[str] = field(default_factory=lambda: TEMPORAL_FEATURES)
    """List of temporal features."""


@dataclass
class ParametersConfigNeighborPositions:
    """Parameter configuration for group colony dynamics subflow - neighbor positions."""

    features: list[str] = field(default_factory=lambda: POSITION_FEATURES)
    """List of position features."""

    seed: int = 0
    """Simulation seed to use for grouping neighbor positions."""

    ticks: list[int] = field(default_factory=lambda: [0])
    """Simulation ticks to use for grouping neighbor positions."""


@dataclass
class ParametersConfig:
    """Parameter configuration for group colony dynamics flow."""

    groups: list[str] = field(default_factory=lambda: GROUPS)
    """List of colony dynamics groups."""

    colony_contours: ParametersConfigColonyContours = field(
        default_factory=ParametersConfigColonyContours
    )
    """Parameters for group colony contours subflow."""

    feature_distributions: ParametersConfigFeatureDistributions = field(
        default_factory=ParametersConfigFeatureDistributions
    )
    """Parameters for group feature distributions subflow."""

    feature_temporal: ParametersConfigFeatureTemporal = field(
        default_factory=ParametersConfigFeatureTemporal
    )
    """Parameters for group feature temporal subflow."""

    neighbor_positions: ParametersConfigNeighborPositions = field(
        default_factory=ParametersConfigNeighborPositions
    )
    """Parameters for group neighbor positions subflow."""


@dataclass
class ContextConfig:
    """Context configuration for group colony dynamics flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for group colony dynamics flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="group-colony-dynamics")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main group colony dynamics flow.

    Calls the following subflows, if the group is specified:

    - :py:func:`run_flow_group_colony_contours`
    - :py:func:`run_flow_group_feature_distributions`
    - :py:func:`run_flow_group_feature_temporal`
    - :py:func:`run_flow_group_neighbor_positions`
    """

    if "colony_contours" in parameters.groups:
        run_flow_group_colony_contours(context, series, parameters.colony_contours)

    if "feature_distributions" in parameters.groups:
        run_flow_group_feature_distributions(context, series, parameters.feature_distributions)

    if "feature_temporal" in parameters.groups:
        run_flow_group_feature_temporal(context, series, parameters.feature_temporal)

    if "neighbor_positions" in parameters.groups:
        run_flow_group_neighbor_positions(context, series, parameters.neighbor_positions)


@flow(name="group-cell-shapes_group-colony-contours")
def run_flow_group_colony_contours(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigColonyContours
) -> None:
    """Group colony dynamics subflow for colony contours."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    group_key = make_key(series.name, "groups", "groups.COLONY_DYNAMICS")
    keys = [condition["key"] for condition in series.conditions]

    projection = parameters.projection
    projection_index = list(reversed(PROJECTIONS)).index(projection)

    for key in keys:
        series_key = f"{series.name}_{key}_{parameters.seed:04d}"
        tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
        tar = load_tar(context.working_location, tar_key)

        ds = parameters.ds if parameters.ds is not None else estimate_spatial_resolution(key)
        dt = parameters.dt if parameters.dt is not None else estimate_temporal_resolution(key)

        tick = int(parameters.time / dt)
        length, width, height = parameters.box
        box = (int((length - 2) / ds) + 2, int((width - 2) / ds) + 2, int((height - 2) / ds) + 2)

        locations = extract_tick_json(tar, series_key, tick, "LOCATIONS")

        for region in parameters.regions:
            all_voxels = [
                voxel
                for location in locations
                for voxel in get_location_voxels(location, None if region == "DEFAULT" else region)
            ]

            if parameters.slice_index is not None:
                all_voxels = [
                    voxel
                    for voxel in all_voxels
                    if voxel[projection_index] == parameters.slice_index
                ]

            contours = [
                (np.array(contour) * ds).astype("int").tolist()
                for contour in extract_voxel_contours(all_voxels, projection, box)
            ]

            contour_key = f"{key}.{parameters.seed:04d}.{parameters.time:03d}.{region}"
            save_json(
                context.working_location,
                make_key(
                    group_key,
                    f"{series.name}.colony_contours.{contour_key}.{projection.upper()}.json",
                ),
                contours,
            )


@flow(name="group-colony-dynamics_group-feature-distributions")
def run_flow_group_feature_distributions(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureDistributions
) -> None:
    """Group colony dynamics subflow for feature distributions."""

    analysis_key = make_key(series.name, "analysis", "analysis.MEASURES")
    group_key = make_key(series.name, "groups", "groups.COLONIES")
    keys = [condition["key"] for condition in series.conditions]

    distribution_bins: dict[str, dict] = {feature: {} for feature in parameters.features}
    distribution_means: dict[str, dict] = {feature: {} for feature in parameters.features}
    distribution_stdevs: dict[str, dict] = {feature: {} for feature in parameters.features}

    for key in keys:
        # Load dataframe.
        dataframe_key = make_key(analysis_key, f"{series.name}_{key}.MEASURES.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)

        for feature in parameters.features:
            values = data[feature.upper()].to_numpy()

            bounds = (parameters.bounds[feature][0], parameters.bounds[feature][1])
            bandwidth = parameters.bandwidth[feature]

            check_data_bounds(values, bounds, f"[ {key} ] feature [ {feature} ]")

            distribution_means[feature][key] = np.mean(values)
            distribution_stdevs[feature][key] = np.std(values, ddof=1)
            distribution_bins[feature][key] = calculate_data_bins(values, bounds, bandwidth)

    for feature, distribution in distribution_bins.items():
        distribution["*"] = {
            "bandwidth": parameters.bandwidth[feature],
            "means": distribution_means[feature],
            "stdevs": distribution_stdevs[feature],
        }

        save_json(
            context.working_location,
            make_key(group_key, f"{series.name}.feature_distributions.{feature.upper()}.json"),
            distribution,
        )


@flow(name="group-colony-dynamics_group-feature-temporal")
def run_flow_group_feature_temporal(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureTemporal
) -> None:
    """Group colony dynamics subflow for temporal features."""

    analysis_key = make_key(series.name, "analysis", "analysis.MEASURES")
    group_key = make_key(series.name, "groups", "groups.COLONIES")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        # Load dataframe.
        dataframe_key = make_key(analysis_key, f"{series.name}_{key}.MEASURES.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)

        for feature in parameters.features:
            if feature == "radius":
                values = data.groupby(["SEED", "time"])["ECCENTRICITY"].min().groupby(["time"])
            elif feature == "diameter":
                values = data.groupby(["SEED", "time"])["ECCENTRICITY"].max().groupby(["time"])
            else:
                values = data.groupby(["SEED", "time"])[feature.upper()].mean().groupby(["time"])

            temporal = {
                "time": list(values.groups.keys()),
                "mean": [v if not np.isnan(v) else "nan" for v in values.mean()],
                "std": [v if not np.isnan(v) else "nan" for v in values.std(ddof=1)],
                "min": [v if not np.isnan(v) else "nan" for v in values.min()],
                "max": [v if not np.isnan(v) else "nan" for v in values.max()],
            }

            save_json(
                context.working_location,
                make_key(group_key, f"{series.name}.feature_temporal.{key}.{feature.upper()}.json"),
                temporal,
            )


@flow(name="group-colony-dynamics_group-neighbor-positions")
def run_flow_group_neighbor_positions(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigNeighborPositions
) -> None:
    """Group colony dynamics subflow for neighbor positions."""

    analysis_key = make_key(series.name, "analysis", "analysis.COLONIES")
    group_key = make_key(series.name, "groups", "groups.COLONIES")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        dataframe_key = make_key(analysis_key, f"{series.name}_{key}.COLONIES.csv")
        data = load_dataframe.with_options(**OPTIONS)(
            context.working_location, dataframe_key, converters={"NEIGHBORS": ast.literal_eval}
        )
        groups = data[data["SEED"] == parameters.seed].groupby("TICK")

        for tick in parameters.ticks:
            if tick not in groups.groups:
                continue

            group = groups.get_group(tick)

            # Save edge data.
            edges = set()
            for item in group[["ID", "NEIGHBORS"]].to_dict("records"):
                edges.update(
                    {tuple(sorted([item["ID"], neighbor])) for neighbor in item["NEIGHBORS"]}
                )

            edge_key = f"{key}.{parameters.seed:04d}.{tick:06d}"
            save_dataframe(
                context.working_location,
                make_key(group_key, f"{series.name}.neighbor_positions.{edge_key}.csv"),
                pd.DataFrame(list(edges), columns=["id1", "id2"]),
                index=False,
            )

            # Save node data for each feature.
            for feature in parameters.features:
                nodes = group[["ID", "cx", "cy", "cz", feature.upper()]].rename(
                    columns={"ID": "id", "cx": "x", "cy": "y", "cz": "z", feature.upper(): "v"}
                )

                node_key = f"{key}.{parameters.seed:04d}.{tick:06d}.{feature.upper()}"
                save_dataframe(
                    context.working_location,
                    make_key(group_key, f"{series.name}.neighbor_positions.{node_key}.csv"),
                    nodes,
                    index=False,
                )
