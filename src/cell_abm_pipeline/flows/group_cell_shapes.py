"""
Workflow for grouping cell shapes.

Working location structure:

.. code-block:: bash

    (name)
    ├── analysis
    │   ├── analysis.CELL_SHAPES_DATA
    │   │   └── (name)_(key).CELL_SHAPES_DATA.csv
    │   └── analysis.CELL_SHAPES_MODELS
    │       └── (name)_(key).CELL_SHAPES_MODELS.pkl
    ├── data
    │   └── data.LOCATIONS
    │       └── (name)_(key)_(seed).LOCATIONS.tar.xz
    └── groups
        └── groups.CELL_SHAPES
            ├── (name).feature_components.csv
            ├── (name).feature_correlations.(key).(region).csv
            ├── (name).feature_correlations.(key).(mode).(property).(region).csv
            ├── (name).feature_distributions.(feature).json
            ├── (name).mode_correlations.csv
            ├── (name).population_counts.(time).csv
            ├── (name).population_stats.json
            ├── (name).shape_average.(key).(projection).json
            ├── (name).shape_contours.(key).(seed).(time).(region).(projection).json
            ├── (name).shape_errors.json
            ├── (name).shape_modes.(key).(region).(mode).(projection).json
            ├── (name).shape_samples.json
            └── (name).variance_explained.csv

Different groups use inputs from **data.LOCATIONS**,
**analysis.CELL_SHAPES_DATA**, and **analysis.CELL_SHAPES_MODELS**. Grouped data
are saved to **groups.CELL_SHAPES**.

Different groups can be visualized using the corresponding plotting workflow or
loaded into alternative tools.
"""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, Union

import numpy as np
import pandas as pd
from abm_shape_collection import (
    construct_mesh_from_array,
    construct_mesh_from_coeffs,
    extract_mesh_projections,
    extract_mesh_wireframe,
    extract_shape_modes,
    extract_voxel_contours,
    make_voxels_array,
)
from arcade_collection.output import extract_tick_json, get_location_voxels
from arcade_collection.output.convert_model_units import (
    estimate_spatial_resolution,
    estimate_temporal_resolution,
)
from io_collection.keys import make_key
from io_collection.load import load_dataframe, load_pickle, load_tar
from io_collection.save import save_dataframe, save_json
from prefect import flow, get_run_logger
from prefect.tasks import task_input_hash
from scipy.spatial import ConvexHull, KDTree
from scipy.stats import pearsonr
from sklearn.decomposition import PCA

from cell_abm_pipeline.flows.analyze_cell_shapes import PCA_COMPONENTS
from cell_abm_pipeline.flows.calculate_coefficients import COEFFICIENT_ORDER
from cell_abm_pipeline.tasks import bin_to_hex, calculate_data_bins, check_data_bounds

OPTIONS = {
    "cache_result_in_memory": False,
    "cache_key_fn": task_input_hash,
    "cache_expiration": timedelta(hours=12),
}

GROUPS: list[str] = [
    "feature_components",
    "feature_correlations",
    "feature_distributions",
    "mode_correlations",
    "population_counts",
    "population_stats",
    "shape_average",
    "shape_contours",
    "shape_errors",
    "shape_modes",
    "shape_samples",
    "variance_explained",
]

COMPONENT_FEATURES: list[str] = [
    "volume",
    "height",
    "area",
    "axis_major_length",
    "axis_minor_length",
    "eccentricity",
    "orientation",
    "perimeter",
    "extent",
    "solidity",
]

CORRELATION_PROPERTIES: list[str] = [
    "volume",
    "height",
    "area",
    "axis_major_length",
    "axis_minor_length",
    "eccentricity",
    "perimeter",
]

DISTRIBUTION_PROPERTIES: list[str] = [
    "volume",
    "height",
]

PROJECTIONS: list[str] = [
    "top",
    "side1",
    "side2",
]

LIMITS: dict[str, list] = {
    "volume.DEFAULT": [500, 4000],
    "volume.NUCLEUS": [0, 1500],
    "height.DEFAULT": [0, 20],
    "height.NUCLEUS": [0, 20],
    "area.DEFAULT": [0, 1000],
    "area.NUCLEUS": [0, 250],
    "axis_major_length.DEFAULT": [0, 100],
    "axis_major_length.NUCLEUS": [0, 50],
    "axis_minor_length.DEFAULT": [0, 50],
    "axis_minor_length.NUCLEUS": [0, 20],
    "eccentricity.DEFAULT": [0, 1],
    "eccentricity.NUCLEUS": [0, 1],
    "perimeter.DEFAULT": [0, 250],
    "perimeter.NUCLEUS": [0, 100],
    "PC1": [-60, 60],
    "PC2": [-50, 50],
    "PC3": [-50, 50],
    "PC4": [-50, 50],
    "PC5": [-40, 40],
    "PC6": [-40, 40],
    "PC7": [-50, 50],
    "PC8": [-50, 50],
}

BOUNDS: dict[str, list] = {
    "volume.DEFAULT": [0, 6000],
    "volume.NUCLEUS": [0, 2000],
    "height.DEFAULT": [0, 21],
    "height.NUCLEUS": [0, 21],
    "area.DEFAULT": [0, 2500],
    "area.NUCLEUS": [0, 1000],
    "perimeter.DEFAULT": [0, 2000],
    "perimeter.NUCLEUS": [0, 700],
    "axis_major_length.DEFAULT": [0, 300],
    "axis_major_length.NUCLEUS": [0, 150],
    "axis_minor_length.DEFAULT": [0, 150],
    "axis_minor_length.NUCLEUS": [0, 100],
    "eccentricity.DEFAULT": [0, 1],
    "eccentricity.NUCLEUS": [0, 1],
    "orientation.DEFAULT": [-2, 2],
    "orientation.NUCLEUS": [-2, 2],
    "extent.DEFAULT": [0, 1],
    "extent.NUCLEUS": [0, 1],
    "solidity.DEFAULT": [0, 1],
    "solidity.NUCLEUS": [0, 1],
    "PC1": [-50, 50],
    "PC2": [-50, 50],
    "PC3": [-50, 50],
    "PC4": [-50, 50],
    "PC5": [-50, 50],
    "PC6": [-50, 50],
    "PC7": [-50, 50],
    "PC8": [-50, 50],
}

BANDWIDTH: dict[str, float] = {
    "volume.DEFAULT": 100,
    "volume.NUCLEUS": 50,
    "height.DEFAULT": 1,
    "height.NUCLEUS": 1,
    "area.DEFAULT": 50,
    "area.NUCLEUS": 10,
    "perimeter.DEFAULT": 50,
    "perimeter.NUCLEUS": 10,
    "axis_major_length.DEFAULT": 10,
    "axis_major_length.NUCLEUS": 5,
    "axis_minor_length.DEFAULT": 5,
    "axis_minor_length.NUCLEUS": 2,
    "eccentricity.DEFAULT": 0.01,
    "eccentricity.NUCLEUS": 0.01,
    "orientation.DEFAULT": 0.05,
    "orientation.NUCLEUS": 0.05,
    "extent.DEFAULT": 0.01,
    "extent.NUCLEUS": 0.01,
    "solidity.DEFAULT": 0.01,
    "solidity.NUCLEUS": 0.01,
    "PC1": 5,
    "PC2": 5,
    "PC3": 5,
    "PC4": 5,
    "PC5": 5,
    "PC6": 5,
    "PC7": 5,
    "PC8": 5,
}


@dataclass
class ParametersConfigFeatureComponents:
    """Parameter configuration for group cell shapes subflow - feature components."""

    features: list[str] = field(default_factory=lambda: COMPONENT_FEATURES)
    """List of shape features."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components."""

    reference_metrics: Optional[str] = None
    """Full key for reference metrics data."""

    reference_properties: Optional[str] = None
    """Full key for reference properties data."""


@dataclass
class ParametersConfigFeatureCorrelations:
    """Parameter configuration for group cell shapes subflow - feature correlations."""

    properties: list[str] = field(default_factory=lambda: CORRELATION_PROPERTIES)
    """List of shape properties."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""

    include_bins: bool = False
    """True if correlations are binned, False otherwise"""

    limits: dict[str, list] = field(default_factory=lambda: LIMITS)
    """Limits for scaling feature correlations bins."""


@dataclass
class ParametersConfigFeatureDistributions:
    """Parameter configuration for group cell shapes subflow - feature distributions."""

    reference_metrics: Optional[str] = None
    """Full key for reference metrics data."""

    reference_properties: Optional[str] = None
    """Full key for reference properties data."""

    reference_coefficients: Optional[str] = None
    """Full key for reference coefficients data."""

    reference_model: Optional[str] = None
    """Full key for reference PCA model."""

    properties: list[str] = field(default_factory=lambda: DISTRIBUTION_PROPERTIES)
    """List of shape properties."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""

    bounds: dict[str, list] = field(default_factory=lambda: BOUNDS)
    """Bounds for feature distributions."""

    bandwidth: dict[str, float] = field(default_factory=lambda: BANDWIDTH)
    """Bandwidths for feature distributions."""


@dataclass
class ParametersConfigModeCorrelations:
    """Parameter configuration for group cell shapes subflow - mode correlations."""

    reference_model: Optional[str] = None
    """Full key for reference PCA model."""

    reference_data: Optional[str] = None
    """Full key for reference coefficients data."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""


@dataclass
class ParametersConfigPopulationCounts:
    """Parameter configuration for group cell shapes subflow - population counts."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seed(s) to use for grouping population counts."""

    time: int = 0
    """Simulation time (in hours) to use for grouping population counts."""


@dataclass
class ParametersConfigPopulationStats:
    """Parameter configuration for group cell shapes subflow - population stats."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""


@dataclass
class ParametersConfigShapeAverage:
    """Parameter configuration for group cell shapes subflow - shape average."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""

    order: int = COEFFICIENT_ORDER
    """Order of the spherical harmonics coefficient parametrization."""

    scale: float = 1
    """Scaling for spherical harmonics reconstruction mesh."""

    projections: list[str] = field(default_factory=lambda: PROJECTIONS)
    """List of shape projections."""


@dataclass
class ParametersConfigShapeContours:
    """Parameter configuration for group cell shapes subflow - shape contours."""

    regions: list[Optional[str]] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    seed: int = 0
    """Simulation random seed to use for grouping shape contours."""

    time: int = 0
    """Simulation time (in hours) to use for grouping shape contours."""

    ds: Optional[float] = None
    """Spatial scaling in units/um."""

    dt: Optional[float] = None
    """Temporal scaling in hours/tick."""

    projection: str = "top"
    """Selected shape projection."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of projection bounding box."""

    slice_index: Optional[int] = None
    """Slice index along the shape projection axis."""


@dataclass
class ParametersConfigShapeErrors:
    """Parameter configuration for group cell shapes subflow - shape errors."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""


@dataclass
class ParametersConfigShapeModes:
    """Parameter configuration for group cell shapes subflow - shape modes."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""

    order: int = COEFFICIENT_ORDER
    """Order of the spherical harmonics coefficient parametrization."""

    delta: float = 0.5
    """Increment for shape mode map points."""

    projections: list[str] = field(default_factory=lambda: PROJECTIONS)
    """List of shape projections."""


@dataclass
class ParametersConfigShapeSamples:
    """Parameter configuration for group cell shapes subflow - shape samples."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    seed: int = 0
    """Simulation random seed to use for grouping shape samples."""

    tick: int = 0
    """Simulation tick to use for grouping shape samples."""

    indices: list[int] = field(default_factory=lambda: [0])
    """Cell indices for shape samples."""


@dataclass
class ParametersConfigVarianceExplained:
    """Parameter configuration for group cell shapes subflow - variance explained."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""


@dataclass
class ParametersConfig:
    """Parameter configuration for group cell shapes flow."""

    groups: list[str] = field(default_factory=lambda: GROUPS)
    """List of cell shapes groups."""

    feature_components: ParametersConfigFeatureComponents = field(
        default_factory=lambda: ParametersConfigFeatureComponents()
    )
    """Parameters for group feature components subflow."""

    feature_correlations: ParametersConfigFeatureCorrelations = field(
        default_factory=lambda: ParametersConfigFeatureCorrelations()
    )

    """Parameters for group feature correlations subflow."""

    feature_distributions: ParametersConfigFeatureDistributions = field(
        default_factory=lambda: ParametersConfigFeatureDistributions()
    )
    """Parameters for group feature distributions subflow."""

    mode_correlations: ParametersConfigModeCorrelations = field(
        default_factory=lambda: ParametersConfigModeCorrelations()
    )
    """Parameters for group mode correlations subflow."""

    population_counts: ParametersConfigPopulationCounts = field(
        default_factory=lambda: ParametersConfigPopulationCounts()
    )
    """Parameters for group population counts subflow."""

    population_stats: ParametersConfigPopulationStats = field(
        default_factory=lambda: ParametersConfigPopulationStats()
    )
    """Parameters for group population stats subflow."""

    shape_average: ParametersConfigShapeAverage = field(
        default_factory=lambda: ParametersConfigShapeAverage()
    )
    """Parameters for group shape average subflow."""

    shape_contours: ParametersConfigShapeContours = field(
        default_factory=lambda: ParametersConfigShapeContours()
    )
    """Parameters for group shape contours subflow."""

    shape_errors: ParametersConfigShapeErrors = field(
        default_factory=lambda: ParametersConfigShapeErrors()
    )
    """Parameters for group shape errors subflow."""

    shape_modes: ParametersConfigShapeModes = field(
        default_factory=lambda: ParametersConfigShapeModes()
    )
    """Parameters for group shape modes subflow."""

    shape_samples: ParametersConfigShapeSamples = field(
        default_factory=lambda: ParametersConfigShapeSamples()
    )
    """Parameters for group shape samples subflow."""

    variance_explained: ParametersConfigVarianceExplained = field(
        default_factory=lambda: ParametersConfigVarianceExplained()
    )
    """Parameters for group variance explained subflow."""


@dataclass
class ContextConfig:
    """Context configuration for group cell shapes flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for group cell shapes flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="group-cell-shapes")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main group cell shapes flow.

    Calls the following subflows, if the group is specified:

    - :py:func:`run_flow_group_feature_components`
    - :py:func:`run_flow_group_feature_correlations`
    - :py:func:`run_flow_group_feature_distributions`
    - :py:func:`run_flow_group_mode_correlations`
    - :py:func:`run_flow_group_population_counts`
    - :py:func:`run_flow_group_population_stats`
    - :py:func:`run_flow_group_shape_average`
    - :py:func:`run_flow_group_shape_contours`
    - :py:func:`run_flow_group_shape_errors`
    - :py:func:`run_flow_group_shape_modes`
    - :py:func:`run_flow_group_shape_samples`
    - :py:func:`run_flow_group_variance_explained`
    """

    if "feature_components" in parameters.groups:
        run_flow_group_feature_components(context, series, parameters.feature_components)

    if "feature_correlations" in parameters.groups:
        run_flow_group_feature_correlations(context, series, parameters.feature_correlations)

    if "feature_distributions" in parameters.groups:
        run_flow_group_feature_distributions(context, series, parameters.feature_distributions)

    if "mode_correlations" in parameters.groups:
        run_flow_group_mode_correlations(context, series, parameters.mode_correlations)

    if "population_counts" in parameters.groups:
        run_flow_group_population_counts(context, series, parameters.population_counts)

    if "population_stats" in parameters.groups:
        run_flow_group_population_stats(context, series, parameters.population_stats)

    if "shape_average" in parameters.groups:
        run_flow_group_shape_average(context, series, parameters.shape_average)

    if "shape_contours" in parameters.groups:
        run_flow_group_shape_contours(context, series, parameters.shape_contours)

    if "shape_errors" in parameters.groups:
        run_flow_group_shape_errors(context, series, parameters.shape_errors)

    if "shape_modes" in parameters.groups:
        run_flow_group_shape_modes(context, series, parameters.shape_modes)

    if "shape_samples" in parameters.groups:
        run_flow_group_shape_samples(context, series, parameters.shape_samples)

    if "variance_explained" in parameters.groups:
        run_flow_group_variance_explained(context, series, parameters.variance_explained)


@flow(name="group-cell-shapes_group-feature-components")
def run_flow_group_feature_components(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureComponents
) -> None:
    """Group cell shapes subflow for feature components."""

    analysis_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_DATA")
    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")

    # Get feature columns
    columns = [
        f"{feature}.{region}" if region != "DEFAULT" else feature
        for region in parameters.regions
        for feature in parameters.features
    ]

    # Load data.
    data_key = make_key(analysis_key, f"{series.name}.CELL_SHAPES_DATA.csv")
    data = load_dataframe.with_options(**OPTIONS)(context.working_location, data_key)

    # Fit model.
    pca_data = data[columns]
    pca_data_mean = pca_data.mean(axis=0)
    pca_data_std = pca_data.std(axis=0)
    pca_data_zscore = (pca_data - pca_data_mean) / pca_data_std
    pca = PCA(n_components=parameters.components)
    pca = pca.fit(pca_data_zscore)
    transform = pca.transform(pca_data_zscore)

    # Create output data.
    feature_components = data[["KEY"]].copy()
    feature_components.rename(columns={"KEY": "key"}, inplace=True)
    for comp in range(parameters.components):
        feature_components[f"component_{comp + 1}"] = transform[:, comp]

    # Save dataframe.
    save_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.feature_components.csv"),
        feature_components,
        index=False,
    )

    # Get reference data convex hull.
    if parameters.reference_metrics is not None and parameters.reference_properties is not None:
        index_columns = ["KEY", "ID", "SEED", "TICK"]
        reference_metrics = load_dataframe.with_options(**OPTIONS)(
            context.working_location, parameters.reference_metrics
        )
        reference_properties = load_dataframe.with_options(**OPTIONS)(
            context.working_location, parameters.reference_properties
        )

        reference_metrics.set_index(index_columns, inplace=True)
        reference_properties.set_index(index_columns, inplace=True)

        reference = reference_metrics.join(reference_properties, on=index_columns).reset_index()
        reference_zscore = (reference[columns] - pca_data_mean) / pca_data_std
        reference_transform = pca.transform(reference_zscore)

        hull = ConvexHull(reference_transform)
        points = pd.DataFrame(reference_transform[hull.vertices, :], columns=["x", "y"])

        save_dataframe(
            context.working_location,
            make_key(group_key, f"{series.name}.feature_components.REFERENCE.csv"),
            pd.DataFrame(points),
            index=False,
        )


@flow(name="group-cell-shapes_group-feature-correlations")
def run_flow_group_feature_correlations(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureCorrelations
) -> None:
    """Group cell shapes subflow for feature correlations."""

    analysis_shapes_key = make_key(series.name, "analysis", "analysis.SHAPES")
    analysis_pca_key = make_key(series.name, "analysis", "analysis.PCA")
    group_key = make_key(series.name, "groups", "groups.SHAPES")
    region_key = "_".join(sorted(parameters.regions))
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        feature_key = f"{series.name}.feature_correlations.{key}"
        series_key = f"{series.name}_{key}_{region_key}"

        # Load model.
        model_key = make_key(analysis_pca_key, f"{series_key}.PCA.pkl")
        model = load_pickle.with_options(**OPTIONS)(context.working_location, model_key)

        # Load dataframe.
        dataframe_key = make_key(analysis_shapes_key, f"{series_key}.SHAPES.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)

        # Transform data into shape mode space.
        columns = data.filter(like="shcoeffs").columns
        transform = model.transform(data[columns].values)

        for region in parameters.regions:
            correlations: list[dict[str, Union[str, float]]] = []

            for component in range(parameters.components):
                mode_key = f"PC{component + 1}"
                component_data = transform[:, component]

                for prop in parameters.properties:
                    prop_key = prop.upper()
                    prop_data = data[f"{prop}.{region}".replace(".DEFAULT", "")]

                    slope, intercept = np.polyfit(component_data, prop_data, 1)

                    correlations.append(
                        {
                            "mode": mode_key,
                            "property": prop.upper(),
                            "correlation": pearsonr(prop_data, component_data).statistic,
                            "correlation_symmetric": pearsonr(
                                prop_data, abs(component_data)
                            ).statistic,
                            "slope": slope,
                            "intercept": intercept,
                        }
                    )

                    if not parameters.include_bins:
                        continue

                    prop_limits = parameters.limits[f"{prop}.{region}"]
                    mode_limits = parameters.limits[mode_key]

                    bins = bin_to_hex(
                        component_data,
                        prop_data,
                        np.ones(len(prop_data)),
                        scale=0.025,
                        limits=(mode_limits[0], mode_limits[1], prop_limits[0], prop_limits[1]),
                    )
                    bins_df = pd.DataFrame(
                        [[x, y, np.sum(v)] for (x, y), v in bins.items()], columns=["x", "y", "v"]
                    )

                    save_dataframe(
                        context.working_location,
                        make_key(group_key, f"{feature_key}.{mode_key}.{prop_key}.{region}.csv"),
                        bins_df,
                        index=False,
                    )

            save_dataframe(
                context.working_location,
                make_key(group_key, f"{feature_key}.{region}.csv"),
                pd.DataFrame(correlations),
                index=False,
            )


@flow(name="group-cell-shapes_group-feature-distributions")
def run_flow_group_feature_distributions(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureDistributions
) -> None:
    """Group cell shapes subflow for feature distributions."""

    analysis_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_DATA")
    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    features = [
        (f"{prop}.{region}", False)
        for prop in parameters.properties
        for region in parameters.regions
    ]

    if parameters.reference_metrics is not None:
        ref_metrics = load_dataframe.with_options(**OPTIONS)(
            context.working_location, parameters.reference_metrics
        )
        features.extend(
            [
                (feature, True)
                for feature, _ in features
                if feature.replace(".DEFAULT", "") in ref_metrics.columns
            ]
        )

    if parameters.reference_properties is not None:
        ref_props = load_dataframe.with_options(**OPTIONS)(
            context.working_location, parameters.reference_properties
        )
        features.extend(
            [
                (feature, True)
                for feature, _ in features
                if feature.replace(".DEFAULT", "") in ref_props.columns
            ]
        )

    if parameters.reference_model is not None and parameters.reference_coefficients is not None:
        ref_coeffs = load_dataframe.with_options(**OPTIONS)(
            context.working_location, parameters.reference_coefficients, nrows=1
        )
        ref_model = load_pickle.with_options(**OPTIONS)(
            context.working_location, parameters.reference_model
        )
        features.extend(
            [(f"PC{component + 1}", False) for component in range(parameters.components)]
        )

    distribution_bins: dict[tuple[str, bool], dict] = {feature: {} for feature in features}
    distribution_means: dict[tuple[str, bool], dict] = {feature: {} for feature in features}
    distribution_stdevs: dict[tuple[str, bool], dict] = {feature: {} for feature in features}
    distribution_mins: dict[tuple[str, bool], dict] = {feature: {} for feature in features}
    distribution_maxs: dict[tuple[str, bool], dict] = {feature: {} for feature in features}

    for key in superkeys:
        dataframe_key = make_key(analysis_key, f"{series.name}_{key}.CELL_SHAPES_DATA.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)

        if parameters.reference_model is not None:
            transform = ref_model.transform(data[ref_coeffs.filter(like="shcoeffs").columns].values)
            for component in range(parameters.components):
                data[f"PC{component + 1}"] = transform[:, component]

        for feature, filtered in features:
            feature_column = feature.replace(".DEFAULT", "")
            values = data[feature_column].values

            if filtered:
                if ref_metrics is not None and feature_column in ref_metrics.columns:
                    ref_max = ref_metrics[feature_column].max()
                    ref_min = ref_metrics[feature_column].min()
                    values = values[(values >= ref_min) & (values <= ref_max)]

                if ref_props is not None and feature_column in ref_props.columns:
                    ref_max = ref_props[feature_column].max()
                    ref_min = ref_props[feature_column].min()
                    values = values[(values >= ref_min) & (values <= ref_max)]

            bounds = (parameters.bounds[feature][0], parameters.bounds[feature][1])
            bandwidth = parameters.bandwidth[feature]

            valid = check_data_bounds(values, bounds, f"[ {key} ] feature [ {feature} ]")

            if not valid:
                continue

            distribution_means[(feature, filtered)][key] = np.mean(values)
            distribution_stdevs[(feature, filtered)][key] = np.std(values, ddof=1)
            distribution_bins[(feature, filtered)][key] = calculate_data_bins(
                values, bounds, bandwidth
            )
            distribution_mins[(feature, filtered)][key] = np.min(values)
            distribution_maxs[(feature, filtered)][key] = np.max(values)

    for (feature, filtered), distribution in distribution_bins.items():
        distribution["*"] = {
            "bandwidth": parameters.bandwidth[feature],
            "means": distribution_means[(feature, filtered)],
            "stdevs": distribution_stdevs[(feature, filtered)],
            "mins": distribution_mins[(feature, filtered)],
            "maxs": distribution_maxs[(feature, filtered)],
        }

        feature_key = f"{feature.upper()}{'.FILTERED' if filtered else ''}"
        save_json(
            context.working_location,
            make_key(group_key, f"{series.name}.feature_distributions.{feature_key}.json"),
            distribution,
        )


@flow(name="group-cell-shapes_group-mode-correlations")
def run_flow_group_mode_correlations(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigModeCorrelations
) -> None:
    """Group cell shapes subflow for mode correlations."""

    analysis_shapes_key = make_key(series.name, "analysis", "analysis.SHAPES")
    analysis_pca_key = make_key(series.name, "analysis", "analysis.PCA")
    group_key = make_key(series.name, "groups", "groups.SHAPES")
    region_key = "_".join(sorted(parameters.regions))
    keys = [condition["key"] for condition in series.conditions]

    all_models = {}
    all_data = {}

    for key in keys:
        series_key = f"{series.name}_{key}_{region_key}"

        # Load model.
        model_key = make_key(analysis_pca_key, f"{series_key}.PCA.pkl")
        model = load_pickle.with_options(**OPTIONS)(context.working_location, model_key)
        all_models[key] = model

        # Load dataframe.
        dataframe_key = make_key(analysis_shapes_key, f"{series_key}.SHAPES.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)
        all_data[key] = data

    if parameters.reference_model is not None and parameters.reference_data is not None:
        keys.append("reference")
        all_models["reference"] = load_pickle.with_options(**OPTIONS)(
            context.working_location, parameters.reference_model
        )
        all_data["reference"] = load_dataframe.with_options(**OPTIONS)(
            context.working_location, parameters.reference_data
        )

    correlations: list[dict[str, Union[str, int, float]]] = []

    for source_key in keys:
        for target_key in keys:
            if source_key == target_key:
                continue

            # Select data sets.
            data_source = all_data[source_key]
            data_target = all_data[target_key]

            # Select models.
            model_source = all_models[source_key]
            model_target = all_models[target_key]

            # Get column order for model.
            columns_source = data_source.filter(like="shcoeffs").columns
            columns_target = data_target.filter(like="shcoeffs").columns

            # Transform the data.
            transform_source = model_source.transform(
                np.append(
                    data_source[columns_source].values,
                    data_target[columns_source].values,
                    axis=0,
                )
            )
            transform_target = model_target.transform(
                np.append(
                    data_source[columns_target].values,
                    data_target[columns_target].values,
                    axis=0,
                )
            )

            # Calculate correlations.
            correlations = correlations + [
                {
                    "source_key": source_key,
                    "target_key": target_key,
                    "source_mode": f"PC{si + 1}",
                    "target_mode": f"PC{ti + 1}",
                    "correlation": pearsonr(
                        transform_source[:, si], transform_target[:, ti]
                    ).statistic,
                }
                for si in range(parameters.components)
                for ti in range(parameters.components)
            ]

    save_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.mode_correlations.csv"),
        pd.DataFrame(correlations),
        index=False,
    )


@flow(name="group-cell-shapes_group-population-counts")
def run_flow_group_population_counts(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigPopulationCounts
) -> None:
    """Group cell shapes subflow for population counts."""

    analysis_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_DATA")
    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    counts: list[dict] = []

    for key in superkeys:
        dataframe_key = make_key(analysis_key, f"{series.name}_{key}.CELL_SHAPES_DATA.csv")
        data = load_dataframe.with_options(**OPTIONS)(
            context.working_location, dataframe_key, usecols=["KEY", "SEED", "time"]
        )
        data = data[data["SEED"].isin(parameters.seeds) & (data["time"] == parameters.time)]

        counts.extend(
            [
                {
                    "key": record["KEY"],
                    "seed": record["SEED"],
                    "count": record[0],
                }
                for record in data.groupby(["KEY", "SEED"]).size().reset_index().to_dict("records")
            ]
        )

    save_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.population_counts.{parameters.time:03d}.csv"),
        pd.DataFrame(counts).drop_duplicates(),
        index=False,
    )


@flow(name="group-cell-shapes_group-population-stats")
def run_flow_group_population_stats(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigPopulationStats
) -> None:
    """Group cell shapes subflow for population stats."""

    analysis_key = make_key(series.name, "analysis", "analysis.STATISTICS")
    group_key = make_key(series.name, "groups", "groups.SHAPES")
    region_key = "_".join(sorted(parameters.regions))
    keys = [condition["key"] for condition in series.conditions]

    stats: dict[str, dict] = {key: {} for key in keys}

    for key in keys:
        dataframe_key = make_key(analysis_key, f"{series.name}_{key}_{region_key}.STATISTICS.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)

        for feature, group in data.groupby("FEATURE"):
            feature_name = f"{feature}.DEFAULT" if feature in ["VOLUME", "HEIGHT"] else feature

            stats[key][feature_name.upper()] = {
                "size": int(group["SIZE"].sum()),
                "replicates": len(group),
                "mean": group["KS_STATISTIC"].mean(),
                "std": group["KS_STATISTIC"].std(ddof=1),
            }

    save_json(
        context.working_location,
        make_key(group_key, f"{series.name}.population_stats.json"),
        stats,
    )


@flow(name="group-cell-shapes_group-shape-average")
def run_flow_group_shape_average(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigShapeAverage
) -> None:
    """
    Group cell shapes subflow for shape average.

    Find the cell closest to the average shape. Extract original mesh slice and
    extent projections. Created reconstructed mesh and extract mesh slice and
    extent projections.
    """

    logger = get_run_logger()

    analysis_shapes_key = make_key(series.name, "analysis", "analysis.SHAPES")
    analysis_pca_key = make_key(series.name, "analysis", "analysis.PCA")
    data_key = make_key(series.name, "data", "data.LOCATIONS")
    group_key = make_key(series.name, "groups", "groups.SHAPES")
    region_key = "_".join(sorted(parameters.regions))
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        series_key = f"{series.name}_{key}_{region_key}"

        # Load model.
        model_key = make_key(analysis_pca_key, f"{series_key}.PCA.pkl")
        model = load_pickle.with_options(**OPTIONS)(context.working_location, model_key)

        # Load dataframe.
        dataframe_key = make_key(analysis_shapes_key, f"{series_key}.SHAPES.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)

        # Transform data into shape mode space.
        columns = data.filter(like="shcoeffs").columns
        transform = model.transform(data[columns].values)

        # Select the cell closest to average.
        distance, index = KDTree(transform).query([0] * parameters.components)
        selected = data.iloc[index, :]
        logger.info(
            "[ %s ] seed [ %d ] tick [ %d ] cell [ %d ] with distance [ %.2f ]",
            key,
            selected["SEED"],
            selected["TICK"],
            selected["ID"],
            distance,
        )

        # Get the matching location for the selected cell.
        series_key = f"{series.name}_{key}_{selected['SEED']:04d}"
        tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
        tar = load_tar(context.working_location, tar_key)

        # Load matching location voxels.
        locations = extract_tick_json(tar, series_key, selected["TICK"], "LOCATIONS")
        location = next(location for location in locations if location["id"] == selected["ID"])
        voxels = get_location_voxels(location)
        array = make_voxels_array(voxels)

        # Create original mesh and get projections.
        original_mesh = construct_mesh_from_array(array, array)
        original_mesh_projections = extract_mesh_projections(original_mesh)

        # Create reconstructed mesh and get projections.
        reconstructed_mesh = construct_mesh_from_coeffs(
            selected, parameters.order, scale=parameters.scale
        )
        reconstructed_mesh_projections = extract_mesh_projections(reconstructed_mesh)

        # Save json for each projection.
        for projection in parameters.projections:
            shape_average: dict[str, dict] = {
                "original_slice": original_mesh_projections[f"{projection}_slice"],
                "original_extent": original_mesh_projections[f"{projection}_extent"],
                "reconstructed_slice": reconstructed_mesh_projections[f"{projection}_slice"],
            }

            save_json(
                context.working_location,
                make_key(group_key, f"{series.name}.shape_average.{key}.{projection.upper()}.json"),
                shape_average,
            )


@flow(name="group-cell-shapes_group-shape-contours")
def run_flow_group_shape_contours(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigShapeContours
) -> None:
    """Group cell shapes subflow for shape contours."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
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
            all_contours = []

            for location in locations:
                voxels = get_location_voxels(location, None if region == "DEFAULT" else region)

                if parameters.slice_index is not None:
                    voxels = [
                        voxel
                        for voxel in voxels
                        if voxel[projection_index] == parameters.slice_index
                    ]

                if len(voxels) == 0:
                    continue

                contours = [
                    (np.array(contour) * ds).astype("int").tolist()
                    for contour in extract_voxel_contours(voxels, projection, box)
                ]

                all_contours.append({"id": location["id"], "contours": contours})

            contour_key = f"{key}.{parameters.seed:04d}.{parameters.time:03d}.{region}"
            save_json(
                context.working_location,
                make_key(
                    group_key,
                    f"{series.name}.shape_contours.{contour_key}.{projection.upper()}.json",
                ),
                all_contours,
            )


@flow(name="group-cell-shapes_group-shape-errors")
def run_flow_group_shape_errors(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigShapeErrors
) -> None:
    """Group cell shapes subflow for shape errors."""

    analysis_key = make_key(series.name, "analysis", "analysis.SHAPES")
    group_key = make_key(series.name, "groups", "groups.SHAPES")
    region_key = "_".join(sorted(parameters.regions))
    keys = [condition["key"] for condition in series.conditions]

    errors: dict[str, dict] = {key: {} for key in keys}

    for key in keys:
        dataframe_key = make_key(analysis_key, f"{series.name}_{key}_{region_key}.SHAPES.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)

        for region in parameters.regions:
            errors[key][region] = {
                "mean": data[f"mse.{region}".replace(".DEFAULT", "")].mean(),
                "std": data[f"mse.{region}".replace(".DEFAULT", "")].std(ddof=1),
            }

    save_json(
        context.working_location,
        make_key(group_key, f"{series.name}.shape_errors.json"),
        errors,
    )


@flow(name="group-cell-shapes_group-shape-modes")
def run_flow_group_shape_modes(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigShapeModes
) -> None:
    """
    Group cell shapes subflow for shape modes.

    Extract shape modes from PCAs as dictionaries of svg paths for each map
    point and projection. Consolidate shape modes from keys into single json.
    """

    analysis_data_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_DATA")
    analysis_model_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_MODELS")
    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    projections = ["top", "side1", "side2"]

    for superkey in superkeys:
        series_key = f"{series.name}_{superkey}"

        # Load model.
        model_key = make_key(analysis_model_key, f"{series_key}.CELL_SHAPES_MODELS.pkl")
        model = load_pickle.with_options(**OPTIONS)(context.working_location, model_key)

        # Load dataframe.
        dataframe_key = make_key(analysis_data_key, f"{series_key}.CELL_SHAPES_DATA.csv")
        data = load_dataframe.with_options(**OPTIONS)(context.working_location, dataframe_key)

        # Extract shape modes.
        shape_modes = extract_shape_modes(
            model,
            data,
            parameters.components,
            parameters.regions,
            parameters.order,
            parameters.delta,
        )

        for region in parameters.regions:
            shape_mode_projections: dict[str, list] = {
                f"PC{component + 1}.{projection}": []
                for component in range(parameters.components)
                for projection in parameters.projections
            }

            for shape_mode in shape_modes[region]:
                for projection in parameters.projections:
                    shape_mode_projections[f"PC{shape_mode['mode']}.{projection}"].append(
                        {
                            "point": shape_mode["point"],
                            "projection": shape_mode["projections"][f"{projection}_slice"],
                        }
                    )

            for proj_key, projections in shape_mode_projections.items():
                save_json(
                    context.working_location,
                    make_key(
                        group_key,
                        f"{series.name}.shape_modes.{superkey}.{region}.{proj_key.upper()}.json",
                    ).replace("..", "."),
                    projections,
                )


@flow(name="group-cell-shapes_group-shape-samples")
def run_flow_group_shape_samples(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigShapeSamples
) -> None:
    """
    Group cell shapes subflow for shape samples.

    Extract sample cell shapes from specified simulations. Construct wireframes
    from the cell shape mesh.
    """

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    group_key = make_key(series.name, "groups", "groups.SHAPES")
    keys = [condition["key"] for condition in series.conditions]

    shape_samples: dict[str, dict] = {}

    for key in keys:
        shape_samples[key] = {region: [] for region in parameters.regions}

        # Load location data.
        series_key = f"{series.name}_{key}_{parameters.seed:04d}"
        tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
        tar = load_tar(context.working_location, tar_key)
        locations = extract_tick_json(tar, series_key, parameters.tick, "LOCATIONS")

        for index in parameters.indices:
            location = locations[index]

            for region in parameters.regions:
                voxels = get_location_voxels(location)
                array = make_voxels_array(voxels)

                if region != "DEFAULT":
                    region_voxels = get_location_voxels(locations[index], region)
                    region_array = make_voxels_array(region_voxels, reference=voxels)
                    mesh = construct_mesh_from_array(region_array, array)
                else:
                    mesh = construct_mesh_from_array(array, array)

                shape_samples[key][region].append(extract_mesh_wireframe(mesh))

    save_json(
        context.working_location,
        make_key(group_key, f"{series.name}.shape_samples.json"),
        shape_samples,
    )


@flow(name="group-cell-shapes_group-variance-explained")
def run_flow_group_variance_explained(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigVarianceExplained
) -> None:
    """Group cell shapes subflow for variance explained."""

    analysis_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_MODELS")
    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")

    keys = [condition["key"] for condition in series.conditions]
    superkeys = {key_group for key in keys for key_group in key.split("_")}

    variance = []

    for superkey in superkeys:
        model_key = make_key(analysis_key, f"{series.name}_{superkey}.CELL_SHAPES_MODELS.pkl")
        model = load_pickle.with_options(**OPTIONS)(context.working_location, model_key)

        variance.append(
            pd.DataFrame(
                {
                    "key": [superkey] * parameters.components,
                    "mode": [f"PC{i}" for i in range(1, parameters.components + 1)],
                    "variance": model.explained_variance_ratio_,
                }
            )
        )

    save_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.variance_explained.csv"),
        pd.concat(variance),
        index=False,
    )
