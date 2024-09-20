"""
Workflow for plotting cell shapes.

Working location structure:

.. code-block:: bash

    (name)
    ├── groups
    │   └── groups.CELL_SHAPES
    │       ├── (name).feature_correlations.(key).(region).csv
    │       ├── (name).feature_distributions.(feature).json
    │       ├── (name).mode_correlations.csv
    │       ├── (name).population_counts.(tick).csv
    │       ├── (name).population_stats.json
    │       ├── (name).shape_average.(key).(projection).json
    │       ├── (name).shape_errors.json
    │       ├── (name).shape_modes.(key).(region).(mode).(projection).json
    │       └── (name).variance_explained.csv
    └── plots
        └── plots.CELL_SHAPES
            ├── (name).feature_correlations.(key).(region).png
            ├── (name).feature_distributions.(feature).png
            ├── (name).mode_correlations.(key).(key).png
            ├── (name).population_counts.(tick).png
            ├── (name).population_stats.png
            ├── (name).shape_average.(key).(projection).svg
            ├── (name).shape_errors.png
            ├── (name).shape_modes.(key).(region).(mode).(projection).(point).svg
            └── (name).variance_explained.png

Plots use grouped data from **groups.CELL_SHAPES**. Plots are saved to
**plots.CELL_SHAPES**.
"""

from dataclasses import dataclass, field

import numpy as np
from io_collection.keys import make_key
from io_collection.load import load_dataframe, load_json
from io_collection.save import save_figure, save_text
from prefect import flow

from cell_abm_pipeline.flows.analyze_cell_shapes import PCA_COMPONENTS
from cell_abm_pipeline.flows.group_cell_shapes import (
    CORRELATION_PROPERTIES,
    DISTRIBUTION_PROPERTIES,
    PROJECTIONS,
)
from cell_abm_pipeline.tasks import (
    build_svg_image,
    make_bar_figure,
    make_heatmap_figure,
    make_histogram_figure,
    make_line_figure,
)

PLOTS: list[str] = [
    "feature_correlations",
    "feature_distributions",
    "mode_correlations",
    "population_counts",
    "population_stats",
    "shape_average",
    "shape_errors",
    "shape_modes",
    "variance_explained",
]


REGION_COLORS: dict[str, str] = {"DEFAULT": "#FF00FF", "NUCLEUS": "#00FFFF"}

KEY_COLORS: list[str] = [
    "#7F3C8D",
    "#11A579",
    "#3969AC",
    "#F2B701",
    "#E73F74",
    "#80BA5A",
    "#E68310",
    "#008695",
    "#CF1C90",
    "#f97b72",
    "#4b4b8f",
    "#A5AA99",
]


@dataclass
class ParametersConfigFeatureCorrelations:
    """Parameter configuration for plot cell shapes subflow - feature correlations."""

    properties: list[str] = field(default_factory=lambda: CORRELATION_PROPERTIES)
    """List of shape properties."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""


@dataclass
class ParametersConfigFeatureDistributions:
    """Parameter configuration for plot cell shapes subflow - feature distributions."""

    properties: list[str] = field(default_factory=lambda: DISTRIBUTION_PROPERTIES)
    """List of shape properties."""

    regions: list[str] = field(default_factory=lambda: ["(region)"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""


@dataclass
class ParametersConfigModeCorrelations:
    """Parameter configuration for plot cell shapes subflow - mode correlations."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""


@dataclass
class ParametersConfigPopulationCounts:
    """Parameter configuration for plot cell shapes subflow - population counts."""

    tick: int = 0
    """Simulation tick to use for plotting population counts."""


@dataclass
class ParametersConfigPopulationStats:
    """Parameter configuration for plot cell shapes subflow - population stats."""


@dataclass
class ParametersConfigShapeAverage:
    """Parameter configuration for plot cell shapes subflow - shape average."""

    projections: list[str] = field(default_factory=lambda: PROJECTIONS)
    """List of shape projections."""

    box: tuple[int, int] = field(default_factory=lambda: (100, 100))
    """Size of bounding box."""

    scale: float = 1
    """Scaling for image."""


@dataclass
class ParametersConfigShapeErrors:
    """Parameter configuration for plot cell shapes subflow - shape errors."""


@dataclass
class ParametersConfigShapeModes:
    """Parameter configuration for plot cell shapes subflow - shape modes."""

    regions: list[str] = field(default_factory=lambda: ["(region)"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""

    projections: list[str] = field(default_factory=lambda: PROJECTIONS)
    """List of shape projections."""

    point: float = 0
    """Selected shape mode map point."""

    box: tuple[int, int] = field(default_factory=lambda: (100, 100))
    """Size of bounding box."""

    scale: float = 1
    """Scaling for image."""

    colors: dict[str, str] = field(default_factory=lambda: REGION_COLORS)
    """Colors for each region."""


@dataclass
class ParametersConfigVarianceExplained:
    """Parameter configuration for plot cell shapes subflow - variance explained."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""

    colors: list[str] = field(default_factory=lambda: KEY_COLORS)
    """Colors for each key."""


@dataclass
class ParametersConfig:
    """Parameter configuration for plot cell shapes flow."""

    plots: list[str] = field(default_factory=lambda: PLOTS)
    """List of cell shape plots."""

    feature_correlations: ParametersConfigFeatureCorrelations = field(
        default_factory=lambda: ParametersConfigFeatureCorrelations()
    )
    """Parameters for plot feature correlations subflow."""

    feature_distributions: ParametersConfigFeatureDistributions = field(
        default_factory=lambda: ParametersConfigFeatureDistributions()
    )
    """Parameters for plot feature distributions subflow."""

    mode_correlations: ParametersConfigModeCorrelations = field(
        default_factory=lambda: ParametersConfigModeCorrelations()
    )
    """Parameters for plot mode correlations subflow."""

    population_counts: ParametersConfigPopulationCounts = field(
        default_factory=lambda: ParametersConfigPopulationCounts()
    )
    """Parameters for plot population counts subflow."""

    population_stats: ParametersConfigPopulationStats = field(
        default_factory=lambda: ParametersConfigPopulationStats()
    )
    """Parameters for plot population stats subflow."""

    shape_average: ParametersConfigShapeAverage = field(
        default_factory=lambda: ParametersConfigShapeAverage()
    )
    """Parameters for plot shape average subflow."""

    shape_errors: ParametersConfigShapeErrors = field(
        default_factory=lambda: ParametersConfigShapeErrors()
    )
    """Parameters for plot shape errors subflow."""

    shape_modes: ParametersConfigShapeModes = field(
        default_factory=lambda: ParametersConfigShapeModes()
    )
    """Parameters for plot shape modes subflow."""

    variance_explained: ParametersConfigVarianceExplained = field(
        default_factory=lambda: ParametersConfigVarianceExplained()
    )
    """Parameters for plot variance explained subflow."""


@dataclass
class ContextConfig:
    """Context configuration for plot cell shapes flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for plot cell shapes flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="plot-cell-shapes")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main plot cell shapes flow.

    Calls the following subflows, if the plot is specified:

    - :py:func:`run_flow_plot_feature_correlations`
    - :py:func:`run_flow_plot_feature_distributions`
    - :py:func:`run_flow_plot_mode_correlations`
    - :py:func:`run_flow_plot_population_counts`
    - :py:func:`run_flow_plot_population_stats`
    - :py:func:`run_flow_plot_shape_average`
    - :py:func:`run_flow_plot_shape_errors`
    - :py:func:`run_flow_plot_shape_modes`
    - :py:func:`run_flow_plot_variance_explained`
    """

    if "feature_correlations" in parameters.plots:
        run_flow_plot_feature_correlations(context, series, parameters.feature_correlations)

    if "feature_distributions" in parameters.plots:
        run_flow_plot_feature_distributions(context, series, parameters.feature_distributions)

    if "mode_correlations" in parameters.plots:
        run_flow_plot_mode_correlations(context, series, parameters.mode_correlations)

    if "population_counts" in parameters.plots:
        run_flow_plot_population_counts(context, series, parameters.population_counts)

    if "population_stats" in parameters.plots:
        run_flow_plot_population_stats(context, series, parameters.population_stats)

    if "shape_average" in parameters.plots:
        run_flow_plot_shape_average(context, series, parameters.shape_average)

    if "shape_errors" in parameters.plots:
        run_flow_plot_shape_errors(context, series, parameters.shape_errors)

    if "shape_modes" in parameters.plots:
        run_flow_plot_shape_modes(context, series, parameters.shape_modes)

    if "variance_explained" in parameters.plots:
        run_flow_plot_variance_explained(context, series, parameters.variance_explained)


@flow(name="plot-cell-shapes_plot-feature-correlations")
def run_flow_plot_feature_correlations(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureCorrelations
) -> None:
    """Plot cell shapes subflow for feature correlations."""

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
    keys = [condition["key"] for condition in series.conditions]

    modes = [f"PC{component + 1}" for component in range(parameters.components)]
    properties = [prop.upper() for prop in parameters.properties]

    for key in keys:
        for region in parameters.regions:
            group = load_dataframe(
                context.working_location,
                make_key(group_key, f"{series.name}.feature_correlations.{key}.{region}.csv"),
            )

            group_sorted = group.set_index(["property", "mode"]).sort_index()
            group_values = [
                [abs(group_sorted.loc[prop, mode]["correlation"]) for mode in modes]
                for prop in properties
            ]

            save_figure(
                context.working_location,
                make_key(plot_key, f"{series.name}.feature_correlations.{key}.{region}.png"),
                make_heatmap_figure(properties, modes, group_values),
            )


@flow(name="plot-cell-shapes_plot-feature-distributions")
def run_flow_plot_feature_distributions(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigFeatureDistributions
) -> None:
    """Plot cell shapes subflow for feature distributions."""

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
    keys = [condition["key"] for condition in series.conditions]

    features = [
        f"{prop}.{region}" for prop in parameters.properties for region in parameters.regions
    ] + [f"PC{component + 1}" for component in range(parameters.components)]

    for feature in features:
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


@flow(name="plot-cell-shapes_plot-mode-correlations")
def run_flow_plot_mode_correlations(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigModeCorrelations
) -> None:
    """Plot cell shapes subflow for mode correlations."""

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
    keys = ["reference"] + [condition["key"] for condition in series.conditions]

    modes = [f"PC{component + 1}" for component in range(parameters.components)]

    group = load_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.mode_correlations.csv"),
    )

    for source_key in keys:
        for target_key in keys:
            if source_key == target_key:
                continue

            group_sorted = (
                group[(group["source_key"] == source_key) & (group["target_key"] == target_key)]
                .set_index(["source_mode", "target_mode"])
                .sort_index()
            )
            group_values = [
                [
                    abs(group_sorted.loc[source_mode, target_mode]["correlation"])
                    for target_mode in modes
                ]
                for source_mode in modes
            ]

            save_figure(
                context.working_location,
                make_key(
                    plot_key, f"{series.name}.mode_correlations.{source_key}.{target_key}.png"
                ),
                make_heatmap_figure(modes, modes, group_values),
            )


@flow(name="plot-cell-shapes_plot-population-counts")
def run_flow_plot_population_counts(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigPopulationCounts
) -> None:
    """Plot cell shapes subflow for population counts."""

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
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


@flow(name="plot-cell-shapes_plot-population-stats")
def run_flow_plot_population_stats(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigPopulationStats
) -> None:
    """Plot cell shapes subflow for population stats."""

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
    keys = [condition["key"] for condition in series.conditions]

    group = load_json(
        context.working_location,
        make_key(group_key, f"{series.name}.population_stats.json"),
    )

    key_group = {key: group[key] for key in keys}

    save_figure(
        context.working_location,
        make_key(plot_key, f"{series.name}.population_stats.png"),
        make_bar_figure(keys, key_group),
    )


@flow(name="plot-cell-shapes_plot-shape-average")
def run_flow_plot_shape_average(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigShapeAverage
) -> None:
    """
    Plot cell shapes subflow for shape average.
    """

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for projection in parameters.projections:
            group = load_json(
                context.working_location,
                make_key(group_key, f"{series.name}.shape_average.{key}.{projection.upper()}.json"),
            )

            assert isinstance(group, dict)

            elements = [
                {"points": item, "stroke": "#000", "stroke-width": 0.2}
                for item in group["original_slice"]
            ]

            elements = elements + [
                {
                    "points": item,
                    "stroke": "#f00",
                    "stroke-width": 0.2,
                    "stroke-dasharray": "0.2,0.2",
                }
                for item in group["reconstructed_slice"]
            ]

            for extent in group["original_extent"].values():
                elements = elements + [
                    {
                        "points": item,
                        "stroke": "#999",
                        "stroke-width": 0.05,
                    }
                    for item in extent
                ]

            rotate = 0 if projection == "top" else 90

            save_text(
                context.working_location,
                make_key(plot_key, f"{series.name}.shape_average.{key}.{projection.upper()}.svg"),
                build_svg_image(elements, *parameters.box, rotate, parameters.scale),
            )


@flow(name="plot-cell-shapes_plot-shape-errors")
def run_flow_plot_shape_errors(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigShapeErrors
) -> None:
    """Plot cell shapes subflow for shape errors."""

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
    keys = [condition["key"] for condition in series.conditions]

    group = load_json(
        context.working_location,
        make_key(group_key, f"{series.name}.shape_errors.json"),
    )

    key_group = {key: group[key] for key in keys}

    save_figure(
        context.working_location,
        make_key(plot_key, f"{series.name}.shape_errors.png"),
        make_bar_figure(keys, key_group),
    )


@flow(name="plot-cell-shapes_plot-shape-modes")
def run_flow_plot_shape_modes(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigShapeModes
) -> None:
    """
    Plot cell shapes subflow for shape modes.
    """

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for component in range(parameters.components):
            for projection in parameters.projections:
                rotate = 0 if projection == "top" else 90
                elements: list[dict] = []

                for region in parameters.regions:
                    full_key = f"{key}.{region}.PC{component+1}.{projection.upper()}"

                    group = load_json(
                        context.working_location,
                        make_key(group_key, f"{series.name}.shape_modes.{full_key}.json"),
                    )

                    assert isinstance(group, list)

                    elements = elements + [
                        {
                            "points": item["projection"][0],
                            "stroke": parameters.colors[region],
                            "stroke-width": 2,
                        }
                        for item in group
                        if item["point"] == parameters.point
                    ]

                if parameters.point > 0:
                    point_key = "P" + f"{round(parameters.point*100):03d}"
                elif parameters.point < 0:
                    point_key = "N" + f"{round(-parameters.point*100):03d}"
                else:
                    point_key = "ZERO"

                save_text(
                    context.working_location,
                    make_key(plot_key, f"{series.name}.shape_modes.{full_key}.{point_key}.svg"),
                    build_svg_image(elements, *parameters.box, rotate, parameters.scale),
                )


@flow(name="plot-cell-shapes_plot-variance-explained")
def run_flow_plot_variance_explained(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigVarianceExplained
) -> None:
    """Plot cell shapes subflow for variance explained."""

    group_key = make_key(series.name, "groups", "groups.CELL_SHAPES")
    plot_key = make_key(series.name, "plots", "plots.CELL_SHAPES")
    keys = [condition["key"] for condition in series.conditions]

    group = load_dataframe(
        context.working_location,
        make_key(group_key, f"{series.name}.variance_explained.csv"),
    )

    group_flat = [
        {
            "x": [component + 1 for component in range(8)],
            "y": np.cumsum(group[group["key"] == key].sort_values("mode")["variance"].values),
            "color": parameters.colors[keys.index(key)],
        }
        for key in keys
    ]

    save_figure(
        context.working_location,
        make_key(plot_key, f"{series.name}.variance_explained.png"),
        make_line_figure(group_flat),
    )
