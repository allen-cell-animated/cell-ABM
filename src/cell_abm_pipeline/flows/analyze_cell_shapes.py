"""
Workflow for analyzing cell shapes.

Working location structure:

.. code-block:: bash

    (name)
    ├── analysis
    │   ├── analysis.BASIC_METRICS
    │   │   └── (name)_(key).BASIC_METRICS.csv
    │   ├── analysis.CELL_SHAPES_COEFFICIENTS
    │   │   └── (name)_(key).CELL_SHAPES_COEFFICIENTS.csv
    │   ├── analysis.CELL_SHAPES_DATA
    │   │   └── (name)_(key).CELL_SHAPES_DATA.csv
    │   ├── analysis.CELL_SHAPES_MODELS
    │   │   └── (name)_(key).CELL_SHAPES_MODELS.pkl
    │   ├── analysis.CELL_SHAPES_PROPERTIES
    │   │   └── (name)_(key).CELL_SHAPES_PROPERTIES.csv
    │   └── analysis.CELL_SHAPES_STATISTICS
    │       └── (name)_(key).CELL_SHAPES_STATISTICS.csv
    └── calculations
        ├── calculations.COEFFICIENTS
        │   ├── (name)_(key)_(seed)_(region).COEFFICIENTS.csv
        │   └── (name)_(key)_(seed)_(region).COEFFICIENTS.tar.xz
        └── calculations.PROPERTIES
            ├── (name)_(key)_(seed)_(region).PROPERTIES.csv
            └── (name)_(key)_(seed)_(region).PROPERTIES.tar.xz

Data from **calculations.PROPERTIES** are processed into
**analysis.CELL_SHAPES_PROPERTIES**. Data from **calculations.COEFFICIENTS** are
processed into **analysis.CELL_SHAPES_COEFFICIENTS**. Data from
**analysis.BASIC_METRICS** are combined with data from
**analysis.CELL_SHAPES_PROPERTIES** and **analysis.CELL_SHAPES_COEFFICIENTS**
into **analysis.CELL_SHAPES_DATA**. PCA models are saved to
**analysis.CELL_SHAPES_MODELS**. Statistical analysis is saved to
**analysis.CELL_SHAPES_STATISTICS**.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

import numpy as np
import pandas as pd
from abm_shape_collection import (
    calculate_feature_statistics,
    calculate_shape_statistics,
    fit_pca_model,
)
from arcade_collection.output import convert_model_units
from io_collection.keys import check_key, group_keys, make_key
from io_collection.load import load_dataframe, load_pickle
from io_collection.save import save_dataframe, save_pickle
from prefect import flow, get_run_logger
from prefect.tasks import task_input_hash

OPTIONS = {
    "cache_result_in_memory": False,
    "cache_key_fn": task_input_hash,
    "cache_expiration": timedelta(hours=12),
}

PCA_COMPONENTS = 8

INDEX_COLUMNS = ["KEY", "ID", "SEED", "TICK"]

VALID_PHASES = ["PROLIFERATIVE_G1", "PROLIFERATIVE_S", "PROLIFERATIVE_G2"]


@dataclass
class ParametersConfig:
    """Parameter configuration for analyze cell shapes flow."""

    reference: dict | None = None
    """Dictionary of keys for reference data and model for statistics."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    components: int = PCA_COMPONENTS
    """Number of principal components (i.e. shape modes)."""

    ds: float | None = None
    """Spatial scaling in um/voxel."""

    dt: float | None = None
    """Temporal scaling in hours/tick."""

    valid_phases: list[str] = field(default_factory=lambda: VALID_PHASES)
    """Valid phases for processing cell shapes."""

    valid_times: list[int] = field(default_factory=lambda: [0])
    """Valid times for processing cell shapes."""

    sample_replicates: int = 100
    """Number of replicates for calculating stats with sampling."""

    sample_size: int = 100
    """Sample size for each tick for calculating stats with sampling."""

    outlier: float | None = None
    """Standard deviation threshold for outliers."""

    features: list[str] = field(default_factory=list)
    """List of features."""


@dataclass
class ContextConfig:
    """Context configuration for analyze cell shapes flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for analyze cell shapes flow."""

    name: str
    """Name of the simulation series."""

    seeds: list[int]
    """List of series random seeds."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="analyze-cell-shapes")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main analyze cell shapes flow.

    Calls the following subflows, in order:

    1. :py:func:`run_flow_process_properties`
    2. :py:func:`run_flow_process_coefficients`
    3. :py:func:`run_flow_combine_data`
    4. :py:func:`run_flow_fit_models`
    5. :py:func:`run_flow_analyze_stats`
    """

    run_flow_process_properties(context, series, parameters)

    run_flow_process_coefficients(context, series, parameters)

    run_flow_combine_data(context, series, parameters)

    run_flow_fit_models(context, series, parameters)

    run_flow_analyze_stats(context, series, parameters)


@flow(name="analyze-cell-shapes_process-properties")
def run_flow_process_properties(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Analyze cell shapes subflow for processing properties.

    Processes cell shape properties and compiles into a single dataframe. If the
    combined dataframe already exists for a given key, that key is skipped.
    """

    logger = get_run_logger()

    tag = "CELL_SHAPES_PROPERTIES"

    props_path_key = make_key(series.name, "calculations", "calculations.PROPERTIES")
    analysis_path_key = make_key(series.name, "analysis", f"analysis.{tag}")

    key_groups = group_keys([condition["key"] for condition in series.conditions])

    for key_group, keys in key_groups.items():
        logger.info("Processing properties for key group [ %s ]", key_group)
        analysis_key = make_key(analysis_path_key, f"{series.name}_{key_group}.{tag}.csv")

        if check_key(context.working_location, analysis_key):
            continue

        all_props = []

        for key in keys:
            for seed in series.seeds:
                props_key_template = f"{series.name}_{key}_{seed:04d}_%s.PROPERTIES.csv"
                props = None

                for region in parameters.regions:
                    props_key = make_key(props_path_key, props_key_template % region)
                    props_key = props_key.replace("_DEFAULT", "")

                    props_df = load_dataframe.with_options(**OPTIONS)(
                        context.working_location, props_key, converters={"KEY": str}
                    )
                    props_df = props_df.set_index(INDEX_COLUMNS)

                    if props is None:
                        props = props_df
                        if region != "DEFAULT":
                            props = props.add_suffix(f".{region}")
                    else:
                        props = props.join(props_df, on=INDEX_COLUMNS, rsuffix=f".{region}")

                all_props.append(props)

        # Combine into single dataframe.
        props_df = pd.concat(all_props).reset_index()

        # Convert units.
        convert_model_units(props_df, parameters.ds, parameters.dt, parameters.regions)

        # Save final dataframe.
        save_dataframe(context.working_location, analysis_key, props_df, index=False)


@flow(name="analyze-cell-shapes_process-coefficients")
def run_flow_process_coefficients(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Analyze cell shapes subflow for processing coefficients.

    Processes cell shape spherical harmonics coefficients and compiles into a
    single dataframe. If the combined dataframe already exists for a given key,
    that key is skipped.
    """

    logger = get_run_logger()

    tag = "CELL_SHAPES_COEFFICIENTS"

    coeffs_path_key = make_key(series.name, "calculations", "calculations.COEFFICIENTS")
    analysis_path_key = make_key(series.name, "analysis", f"analysis.{tag}")

    key_groups = group_keys([condition["key"] for condition in series.conditions])

    for key_group, keys in key_groups.items():
        logger.info("Processing coefficients for key group [ %s ]", key_group)
        analysis_key = make_key(analysis_path_key, f"{series.name}_{key_group}.{tag}.csv")

        if check_key(context.working_location, analysis_key):
            continue

        all_coeffs = []

        for key in keys:
            for seed in series.seeds:
                coeffs_key_template = f"{series.name}_{key}_{seed:04d}_%s.COEFFICIENTS.csv"
                coeffs = None

                for region in parameters.regions:
                    coeffs_key = make_key(coeffs_path_key, coeffs_key_template % region)
                    coeffs_key = coeffs_key.replace("_DEFAULT", "")

                    coeffs_df = load_dataframe.with_options(**OPTIONS)(
                        context.working_location, coeffs_key, converters={"KEY": str}
                    )
                    coeffs_df = coeffs_df.set_index(INDEX_COLUMNS)

                    if coeffs is None:
                        coeffs = coeffs_df
                        if region != "DEFAULT":
                            coeffs = coeffs.add_suffix(f".{region}")
                    else:
                        coeffs = coeffs.join(coeffs_df, on=INDEX_COLUMNS, rsuffix=f".{region}")

                all_coeffs.append(coeffs)

        # Combine into single dataframe.
        coeffs_df = pd.concat(all_coeffs).reset_index()

        # Convert units.
        convert_model_units(coeffs_df, parameters.ds, parameters.dt, parameters.regions)

        # Save final dataframe.
        save_dataframe(context.working_location, analysis_key, coeffs_df, index=False)


@flow(name="analyze-cell-shapes_combine-data")
def run_flow_combine_data(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Analyze cell shapes subflow for combining data.

    Combine processed spherical harmonics coefficients, cell shape properties,
    and parsed simulation results into a single dataframe that can be used for
    PCA. If the combined dataframe already exists for a given key, that key is
    skipped.
    """

    logger = get_run_logger()
    tag = "CELL_SHAPES_DATA"

    metrics_path_key = make_key(series.name, "analysis", "analysis.BASIC_METRICS")
    props_path_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_PROPERTIES")
    coeffs_path_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_COEFFICIENTS")
    analysis_path_key = make_key(series.name, "analysis", f"analysis.{tag}")

    keys = [condition["key"] for condition in series.conditions]
    key_groups = group_keys(keys)

    for key_group in key_groups:
        logger.info("Combining data for key group [ %s ]", key_group)

        key_template = f"{series.name}_{key_group}.%s.csv"
        analysis_key = make_key(analysis_path_key, key_template % tag)

        if check_key(context.working_location, analysis_key):
            continue

        metrics_key = make_key(metrics_path_key, key_template % "BASIC_METRICS")
        metrics = load_dataframe.with_options(**OPTIONS)(context.working_location, metrics_key)
        metrics = metrics.set_index(INDEX_COLUMNS)

        props_key = make_key(props_path_key, key_template % "CELL_SHAPES_PROPERTIES")
        if check_key(context.working_location, props_key):
            props = load_dataframe.with_options(**OPTIONS)(context.working_location, props_key)
            props = props.drop("time", axis=1, errors="ignore")
            props = props.set_index(INDEX_COLUMNS)
        else:
            props = None

        coeffs_key = make_key(coeffs_path_key, key_template % "CELL_SHAPES_COEFFICIENTS")
        if check_key(context.working_location, coeffs_key):
            coeffs = load_dataframe.with_options(**OPTIONS)(context.working_location, coeffs_key)
            coeffs = coeffs.drop("time", axis=1, errors="ignore")
            coeffs = coeffs.set_index(INDEX_COLUMNS)
        else:
            coeffs = None

        # Skip if both coefficients and properties are missing.
        if props is None and coeffs is None:
            continue

        # Filter coefficient outliers.
        if parameters.outlier is not None and coeffs is not None:
            outlier_filter = abs(coeffs - coeffs.mean()) <= parameters.outlier * coeffs.std(ddof=1)
            coeffs = coeffs[outlier_filter].dropna()

        # Join metrics, coefficients, and properties data.
        if props is None:
            data = metrics.join(coeffs, on=INDEX_COLUMNS).reset_index()
        elif coeffs is None:
            data = metrics.join(props, on=INDEX_COLUMNS).reset_index()
        else:
            data = metrics.join(props, on=INDEX_COLUMNS)
            data = data.join(coeffs, on=INDEX_COLUMNS).reset_index()

        # Filter for cell phase and selected ticks.
        data = data[data["PHASE"].isin(parameters.valid_phases)]
        data = data[data["time"].isin(parameters.valid_times)]

        # Remove nans.
        nan_indices = np.isnan(data.filter(like="shcoeff")).any(axis=1)
        data = data[~nan_indices]
        nan_indices = np.isnan(data.filter(like="CENTER")).any(axis=1)
        data = data[~nan_indices]

        # Save final dataframe.
        save_dataframe(context.working_location, analysis_key, data, index=False)

    # Save final combined dataframe with all data.
    combined_key = make_key(analysis_path_key, f"{series.name}.{tag}.csv")

    if check_key(context.working_location, combined_key):
        return

    logger.info("Combining data for all keys")

    combined_template = make_key(analysis_path_key, f"{series.name}_%s.{tag}.csv")
    combined_data = [
        load_dataframe(context.working_location, combined_template % key_group)
        for key_group in sorted({key.split("_")[0] for key in keys})
    ]

    save_dataframe(context.working_location, combined_key, pd.concat(combined_data), index=False)


@flow(name="analyze-cell-shapes_fit-models")
def run_flow_fit_models(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Analyze cell shapes subflow for fitting PCA model.

    Fit PCA for each key and save the resulting PCA object as a pickle. If the
    model already exits for a given key, that key is skipped.
    """

    logger = get_run_logger()

    data_path_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_DATA")
    model_path_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_MODELS")

    key_groups = group_keys([condition["key"] for condition in series.conditions])

    for key_group in key_groups:
        logger.info("Fitting models for key group [ %s ]", key_group)

        key_template = f"{series.name}_{key_group}.%s"
        data_key = make_key(data_path_key, key_template % "CELL_SHAPES_DATA.csv")
        model_key = make_key(model_path_key, key_template % "CELL_SHAPES_MODELS.pkl")

        if check_key(context.working_location, model_key):
            continue

        data = load_dataframe.with_options(**OPTIONS)(context.working_location, data_key)
        ordering = data["volume"].to_numpy()

        # Get coefficient columns
        coeff_columns = [
            column
            for column in data.filter(like="shcoeff")
            if ("." not in column and "DEFAULT" in parameters.regions)
            or ("." in column and column.split(".")[1] in parameters.regions)
        ]
        coeffs = data[coeff_columns].to_numpy()

        if not coeffs.any():
            continue

        # Fit model for shape modes.
        model = fit_pca_model(coeffs, parameters.components, ordering)

        # Save models.
        save_pickle(context.working_location, model_key, model)


@flow(name="analyze-cell-shapes_analyze-stats")
def run_flow_analyze_stats(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Analyze cell shapes subflow for analyzing distribution statistics.

    Perform statistical analysis of shape distributions. If the analysis file
    already exists for a given key, that key is skipped.
    """

    logger = get_run_logger()

    data_path_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_DATA")
    stats_path_key = make_key(series.name, "analysis", "analysis.CELL_SHAPES_STATISTICS")

    key_groups = group_keys([condition["key"] for condition in series.conditions])

    if parameters.reference is None:
        return

    ref_data = load_dataframe.with_options(**OPTIONS)(
        context.working_location, parameters.reference["data"]
    )
    ref_model = load_pickle.with_options(**OPTIONS)(
        context.working_location, parameters.reference["model"]
    )

    features = [
        f"{feature}.{region}" if region != "DEFAULT" else feature
        for region in parameters.regions
        for feature in parameters.features
    ]

    for key_group in key_groups:
        logger.info("Fitting models for key group [ %s ]", key_group)

        key_template = f"{series.name}_{key_group}.%s"
        data_key = make_key(data_path_key, key_template % "CELL_SHAPES_DATA.csv")
        stats_key = make_key(stats_path_key, key_template % "CELL_SHAPES_STATISTICS.csv")

        if check_key(context.working_location, stats_key):
            continue

        data = load_dataframe.with_options(**OPTIONS)(context.working_location, data_key)

        all_stats = []

        contains_features = all(feature in data.columns for feature in features)
        contains_coeffs = any(column for column in data.columns if "shcoeff" in column)

        for sample in range(parameters.sample_replicates):
            sample_data = (
                data.sample(frac=1, random_state=sample)
                .groupby("time")
                .head(parameters.sample_size)
            )

            if contains_features:
                feature_stats = calculate_feature_statistics(features, sample_data, ref_data)
            else:
                feature_stats = pd.DataFrame()

            if contains_coeffs:
                shape_stats = calculate_shape_statistics(
                    ref_model, sample_data, ref_data, parameters.components
                )
            else:
                shape_stats = pd.DataFrame()

            stats = pd.concat([feature_stats, shape_stats])
            stats["INDEX"] = sample

            all_stats.append(stats)

        all_stats_df = pd.concat(all_stats)

        save_dataframe(context.working_location, stats_key, all_stats_df, index=False)
