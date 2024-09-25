"""
Workflow for analyzing colony dynamics.

Working location structure:

.. code-block:: bash

    (name)
    └── analysis
        ├── analysis.COLONIES
        │   └── (name)_(key).COLONIES.csv
        ├── analysis.MEASURES
        │   └── (name)_(key).MEASURES.csv
        ├── analysis.NEIGHBORS
        │   ├── (name)_(key)_(seed).NEIGHBORS.csv
        │   └── (name)_(key)_(seed).NEIGHBORS.tar.xz
        └── analysis.NETWORKS
            └── (name)_(key).NETWORKS.pkl

Data from **analysis.NEIGHBORS** are processed into **analysis.COLONIES**.
Networks are saved to **analysis.NETWORKS**. Graph analysis is saved to
**analysis.MEASURES**.

TODO: update for new calculate_neighbors flow
"""

import ast
from dataclasses import dataclass, field
from datetime import timedelta

import pandas as pd
from abm_colony_collection import (
    calculate_centrality_measures,
    calculate_degree_measures,
    calculate_distance_measures,
    convert_to_network,
)
from arcade_collection.output import convert_model_units
from io_collection.keys import check_key, make_key
from io_collection.load import load_dataframe, load_pickle
from io_collection.save import save_dataframe, save_pickle
from prefect import flow
from prefect.tasks import task_input_hash

OPTIONS = {
    "cache_result_in_memory": False,
    "cache_key_fn": task_input_hash,
    "cache_expiration": timedelta(hours=12),
}

INDEX_COLUMNS = ["KEY", "ID", "SEED", "TICK"]


@dataclass
class ParametersConfig:
    """Parameter configuration for analyze colony dynamics flow."""

    ds: float = 1.0
    """Spatial scaling in um/voxel."""

    dt: float = 1.0
    """Temporal scaling in hours/tick."""

    valid_ticks: list[int] = field(default_factory=lambda: [0])
    """Valid ticks for processing colony dynamics."""


@dataclass
class ContextConfig:
    """Context configuration for analyze colony dynamics flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for analyze colony dynamics flow."""

    name: str
    """Name of the simulation series."""

    seeds: list[int]
    """List of series random seeds."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="analyze-colony-dynamics")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main analyze colony dynamics flow.

    Calls the following subflows, in order:

    1. :py:func:`run_flow_process_data`
    2. :py:func:`run_flow_generate_networks`
    3. :py:func:`run_flow_analyze_measures`
    """

    run_flow_process_data(context, series, parameters)

    run_flow_generate_networks(context, series, parameters)

    run_flow_analyze_measures(context, series, parameters)


@flow(name="analyze-colony-dynamics_process-data")
def run_flow_process_data(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Analyze colony dynamics subflow for processing data.

    Process neighbor connections and parsed simulation results to compile into a
    single dataframe that can used for further analysis. If the combined data
    already exists for a given key, that key is skipped.
    """

    results_path_key = make_key(series.name, "results")
    neighbors_path_key = make_key(series.name, "analysis", "analysis.NEIGHBORS")
    colonies_path_key = make_key(series.name, "analysis", "analysis.COLONIES")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        data_key = make_key(colonies_path_key, f"{series.name}_{key}.COLONIES.csv")

        if check_key(context.working_location, data_key):
            continue

        all_results = []
        all_neighbors = []

        for seed in series.seeds:
            # Load parsed results
            results_key = make_key(results_path_key, f"{series.name}_{key}_{seed:04d}.csv")
            results = load_dataframe(context.working_location, results_key)
            results["KEY"] = key
            results["SEED"] = seed
            results = results.set_index(INDEX_COLUMNS)
            all_results.append(results)

            # Load neighbors.
            neighbors_key = make_key(
                neighbors_path_key, f"{series.name}_{key}_{seed:04d}.NEIGHBORS.csv"
            )
            neighbors = load_dataframe(
                context.working_location, neighbors_key, converters={"NEIGHBORS": ast.literal_eval}
            )
            neighbors = neighbors.set_index(INDEX_COLUMNS)
            all_neighbors.append(neighbors)

        results_data = pd.concat(all_results)
        neighbors_data = pd.concat(all_neighbors)

        # Join results and neighbors data.
        data = neighbors_data.join(results_data, on=INDEX_COLUMNS).reset_index()

        # Filter for selected ticks.
        data = data[data["TICK"].isin(parameters.valid_ticks)]

        # Convert units.
        convert_model_units(data, parameters.ds, parameters.dt)

        # Save final dataframe.
        save_dataframe(context.working_location, data_key, data, index=False)


@flow(name="analyze-colony-dynamics_generate-networks")
def run_flow_generate_networks(
    context: ContextConfig, series: SeriesConfig, _: ParametersConfig
) -> None:
    """
    Analyze colony dynamics subflow for generating network objects.

    Process neighbor connections to generate graph objects where nodes represent
    cells and edges represent cells that share borders. If the network already
    exists for a given key and seed, that key and seed are skipped.
    """

    colonies_path_key = make_key(series.name, "analysis", "analysis.COLONIES")
    networks_path_key = make_key(series.name, "analysis", "analysis.NETWORKS")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        data_key = make_key(colonies_path_key, f"{series.name}_{key}.COLONIES.csv")
        networks_key = make_key(networks_path_key, f"{series.name}_{key}.NETWORKS.pkl")

        if check_key(context.working_location, networks_key):
            continue

        data = load_dataframe.with_options(**OPTIONS)(
            context.working_location, data_key, converters={"NEIGHBORS": ast.literal_eval}
        )

        networks = {
            (seed, tick): convert_to_network(group)
            for (seed, tick), group in data.groupby(["SEED", "TICK"])
        }

        save_pickle(context.working_location, networks_key, networks)


@flow(name="analyze-colony-dynamics_analyze-measures")
def run_flow_analyze_measures(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Analyze colony dynamics subflow for analyzing graph measures.

    Perform graph analysis on neighbor connections. If the analysis file already
    exists for a given key, that key is skipped.
    """

    networks_path_key = make_key(series.name, "analysis", "analysis.NETWORKS")
    measures_path_key = make_key(series.name, "analysis", "analysis.MEASURES")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        measures_key = make_key(measures_path_key, f"{series.name}_{key}.MEASURES.csv")

        if check_key(context.working_location, measures_key):
            continue

        networks_key = make_key(networks_path_key, f"{series.name}_{key}.NETWORKS.pkl")
        networks = load_pickle(context.working_location, networks_key)

        all_measures = []

        for (seed, tick), network in networks.items():
            degree_measures = calculate_degree_measures(network)
            distance_measures = calculate_distance_measures(network)
            centrality_measures = calculate_centrality_measures(network)

            measures = degree_measures.merge(distance_measures, on=["ID"])
            measures = measures.merge(centrality_measures, on=["ID"])
            measures["SEED"] = seed
            measures["TICK"] = tick

            all_measures.append(measures)

        all_measures_df = pd.concat(all_measures)

        convert_model_units(all_measures_df, parameters.ds, parameters.dt)

        save_dataframe(context.working_location, measures_key, all_measures_df, index=False)
