"""
Workflow for running containerized calculations using Fargate.

This workflow is used to run registered calculation flows across different
simulation conditions and random seeds in parallel. The configurations for the
selected calculation are passed into the corresponding flows.

Some calculations can be chunked (in which the calculation is only run on a
subset of the cells for the given condition, seed, and tick) in order to further
parallelize the flow. For chunked calculations, re-running the flow will merge
completed chunks into a single file.

The flow will aim to avoid re-running any existing calculations. Calculations
are skipped if the calculation output file already exists, or if the specific
chunk already exists. Calculations for additional ticks are appended into the
existing calculation output file.

If the submit tasks option is turned off, the flow will print the full pipeline
command instead, which can then be run locally. Running commands locally can be
useful for conditions that require more CPUs/memory than are available.

Note that this workflow works only if working location is an S3 bucket.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd
from container_collection.fargate import (
    make_fargate_task,
    register_fargate_task,
    submit_fargate_task,
)
from io_collection.keys import check_key, make_key, remove_key
from io_collection.load import load_dataframe
from io_collection.save import save_dataframe
from prefect import flow, get_run_logger

from cell_abm_pipeline.__config__ import make_dotlist_from_config


class Calculation(Enum):
    """Registered calculation types."""

    COEFFICIENTS = ("calculate_coefficients", "COEFFICIENTS", True)

    NEIGHBORS = ("calculate_neighbors", "NEIGHBORS", False)

    POSITIONS = ("calculate_positions", "POSITIONS", False)

    PROPERTIES = ("calculate_properties", "PROPERTIES", True)

    IMAGE_PROPERTIES = ("calculate_image_properties", "PROPERTIES", False)


@dataclass
class ParametersConfig:
    """Parameter configuration for run fargate calculations flow."""

    image: str
    """Name of pipeline image."""

    ticks: list[int]
    """List of ticks to run flow on."""

    calculate: Calculation | None = None
    """Calculation type."""

    chunk: int | None = None
    """Chunk size, if possible for the given calculation type."""

    submit_tasks: bool = True
    """True to submit calculation tasks, False otherwise."""

    overrides: dict = field(default_factory=dict)
    """Overrides for the specific calculation type."""


@dataclass
class ContextConfig:
    """Context configuration for run fargate calculations flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""

    account: str
    """AWS account number."""

    region: str
    """AWS region."""

    user: str
    """User name prefix."""

    vcpus: int
    """Requested number of vcpus for AWS Fargate task."""

    memory: int
    """Requested memory for AWS Fargate task."""

    cluster: str
    """AWS Fargate cluster name."""

    security_groups: str
    """AWS Fargate security groups, separated by colon."""

    subnets: str
    """AWS Fargate subnets groups, separated by colon."""


@dataclass
class SeriesConfig:
    """Series configuration for run fargate calculations flow."""

    name: str
    """Name of the simulation series."""

    seeds: list[int]
    """List of series random seeds."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="run-fargate-calculations")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main run fargate calculations flow."""

    # Check that a valid calculation type is selected.
    if parameters.calculate is None:
        logger = get_run_logger()
        logger.error(
            "No valid calculation type selected. Valid options: [ %s ]",
            " | ".join([member.name for member in Calculation]),
        )
        return

    # Check that the working location is vali.d
    if not context.working_location.startswith("s3://"):
        logger = get_run_logger()
        logger.error("Fargate calculations can only be run with S3 working location.")
        return

    # Get the calculation type.
    module_name, suffix, chunkable = parameters.calculate.value
    calc_path_key = make_key(series.name, "calculations", f"calculations.{suffix}")

    # Get the region suffix, if it exists.
    region = ""
    if "region" in parameters.overrides is not None:
        region = f"_{parameters.overrides['region']}"

    # Create and register the task definition for the calculation.
    if parameters.submit_tasks:
        account = context.account
        task_definition = make_fargate_task(
            module_name,
            f"{account}.dkr.ecr.{context.region}.amazonaws.com/{context.user}/{parameters.image}",
            context.region,
            context.user,
            context.vcpus,
            context.memory,
            f"arn:aws:iam::{account}:role/BatchJobRole",
            f"arn:aws:iam::{account}:role/ecsTaskExecutionRole",
        )
        task_definition_arn = register_fargate_task(task_definition)

    # Import the module for the specified calculation.
    module = importlib.import_module(f"..{module_name}", package=__name__)

    # Create the context and series configs for the calculation.
    context_config = module.ContextConfig(working_location=context.working_location)
    series_config = module.SeriesConfig(name=series.name)

    for condition in series.conditions:
        for seed in series.seeds:
            series_key = f"{series.name}_{condition['key']}_{seed:04d}"

            # If the calculation is chunkable, load results to identify chunks.
            if chunkable:
                results_key = make_key(series.name, "results", f"{series_key}.csv")
                results = load_dataframe(context.working_location, results_key)

            # Check if the compiled calculation result already exists.
            calc_key = make_key(calc_path_key, f"{series_key}{region}.{suffix}.csv")
            calc_key_exists = check_key(context.working_location, calc_key)

            # If the compiled calculation result already exists, load the calculated ticks.
            existing_ticks = []
            if calc_key_exists:
                existing_calc = load_dataframe(context.working_location, calc_key, usecols=["TICK"])
                existing_ticks = list(existing_calc["TICK"].unique())

            for tick in parameters.ticks:
                # Skip the tick if it exists in the compiled calculation result.
                if tick in existing_ticks:
                    continue

                # Check if the individual calculation result already exists.
                tick_key = make_key(calc_path_key, f"{series_key}_{tick:06d}{region}.{suffix}.csv")
                tick_key_exists = check_key(context.working_location, tick_key)

                # Skip the tick if the individual calculation result exists.
                if tick_key_exists:
                    continue

                completed_offset_keys = []
                missing_offsets_overrides = []

                # If the calculation is chunkable, get the completed and missing chunk offsets.
                if chunkable:
                    total = results[results["TICK"] == tick].shape[0]
                    chunk = parameters.chunk
                    all_offsets = list(range(0, total, chunk)) if chunk is not None else [0]

                    for offset in all_offsets:
                        if chunk is not None:
                            offset_key = tick_key.replace(
                                f".{suffix}.csv",
                                f".{offset:04d}.{chunk:04d}.{suffix}.csv",
                            )
                            offset_key_exists = check_key(context.working_location, offset_key)

                            if offset_key_exists:
                                completed_offset_keys.append(offset_key)
                                continue

                        missing_offsets_overrides.append({"offset": offset, "chunk": chunk})
                else:
                    missing_offsets_overrides.append({})

                # Create commands and submit (or display) tasks.
                for offset_overrides in missing_offsets_overrides:
                    parameters_config = module.ParametersConfig(
                        key=condition["key"],
                        seed=seed,
                        tick=tick,
                        **offset_overrides,
                        **parameters.overrides,
                    )

                    config = {
                        "context": context_config,
                        "series": series_config,
                        "parameters": parameters_config,
                    }

                    command = ["abmpipe", module_name, "::", *make_dotlist_from_config(config)]

                    if parameters.submit_tasks:
                        submit_fargate_task.with_options(retries=2, retry_delay_seconds=1)(
                            module_name,
                            task_definition_arn,
                            context.user,
                            context.cluster,
                            context.security_groups.split(":"),
                            context.subnets.split(":"),
                            command,
                        )
                    else:
                        logger.info(" ".join(command))

                # If all chunk results exist, compile into unchunked result.
                if (
                    chunkable
                    and len(completed_offset_keys) == len(all_offsets)
                    and chunk is not None
                ):
                    tick_calcs = [
                        load_dataframe(context.working_location, key)
                        for key in completed_offset_keys
                    ]
                    calc_dataframe = pd.concat(tick_calcs, ignore_index=True)
                    save_dataframe(context.working_location, tick_key, calc_dataframe, index=False)

                    for key in completed_offset_keys:
                        remove_key(context.working_location, key)
