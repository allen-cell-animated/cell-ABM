"""
Workflow for initializing ARCADE simulations.

Working location structure:

.. code-block:: bash

    (name)
    ├── images
    │   └── (name)_(key).(extension)
    ├── inits
    │   └── inits.ARCADE
    │       ├── (name)_(key)_(margin)_(resolution).CELLS.json
    │       ├── (name)_(key)_(margin)_(resolution).LOCATIONS.json
    │       └── (name)_(key)_(margin)_(resolution).xml
    ├── plots
    │   └── plots.SAMPLE
    │       └── (name)_(key).SAMPLE.png
    └── samples
        ├── samples.PROCESSED
        │   └── (name)_(key).PROCESSED.csv
        └── samples.RAW
            └── (name)_(key).RAW.csv

Images are loaded from **images**, which are then sampled and processed into
**samples**. ARCADE initialization files are then generated and placed into
**inits.ARCADE**.
"""

import copy
from dataclasses import dataclass, field
from typing import Optional

from arcade_collection.input import (
    convert_to_cells_file,
    convert_to_locations_file,
    generate_setup_file,
    merge_region_samples,
)
from container_collection.docker import (
    create_docker_volume,
    remove_docker_volume,
    run_docker_command,
)
from io_collection.keys import check_key, make_key
from io_collection.load import load_dataframe
from io_collection.save import save_json, save_text
from prefect import flow

from cell_abm_pipeline.__config__ import make_dotlist_from_config
from cell_abm_pipeline.flows.process_sample import ContextConfig as ContextConfigProcessSample
from cell_abm_pipeline.flows.process_sample import ParametersConfig as ParametersConfigProcessSample
from cell_abm_pipeline.flows.process_sample import SeriesConfig as SeriesConfigProcessSample
from cell_abm_pipeline.flows.sample_image import ContextConfig as ContextConfigSampleImage
from cell_abm_pipeline.flows.sample_image import ParametersConfig as ParametersConfigSampleImage
from cell_abm_pipeline.flows.sample_image import SeriesConfig as SeriesConfigSampleImage

# Command for running sample image flow.
SAMPLE_IMAGE_COMMAND = ["abmpipe", "sample-image", "::"]

# Command for running process sample flow.
PROCESS_SAMPLE_COMMAND = ["abmpipe", "process-sample", "::"]

# Default volume means and standard deviations in um^3.
VOLUMES: dict[str, tuple[float, float]] = {
    "DEFAULT": (1865.0, 517.0),
    "NUCLEUS": (543.0, 157.0),
}

# Default height means and standard deviations in um.
HEIGHTS: dict[str, tuple[float, float]] = {
    "DEFAULT": (9.75, 2.4),
    "NUCLEUS": (6.86, 1.7),
}

# Default critical volume means and standard deviations in um^3.
CRITICAL_VOLUMES: dict[str, tuple[float, float]] = {
    "DEFAULT": (1300.0, 200.0),
    "NUCLEUS": (400.0, 50.0),
}

# Default critical height means and standard deviations in um.
CRITICAL_HEIGHTS: dict[str, tuple[float, float]] = {
    "DEFAULT": (9.0, 2.0),
    "NUCLEUS": (6.5, 1.5),
}

# Default cell state phase thresholds.
STATE_THRESHOLDS: dict[str, float] = {
    "APOPTOTIC_LATE": 0.25,
    "APOPTOTIC_EARLY": 0.90,
    "PROLIFERATIVE_G1": 1.124,
    "PROLIFERATIVE_S": 1.726,
    "PROLIFERATIVE_G2": 1.969,
    "PROLIFERATIVE_M": 2,
}

# Default list of Cellular Potts Model Hamiltonian terms.
POTTS_TERMS: list[str] = [
    "volume",
    "adhesion",
]


@dataclass
class ParametersConfigConvertToArcade:
    """Parameter configuration for initialize ARCADE simulations subflow - convert to ARCADE."""

    regions: dict[str, str] = field(default_factory=lambda: {"DEFAULT": "%s"})
    """Subcellular region samples used to initialize voxels."""

    margins: tuple[int, int, int] = (0, 0, 0)
    """Margins around initial voxel positions."""

    volumes: dict = field(default_factory=lambda: VOLUMES)
    """Volume means and standard deviations in um^3."""

    heights: dict = field(default_factory=lambda: HEIGHTS)
    """Height means and standard deviations in um."""

    critical_volumes: dict = field(default_factory=lambda: CRITICAL_VOLUMES)
    """Critical volume means and standard deviations in um^3."""

    critical_heights: dict = field(default_factory=lambda: CRITICAL_HEIGHTS)
    """Critical height means and standard deviations in um."""

    state_thresholds: dict[str, float] = field(default_factory=lambda: STATE_THRESHOLDS)
    """Cell state phase thresholds."""

    potts_terms: list[str] = field(default_factory=lambda: POTTS_TERMS)
    """List of Cellular Potts Model Hamiltonian terms."""


@dataclass
class ParametersConfig:
    """Parameter configuration for initialize ARCADE simulations flow."""

    image: str
    """Name of pipeline image."""

    resolution: float
    """Distance between samples in um."""

    sample_images: dict[str, ParametersConfigSampleImage]
    """Configs for sample images flow, keyed by region."""

    process_samples: dict[str, ParametersConfigProcessSample]
    """Configs for process samples flow, keyed by region."""

    convert_to_arcade: ParametersConfigConvertToArcade = field(default_factory=lambda: ParametersConfigConvertToArcade)
    """Convert to ARCADE configuration instance."""


@dataclass
class ContextConfig:
    """Context configuration for initialize ARCADE simulations flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""

    reference_location: str
    """Location of reference file (local path or S3 bucket)."""

    access_key_id: Optional[str] = None
    """AWS access key id for accessing S3 in Docker image."""

    secret_access_key: Optional[str] = None
    """AWS secret access key for accessing S3 in Docker image."""


@dataclass
class SeriesConfig:
    """Series configuration for initialize ARCADE simulations flow."""

    name: str
    """Name of the simulation series."""

    reference_key: str
    """Key for reference file."""

    conditions: list
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="initialize-arcade-simulations")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main initialize ARCADE simulations flow.

    Calls the following subflows, in order:

    1. :py:func:`run_flow_sample_images`
    2. :py:func:`run_flow_process_samples`
    3. :py:func:`run_flow_convert_to_arcade`
    """

    run_flow_sample_images(context, series, parameters)

    run_flow_process_samples(context, series, parameters)

    run_flow_convert_to_arcade(context, series, parameters)


@flow(name="initialize-arcade-simulations_sample-images")
def run_flow_sample_images(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Initialize ARCADE simulations subflow for sampling images.

    Iterate through conditions to sample images for each specified channel. The
    subflow `sample_image` is run via Docker for each condition and channel
    combination by passing in the subflow configuration as a dotlist.
    """

    docker_args = get_docker_arguments(context)

    if context.working_location.startswith("s3://"):
        context_config = ContextConfigSampleImage(working_location=context.working_location)
    else:
        context_config = ContextConfigSampleImage(working_location="/mnt")

    series_config = SeriesConfigSampleImage(name=series.name)

    for fov in series.conditions:
        for _, sample_image in parameters.sample_images.items():
            parameters_config = copy.deepcopy(sample_image)
            parameters_config.key = parameters_config.key % fov["key"]
            parameters_config.resolution = parameters.resolution

            config = {
                "context": context_config,
                "series": series_config,
                "parameters": parameters_config,
            }

            sample_image_command = SAMPLE_IMAGE_COMMAND + make_dotlist_from_config(config)
            run_docker_command(parameters.image, sample_image_command, **docker_args)

    if "volume" in docker_args:
        remove_docker_volume(docker_args["volume"])


@flow(name="initialize-arcade-simulations_process-samples")
def run_flow_process_samples(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Initialize ARCADE simulations subflow for processing samples.

    Iterate through conditions to process samples for each specified channel.
    The subflow `process_sample` is run via Docker for each condition and
    channel combination by passing in the subflow configuration as a dotlist.
    """
    docker_args = get_docker_arguments(context)

    if context.working_location.startswith("s3://"):
        context_config = ContextConfigProcessSample(working_location=context.working_location)
    else:
        context_config = ContextConfigProcessSample(working_location="/mnt")

    series_config = SeriesConfigProcessSample(name=series.name)
    resolution_key = f"R{round(parameters.resolution * 10):03d}"

    for fov in series.conditions:
        fov_key = fov["key"]

        for _, process_sample in parameters.process_samples.items():
            parameters_config = copy.deepcopy(process_sample)
            parameters_config.key = f"{parameters_config.key % fov_key}_{resolution_key}"

            if "include_ids" in fov:
                parameters_config.include_ids = fov["include_ids"]

            if "exclude_ids" in fov:
                parameters_config.exclude_ids = fov["exclude_ids"]

            config = {
                "context": context_config,
                "series": series_config,
                "parameters": parameters_config,
            }

            process_sample_command = PROCESS_SAMPLE_COMMAND + make_dotlist_from_config(config)
            run_docker_command(parameters.image, process_sample_command, **docker_args)

    if "volume" in docker_args:
        remove_docker_volume(docker_args["volume"])


@flow(name="initialize-arcade-simulations_convert-to-arcade")
def run_flow_convert_to_arcade(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig
) -> None:
    """
    Initialize ARCADE simulations subflow for converting to ARCADE.

    Converted processed samples into the ARCADE .CELLS and .LOCATIONS formats,
    along with a basic simulation setup XML file.
    """

    samples_key = make_key(series.name, "samples", "samples.PROCESSED")
    inits_key = make_key(series.name, "inits", "inits.ARCADE")

    resolution = parameters.resolution
    resolution_key = f"R{round(resolution * 10):03d}"

    if check_key(context.reference_location, series.reference_key):
        reference = load_dataframe(context.reference_location, series.reference_key)

        volume_columns = [column for column in reference.columns if "volume" in column]
        reference[volume_columns] = reference[volume_columns] / resolution**3

        height_columns = [column for column in reference.columns if "height" in column]
        reference[height_columns] = reference[height_columns] / resolution
    else:
        reference = None

    volumes = {
        region: (values[0] / resolution**3, values[1] / resolution**3)
        for region, values in parameters.convert_to_arcade.volumes.items()
    }
    heights = {
        region: (values[0] / resolution, values[1] / resolution)
        for region, values in parameters.convert_to_arcade.heights.items()
    }

    critical_volumes: dict[str, tuple[float, float]] = {
        region: (values[0] / resolution**3, values[1] / resolution**3)
        for region, values in parameters.convert_to_arcade.critical_volumes.items()
    }
    critical_heights: dict[str, tuple[float, float]] = {
        region: (values[0] / resolution, values[1] / resolution)
        for region, values in parameters.convert_to_arcade.critical_heights.items()
    }

    for fov in series.conditions:
        samples = {}

        for region, region_key_template in parameters.convert_to_arcade.regions.items():
            region_key = region_key_template % fov["key"]
            key = make_key(
                samples_key, f"{series.name}_{region_key}_{resolution_key}.PROCESSED.csv"
            )
            samples[region] = load_dataframe(context.working_location, key)

        margins = fov["margins"] if "margins" in fov else parameters.convert_to_arcade.margins
        merged_samples = merge_region_samples(samples, margins)
        x, y, z = margins
        key = f"{series.name}_{fov['key']}_X{x:03d}_Y{y:03d}_Z{z:03d}_{resolution_key}"

        cells = convert_to_cells_file(
            merged_samples,
            reference[reference["KEY"] == fov["key"]],
            volumes,
            heights,
            critical_volumes,
            critical_heights,
            parameters.convert_to_arcade.state_thresholds,
        )
        cells_key = make_key(inits_key, f"{key}.CELLS.json")
        save_json(context.working_location, cells_key, cells)

        locations = convert_to_locations_file(merged_samples)
        locations_key = make_key(inits_key, f"{key}.LOCATIONS.json")
        save_json(context.working_location, locations_key, locations)

        setup = generate_setup_file(
            merged_samples, margins, parameters.convert_to_arcade.potts_terms
        )
        setup_key = make_key(inits_key, f"{key}.xml")
        save_text(context.working_location, setup_key, setup)


def get_docker_arguments(context: ContextConfig) -> dict:
    """Compile Docker arguments for the given context."""

    if context.working_location.startswith("s3://"):
        environment = []

        if context.access_key_id is not None:
            environment.append(f"AWS_ACCESS_KEY_ID={context.access_key_id}")

        if context.secret_access_key is not None:
            environment.append(f"AWS_SECRET_ACCESS_KEY={context.secret_access_key}")

        docker_args = {"environment": environment}
    else:
        volume = create_docker_volume(context.working_location)
        docker_args = {"volume": volume}

    return docker_args
