"""
Workflow for converting ARCADE simulations to other formats.

Working location structure:

.. code-block:: bash

    (name)
    ├── converted
    │   ├── converted.COLORIZER
    │   │   └── (name)_(key)_(seed)
    │   │       ├── feature_(index).json
    │   │       ├── frame_(index).png
    │   │       ├── manifest.json
    │   │       ├── outliers.json
    │   │       ├── times.json
    │   │       └── tracks.json
    │   ├── converted.IMAGE
    │   │   └── (name)_(key)_(seed)_(chunk)_(chunk).IMAGE.ome.tiff
    │   ├── converted.MESH
    │   │   └── (name)_(key)_(seed)_(tick)_(id)_(region).MESH.obj
    │   ├── converted.PROJECTION
    │   │   └── (name)_(key)_(seed)_(tick)_(regions).PROJECTION.png
    │   └── converted.SIMULARIUM
    │       └── (name)_(key)_(seed).simularium
    ├── data
    │   ├── data.CELLS
    │   │   └── (name)_(key)_(seed).CELLS.tar.xz
    │   └── data.LOCATIONS
    │       └── (name)_(key)_(seed).LOCATIONS.tar.xz
    └── results
        └── (name)_(key)_(seed).csv

Different formats use inputs from **results**, **data.CELLS**, and
**data.LOCATIONS**. Formatted data are saved to **converted**.
"""

from dataclasses import dataclass, field

import numpy as np
from arcade_collection.convert import (
    convert_to_colorizer,
    convert_to_images,
    convert_to_meshes,
    convert_to_projection,
    convert_to_simularium_objects,
    convert_to_simularium_shapes,
)
from io_collection.keys import make_key
from io_collection.load import load_dataframe, load_tar
from io_collection.save import save_figure, save_image, save_json, save_text
from prefect import flow

from cell_abm_pipeline.flows.plot_basic_metrics import PHASE_COLORS
from cell_abm_pipeline.flows.plot_cell_shapes import REGION_COLORS

FORMATS: list[str] = [
    "colorizer",
    "images",
    "meshes",
    "projections",
    "simularium_shapes",
    "simularium_objects",
]

COLORIZER_FEATURES: list[str] = [
    "volume",
    "height",
]


@dataclass
class ParametersConfigColorizer:
    """Parameter configuration for convert ARCADE format flow - colorizer."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for converting to colorizer."""

    frame_spec: tuple[int, int, int] = (0, 1153, 1152)
    """Specification for simulation ticks to use for converting to colorizer."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of bounding box."""

    ds: float = 1.0
    """Spatial scaling in units/um."""

    dt: float = 1.0
    """Temporal scaling in hours/tick."""

    chunk_size: int = 500
    """Image chunk size."""

    features: list[str] = field(default_factory=lambda: COLORIZER_FEATURES)
    """List of colorizer features."""


@dataclass
class ParametersConfigImages:
    """Parameter configuration for convert ARCADE format flow - images."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for converting to images."""

    frame_spec: tuple[int, int, int] = (0, 1153, 1152)
    """Specification for simulation ticks to use for converting to images."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of bounding box."""

    chunk_size: int = 500
    """Image chunk size."""

    binary: bool = False
    """True to generate binary images, False otherwise."""

    separate: bool = False
    """True to generate separate images for each tick, False otherwise."""


@dataclass
class ParametersConfigMeshes:
    """Parameter configuration for convert ARCADE format flow - meshes."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for converting to meshes."""

    frame_spec: tuple[int, int, int] = (0, 1153, 1152)
    """Specification for simulation ticks to use for converting to meshes."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of bounding box."""

    invert: bool = False
    """True if mesh should have inverted faces, False otherwise."""


@dataclass
class ParametersConfigProjections:
    """Parameter configuration for convert ARCADE format flow - projections."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for converting to projections."""

    frame_spec: tuple[int, int, int] = (0, 1153, 1152)
    """Specification for simulation ticks to use for converting to projections."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of bounding box."""

    ds: float = 1.0
    """Spatial scaling in units/um."""

    dt: float = 1.0
    """Temporal scaling in hours/tick."""

    scale: int = 100
    """Size of scale bar (in um)."""

    region_colors: dict[str, str] = field(default_factory=lambda: REGION_COLORS)
    """Colors for each cell region."""


@dataclass
class ParametersConfigSimulariumShapes:
    """Parameter configuration for convert ARCADE format flow - simularium shapes."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for converting to simularium."""

    frame_spec: tuple[int, int, int] = (0, 1153, 1152)
    """Specification for simulation ticks to use for converting to simularium."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of bounding box."""

    ds: float = 1.0
    """Spatial scaling in units/um."""

    dt: float = 1.0
    """Temporal scaling in hours/tick."""

    phase_colors: dict[str, str] = field(default_factory=lambda: PHASE_COLORS)
    """Colors for each cell cycle phase."""

    resolution: int = 0
    """Number of voxels represented by a sphere (0 for single sphere per cell)."""


@dataclass
class ParametersConfigSimulariumObjects:
    """Parameter configuration for convert ARCADE format flow - simularium objects."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for converting to simularium."""

    frame_spec: tuple[int, int, int] = (0, 1153, 1152)
    """Specification for simulation ticks to use for converting to simularium."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of bounding box."""

    ds: float = 1.0
    """Spatial scaling in units/um."""

    dt: float = 1.0
    """Temporal scaling in hours/tick."""

    phase_colors: dict[str, str] = field(default_factory=lambda: PHASE_COLORS)
    """Colors for each cell cycle phase."""

    url: str = ""
    """URL for object files."""

    group_size: int = 1
    """Mesh group size."""


@dataclass
class ParametersConfig:
    """Parameter configuration for convert ARCADE format flow."""

    formats: list[str] = field(default_factory=lambda: FORMATS)
    """List of convert formats."""

    colorizer: ParametersConfigColorizer = field(
        default_factory=lambda: ParametersConfigColorizer()
    )
    """Parameters for colorizer subflow."""

    images: ParametersConfigImages = field(default_factory=lambda: ParametersConfigImages())
    """Parameters for images subflow."""

    meshes: ParametersConfigMeshes = field(default_factory=lambda: ParametersConfigMeshes())
    """Parameters for meshes subflow."""

    projections: ParametersConfigProjections = field(
        default_factory=lambda: ParametersConfigProjections()
    )
    """Parameters for projections subflow."""

    simularium_shapes: ParametersConfigSimulariumShapes = field(
        default_factory=lambda: ParametersConfigSimulariumShapes()
    )
    """Parameters for simularium shapes subflow."""

    simularium_objects: ParametersConfigSimulariumObjects = field(
        default_factory=lambda: ParametersConfigSimulariumObjects()
    )
    """Parameters for simularium objects subflow."""


@dataclass
class ContextConfig:
    """Context configuration for convert ARCADE format flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for convert ARCADE format flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="convert-arcade-format")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main convert ARCADE format flow.

    Calls the following subflows, if the format is specified:

    - :py:func:`run_flow_convert_to_colorizer`
    - :py:func:`run_flow_convert_to_images`
    - :py:func:`run_flow_convert_to_meshes`
    - :py:func:`run_flow_convert_to_projections`
    - :py:func:`run_flow_convert_to_simularium_shapes`
    - :py:func:`run_flow_convert_to_simularium_objects`
    """

    if "colorizer" in parameters.formats:
        run_flow_convert_to_colorizer(context, series, parameters.colorizer)

    if "images" in parameters.formats:
        run_flow_convert_to_images(context, series, parameters.images)

    if "meshes" in parameters.formats:
        run_flow_convert_to_meshes(context, series, parameters.meshes)

    if "projections" in parameters.formats:
        run_flow_convert_to_projections(context, series, parameters.projections)

    if "simularium_shapes" in parameters.formats:
        run_flow_convert_to_simularium_shapes(context, series, parameters.simularium_shapes)

    if "simularium_objects" in parameters.formats:
        run_flow_convert_to_simularium_objects(context, series, parameters.simularium_objects)


@flow(name="convert-arcade-format_convert-to-colorizer")
def run_flow_convert_to_colorizer(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigColorizer
) -> None:
    """Convert ARCADE format subflow for colorizer."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    converted_key = make_key(series.name, "converted", "converted.COLORIZER")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for seed in parameters.seeds:
            series_key = f"{series.name}_{key}_{seed:04d}"

            tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
            tar = load_tar(context.working_location, tar_key)

            chunks = convert_to_images(
                series_key,
                tar,
                parameters.frame_spec,
                parameters.regions,
                parameters.box,
                parameters.chunk_size,
                binary=False,
                separate=True,
                flatten=True,
            )

            for frame_index, (_, _, chunk, _) in enumerate(chunks):
                image_key = make_key(converted_key, series_key, f"frame_{frame_index}.png")
                save_image(context.working_location, image_key, chunk)

            results_key = make_key(series.name, "results", f"{series_key}.csv")
            results = load_dataframe(context.working_location, results_key)

            colorizer = convert_to_colorizer(
                results,
                parameters.features,
                parameters.frame_spec,
                parameters.ds,
                parameters.dt,
                parameters.regions,
            )

            manifest_key = make_key(converted_key, series_key, "manifest.json")
            save_json(context.working_location, manifest_key, colorizer["manifest"])

            outliers_key = make_key(converted_key, series_key, "outliers.json")
            save_json(context.working_location, outliers_key, colorizer["outliers"])

            tracks_key = make_key(converted_key, series_key, "tracks.json")
            save_json(context.working_location, tracks_key, colorizer["tracks"])

            times_key = make_key(converted_key, series_key, "times.json")
            save_json(context.working_location, times_key, colorizer["times"])

            for feature_index, feature in enumerate(parameters.features):
                feature_key = make_key(converted_key, series_key, f"feature_{feature_index}.json")
                save_json(context.working_location, feature_key, colorizer[feature])


@flow(name="convert-arcade-format_convert-to-images")
def run_flow_convert_to_images(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigImages
) -> None:
    """Convert ARCADE format subflow for images."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    converted_key = make_key(series.name, "converted", "converted.IMAGE")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for seed in parameters.seeds:
            series_key = f"{series.name}_{key}_{seed:04d}"

            tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
            tar = load_tar(context.working_location, tar_key)

            chunks = convert_to_images(
                series_key,
                tar,
                parameters.frame_spec,
                parameters.regions,
                parameters.box,
                parameters.chunk_size,
                binary=parameters.binary,
                separate=parameters.separate,
                flatten=False,
            )

            for i, j, chunk, frame in chunks:
                chunk_key = f"{i:02d}_{j:02d}.IMAGE.ome.tiff"

                if frame is None:
                    image_key = make_key(converted_key, f"{series_key}_{chunk_key}")
                else:
                    image_key = make_key(converted_key, f"{series_key}_{frame:06d}_{chunk_key}")

                save_image(context.working_location, image_key, chunk)


@flow(name="convert-arcade-format_convert-to-meshes")
def run_flow_convert_to_meshes(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigMeshes
) -> None:
    """Convert ARCADE format subflow for meshes."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    converted_key = make_key(series.name, "converted", "converted.MESH")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for seed in parameters.seeds:
            series_key = f"{series.name}_{key}_{seed:04d}"

            tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
            tar = load_tar(context.working_location, tar_key)

            meshes = convert_to_meshes(
                series_key,
                tar,
                parameters.frame_spec,
                parameters.regions,
                parameters.box,
                parameters.invert,
            )

            for frame, cell_id, region, mesh in meshes:
                mesh_key = make_key(
                    converted_key, f"{series_key}_{frame:06d}_{cell_id:06d}_{region}.MESH.obj"
                )
                save_text(context.working_location, mesh_key, mesh)


@flow(name="convert-arcade-format_convert-to-projections")
def run_flow_convert_to_projections(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigProjections
) -> None:
    """Convert ARCADE format subflow for projections."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    converted_key = make_key(series.name, "converted", "converted.PROJECTION")
    region_key = "_".join(sorted(parameters.regions))
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for seed in parameters.seeds:
            series_key = f"{series.name}_{key}_{seed:04d}"

            tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
            tar = load_tar(context.working_location, tar_key)

            for frame in np.arange(*parameters.frame_spec):
                projection = convert_to_projection(
                    series_key,
                    tar,
                    frame,
                    parameters.regions,
                    parameters.box,
                    parameters.ds,
                    parameters.dt,
                    parameters.scale,
                    parameters.region_colors,
                )

                projection_key = make_key(
                    converted_key, f"{series_key}_{frame:06d}_{region_key}.PROJECTION.png"
                )
                save_figure(
                    context.working_location, projection_key, projection, bbox_inches="tight"
                )


@flow(name="convert-arcade-format_convert-to-simularium-shapes")
def run_flow_convert_to_simularium_shapes(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigSimulariumShapes
) -> None:
    """Convert ARCADE format subflow for Simularium with shapes."""

    cells_data_key = make_key(series.name, "data", "data.CELLS")
    locs_data_key = make_key(series.name, "data", "data.LOCATIONS")
    converted_key = make_key(series.name, "converted", "converted.SIMULARIUM")
    keys = [condition["key"] for condition in series.conditions]

    suffix = f"SHAPES{parameters.resolution}"

    for key in keys:
        for seed in parameters.seeds:
            series_key = f"{series.name}_{key}_{seed:04d}"

            cells_tar_key = make_key(cells_data_key, f"{series_key}.CELLS.tar.xz")
            cells_tar = load_tar(context.working_location, cells_tar_key)

            locs_tar_key = make_key(locs_data_key, f"{series_key}.LOCATIONS.tar.xz")
            locs_tar = load_tar(context.working_location, locs_tar_key)

            simularium = convert_to_simularium_shapes(
                series_key,
                "potts",
                {"cells": cells_tar, "locations": locs_tar},
                parameters.frame_spec,
                parameters.box,
                parameters.ds,
                parameters.ds,
                parameters.dt,
                parameters.phase_colors,
                parameters.resolution,
            )

            simularium_key = make_key(converted_key, f"{series_key}.{suffix}.simularium")
            save_text(context.working_location, simularium_key, simularium)


@flow(name="convert-arcade-format_convert-to-simularium-objects")
def run_flow_convert_to_simularium_objects(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigSimulariumObjects
) -> None:
    """Convert ARCADE format subflow for Simularium with objects."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    converted_key = make_key(series.name, "converted", "converted.SIMULARIUM")
    keys = [condition["key"] for condition in series.conditions]

    suffix = f"OBJECTS{parameters.group_size}"
    regions = ["DEFAULT", "NUCLEUS"]
    invert = {"DEFAULT": True, "NUCLEUS": False}

    for key in keys:
        for seed in parameters.seeds:
            series_key = f"{series.name}_{key}_{seed:04d}"

            results_key = make_key(series.name, "results", f"{series_key}.csv")
            results = load_dataframe(context.working_location, results_key)

            tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
            tar = load_tar(context.working_location, tar_key)

            categories = results[["TICK", "PHASE", "ID"]].rename(
                columns={"TICK": "FRAME", "PHASE": "CATEGORY"}
            )

            meshes = convert_to_meshes(
                series_key,
                tar,
                parameters.frame_spec,
                regions,
                parameters.box,
                invert,
                parameters.group_size,
                categories,
            )

            mesh_path_key = make_key(converted_key, f"{series_key}.{suffix}")

            for frame, index, region, mesh in meshes:
                mesh_key = make_key(mesh_path_key, f"{frame:06d}_{region}_{index:03d}.MESH.obj")
                save_text(context.working_location, mesh_key, mesh)

            simularium = convert_to_simularium_objects(
                series_key,
                "potts",
                categories,
                parameters.frame_spec,
                regions,
                parameters.box,
                parameters.ds,
                parameters.ds,
                parameters.dt,
                parameters.phase_colors,
                parameters.group_size,
                make_key(parameters.url, mesh_path_key),
            )

            simularium_key = make_key(converted_key, f"{series_key}.{suffix}.simularium")
            save_text(context.working_location, simularium_key, simularium)
