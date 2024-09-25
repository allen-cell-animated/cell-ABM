"""
Workflow for calculating shape properties.

Working location structure:

.. code-block:: bash

    (name)
    ├── data
    │   └── data.LOCATIONS
    │       └── (name)_(key)_(seed).LOCATIONS.tar.xz
    └── calculations
        └── calculations.PROPERTIES
            ├── (name)_(key)_(seed)_(tick).PROPERTIES.csv
            └── (name)_(key)_(seed)_(tick)_(region).PROPERTIES.csv

Data from **data.LOCATIONS** are used to calculate properties, which are saved
to **calculations.PROPERTIES**.

If region is specified, the region is included in the output key. For
calculations with offset but no chunking, the output key extension starts with
``.(offset).`` to specify the index offset. For calculations with chunking, the
output key extension starts with ``.(offset).(chunk).`` to specify the index
offset and chunk size.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from abm_shape_collection import get_shape_properties, make_voxels_array
from arcade_collection.output import extract_tick_json, get_location_voxels
from io_collection.keys import make_key
from io_collection.load import load_tar
from io_collection.save import save_dataframe
from prefect import flow

SHAPE_PROPERTIES = [
    "area",
    "axis_major_length",
    "axis_minor_length",
    "eccentricity",
    "orientation",
    "perimeter",
    "extent",
    "solidity",
]


@dataclass
class ParametersConfig:
    """Parameter configuration for calculate properties flow."""

    key: str
    """Simulation key to calculate."""

    seed: int
    """Simulation random seed to calculate."""

    tick: int
    """Simulation tick to calculate."""

    offset: int = 0
    """Index offset for skipped calculations."""

    chunk: int | None = None
    """Number of indices to calculate, starting from offset."""

    region: str | None = None
    """Subcellular region to calculate."""

    properties: list[str] = field(default_factory=lambda: SHAPE_PROPERTIES)
    """List of shape properties to calculate."""


@dataclass
class ContextConfig:
    """Context configuration for calculate properties flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for calculate properties flow."""

    name: str
    """Name of the simulation series."""


@flow(name="calculate-properties")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main calculate properties flow."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    calc_key = make_key(series.name, "calculations", "calculations.PROPERTIES")
    series_key = f"{series.name}_{parameters.key}_{parameters.seed:04d}"

    locations_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
    locations_tar = load_tar(context.working_location, locations_key)
    locations_json = extract_tick_json(locations_tar, series_key, parameters.tick, "LOCATIONS")

    all_props = []

    count = 0

    for i, location in enumerate(locations_json):
        if i < parameters.offset:
            continue

        count = count + 1
        if parameters.chunk is not None and count > parameters.chunk:
            break

        voxels = get_location_voxels(location, parameters.region)

        if len(voxels) == 0:
            continue

        array = make_voxels_array(voxels)
        props = get_shape_properties(array, parameters.properties)

        props["KEY"] = parameters.key
        props["ID"] = location["id"]
        props["SEED"] = parameters.seed
        props["TICK"] = parameters.tick

        all_props.append(props)

    props_dataframe = pd.DataFrame(all_props)

    chunk_key = ""
    offset_key = f".{parameters.offset:04d}" if parameters.offset > 0 else ""

    if parameters.chunk is not None:
        chunk_key = f".{parameters.chunk:04d}"
        offset_key = f".{parameters.offset:04d}"

    region_key = f"_{parameters.region}" if parameters.region is not None else ""
    suffix = f"{region_key}{offset_key}{chunk_key}"

    props_key = make_key(calc_key, f"{series_key}_{parameters.tick:06d}{suffix}.PROPERTIES.csv")
    save_dataframe(context.working_location, props_key, props_dataframe, index=False)
