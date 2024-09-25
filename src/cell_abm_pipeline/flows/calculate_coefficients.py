"""
Workflow for calculating spherical harmonic coefficients.

Working location structure:

.. code-block:: bash

    (name)
    ├── data
    │   └── data.LOCATIONS
    │       └── (name)_(key)_(seed).LOCATIONS.tar.xz
    └── calculations
        └── calculations.COEFFICIENTS
            ├── (name)_(key)_(seed)_(tick).COEFFICIENTS.csv
            └── (name)_(key)_(seed)_(tick)_(region).COEFFICIENTS.csv

Data from **data.LOCATIONS** are used to calculate coefficients, which are saved
to **calculations.COEFFICIENTS**.

If region is specified, the region is included in the output key. For
calculations with offset but no chunking, the output key extension starts with
``.(offset).`` to specify the index offset. For calculations with chunking, the
output key extension starts with ``.(offset).(chunk).`` to specify the index
offset and chunk size.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from abm_shape_collection import get_shape_coefficients, make_voxels_array
from arcade_collection.output import extract_tick_json, get_location_voxels
from io_collection.keys import make_key
from io_collection.load import load_tar
from io_collection.save import save_dataframe
from prefect import flow

COEFFICIENT_ORDER = 16


@dataclass
class ParametersConfig:
    """Parameter configuration for calculate coefficients flow."""

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

    scale: int = 1
    """Rescaling factor for image array."""

    order: int = COEFFICIENT_ORDER
    """Order of the spherical harmonics coefficient parametrization."""


@dataclass
class ContextConfig:
    """Context configuration for calculate coefficients flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for calculate coefficients flow."""

    name: str
    """Name of the simulation series."""


@flow(name="calculate-coefficients")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main calculate coefficients flow."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    calc_key = make_key(series.name, "calculations", "calculations.COEFFICIENTS")
    series_key = f"{series.name}_{parameters.key}_{parameters.seed:04d}"

    locations_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
    locations_tar = load_tar(context.working_location, locations_key)
    locations_json = extract_tick_json(locations_tar, series_key, parameters.tick, "LOCATIONS")

    all_coeffs = []

    count = 0

    for i, location in enumerate(locations_json):
        if i < parameters.offset:
            continue

        count = count + 1
        if parameters.chunk is not None and count > parameters.chunk:
            break

        voxels = get_location_voxels(location)

        if len(voxels) == 0:
            continue

        array = make_voxels_array(voxels, None, parameters.scale)

        if parameters.region is not None:
            region_voxels = get_location_voxels(location, parameters.region)

            if len(region_voxels) == 0:
                continue

            region_array = make_voxels_array(region_voxels, None, parameters.scale)
            coeffs = get_shape_coefficients(region_array, array, parameters.order)
        else:
            coeffs = get_shape_coefficients(array, array, parameters.order)

        coeffs["KEY"] = parameters.key
        coeffs["ID"] = location["id"]
        coeffs["SEED"] = parameters.seed
        coeffs["TICK"] = parameters.tick

        all_coeffs.append(coeffs)

    coeffs_dataframe = pd.DataFrame(all_coeffs)

    chunk_key = ""
    offset_key = f".{parameters.offset:04d}" if parameters.offset > 0 else ""

    if parameters.chunk is not None:
        chunk_key = f".{parameters.chunk:04d}"
        offset_key = f".{parameters.offset:04d}"

    region_key = f"_{parameters.region}" if parameters.region is not None else ""
    suffix = f"{region_key}{offset_key}{chunk_key}"

    coeffs_key = make_key(calc_key, f"{series_key}_{parameters.tick:06d}{suffix}.COEFFICIENTS.csv")
    save_dataframe(context.working_location, coeffs_key, coeffs_dataframe, index=False)
