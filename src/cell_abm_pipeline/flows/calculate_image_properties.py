"""
Workflow for calculating shape properties from existing images.

Working location structure:

.. code-block:: bash

    (name)
    ├── results
    │   └── (name)_(key)_(seed).csv
    └── calculations
        └── calculations.PROPERTIES
            ├── (name)_(key)_(seed)_(tick).PROPERTIES.csv
            └── (name)_(key)_(seed)_(tick)_(region).PROPERTIES.csv

Data from **results** are used to specify existing images, which are then used
to calculate properties. Calculations are saved to **calculations.PROPERTIES**.

If region is specified, the region is included in the output key.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from abm_shape_collection import get_shape_properties
from io_collection.keys import make_key
from io_collection.load import load_dataframe, load_image
from io_collection.save import save_dataframe
from prefect import flow

from cell_abm_pipeline.flows.calculate_properties import SHAPE_PROPERTIES


@dataclass
class ParametersConfig:
    """Parameter configuration for calculate image properties flow."""

    key: str
    """Simulation key to calculate."""

    seed: int
    """Simulation random seed to calculate."""

    tick: int
    """Simulation tick to calculate."""

    channel: int
    """Index of channel to calculate."""

    region: str | None = None
    """Subcellular region to calculate."""

    properties: list[str] = field(default_factory=lambda: SHAPE_PROPERTIES)
    """List of shape properties to calculate."""


@dataclass
class ContextConfig:
    """Context configuration for calculate image properties flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for calculate image properties flow."""

    name: str
    """Name of the simulation series."""


@flow(name="calculate-image-properties")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main calculate image properties flow."""

    calc_key = make_key(series.name, "calculations", "calculations.PROPERTIES")
    series_key = f"{series.name}_{parameters.key}_{parameters.seed:04d}"

    results_key = make_key(series.name, "results", f"{series_key}.csv")
    results = load_dataframe(context.working_location, results_key)

    all_props = []
    fields = ["ID", "IMAGE"]

    for cell_id, image_file in results[results["TICK"] == parameters.tick][fields].to_numpy():
        image = load_image("s3://allencell", f"aics/hipsc_single_cell_image_dataset/{image_file}")
        array = image.get_image_data("ZYX", T=0, C=parameters.channel)
        props = get_shape_properties(array, parameters.properties)

        props["KEY"] = parameters.key
        props["ID"] = cell_id
        props["SEED"] = parameters.seed
        props["TICK"] = parameters.tick

        all_props.append(props)

    props_dataframe = pd.DataFrame(all_props)

    region_key = f"_{parameters.region}" if parameters.region is not None else ""
    suffix = region_key

    props_key = make_key(calc_key, f"{series_key}_{parameters.tick:06d}{suffix}.PROPERTIES.csv")
    save_dataframe(context.working_location, props_key, props_dataframe, index=False)
