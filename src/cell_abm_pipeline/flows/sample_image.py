"""
Workflow for sampling cell ids from an image.

Working location structure:

.. code-block:: bash

    (name)
    ├── images
    │   └── (name)_(key).(extension)
    ├── plots
    │   └── plots.SAMPLE
    │       └── (name)_(key)_C(channel)_R(resolution).SAMPLE.png
    └── samples
        └── samples.RAW
            └── (name)_(key)_C(channel)_R(resolution).RAW.csv

Input images to be sampled are loaded from **images**. Resulting image sample(s)
are placed into **samples.RAW** and corresponding contact sheet(s) are placed
into **plots.SAMPLE**.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from abm_initialization_collection.image import get_image_bounds, plot_contact_sheet
from abm_initialization_collection.sample import (
    get_image_samples,
    get_sample_indices,
    scale_sample_coordinates,
)
from io_collection.keys import make_key
from io_collection.load import load_image
from io_collection.save import save_dataframe, save_figure
from prefect import flow

# Default pixel resolution for images in x/y in um/pixel
SCALE_MICRONS_XY: float = 0.108333

# Default pixel resolution for images in z in um/pixel
SCALE_MICRONS_Z: float = 0.29


@dataclass
class ParametersConfig:
    """Parameter configuration for sample image flow."""

    key: str
    """Image key to sample."""

    channels: list[int] = field(default_factory=lambda: [0])
    """Image channels to sample."""

    grid: str = "rect"
    """Type of sampling grid (rect = rectangular, hex = hexagonal)."""

    coordinate_type: str | None = None
    """Coordinate scaling type (None = unscaled, absolute = in um, step = by step size)."""

    resolution: float = 1.0
    """Distance between samples in um."""

    scale_xy: float = SCALE_MICRONS_XY
    """ Resolution scaling in x/y in um/pixel."""

    scale_z: float = SCALE_MICRONS_Z
    """Resolution scaling in z in um/pixel."""

    extension: str = ".ome.tiff"
    """Image extension."""

    contact_sheet: bool = True
    """True to save contact sheet of processed samples, False otherwise."""


@dataclass
class ContextConfig:
    """Context configuration for sample image flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for sample image flow."""

    name: str
    """Name of the simulation series."""


@flow(name="sample-image")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main sample image flow."""

    image_key = make_key(
        series.name, "images", f"{series.name}_{parameters.key}{parameters.extension}"
    )
    image = load_image(
        context.working_location,
        image_key,
        dim_order="ZYX" if parameters.extension == ".tiff" else None,
    )
    image_bounds = get_image_bounds(image)

    sample_indices = get_sample_indices(
        parameters.grid,
        image_bounds,
        parameters.resolution,
        parameters.scale_xy,
        parameters.scale_z,
    )

    for channel in parameters.channels:
        resolution_key = f"R{round(parameters.resolution * 10):03d}"
        channel_key = f"C{channel:02d}"
        item_key = f"{series.name}_{parameters.key}_{channel_key}_{resolution_key}"

        samples = get_image_samples(image, sample_indices, channel)
        samples = scale_sample_coordinates(
            samples,
            parameters.coordinate_type,
            parameters.resolution,
            parameters.scale_xy,
            parameters.scale_z,
        )
        sample_key = make_key(series.name, "samples", "samples.RAW", f"{item_key}.RAW.csv")
        save_dataframe(context.working_location, sample_key, samples, index=False)

        if parameters.contact_sheet:
            contact_sheet = plot_contact_sheet(samples)
            plot_key = make_key(series.name, "plots", "plots.SAMPLE", f"{item_key}.SAMPLE.png")
            save_figure(context.working_location, plot_key, contact_sheet)
