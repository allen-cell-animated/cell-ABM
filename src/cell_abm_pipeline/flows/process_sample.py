"""
Workflow to process samples with selected processing steps.

Working location structure:

.. code-block:: bash

    (name)
    ├── plots
    │    └── plots.SAMPLE
    │        └── (name)_(key)_channel_(channel).SAMPLE.png
    └── samples
        ├── samples.PROCESSED
        │    └── (name)_(key)_channel_(channel).PROCESSED.csv
        └── samples.RAW
            └── (name)_(key)_channel_(channel).RAW.csv

Samples to be processed are loaded from the **samples/samples.RAW**
directory.
Contact sheet plots from this task will overwrite existing contact sheets
generated by the sample images task.
"""

from dataclasses import dataclass
from typing import Optional
from prefect import flow

from io_collection.keys import make_key
from io_collection.load import load_dataframe
from io_collection.save import save_dataframe, save_figure
from abm_initialization_collection.image import plot_contact_sheet
from abm_initialization_collection.sample import (
    remove_unconnected_regions,
    remove_edge_regions,
    include_selected_ids,
    exclude_selected_ids,
)

# Threshold for number of samples touching edge of FOV to be considered edge
EDGE_THRESHOLD: int = 1

# Distance (in um) to nearest neighbor to be considered unconnected
UNCONNECTED_THRESHOLD: float = 2.0


@dataclass
class ParametersConfig:
    key: str

    channel: int

    remove_unconnected: bool = True

    unconnected_threshold: float = UNCONNECTED_THRESHOLD

    unconnected_filter: str = "connectivity"

    remove_edges: bool = True

    edge_threshold: int = EDGE_THRESHOLD

    edge_padding: float = 1.0

    include_ids: Optional[list[int]] = None

    exclude_ids: Optional[list[int]] = None

    contact_sheet: bool = True


@dataclass
class ContextConfig:
    working_location: str


@dataclass
class SeriesConfig:
    name: str


@flow(name="process-sample")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    channel_key = f"{series.name}_{parameters.key}_channel_{parameters.channel}"
    sample_key = make_key(series.name, "samples", "samples.RAW", f"{channel_key}.RAW.csv")

    raw_samples = load_dataframe(context.working_location, sample_key)
    processed_samples = raw_samples.copy()

    if parameters.remove_unconnected:
        processed_samples = remove_unconnected_regions(
            processed_samples, parameters.unconnected_threshold, parameters.unconnected_filter
        )

    if parameters.remove_edges:
        processed_samples = remove_edge_regions(
            processed_samples, parameters.edge_threshold, parameters.edge_padding
        )

    if parameters.include_ids is not None:
        processed_samples = include_selected_ids(processed_samples, parameters.include_ids)

    if parameters.exclude_ids is not None:
        processed_samples = exclude_selected_ids(processed_samples, parameters.exclude_ids)

    processed_key = make_key(
        series.name, "samples", "samples.PROCESSED", f"{channel_key}.PROCESSED.csv"
    )
    save_dataframe(context.working_location, processed_key, processed_samples, index=False)

    if parameters.contact_sheet:
        contact_sheet = plot_contact_sheet(processed_samples, raw_samples)
        plot_key = make_key(series.name, "plots", "plots.SAMPLE", f"{channel_key}.SAMPLE.png")
        save_figure(context.working_location, plot_key, contact_sheet)
