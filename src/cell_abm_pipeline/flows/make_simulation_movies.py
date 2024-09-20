"""
Workflow for making simulation movies.

Working location structure:

.. code-block:: bash

    (name)
    ├── data
    │   └── data.LOCATIONS
    │       └── (name)_(key)_(seed).LOCATIONS.tar.xz
    ├── movies
    │   ├── movies.CENTROIDS
    │   │   ├── (name)_(key)_(seed).CENTROIDS.gif
    │   │   └── (name)_(key)_(seed)
    │   │       └── (frame).CENTROIDS.png
    │   └── movies.SCAN
    │       ├── (name)_(key)_(seed)_(view)_(frame).SCAN.gif
    │       └── (name)_(key)_(seed)_(view)_(frame)
    │           └── (index).SCAN.png
    └── results
        └── (name)_(key)_(seed).csv

Different formats use inputs from **results** and **data.LOCATIONS**. Movies are
saved to **movies**.
"""

from dataclasses import dataclass, field

import numpy as np
from arcade_collection.output import get_voxel_contours
from io_collection.keys import check_key, make_key
from io_collection.load import load_dataframe, load_tar
from io_collection.save import save_figure, save_gif
from prefect import flow, get_run_logger

from cell_abm_pipeline.flows.plot_cell_shapes import REGION_COLORS
from cell_abm_pipeline.tasks import make_centroids_figure, make_contour_figure

FORMATS: list[str] = [
    "centroids",
    "scan",
]


@dataclass
class ParametersConfigScan:
    """Parameter configuration for make simulation movies flow - scan."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for creating scan movies."""

    frame_spec: tuple[int, int, int] = (0, 1153, 1152)
    """Specification for simulation ticks to use for creating scan movies."""

    index_spec: tuple[int, int, int] = (0, 1, 1)
    """Specification for contour indices to use for creating scan movies."""

    regions: list[str] = field(default_factory=lambda: ["DEFAULT"])
    """List of subcellular regions."""

    box: tuple[int, int, int] = field(default_factory=lambda: (1, 1, 1))
    """Size of bounding box."""

    view: str = "top"
    """Projection view."""

    x_bounds: tuple[int, int] = field(default_factory=lambda: (0, 1))
    """Size of x bounds."""

    y_bounds: tuple[int, int] = field(default_factory=lambda: (0, 1))
    """Size of y bounds."""

    region_colors: dict[str, str] = field(default_factory=lambda: REGION_COLORS)
    """Colors for each cell region."""


@dataclass
class ParametersConfigCentroids:
    """Parameter configuration for make simulation movies flow - centroids."""

    seeds: list[int] = field(default_factory=lambda: [0])
    """Simulation seeds to use for creating centroid movies."""

    frame_spec: tuple[int, int, int] = (0, 1, 1)
    """Specification for simulation ticks to use for creating centroid movies."""

    x_bounds: tuple[int, int] = field(default_factory=lambda: (0, 1))
    """Size of x bounds."""

    y_bounds: tuple[int, int] = field(default_factory=lambda: (0, 1))
    """Size of y bounds."""

    dt: float = 1.0
    """Temporal scaling in hours/tick."""

    window: int = 0
    """Window size for centroid tail."""


@dataclass
class ParametersConfig:
    """Parameter configuration for make simulation movies flow."""

    formats: list[str] = field(default_factory=lambda: FORMATS)
    """List of movie formats."""

    scan: ParametersConfigScan = field(default_factory=lambda: ParametersConfigScan)
    """Parameters for scan movie subflow."""

    centroids: ParametersConfigCentroids = field(default_factory=lambda: ParametersConfigCentroids)
    """Parameters for centroids movie subflow."""


@dataclass
class ContextConfig:
    """Context configuration for make simulation movies flow."""

    working_location: str
    """Location for input and output files (local path or S3 bucket)."""


@dataclass
class SeriesConfig:
    """Series configuration for make simulation movies flow."""

    name: str
    """Name of the simulation series."""

    conditions: list[dict]
    """List of series condition dictionaries (must include unique condition "key")."""


@flow(name="make-simulation-movies")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """
    Main make simulation movies flow.

    Calls the following subflows, if the format is specified.

    - :py:func:`run_flow_make_centroids_movie`
    - :py:func:`run_flow_make_scan_movie`
    """

    if "centroids" in parameters.formats:
        run_flow_make_centroids_movie(context, series, parameters.centroids)

    if "scan" in parameters.formats:
        run_flow_make_scan_movie(context, series, parameters.scan)


@flow(name="make-simulation-movies_make-centroids-movie")
def run_flow_make_centroids_movie(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigCentroids
) -> None:
    """Make simulation movies subflow for centroids."""

    movie_key = make_key(series.name, "movies", "movies.CENTROIDS")
    keys = [condition["key"] for condition in series.conditions]

    for key in keys:
        for seed in parameters.seeds:
            series_key = f"{series.name}_{key}_{seed:04d}"

            results_key = make_key(series.name, "results", f"{series_key}.csv")
            results = load_dataframe(context.working_location, results_key)

            frame_keys = []

            for frame in np.arange(*parameters.frame_spec):
                frame_key = make_key(movie_key, f"{series_key}", f"{frame:06d}.CENTROIDS.png")
                frame_keys.append(frame_key)

                if check_key(context.working_location, frame_key):
                    continue

                save_figure(
                    context.working_location,
                    frame_key,
                    make_centroids_figure(
                        results,
                        frame,
                        parameters.x_bounds,
                        parameters.y_bounds,
                        parameters.dt,
                        parameters.window,
                    ),
                )

            output_key = make_key(movie_key, f"{series_key}.CENTROIDS.gif")
            save_gif(context.working_location, output_key, frame_keys)


@flow(name="make-simulation-movies_make-scan-movie")
def run_flow_make_scan_movie(
    context: ContextConfig, series: SeriesConfig, parameters: ParametersConfigScan
) -> None:
    """Make simulation movies subflow for scan."""

    data_key = make_key(series.name, "data", "data.LOCATIONS")
    movie_key = make_key(series.name, "movies", "movies.SCAN")
    keys = [condition["key"] for condition in series.conditions]

    if parameters.view not in ("top", "side"):
        logger = get_run_logger()
        logger.error("View [ %s ] not valid for scan movie.", parameters.view)
        return

    indices = list(np.arange(*parameters.index_spec))
    view = "top" if parameters.view == "top" else "side1"

    for key in keys:
        for seed in parameters.seeds:
            series_key = f"{series.name}_{key}_{seed:04d}"

            tar_key = make_key(data_key, f"{series_key}.LOCATIONS.tar.xz")
            tar = load_tar(context.working_location, tar_key)

            for frame in np.arange(*parameters.frame_spec):
                frame_key = f"{series_key}_{parameters.view}_{frame:06d}"
                contours = get_voxel_contours(
                    series_key,
                    tar,
                    frame,
                    parameters.regions,
                    parameters.box,
                    {view: indices},
                )

                index_keys = []

                for index in indices:
                    index_key = make_key(movie_key, frame_key, f"{index:03d}.SCAN.png")
                    index_keys.append(index_key)

                    if check_key(context.working_location, index_key):
                        continue

                    save_figure(
                        context.working_location,
                        index_key,
                        make_contour_figure(
                            contours,
                            index,
                            view,
                            parameters.regions,
                            parameters.x_bounds,
                            parameters.y_bounds,
                            parameters.region_colors,
                        ),
                    )

                output_key = make_key(movie_key, f"{frame_key}.SCAN.gif")
                save_gif(context.working_location, output_key, index_keys)
