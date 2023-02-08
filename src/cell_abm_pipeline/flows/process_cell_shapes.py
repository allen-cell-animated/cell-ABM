import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from io_collection.keys import check_key, make_key, remove_key
from io_collection.load import load_dataframe, load_tar
from io_collection.save import save_dataframe, save_tar
from prefect import flow


@dataclass
class ParametersConfig:
    frames: list[int]

    region: Optional[str] = None


@dataclass
class ContextConfig:
    working_location: str


@dataclass
class SeriesConfig:
    name: str

    seeds: list[int]

    conditions: list[dict]


@flow(name="process-cell-shapes")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    region_key = f"_{parameters.region}" if parameters.region is not None else ""

    for condition in series.conditions:
        for seed in series.seeds:
            merge_cell_shapes(
                series.name,
                region_key,
                condition["key"],
                seed,
                context.working_location,
                parameters.frames,
            )
            compress_cell_shapes(
                series.name,
                region_key,
                condition["key"],
                seed,
                context.working_location,
                parameters.frames,
            )
            remove_cell_shapes(
                series.name,
                region_key,
                condition["key"],
                seed,
                context.working_location,
            )


@flow(name="merge-cell-shapes")
def merge_cell_shapes(name, region_key, condition, seed, working_location, frames) -> None:
    series_key = f"{name}_{condition}_{seed:04d}"
    coeff_key = make_key(
        name, "analysis", "analysis.COEFFS", f"{series_key}{region_key}.COEFFS.csv"
    )
    coeff_key_exists = check_key(working_location, coeff_key)

    existing_frames = []
    if coeff_key_exists:
        existing_coeffs = load_dataframe(working_location, coeff_key)
        existing_frames = list(existing_coeffs["TICK"].unique())

    frame_coeffs = []

    for frame in frames:
        if frame in existing_frames:
            continue

        frame_key = make_key(
            name,
            "analysis",
            "analysis.COEFFS",
            f"{series_key}_{frame:06d}{region_key}.COEFFS.csv",
        )

        frame_coeffs.append(load_dataframe(working_location, frame_key))

    if not frame_coeffs:
        return

    coeff_dataframe = pd.concat(frame_coeffs, ignore_index=True)

    if coeff_key_exists:
        coeff_dataframe = pd.concat([existing_coeffs, coeff_dataframe], ignore_index=True)

    save_dataframe(working_location, coeff_key, coeff_dataframe, index=False)


@flow(name="compress-cell-shapes")
def compress_cell_shapes(name, region_key, condition, seed, working_location, frames) -> None:
    series_key = f"{name}_{condition}_{seed:04d}"
    coeff_key = make_key(
        name,
        "analysis",
        "analysis.COEFFS",
        f"{series_key}{region_key}.COEFFS.tar.xz",
    )
    coeff_key_exists = check_key(working_location, coeff_key)

    existing_frames = []
    if coeff_key_exists:
        existing_coeffs = load_tar(working_location, coeff_key)
        existing_frames = [
            int(re.findall(r"[0-9]{6}", member.name)[0]) for member in existing_coeffs.getmembers()
        ]

    contents = []

    for frame in frames:
        if frame in existing_frames:
            continue

        frame_key = make_key(
            name,
            "analysis",
            "analysis.COEFFS",
            f"{series_key}_{frame:06d}{region_key}.COEFFS.csv",
        )

        contents.append(frame_key)

    if not contents:
        return

    save_tar(working_location, coeff_key, contents)


@flow(name="remove-cell-shapes")
def remove_cell_shapes(name, region_key, condition, seed, working_location) -> None:
    series_key = f"{name}_{condition}_{seed:04d}"
    coeff_key = make_key(
        name,
        "analysis",
        "analysis.COEFFS",
        f"{series_key}{region_key}.COEFFS.tar.xz",
    )
    coeff_key_exists = check_key(working_location, coeff_key)

    if not coeff_key_exists:
        return

    existing_coeffs = load_tar(working_location, coeff_key)

    for member in existing_coeffs.getmembers():
        frame_key = make_key(name, "analysis", "analysis.COEFFS", member.name)

        if check_key(working_location, frame_key):
            remove_key(working_location, frame_key)