from itertools import groupby
from math import sqrt

import matplotlib.figure as mpl
import numpy as np
import pandas as pd
from prefect import task
from scipy.stats import gamma

from cell_abm_pipeline.utilities.plot import make_grid_figure

PHASE_BINS = {
    "PROLIFERATIVE_G1": np.arange(0, 7, 1),
    "PROLIFERATIVE_S": np.arange(0, 21, 1),
    "PROLIFERATIVE_G2": np.arange(0, 20, 1),
    "PROLIFERATIVE_M": np.arange(0, 20, 1),
}

PHASE_LAMBDAS = {
    "PROLIFERATIVE_G1": 8.33,
    "PROLIFERATIVE_S": 4.35,
    "PROLIFERATIVE_G2": 0.752,
    "PROLIFERATIVE_M": 28,
}

PHASE_KS = {
    "PROLIFERATIVE_G1": 17,
    "PROLIFERATIVE_S": 43,
    "PROLIFERATIVE_G2": 3,
    "PROLIFERATIVE_M": 14,
}


@task
def plot_phase_durations(
    keys: list[str], data: dict[str, pd.DataFrame], phase: str, phase_colors: dict[str, str]
) -> mpl.Figure:
    fig, gridspec, indices = make_grid_figure(keys)

    for i, j, key in indices:
        ax = fig.add_subplot(gridspec[i, j])
        ax.set_title(key)
        ax.set_xlabel("Duration (hrs)")
        ax.set_ylabel("Frequency")

        phase_durations = get_phase_durations(data[key])

        if phase not in PHASE_BINS or phase not in phase_durations:
            return None

        durations = np.array(phase_durations[phase])
        color = phase_colors[phase]

        counts, labels = np.histogram(durations, bins=PHASE_BINS[phase])
        counts = counts / np.sum(counts)
        label = f"simulated ({durations.mean():.2f} $\\pm$ {durations.std():.2f} hr)"
        ax.bar(labels[:-1], counts, align="center", color=color, alpha=0.7, label=label)

        scale = 1.0 / PHASE_LAMBDAS[phase]
        k = PHASE_KS[phase]
        x = np.linspace(gamma.ppf(0.001, k, scale=scale), gamma.ppf(0.999, k, scale=scale), 100)

        ref_mean = k / PHASE_LAMBDAS[phase]
        ref_std = sqrt(k / PHASE_LAMBDAS[phase] ** 2)
        ref_label = (
            f"reference ({ref_mean:.2f} $\\pm$ {ref_std:.2f} hr)"
            + f"\nk = {k}, $\\lambda$ = {PHASE_LAMBDAS[phase]}"
        )
        ax.plot(x, gamma.pdf(x, k, scale=scale), color=color, lw=2, label=ref_label)

        ax.legend(loc="upper right")

    return fig


def get_phase_durations(data: pd.DataFrame) -> dict[str, list[float]]:
    """Calculates phase durations for given dataframe."""
    phase_durations: dict[str, list[float]] = {}

    for _, group in data.groupby(["SEED", "ID"]):
        group.sort_values("TICK", inplace=True)
        phase_list = group[["PHASE", "time"]].to_records(index=False)
        phase_groups = [list(g) for k, g in groupby(phase_list, lambda g: g[0])]

        for group, next_group in zip(phase_groups[:-1], phase_groups[1:]):
            key, start_time = group[0]
            _, stop_time = next_group[0]

            if key not in phase_durations:
                phase_durations[key] = []

            duration = stop_time - start_time
            phase_durations[key].append(duration)

    return phase_durations
