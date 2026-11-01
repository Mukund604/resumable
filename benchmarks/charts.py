"""Shared chart style. Every figure is saved in light and dark versions for GitHub's themes."""
import pathlib

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", muted="#52514e", grid="#e4e3df",
                  series=["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]),
    "dark": dict(surface="#1a1a19", ink="#f0efec", muted="#c3c2b7", grid="#3a3a37",
                 series=["#3987e5", "#d95926", "#199e70", "#c98500"]),
}


def save(draw, name, size=(8, 4.2), folder="results/charts"):
    """Calls draw(figure, theme) once per theme and writes name-light.png and name-dark.png."""
    pathlib.Path(folder).mkdir(parents=True, exist_ok=True)
    for mode, theme in THEMES.items():
        plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "text.color": theme["ink"],
                             "axes.labelcolor": theme["muted"], "xtick.color": theme["muted"], "ytick.color": theme["muted"],
                             "axes.edgecolor": theme["grid"], "axes.titlelocation": "left", "axes.titlesize": 11,
                             "axes.titleweight": "bold", "legend.frameon": False})
        figure = plt.figure(figsize=size, dpi=170, facecolor=theme["surface"])
        draw(figure, theme)
        for ax in figure.axes:
            ax.set_facecolor(theme["surface"])
            ax.spines[["top", "right", "left"]].set_visible(False)
            ax.grid(axis="y", color=theme["grid"], linewidth=0.7)
            ax.set_axisbelow(True)
            ax.tick_params(length=0)
        figure.savefig(f"{folder}/{name}-{mode}.png", bbox_inches="tight", facecolor=theme["surface"])
        plt.close(figure)
