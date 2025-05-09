from matplotlib import pyplot as plt
from pandas._typing import RandomState  # noqa


def module_available(module_name):
    """Check if a Python module is available for import."""
    try:
        exec(f"import {module_name}")
    except ModuleNotFoundError:
        return False
    else:
        return True


def clean_axis(ax=None, title=None):
    ax = plt.gca() if ax is None else ax

    if title is not None:
        ax.set_title(title)

    ax.set_aspect('equal')
    ax.set_xlabel('')
    ax.set_ylabel('')
