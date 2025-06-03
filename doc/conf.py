import sys
from pathlib import Path

_root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(_root_dir))

import worldpoppy as wpp

project = "WorldPopPy"
copyright = f"2025, {wpp.__author__}"
author = wpp.__author__
version = release = wpp.__version__

extensions = [
    'sphinx.ext.autodoc',  # API docs from docstrings
    'sphinx.ext.napoleon',  # Google/NumPy style docstrings
    'myst_parser',  # support .md files
]

templates_path = ['_templates']
exclude_patterns = []

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_logo = "_static/icon.png"
html_css_files = ['custom.css']