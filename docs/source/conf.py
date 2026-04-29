"""Sphinx configuration file for the Engi-Stream Core project."""
from typing import Any

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Engi-Stream Core"
copyright = "2026, Engi-Stream Team"
author = "Engi-Stream Team"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
]

templates_path: list[str] = ["_templates"]
exclude_patterns: list[str] = []

myst_enable_extensions: list[str] = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "linkify",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme: str = "furo"
html_static_path: list[str] = ["_static"]
html_title: str = "Engi-Stream Core"

html_theme_options: dict[str, Any] = {
    "light_css_variables": {
        "color-brand-primary": "#303f9f",
        "color-brand-content": "#303f9f",
    },
    "dark_css_variables": {
        "color-brand-primary": "#7986cb",
        "color-brand-content": "#7986cb",
    },
}
