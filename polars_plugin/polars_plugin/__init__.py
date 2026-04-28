"""Polars expression plugin for reconstructing OpenAlex abstracts."""

from pathlib import Path

import polars as pl
from polars.plugins import register_plugin_function

_LIB = Path(__file__).parent


def reconstruct_abstract(expr: pl.Expr) -> pl.Expr:
    """Reconstruct abstract text from inverted index JSON column.

    Usage:
        from polars_plugin import reconstruct_abstract

        lf.select(reconstruct_abstract(pl.col("abstract_inverted_index")))
    """
    return register_plugin_function(
        plugin_path=_LIB,
        function_name="reconstruct_abstract",
        args=[expr],
        is_elementwise=True,
    )
