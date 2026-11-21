"""Result mapping of `TradingViewClient.get_historical_batch`."""

from collections.abc import Iterable, Mapping
from typing import Any


class HistoricalBatch(dict[str, Any]):
    """Results keyed by ``"EXCHANGE:SYMBOL"``, plus the symbols that failed.

    Values are ``HistoricalSeries`` or, with ``as_dataframe=True``, Polars
    DataFrames. ``failed`` maps ``"EXCHANGE:SYMBOL"`` to an error message such
    as ``"TimeoutError: ..."`` for every symbol that is not in the mapping:
    symbols that raised, and with ``as_dataframe=True`` and
    ``allow_partial=False`` symbols whose history came back incomplete.
    """

    failed: dict[str, str]

    def __init__(
        self,
        results: Mapping[str, Any] | Iterable[tuple[str, Any]] = (),
        failed: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(results)
        self.failed = dict(failed or {})

    def __repr__(self) -> str:
        return f"HistoricalBatch({dict.__repr__(self)}, failed={self.failed!r})"
