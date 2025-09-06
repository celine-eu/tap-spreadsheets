"""Storage abstraction using fsspec."""

from __future__ import annotations

import typing as t
from fsspec.core import url_to_fs


class Storage:
    """Filesystem abstraction to list and open files using fsspec."""

    def __init__(self, path_glob: str, protocol: str | None = None) -> None:
        self.path_glob = path_glob
        self.fs, _ = url_to_fs(path_glob)

    def glob(self) -> list[str]:
        """Return matching files for glob."""
        return self.fs.glob(self.path_glob)

    def open(self, path: str, mode: str = "rb") -> t.IO:
        """Open a file handle with fsspec."""
        return self.fs.open(path, mode)
