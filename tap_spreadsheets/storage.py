"""Storage abstraction using fsspec."""

from __future__ import annotations
import os
import typing as t
from fsspec.core import url_to_fs
from urllib.parse import urlparse
from datetime import timezone, datetime


class Storage:
    """Filesystem abstraction to list and open files using fsspec."""

    def __init__(self, path_glob: str, protocol: str | None = None) -> None:
        self.path_glob = path_glob
        # Ensure fsspec knows how to reach MinIO/S3
        if path_glob.startswith("s3://"):
            storage_options = {
                "key": os.getenv("AWS_ACCESS_KEY_ID", None),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY", None),
                "client_kwargs": {
                    "endpoint_url": os.getenv("AWS_S3_ENDPOINT_URL", None)
                },
            }
        else:
            storage_options = {}

        self.fs, _ = url_to_fs(path_glob, **storage_options)

    def glob(self) -> list[str]:
        """Return matching files for glob."""
        return self.fs.glob(self.path_glob)

    def open(self, path: str, mode: str = "rb") -> t.IO:
        """Open a file handle with fsspec."""
        return self.fs.open(path, mode)

    def describe(self, path: str) -> dict[str, t.Any]:
        """Return normalized file metadata across local and remote storages."""
        try:
            info = self.fs.info(path)
        except Exception:
            # fallback for local files
            st = os.stat(path)
            info = {"name": path, "size": st.st_size, "mtime": st.st_mtime}

        # Normalize mtime field
        mtime_val: t.Any = info.get("mtime") or info.get("last_modified")
        if isinstance(mtime_val, (int, float)):
            mtime = datetime.fromtimestamp(mtime_val, tz=timezone.utc)
        elif hasattr(mtime_val, "timestamp"):
            mtime = datetime.fromtimestamp(mtime_val.timestamp(), tz=timezone.utc)
        elif isinstance(mtime_val, str):
            # e.g., '2025-10-05T12:34:56Z'
            try:
                clean = mtime_val.replace("Z", "+00:00")
                mtime = datetime.fromisoformat(clean).astimezone(timezone.utc)
            except Exception:
                mtime = datetime.now(timezone.utc)
        else:
            mtime = datetime.now(timezone.utc)

        return {
            "path": self.normalize_path(path),
            "size": info.get("size"),
            "mtime": mtime.replace(microsecond=0),
        }

    def normalize_path(self, path: str) -> str:
        """Normalize local/remote path."""
        if path.startswith(("s3://", "gs://", "http://", "https://")):
            return path
        if path.startswith("file://"):
            return urlparse(path).path
        return os.path.abspath(path)
