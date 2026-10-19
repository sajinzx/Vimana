"""Gzip JSONL archive of raw envelopes: data/raw/{source}/{YYYY-MM-DD}/{HH}.jsonl.gz.

The archive is the cold-storage copy of the ingest stream and the input for
the replay provider. One file per UTC hour; each flush is a complete gzip
member so a partly written file is still readable.
"""

from __future__ import annotations

import gzip
from datetime import UTC, datetime
from pathlib import Path

import orjson


class Recorder:
    def __init__(self, root: str | Path, source: str) -> None:
        self.root = Path(root)
        self.source = source
        self._buf: list[bytes] = []
        self._path: Path | None = None

    def path_for(self, ts_ms: int) -> Path:
        t = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
        return self.root / self.source / t.strftime("%Y-%m-%d") / f"{t:%H}.jsonl.gz"

    def write(self, envelope: dict, ts_ms: int) -> None:
        path = self.path_for(ts_ms)
        if self._path is not None and path != self._path:
            self.flush()
        self._path = path
        self._buf.append(orjson.dumps(envelope))

    def flush(self) -> None:
        if not self._buf or self._path is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = b"\n".join(self._buf) + b"\n"
        with gzip.open(self._path, "ab") as fh:
            fh.write(data)
        self._buf.clear()

    def close(self) -> None:
        self.flush()


def read_envelopes(path: str | Path):
    """Yield envelopes from one archive file."""
    with gzip.open(path, "rb") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield orjson.loads(line)
