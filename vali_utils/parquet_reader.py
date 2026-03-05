"""
Parquet reader utilities for S3 validation.

Reads specific row groups from remote parquet files via HTTP Range requests,
avoiding full-file downloads. Used by DuckDBSampledValidator for dedup,
empty checks, content matching, and scraper validation.
"""

import random
import requests
import pandas as pd
import pyarrow.parquet as pq
import bittensor as bt
from typing import List, Optional


class RangeRequestFile:
    """File-like object that reads from HTTP presigned URLs via Range requests.

    PyArrow's ParquetFile needs a seekable file. Presigned URLs don't support
    HEAD requests (403), so fsspec fails. This class wraps a presigned GET URL
    and translates read() calls into Range GET requests.

    Usage:
        f = RangeRequestFile(presigned_url, file_size)
        pf = pq.ParquetFile(f)
        df = pf.read_row_group(0, columns=['url']).to_pandas()
        f.close()
    """

    def __init__(self, url: str, size: int):
        self.url = url
        self.size = size
        self.pos = 0
        self.closed = False
        self._session = requests.Session()

    def seek(self, pos, whence=0):
        if whence == 0:
            self.pos = pos
        elif whence == 1:
            self.pos += pos
        elif whence == 2:
            self.pos = self.size + pos
        return self.pos

    def tell(self):
        return self.pos

    def read(self, n=-1):
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if n == -1:
            n = self.size - self.pos
        if n <= 0:
            return b""
        end = min(self.pos + n - 1, self.size - 1)
        resp = self._session.get(
            self.url, headers={"Range": f"bytes={self.pos}-{end}"}, timeout=30
        )
        if resp.status_code not in (200, 206):
            raise IOError(f"Range request failed: {resp.status_code}")
        data = resp.content
        self.pos += len(data)
        return data

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    @property
    def mode(self):
        return "rb"

    def __len__(self):
        return self.size

    def close(self):
        self.closed = True
        self._session.close()


def read_random_row_group(
    presigned_url: str,
    file_size: int,
    columns: Optional[List[str]] = None,
    max_rows: Optional[int] = None,
) -> Optional[pd.DataFrame]:
    """Read a random row group from a remote parquet file via Range requests.

    Downloads only the selected row group's column chunks — not the entire file.
    For a 512MB file with 10K row groups, this reads ~600KB (url+text) or ~3MB (all cols).

    Args:
        presigned_url: Presigned GET URL for the parquet file.
        file_size: Known file size in bytes (from S3 listing, avoids HEAD request).
        columns: Column names to read. None = all columns.
        max_rows: If set, randomly sample this many rows from the row group.

    Returns:
        pandas DataFrame with the row group data, or None on error.
    """
    f = None
    try:
        f = RangeRequestFile(presigned_url, file_size)
        pf = pq.ParquetFile(f)

        num_rg = pf.metadata.num_row_groups
        if num_rg == 0:
            return None

        rg_idx = random.randint(0, num_rg - 1)
        table = pf.read_row_group(rg_idx, columns=columns)
        df = table.to_pandas()

        if max_rows and len(df) > max_rows:
            df = df.sample(n=max_rows)

        return df

    except Exception as e:
        bt.logging.debug(f"Failed to read row group: {e}")
        return None
    finally:
        if f is not None:
            try:
                f.close()
            except Exception:
                pass
