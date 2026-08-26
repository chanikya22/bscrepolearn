"""
Reads data out of an Excel workbook stored in SharePoint via Microsoft Graph,
returning a plain pandas DataFrame. No Airflow or Postgres knowledge lives
here on purpose -- this file only knows how to talk to Graph.

Rate limiting: Microsoft Graph throttles with 429 (Too Many Requests) or,
less often, 502/503/504, and (per Microsoft's own throttling guidance)
almost always includes a Retry-After header telling you exactly how long to
wait -- that number is authoritative, never guessed or shortened. SharePoint
specifically may also send RateLimit-Reset alongside it; when both appear,
the documented advice is to honor whichever is larger. Numeric request-per-
second limits vary by tenant size and change over time, so this deliberately
does not hard-code a request rate -- it reacts correctly to what Graph says
on each response instead, which is the durable strategy Microsoft recommends.
"""
import logging
import random
import time
import urllib.parse
import requests
import pandas as pd

logger = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# 429 = throttled. 502/503/504 = transient upstream issues, safe to retry the
# same way. Deliberately NOT retrying on other 4xx (400/401/403/404) -- those
# are real problems (bad path, bad auth, permission, not found) that a retry
# won't fix and would just slow down surfacing the actual error.
RETRYABLE_STATUS_CODES = {429, 502, 503, 504}


def _write_stream(resp: requests.Response, dest_path: str) -> None:
    with open(dest_path, "wb") as handle:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)


def _odata_literal(value: str) -> str:
    """
    Escapes a value for safe use inside a single-quoted OData string literal,
    e.g. worksheets('...') or range(address='...'). Per the OData spec, an
    embedded single quote must be doubled ('') BEFORE percent-encoding --
    plain urllib.parse.quote() alone percent-encodes an apostrophe to %27
    without doubling it, which Graph decodes back to a single ' server-side
    and treats as the end of the literal, silently corrupting the rest of
    the name. This makes worksheet names like "Prakash's Sheet" work the
    same as any other name; ordinary spaces and mixed case already worked
    correctly through plain percent-encoding and don't need this.
    """
    return urllib.parse.quote(value.replace("'", "''"))


class SharePointExcelReader:
    def __init__(self, get_token_fn, max_retries: int = 5, base_backoff_seconds: float = 2.0):
        """
        get_token_fn: zero-arg callable returning a valid Graph bearer token.
        Passed in rather than imported directly so this class doesn't care
        which auth implementation (yours or excel_sync's) you use.
        max_retries: how many throttling/transient-error retries before giving up.
        base_backoff_seconds: starting point for exponential backoff, used only
            when Graph doesn't supply a Retry-After header (rare, but documented
            as possible for a handful of resource types).
        """
        self._get_token = get_token_fn
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds

    def _headers(self):
        return {"Authorization": f"Bearer {self._get_token()}"}

    def _worksheet_url(self, site_id: str, file_path: str, sheet_name: str) -> str:
        # file_path must start with "/", e.g. "/Shared Documents/Reports/Sales.xlsx"
        # (not inside a quoted OData literal, so plain percent-encoding is correct here)
        encoded_path = urllib.parse.quote(file_path)
        return (
            f"{GRAPH_BASE}/sites/{site_id}/drive/root:{encoded_path}:"
            f"/workbook/worksheets('{_odata_literal(sheet_name)}')"
        )

    def get_full_sheet(self, site_id: str, file_path: str, sheet_name: str, header_row: bool = True) -> pd.DataFrame:
        """Full-refresh read: entire used range of the sheet, all columns."""
        url = f"{self._worksheet_url(site_id, file_path, sheet_name)}/usedRange(valuesOnly=true)"
        return self._fetch_values(url, header_row)

    def get_range(self, site_id: str, file_path: str, sheet_name: str, address: str, header_row: bool = True) -> pd.DataFrame:
        """Read a fixed A1-style range, e.g. address='A1:M500'."""
        url = f"{self._worksheet_url(site_id, file_path, sheet_name)}/range(address='{_odata_literal(address)}')"
        return self._fetch_values(url, header_row)

    def download_file(self, site_id: str, file_path: str, dest_path: str) -> None:
        """
        Download the .xlsx bytes via /drive/.../content. Does not open Excel
        Online, so it does not recalculate formulas or follow external links.
        Follows the Graph CDN redirect without forwarding the bearer token.
        """
        encoded_path = urllib.parse.quote(file_path)
        url = f"{GRAPH_BASE}/sites/{site_id}/drive/root:{encoded_path}:/content"
        timeout = (30, 1800)
        attempt = 0
        while True:
            meta = requests.get(
                url, headers=self._headers(), allow_redirects=False, stream=True, timeout=timeout
            )
            try:
                if meta.status_code in RETRYABLE_STATUS_CODES:
                    attempt += 1
                    if attempt > self.max_retries:
                        meta.raise_for_status()
                    wait_seconds = self._compute_wait_seconds(meta, attempt)
                    logger.warning(
                        f"Graph content returned {meta.status_code} "
                        f"(attempt {attempt}/{self.max_retries}); waiting {wait_seconds:.1f}s: {url}"
                    )
                    time.sleep(wait_seconds)
                    continue
                if meta.status_code in (301, 302, 303, 307, 308):
                    download_url = meta.headers.get("Location")
                    if not download_url:
                        raise RuntimeError(f"Graph content redirect had no Location: {url}")
                else:
                    meta.raise_for_status()
                    _write_stream(meta, dest_path)
                    return
            finally:
                meta.close()

            with requests.get(download_url, stream=True, timeout=timeout) as resp:
                resp.raise_for_status()
                _write_stream(resp, dest_path)
            return

    def _compute_wait_seconds(self, resp: requests.Response, attempt: int) -> float:
        candidates = []
        for header_name in ("Retry-After", "RateLimit-Reset"):
            raw = resp.headers.get(header_name)
            if raw is not None:
                try:
                    candidates.append(float(raw))
                except ValueError:
                    pass

        if candidates:
            # Both headers can appear together; the documented guidance is to
            # honor whichever value is larger, not just Retry-After alone.
            return max(candidates)

        # No header at all (documented as possible for a few resource types):
        # exponential backoff with jitter so parallel tasks don't all retry
        # in lockstep and immediately re-trigger the same throttling.
        return (self.base_backoff_seconds * (2 ** (attempt - 1))) + random.uniform(0, 1)

    def _get_with_retry(self, url: str) -> requests.Response:
        attempt = 0
        while True:
            resp = requests.get(url, headers=self._headers(), timeout=60)

            if resp.status_code not in RETRYABLE_STATUS_CODES:
                resp.raise_for_status()
                return resp

            attempt += 1
            if attempt > self.max_retries:
                logger.error(
                    f"Graph API still returning {resp.status_code} after {self.max_retries} "
                    f"retries, giving up: {url}"
                )
                resp.raise_for_status()  # raises HTTPError with the real status/body

            wait_seconds = self._compute_wait_seconds(resp, attempt)
            logger.warning(
                f"Graph API returned {resp.status_code} (attempt {attempt}/{self.max_retries}); "
                f"waiting {wait_seconds:.1f}s before retrying, per Retry-After/RateLimit-Reset: {url}"
            )
            time.sleep(wait_seconds)

    def _fetch_values(self, url: str, header_row: bool) -> pd.DataFrame:
        resp = self._get_with_retry(url)
        values = resp.json().get("values", [])

        if not values:
            return pd.DataFrame()

        if header_row:
            headers, *rows = values
            df = pd.DataFrame(rows, columns=headers)
        else:
            df = pd.DataFrame(values)
            df.columns = [f"col_{i}" for i in range(len(df.columns))]

        logger.info(f"Fetched {len(df)} rows x {len(df.columns)} cols from {url.split('?')[0]}")
        return df