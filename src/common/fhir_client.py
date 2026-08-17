"""
fhir_client.py
--------------
Thin, dependency-light client for the public HAPI FHIR R4 server.

Responsibilities (kept separate from Spark code so it's unit-testable
without a cluster):
  1. Build an incremental search request (_lastUpdated + _count).
  2. Walk pagination via Bundle.link[relation=next] until exhausted.
  3. Yield each page's raw JSON text + the request metadata used to
     produce it (url/params) -- this is what lands, untouched, in the
     Raw layer.
  4. Track the max meta.lastUpdated seen, so the caller can advance the
     watermark after a successful run.

Retries use exponential backoff and respect HTTP 429 (the public HAPI
server rate-limits aggressively).
"""

import time
import random
from dataclasses import dataclass, field
from typing import Iterator, Optional
import requests


@dataclass
class FhirPage:
    resource_type: str
    page_number: int
    raw_json: str                 # exact response body, byte-for-byte
    request_url: str              # full URL incl. params -> api_url_or_params
    fetched_at_iso: str
    entry_count: int


@dataclass
class FetchResult:
    pages: list = field(default_factory=list)
    max_last_updated: Optional[str] = None
    total_entries: int = 0


class FhirClient:
    def __init__(self, base_url: str, page_size: int = 100,
                 max_retries: int = 5, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/fhir+json"})

    def _get(self, url: str) -> requests.Response:
        """GET with exponential backoff on 429 / 5xx / transient errors."""
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = (2 ** attempt) + random.uniform(0, 0.5)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep((2 ** attempt) + random.uniform(0, 0.5))
        raise RuntimeError(f"GET {url} failed after {self.max_retries} retries") from last_exc

    @staticmethod
    def _next_link(bundle: dict) -> Optional[str]:
        for link in bundle.get("link", []):
            if link.get("relation") == "next":
                return link.get("url")
        return None

    @staticmethod
    def _bundle_max_last_updated(bundle: dict, current_max: Optional[str]) -> Optional[str]:
        for entry in bundle.get("entry", []):
            lu = entry.get("resource", {}).get("meta", {}).get("lastUpdated")
            if lu and (current_max is None or lu > current_max):
                current_max = lu
        return current_max

    def fetch_incremental(self, resource_type: str,
                           since_iso: Optional[str],
                           now_iso: str) -> FetchResult:
        """
        Fetch all pages for `resource_type` updated after `since_iso`
        (None => full initial load). Returns raw JSON pages untouched,
        plus the watermark to persist on success.
        """
        params = f"_count={self.page_size}&_sort=_lastUpdated"
        if since_iso:
            params += f"&_lastUpdated=gt{since_iso}"
        url = f"{self.base_url}/{resource_type}?{params}"

        result = FetchResult()
        page_num = 0
        while url:
            resp = self._get(url)
            bundle = resp.json()
            entries = bundle.get("entry", [])

            result.pages.append(FhirPage(
                resource_type=resource_type,
                page_number=page_num,
                raw_json=resp.text,
                request_url=url,
                fetched_at_iso=now_iso,
                entry_count=len(entries),
            ))
            result.total_entries += len(entries)
            result.max_last_updated = self._bundle_max_last_updated(
                bundle, result.max_last_updated
            )

            url = self._next_link(bundle)
            page_num += 1

        return result
