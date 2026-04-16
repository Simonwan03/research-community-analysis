"""Reusable Semantic Scholar Academic Graph API client."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from config import ApiConfig

LOGGER = logging.getLogger(__name__)


class SemanticScholarClient:
    """Small Semantic Scholar API client with retry, rate-limit, and disk cache support."""

    def __init__(self, config: ApiConfig, cache_dir: Path | None = None, use_cache: bool = True):
        self.config = config
        self.cache_dir = cache_dir
        self.use_cache = use_cache and cache_dir is not None
        self.api_key = os.getenv(config.api_key_env_var, "").strip()
        self._last_request_at = 0.0
        self._throttle_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        if self.use_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def search_papers(self, query: str, year: int | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        """Search for papers by title or query text."""

        params: dict[str, Any] = {
            "query": query,
            "limit": limit or self.config.search_limit,
            "fields": ",".join(
                [
                    "paperId",
                    "title",
                    "year",
                    "authors",
                    "externalIds",
                    "venue",
                    "url",
                    "citationCount",
                    "referenceCount",
                ]
            ),
        }
        if year is not None:
            params["year"] = str(year)

        payload = self._get("/paper/search", params=params)
        return list(payload.get("data") or [])

    def get_paper(self, paper_id: str, fields: tuple[str, ...]) -> dict[str, Any] | None:
        """Fetch a paper by Semantic Scholar paperId or supported external ID."""

        safe_id = urllib.parse.quote(paper_id, safe=":")
        try:
            return self._get(f"/paper/{safe_id}", params={"fields": ",".join(fields)})
        except FileNotFoundError:
            return None

    def get_paper_by_doi(self, doi: str, fields: tuple[str, ...]) -> dict[str, Any] | None:
        """Fetch a paper by DOI using Semantic Scholar's DOI external-id lookup."""

        normalized = doi.strip()
        if not normalized:
            return None
        return self.get_paper(f"DOI:{normalized}", fields=fields)

    def get_papers_batch(self, paper_ids: list[str], fields: tuple[str, ...]) -> list[dict[str, Any] | None]:
        """Fetch paper metadata in batches. Caller controls batch size."""

        if not paper_ids:
            return []
        payload = {"ids": paper_ids}
        response = self._post("/paper/batch", params={"fields": ",".join(fields)}, payload=payload)
        if isinstance(response, list):
            return response
        return list(response.get("data") or [])

    def get_references(self, paper_id: str, fields: tuple[str, ...]) -> list[dict[str, Any]]:
        """Fetch all outgoing references for a paper through the paginated references endpoint."""

        references: list[dict[str, Any]] = []
        offset = 0
        safe_id = urllib.parse.quote(paper_id, safe=":")

        while True:
            params = {
                "fields": ",".join(fields),
                "limit": self.config.reference_page_size,
                "offset": offset,
            }
            try:
                payload = self._get(f"/paper/{safe_id}/references", params=params)
            except FileNotFoundError:
                return references

            batch = list(payload.get("data") or [])
            references.extend(batch)

            next_offset = payload.get("next")
            if next_offset is None:
                break
            offset = int(next_offset)

        return references

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._request_json("GET", path, params=params)

    def _post(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        return self._request_json("POST", path, params=params, payload=payload)

    def _request_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        cache_path = self._cache_path(method, path, params, payload)
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached

        url = self._url(path, params)
        body = None
        headers = {
            "Accept": "application/json",
            "User-Agent": "research-community-analysis/1.0 paper-citation-graph",
        }
        if self.api_key:
            headers["x-api-key"] = self.api_key
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        for attempt in range(self.config.max_retries + 1):
            self._throttle()
            request = urllib.request.Request(url, data=body, headers=headers, method=method)
            try:
                with urllib.request.urlopen(request, timeout=self.config.timeout_seconds) as response:
                    text = response.read().decode("utf-8")
                    data = json.loads(text) if text else {}
                if cache_path:
                    self._write_cache(cache_path, data)
                return data
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    raise FileNotFoundError(url) from exc
                if exc.code in {429, 500, 502, 503, 504} and attempt < self.config.max_retries:
                    self._sleep_before_retry(exc, attempt, url)
                    continue
                error_body = self._safe_error_body(exc)
                raise RuntimeError(f"HTTP {exc.code} for {url}: {error_body}") from exc
            except urllib.error.URLError as exc:
                if attempt < self.config.max_retries:
                    LOGGER.warning("Network error for %s: %s", url, exc)
                    time.sleep(self._backoff_seconds(attempt))
                    continue
                raise RuntimeError(f"Network error for {url}: {exc}") from exc

        raise RuntimeError(f"Exceeded retry limit for {url}")

    def _url(self, path: str, params: dict[str, Any] | None) -> str:
        url = self.config.base_url.rstrip("/") + path
        if params:
            url += "?" + urllib.parse.urlencode(params, doseq=True)
        return url

    def _cache_path(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        payload: dict[str, Any] | None,
    ) -> Path | None:
        if not self.use_cache or self.cache_dir is None:
            return None
        key = json.dumps(
            {"method": method, "path": path, "params": params or {}, "payload": payload or {}},
            sort_keys=True,
        )
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def _throttle(self) -> None:
        with self._throttle_lock:
            elapsed = time.monotonic() - self._last_request_at
            remaining = self.config.request_interval_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
            self._last_request_at = time.monotonic()

    def _read_cache(self, cache_path: Path | None) -> Any | None:
        if cache_path is None or not cache_path.exists():
            return None
        with self._cache_lock:
            if not cache_path.exists():
                return None
            return json.loads(cache_path.read_text(encoding="utf-8"))

    def _write_cache(self, cache_path: Path, data: Any) -> None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = cache_path.with_name(f"{cache_path.name}.{threading.get_ident()}.tmp")
        text = json.dumps(data, ensure_ascii=False, indent=2)
        with self._cache_lock:
            temp_path.write_text(text, encoding="utf-8")
            temp_path.replace(cache_path)

    def _sleep_before_retry(self, exc: urllib.error.HTTPError, attempt: int, url: str) -> None:
        retry_after = exc.headers.get("Retry-After") if exc.headers else None
        if retry_after:
            try:
                wait_seconds = max(float(retry_after), self.config.request_interval_seconds)
            except ValueError:
                wait_seconds = self._backoff_seconds(attempt)
        else:
            wait_seconds = self._backoff_seconds(attempt)
        LOGGER.warning("HTTP %s for %s; retrying in %.1fs", exc.code, url, wait_seconds)
        time.sleep(wait_seconds)

    def _backoff_seconds(self, attempt: int) -> float:
        return min(120.0, self.config.backoff_base_seconds * (2**attempt))

    @staticmethod
    def _safe_error_body(exc: urllib.error.HTTPError) -> str:
        try:
            return exc.read().decode("utf-8")[:500]
        except Exception:
            return ""
