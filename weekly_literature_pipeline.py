"""Daily literature digest pipeline with separate daily and weekly archives.

Designed for Google Colab + Europe PMC + Gemini + GitHub Pages.

Main behavior
-------------
1. Search a rolling Europe PMC lookback window every day.
2. Upsert papers into a persistent paper database.
3. Send only unprocessed/retryable papers to Gemini, within per-run limits.
4. Keep failed papers in the queue for a later run instead of marking them seen.
5. Render ``latest.html`` as the current day's newly detected papers.
6. Preserve one HTML page per first-seen day under ``daily/``.
7. Build completed Monday-Sunday archives under ``weeks/`` from saved results,
   without calling Gemini again for already completed papers.
"""

from __future__ import annotations

import base64
import hashlib
import html
import json
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

EPMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
GITHUB_API = "https://api.github.com"
PROMPT_VERSION = "2026-07-27-gemini-v7-five-categories"
CATEGORY_SCHEME_VERSION = "2026-07-27-five-research-categories-v1"
TOKYO = ZoneInfo("Asia/Tokyo")


DEFAULT_CATEGORIES = [
    {
        "id": "gut_microbiome",
        "ja": "腸内細菌",
        "en": "Gut microbiome",
        "description": (
            "Gut microorganisms, microbial communities, bacterial metabolism, "
            "host-microbe interactions, intestinal colonization, and microbiome-derived metabolites. "
            "Use this category when none of the more specific categories below is the central focus."
        ),
    },
    {
        "id": "enteric_nervous_system",
        "ja": "腸管神経",
        "en": "Enteric nervous system",
        "description": (
            "Enteric neurons, enteric glia, gut sensory or motor circuits, intestinal motility, "
            "visceral sensation, and gut-brain neural pathways. Use neuroimmunology instead when "
            "the neural-immune interaction is the central question."
        ),
    },
    {
        "id": "supersulfides",
        "ja": "超硫黄分子・超硫黄修飾",
        "en": "Supersulfides and supersulfidation",
        "description": (
            "Supersulfides, persulfides, polysulfides, protein persulfidation or supersulfidation, "
            "reactive sulfur species, and closely related sulfur-redox biology. This category takes "
            "priority over the general post-translational-modification category."
        ),
    },
    {
        "id": "post_translational_modification",
        "ja": "翻訳後修飾",
        "en": "Post-translational modification",
        "description": (
            "Protein post-translational modifications such as phosphorylation, ubiquitination, "
            "acetylation, methylation, glycosylation, lipidation, nitrosylation, sulfenylation, "
            "and other covalent protein modifications, excluding supersulfidation-focused work."
        ),
    },
    {
        "id": "neuroimmunology",
        "ja": "神経免疫",
        "en": "Neuroimmunology",
        "description": (
            "Bidirectional interactions between nervous and immune systems, neuroinflammation, "
            "immune control of neuronal or glial function, and neural regulation of immunity. "
            "This includes gut neuroimmune studies when immune-neural crosstalk is central."
        ),
    },
]


@dataclass
class PipelineConfig:
    # Site
    site_title: str = "Weekly Literature Digest"
    site_subtitle: str = "AI-assisted literature update"
    language_note: str = (
        "Summaries are generated from titles and abstracts. "
        "Verify the original paper before use."
    )
    public_base_url: str = ""

    # Daily retrieval and display windows
    run_date: str = ""  # YYYY-MM-DD; empty = today in Asia/Tokyo
    fetch_lookback_days: int = 7
    # Search still looks back several days to catch indexing delays.
    # The HTML ``latest.html`` itself shows only the current first-seen date.
    latest_window_days: int = 1
    include_preprints: bool = False
    max_raw_results_per_query: int = 1000

    # Search
    journals: list[str] = field(default_factory=list)
    always_include_journals: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    search_keywords_outside_journals: bool = False
    topic_scope: str = (
        "Immunology, microbiology, host-microbe interactions, mucosal biology, "
        "infection, inflammation, and related mechanistic biomedical research."
    )
    categories: list[dict[str, str]] = field(default_factory=lambda: list(DEFAULT_CATEGORIES))

    # Gemini
    enable_ai: bool = True
    gemini_api_key: str = ""
    gemini_model: str = "gemini-3.5-flash"
    max_screening_per_run: int = 40
    max_summaries_per_run: int = 10
    max_reclassification_per_run: int = 50
    screening_batch_size: int = 20
    summary_batch_size: int = 5
    reclassification_batch_size: int = 25
    request_interval_seconds: float = 2.0
    retry_failed_same_day: bool = False
    max_retries_per_paper: int = 10

    # Weekly archives
    archive_weeks_to_rebuild: int = 2
    max_papers_per_week: int = 100
    daily_entries_on_home: int = 14
    weekly_entries_on_home: int = 12

    # GitHub Pages
    publish_to_github: bool = False
    github_token: str = ""
    github_repo: str = ""
    github_branch: str = "main"
    pages_dir: str = "docs"

    # Storage
    workdir: str = "/content/weekly_literature"

    def validate(self) -> None:
        if not self.journals and not self.keywords:
            raise ValueError("At least one journal or keyword must be configured.")
        if self.fetch_lookback_days < 1 or self.latest_window_days < 1:
            raise ValueError("fetch_lookback_days and latest_window_days must be >= 1")
        if self.daily_entries_on_home < 1 or self.weekly_entries_on_home < 1:
            raise ValueError("Archive entry limits must be >= 1")
        if self.enable_ai and not self.gemini_api_key:
            raise ValueError("enable_ai=True requires a Gemini API key.")
        if (
            self.max_screening_per_run < 0
            or self.max_summaries_per_run < 0
            or self.max_reclassification_per_run < 0
        ):
            raise ValueError("Per-run AI limits must be >= 0")
        if (
            self.screening_batch_size < 1
            or self.summary_batch_size < 1
            or self.reclassification_batch_size < 1
        ):
            raise ValueError("Batch sizes must be >= 1")
        if self.publish_to_github and (not self.github_token or not self.github_repo):
            raise ValueError("GitHub publishing requires github_token and github_repo.")
        if self.github_repo and "/" not in self.github_repo:
            raise ValueError("github_repo must be in owner/repository format.")

    def today(self) -> date:
        return date.fromisoformat(self.run_date) if self.run_date else datetime.now(TOKYO).date()

    def fetch_dates(self) -> dict[str, date]:
        end = self.today()
        start = end - timedelta(days=self.fetch_lookback_days - 1)
        return {"start": start, "end": end}

    def latest_dates(self) -> dict[str, date]:
        end = self.today()
        start = end - timedelta(days=self.latest_window_days - 1)
        return {"start": start, "end": end}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def batched(items: list[Any], size: int) -> Iterable[list[Any]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def parse_partial_date(value: str) -> Optional[date]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            parsed = datetime.strptime(value, fmt).date()
            if fmt == "%Y-%m":
                return parsed.replace(day=1)
            if fmt == "%Y":
                return parsed.replace(month=1, day=1)
            return parsed
        except ValueError:
            pass
    match = re.search(r"(19|20)\d{2}(?:-\d{2})?(?:-\d{2})?", value)
    return parse_partial_date(match.group(0)) if match else None


def sha1_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def now_iso() -> str:
    return datetime.now(TOKYO).isoformat(timespec="seconds")


def request_with_retry(
    method: str,
    url: str,
    *,
    max_attempts: int = 5,
    timeout: int = 60,
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504),
    **kwargs: Any,
) -> requests.Response:
    error: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
            if response.status_code not in retry_statuses:
                return response
            error = RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")
        except requests.RequestException as exc:
            error = exc
        if attempt < max_attempts - 1:
            time.sleep(min(2**attempt, 20))
    raise RuntimeError(f"Request failed after {max_attempts} attempts: {url}") from error


def extract_json(text: str) -> Any:
    text = text.strip()
    fenced = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.S | re.I)
    if fenced:
        text = fenced.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        starts = [pos for pos in (text.find("{"), text.find("[")) if pos >= 0]
        if not starts:
            raise
        start = min(starts)
        for end in range(len(text), start, -1):
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                continue
        raise


def week_bounds(day: date) -> tuple[date, date]:
    start = day - timedelta(days=day.weekday())
    return start, start + timedelta(days=6)


def completed_week_bounds(today: date, offset: int = 0) -> tuple[date, date]:
    current_week_start, _ = week_bounds(today)
    end = current_week_start - timedelta(days=1 + offset * 7)
    return end - timedelta(days=6), end


# ---------------------------------------------------------------------------
# Europe PMC
# ---------------------------------------------------------------------------


def epmc_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def build_field_or(field_name: str, values: list[str]) -> str:
    return "(" + " OR ".join(f"{field_name}:{epmc_quote(v)}" for v in values) + ")"


class EuropePMCClient:
    def search(self, query: str, max_results: int = 1000) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        cursor = "*"
        while len(results) < max_results:
            page_size = min(1000, max_results - len(results))
            response = request_with_retry(
                "GET",
                EPMC_SEARCH_URL,
                params={
                    "query": query,
                    "format": "json",
                    "resultType": "core",
                    "pageSize": page_size,
                    "cursorMark": cursor,
                },
            )
            response.raise_for_status()
            payload = response.json()
            page = payload.get("resultList", {}).get("result", []) or []
            results.extend(page)
            next_cursor = payload.get("nextCursorMark")
            if not page or not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        return results[:max_results]


def _publication_date(raw: dict[str, Any]) -> str:
    journal_info = raw.get("journalInfo") or {}
    return (
        raw.get("firstPublicationDate")
        or raw.get("electronicPublicationDate")
        or journal_info.get("electronicPublicationDate")
        or journal_info.get("printPublicationDate")
        or raw.get("pubYear")
        or ""
    )


def normalize_epmc_record(raw: dict[str, Any], matched_by: str) -> dict[str, Any]:
    doi = clean_text(raw.get("doi")).lower()
    pmid = clean_text(raw.get("pmid"))
    pmcid = clean_text(raw.get("pmcid"))
    source = clean_text(raw.get("source"))
    source_id = clean_text(raw.get("id"))
    stable = (
        doi
        or (f"PMID:{pmid}" if pmid else "")
        or (f"PMCID:{pmcid}" if pmcid else "")
        or f"{source}:{source_id}"
    )
    uid = sha1_text(stable)[:16]
    pub_types = (
        raw.get("pubTypeList", {}).get("pubType", [])
        if isinstance(raw.get("pubTypeList"), dict)
        else []
    )
    if isinstance(pub_types, str):
        pub_types = [pub_types]
    if pmid:
        epmc_url = f"https://europepmc.org/article/MED/{quote(pmid)}"
    elif source and source_id:
        epmc_url = f"https://europepmc.org/article/{quote(source)}/{quote(source_id)}"
    else:
        epmc_url = "https://europepmc.org/"
    publication_date = _publication_date(raw)
    parsed = parse_partial_date(publication_date)
    return {
        "uid": uid,
        "stable_id": stable,
        "source": source,
        "source_id": source_id,
        "pmid": pmid,
        "pmcid": pmcid,
        "doi": doi,
        "title": clean_text(raw.get("title")),
        "abstract": clean_text(raw.get("abstractText")),
        "authors": clean_text(raw.get("authorString")),
        "journal": clean_text(
            raw.get("journalTitle")
            or (raw.get("journalInfo") or {}).get("journal", {}).get("title")
        ),
        "publication_date": publication_date,
        "publication_date_parsed": parsed.isoformat() if parsed else "",
        "pub_types": [clean_text(item) for item in pub_types],
        "is_open_access": str(raw.get("isOpenAccess", "")).upper() == "Y",
        "cited_by_count": int(raw.get("citedByCount") or 0),
        "epmc_url": epmc_url,
        "doi_url": f"https://doi.org/{quote(doi, safe='/')}" if doi else "",
        "matched_by": [matched_by],
        "keyword_hits": [],
        "journal_hits": [],
    }


def merge_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for record in records:
        key = record["stable_id"]
        if key not in merged:
            merged[key] = record
            continue
        current = merged[key]
        current["matched_by"] = sorted(set(current["matched_by"] + record["matched_by"]))
        for field_name in ("abstract", "authors", "journal", "doi", "pmid", "pmcid"):
            if not current.get(field_name) and record.get(field_name):
                current[field_name] = record[field_name]
    return list(merged.values())


def retrieve_candidates(config: PipelineConfig) -> tuple[list[dict[str, Any]], dict[str, str]]:
    dates = config.fetch_dates()
    date_clause = f"FIRST_PDATE:[{dates['start'].isoformat()} TO {dates['end'].isoformat()}]"
    source_clause = "" if config.include_preprints else " AND NOT SRC:PPR"
    client = EuropePMCClient()
    raw_records: list[dict[str, Any]] = []
    queries: dict[str, str] = {}

    if config.journals:
        query = f"{date_clause} AND {build_field_or('JOURNAL', config.journals)}{source_clause}"
        queries["journals"] = query
        raw_records.extend(
            normalize_epmc_record(item, "journal")
            for item in client.search(query, config.max_raw_results_per_query)
        )
    if config.keywords and (config.search_keywords_outside_journals or not config.journals):
        query = f"{date_clause} AND {build_field_or('TITLE_ABS', config.keywords)}{source_clause}"
        queries["keywords"] = query
        raw_records.extend(
            normalize_epmc_record(item, "keyword")
            for item in client.search(query, config.max_raw_results_per_query)
        )

    papers = merge_records(raw_records)
    journal_norms = {normalize_name(journal): journal for journal in config.journals}
    for paper in papers:
        haystack = f"{paper['title']} {paper['abstract']}".lower()
        paper["keyword_hits"] = [
            keyword for keyword in config.keywords if keyword.lower() in haystack
        ]
        paper_journal_norm = normalize_name(paper.get("journal", ""))
        paper["journal_hits"] = [
            original
            for norm, original in journal_norms.items()
            if norm
            and (
                norm == paper_journal_norm
                or norm in paper_journal_norm
                or paper_journal_norm in norm
            )
        ]
    return papers, queries


# ---------------------------------------------------------------------------
# Persistent paper database
# ---------------------------------------------------------------------------


BIBLIOGRAPHIC_FIELDS = (
    "uid",
    "stable_id",
    "source",
    "source_id",
    "pmid",
    "pmcid",
    "doi",
    "title",
    "abstract",
    "authors",
    "journal",
    "publication_date",
    "publication_date_parsed",
    "pub_types",
    "is_open_access",
    "cited_by_count",
    "epmc_url",
    "doi_url",
    "matched_by",
    "keyword_hits",
    "journal_hits",
)


def paper_content_hash(paper: dict[str, Any]) -> str:
    return sha1_text(
        "\n".join(
            [
                paper.get("title", ""),
                paper.get("abstract", ""),
                paper.get("journal", ""),
            ]
        )
    )


def new_database_record(paper: dict[str, Any], first_seen: date) -> dict[str, Any]:
    record = {field: paper.get(field) for field in BIBLIOGRAPHIC_FIELDS}
    record.update(
        {
            "first_seen_date": first_seen.isoformat(),
            "last_seen_date": first_seen.isoformat(),
            "content_hash": paper_content_hash(paper),
            "late_indexed": bool(
                parse_partial_date(paper.get("publication_date", ""))
                and parse_partial_date(paper.get("publication_date", "")) <= first_seen - timedelta(days=2)
            ),
            "screening_status": "pending",
            "screening_attempts": 0,
            "screening_last_attempt": "",
            "screening_error": "",
            "summary_status": "pending",
            "summary_attempts": 0,
            "summary_last_attempt": "",
            "summary_error": "",
            "category_status": "pending",
            "category_attempts": 0,
            "category_last_attempt": "",
            "category_error": "",
            "category_version": "",
        }
    )
    return record


def upsert_papers(
    database: dict[str, dict[str, Any]],
    retrieved: list[dict[str, Any]],
    run_date: date,
) -> dict[str, int]:
    new_count = 0
    changed_count = 0
    unchanged_count = 0
    for paper in retrieved:
        stable_id = paper["stable_id"]
        if stable_id not in database:
            database[stable_id] = new_database_record(paper, run_date)
            new_count += 1
            continue
        record = database[stable_id]
        old_hash = record.get("content_hash", "")
        new_hash = paper_content_hash(paper)
        for field_name in BIBLIOGRAPHIC_FIELDS:
            value = paper.get(field_name)
            if value not in (None, "", []):
                if field_name in ("matched_by", "keyword_hits", "journal_hits"):
                    record[field_name] = sorted(
                        set((record.get(field_name) or []) + (value or []))
                    )
                else:
                    record[field_name] = value
        record["last_seen_date"] = run_date.isoformat()
        pub_date = parse_partial_date(record.get("publication_date", ""))
        record["late_indexed"] = bool(pub_date and pub_date <= date.fromisoformat(record["first_seen_date"]) - timedelta(days=2))
        if old_hash and old_hash != new_hash:
            record["content_hash"] = new_hash
            record["screening_status"] = "pending"
            record["screening_error"] = ""
            record["summary_status"] = "pending"
            record["summary_error"] = ""
            record["category_status"] = "pending"
            record["category_error"] = ""
            changed_count += 1
        else:
            record["content_hash"] = new_hash
            unchanged_count += 1
    return {"new": new_count, "changed": changed_count, "unchanged": unchanged_count}


def _is_always_include(paper: dict[str, Any], config: PipelineConfig) -> bool:
    paper_journal = normalize_name(paper.get("journal", ""))
    return any(
        norm
        and (
            norm == paper_journal
            or norm in paper_journal
            or paper_journal in norm
        )
        for norm in map(normalize_name, config.always_include_journals)
    )


def apply_rule_prefilter(database: dict[str, dict[str, Any]], config: PipelineConfig) -> dict[str, int]:
    included = 0
    excluded = 0
    excluded_type_terms = (
        "editorial",
        "news",
        "comment",
        "correction",
        "retraction",
        "retracted",
        "letter",
    )
    for paper in database.values():
        if paper.get("screening_status") == "completed":
            continue
        if _is_always_include(paper, config):
            paper.update(
                {
                    "include": True,
                    "relevance_score": 100,
                    "screening_reason": "Configured as an unconditional journal.",
                    "screening_method": "journal_rule",
                    "screening_status": "completed",
                    "screening_error": "",
                    "screening_version": PROMPT_VERSION,
                }
            )
            included += 1
            continue
        pub_type_text = " ".join(paper.get("pub_types", [])).lower()
        direct_keyword = bool(paper.get("keyword_hits"))
        if any(term in pub_type_text for term in excluded_type_terms) and not direct_keyword:
            paper.update(
                {
                    "include": False,
                    "relevance_score": 5,
                    "screening_reason": "Excluded by article-type rule before AI screening.",
                    "screening_method": "type_rule",
                    "screening_status": "completed",
                    "summary_status": "not_needed",
                    "category_status": "not_needed",
                    "screening_error": "",
                    "screening_version": PROMPT_VERSION,
                }
            )
            excluded += 1
            continue
        if not paper.get("abstract") and not direct_keyword:
            paper.update(
                {
                    "include": False,
                    "relevance_score": 10,
                    "screening_reason": "No abstract and no direct configured-keyword match.",
                    "screening_method": "metadata_rule",
                    "screening_status": "completed",
                    "summary_status": "not_needed",
                    "category_status": "not_needed",
                    "screening_error": "",
                    "screening_version": PROMPT_VERSION,
                }
            )
            excluded += 1
    return {"rule_included": included, "rule_excluded": excluded}


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------


SCREENING_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "decisions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "include": {"type": "boolean"},
                    "relevance_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "reason": {"type": "string"},
                },
                "required": ["id", "include", "relevance_score", "reason"],
            },
        }
    },
    "required": ["decisions"],
}


CATEGORY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "papers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "category_id": {
                        "type": "string",
                        "enum": [item["id"] for item in DEFAULT_CATEGORIES],
                    },
                },
                "required": ["id", "category_id"],
            },
        }
    },
    "required": ["papers"],
}


def summary_schema(categories: list[dict[str, str]]) -> dict[str, Any]:
    category_ids = [item["id"] for item in categories]
    return {
        "type": "object",
        "properties": {
            "papers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "category_id": {"type": "string", "enum": category_ids},
                        "summary_ja": {"type": "string"},
                        "summary_en": {"type": "string"},
                        "key_finding_ja": {"type": "string"},
                        "why_it_matters_ja": {"type": "string"},
                        "article_type": {
                            "type": "string",
                            "enum": ["Research article", "Review", "Clinical study", "Methods", "Other"],
                        },
                    },
                    "required": [
                        "id",
                        "category_id",
                        "summary_ja",
                        "summary_en",
                        "key_finding_ja",
                        "why_it_matters_ja",
                        "article_type",
                    ],
                },
            }
        },
        "required": ["papers"],
    }


class GeminiHelper:
    def __init__(self, api_key: str, model: str) -> None:
        from google import genai

        self.client = genai.Client(api_key=api_key)
        self.model = model

    @staticmethod
    def _legacy_schema(schema: Any) -> Any:
        if isinstance(schema, dict):
            return {
                key: (value.upper() if key == "type" and isinstance(value, str) else GeminiHelper._legacy_schema(value))
                for key, value in schema.items()
            }
        if isinstance(schema, list):
            return [GeminiHelper._legacy_schema(value) for value in schema]
        return schema

    def json_response(
        self,
        system: str,
        user: str,
        schema: dict[str, Any],
        max_tokens: int,
    ) -> Any:
        from google.genai import types

        fields = getattr(types.GenerateContentConfig, "model_fields", {})
        config_kwargs: dict[str, Any] = {
            "system_instruction": system,
            "temperature": 0,
            "max_output_tokens": max_tokens,
        }
        effective_user = user
        if "response_json_schema" in fields:
            config_kwargs.update(
                response_mime_type="application/json",
                response_json_schema=schema,
            )
        elif "response_schema" in fields:
            config_kwargs.update(
                response_mime_type="application/json",
                response_schema=self._legacy_schema(schema),
            )
        elif "response_format" in fields:
            config_kwargs["response_format"] = {
                "text": {"mime_type": "application/json", "schema": schema}
            }
        else:
            if "response_mime_type" in fields:
                config_kwargs["response_mime_type"] = "application/json"
            effective_user += "\n\nReturn only JSON matching this schema:\n" + json.dumps(
                schema, ensure_ascii=False
            )
        config = types.GenerateContentConfig(**config_kwargs)
        last_error: Optional[Exception] = None
        for attempt in range(4):
            try:
                response = self.client.models.generate_content(
                    model=self.model,
                    contents=effective_user,
                    config=config,
                )
                text = (response.text or "").strip()
                if not text:
                    raise RuntimeError("Gemini returned an empty response.")
                return extract_json(text)
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep((3, 12, 30)[attempt])
        detail = f"{type(last_error).__name__}: {last_error}" if last_error else "unknown error"
        raise RuntimeError(f"Gemini API request failed after retries. {detail}") from last_error


def _paper_packet(paper: dict[str, Any], abstract_limit: int = 7000) -> dict[str, Any]:
    return {
        "id": paper["uid"],
        "title": paper.get("title", ""),
        "journal": paper.get("journal", ""),
        "publication_date": paper.get("publication_date", ""),
        "abstract": paper.get("abstract", "")[:abstract_limit],
        "keyword_hits": paper.get("keyword_hits", []),
        "matched_by": paper.get("matched_by", []),
    }


def retryable_status(paper: dict[str, Any], prefix: str, config: PipelineConfig) -> bool:
    status = paper.get(f"{prefix}_status", "pending")
    if status == "pending":
        return True
    if status != "failed":
        return False
    if int(paper.get(f"{prefix}_attempts", 0)) >= config.max_retries_per_paper:
        return False
    last_attempt = str(paper.get(f"{prefix}_last_attempt", ""))[:10]
    return config.retry_failed_same_day or last_attempt != config.today().isoformat()


def category_map_for(config: PipelineConfig) -> dict[str, dict[str, str]]:
    return {item["id"]: item for item in config.categories}


def infer_provisional_category(
    paper: dict[str, Any], config: PipelineConfig
) -> dict[str, str]:
    """Assign a safe provisional category without spending an API request.

    This keeps old cached papers visible immediately after the category scheme
    changes. Gemini subsequently replaces the provisional decision.
    """
    categories = category_map_for(config)
    text = " ".join(
        [
            str(paper.get("title", "")),
            str(paper.get("abstract", "")),
            str(paper.get("summary_ja", "")),
            " ".join(paper.get("keyword_hits", []) or []),
        ]
    ).lower()

    supersulfide_terms = (
        "supersulfide", "persulfide", "persulfidation", "persulfidated",
        "polysulfide", "reactive sulfur species", "cys-ssh", "cysSSH".lower(),
        "protein-ssh", "sulfane sulfur", "超硫黄", "パースルフィド",
    )
    enteric_terms = (
        "enteric nervous", "enteric neuron", "enteric glia", "myenteric",
        "submucosal plexus", "intestinal neuron", "gut-brain", "visceral sensory",
        "intestinal motility", "腸管神経", "腸神経", "腸管グリア",
    )
    immune_terms = (
        "immune", "immun", "inflamm", "cytokine", "macrophage", "microglia",
        "t cell", "b cell", "neuroinflamm", "神経免疫", "炎症", "免疫",
    )
    ptm_terms = (
        "post-translational", "posttranslational", "phosphorylation", "ubiquitin",
        "acetylation", "methylation", "glycosylation", "sumoylation", "lipidation",
        "nitrosylation", "s-nitros", "sulfenylation", "carbonylation",
        "翻訳後修飾", "リン酸化", "ユビキチン", "アセチル化", "糖鎖修飾",
    )
    neuroimmune_terms = (
        "neuroimmune", "neuro-immun", "neuroinflamm", "neural regulation of immunity",
        "vagus", "vagal", "microglia", "astrocyte", "神経免疫", "神経炎症",
    )

    if any(term in text for term in supersulfide_terms):
        category_id = "supersulfides"
    elif any(term in text for term in enteric_terms) and any(term in text for term in immune_terms):
        category_id = "neuroimmunology"
    elif any(term in text for term in enteric_terms):
        category_id = "enteric_nervous_system"
    elif any(term in text for term in ptm_terms):
        category_id = "post_translational_modification"
    elif any(term in text for term in neuroimmune_terms):
        category_id = "neuroimmunology"
    else:
        category_id = "gut_microbiome"

    return categories.get(category_id, config.categories[0])


def prepare_category_scheme(
    database: dict[str, dict[str, Any]], config: PipelineConfig
) -> dict[str, int]:
    """Mark legacy categories for low-cost reclassification.

    Existing Japanese/English summaries remain usable. Only the category is
    queued again, so changing category definitions does not force expensive
    full-summary regeneration.
    """
    valid_ids = set(category_map_for(config))
    queued = 0
    provisional = 0
    for paper in database.values():
        if not paper.get("include"):
            paper["category_status"] = "not_needed"
            continue
        if paper.get("summary_status") != "completed":
            paper.setdefault("category_status", "pending")
            continue
        valid_current = paper.get("category_id") in valid_ids
        current_version = paper.get("category_version") == CATEGORY_SCHEME_VERSION
        if valid_current and current_version:
            paper["category_status"] = "completed"
            continue
        category = infer_provisional_category(paper, config)
        previous_status = paper.get("category_status", "pending")
        keep_failed = previous_status == "failed"
        paper.update(
            {
                "category_id": category["id"],
                "category_ja": category["ja"],
                "category_en": category["en"],
                "category_status": "failed" if keep_failed else "pending",
                "category_error": paper.get("category_error", "") if keep_failed else "",
            }
        )
        queued += 1
        provisional += 1
    return {"queued": queued, "provisional": provisional}


def process_screening_queue(
    database: dict[str, dict[str, Any]], config: PipelineConfig
) -> dict[str, Any]:
    queue = [
        paper
        for paper in database.values()
        if retryable_status(paper, "screening", config)
    ]
    queue.sort(
        key=lambda paper: (
            0 if paper.get("keyword_hits") else 1,
            paper.get("first_seen_date", ""),
            paper.get("title", ""),
        )
    )
    queue = queue[: config.max_screening_per_run]
    if not queue or not config.enable_ai:
        return {"attempted": 0, "completed": 0, "failed": 0, "errors": []}

    gemini = GeminiHelper(config.gemini_api_key, config.gemini_model)
    system = (
        "You are a rigorous senior editor screening biomedical literature. "
        "Use only the supplied title and abstract. Do not infer unsupported findings. "
        "Return data following the supplied JSON schema."
    )
    completed = 0
    failed = 0
    errors: list[dict[str, str]] = []

    for group in batched(queue, config.screening_batch_size):
        payload = [_paper_packet(paper) for paper in group]
        user = f"""
Research scope:
{config.topic_scope}

For each paper, decide whether its central biological question or result is materially within the scope.
Include mechanistic, translational, clinical, or methods work only when the connection is direct.
Exclude papers with merely incidental keyword mentions.
The relevance_score must be an integer from 0 to 100.
Return one concise reason and one decision for every supplied paper id.

Papers:
{json.dumps(payload, ensure_ascii=False)}
""".strip()
        attempt_time = now_iso()
        for paper in group:
            paper["screening_attempts"] = int(paper.get("screening_attempts", 0)) + 1
            paper["screening_last_attempt"] = attempt_time
        try:
            parsed = gemini.json_response(system, user, SCREENING_SCHEMA, max_tokens=5000)
            decisions = {
                item["id"]: item
                for item in parsed.get("decisions", [])
                if isinstance(item, dict) and item.get("id")
            }
            batch_error = ""
        except Exception as exc:
            decisions = {}
            batch_error = str(exc)

        for paper in group:
            decision = decisions.get(paper["uid"])
            if decision:
                try:
                    score = int(decision.get("relevance_score", 0))
                except (TypeError, ValueError):
                    score = 0
                include = bool(decision.get("include"))
                paper.update(
                    {
                        "include": include,
                        "relevance_score": max(0, min(100, score)),
                        "screening_reason": clean_text(decision.get("reason")),
                        "screening_method": "gemini",
                        "screening_status": "completed",
                        "screening_error": "",
                        "screening_version": PROMPT_VERSION,
                        "summary_status": "pending" if include else "not_needed",
                        "category_status": "pending" if include else "not_needed",
                    }
                )
                completed += 1
            else:
                error = batch_error or "Gemini response did not contain this paper id."
                paper.update(
                    {
                        "screening_status": "failed",
                        "screening_error": error,
                        "screening_method": "gemini_failed",
                    }
                )
                errors.append({"title": paper.get("title", ""), "error": error})
                failed += 1
        if config.request_interval_seconds:
            time.sleep(config.request_interval_seconds)

    return {
        "attempted": len(queue),
        "completed": completed,
        "failed": failed,
        "errors": errors,
    }


def process_summary_queue(
    database: dict[str, dict[str, Any]], config: PipelineConfig
) -> dict[str, Any]:
    queue = [
        paper
        for paper in database.values()
        if paper.get("screening_status") == "completed"
        and paper.get("include")
        and retryable_status(paper, "summary", config)
    ]
    queue.sort(
        key=lambda paper: (
            paper.get("first_seen_date", ""),
            -int(paper.get("relevance_score", 0)),
            paper.get("title", ""),
        )
    )
    queue = queue[: config.max_summaries_per_run]
    if not queue or not config.enable_ai:
        return {"attempted": 0, "completed": 0, "failed": 0, "errors": []}

    gemini = GeminiHelper(config.gemini_api_key, config.gemini_model)
    category_map = category_map_for(config)
    schema = summary_schema(config.categories)
    system = (
        "You are a bilingual scientific editor. Summarize biomedical papers accurately from the supplied "
        "title and abstract only. Distinguish reported results from interpretation. Never invent sample size, "
        "methods, causality, or clinical significance. Return data following the supplied JSON schema."
    )
    completed = 0
    failed = 0
    errors: list[dict[str, str]] = []

    for group in batched(queue, config.summary_batch_size):
        payload = [_paper_packet(paper, abstract_limit=9000) for paper in group]
        user = f"""
Assign exactly one category from this list:
{json.dumps(config.categories, ensure_ascii=False)}

For every supplied paper id, produce:
- category_id: one listed category id
- summary_ja: 3-5 concise Japanese sentences
- summary_en: 2-4 concise English sentences
- key_finding_ja: one Japanese sentence
- why_it_matters_ja: one cautious Japanese sentence
- article_type: Research article, Review, Clinical study, Methods, or Other

Do not use markdown. Do not add facts absent from the title or abstract.

Papers:
{json.dumps(payload, ensure_ascii=False)}
""".strip()
        attempt_time = now_iso()
        for paper in group:
            paper["summary_attempts"] = int(paper.get("summary_attempts", 0)) + 1
            paper["summary_last_attempt"] = attempt_time
        try:
            parsed = gemini.json_response(system, user, schema, max_tokens=10000)
            outputs = {
                item["id"]: item
                for item in parsed.get("papers", [])
                if isinstance(item, dict) and item.get("id")
            }
            batch_error = ""
        except Exception as exc:
            outputs = {}
            batch_error = str(exc)

        for paper in group:
            output = outputs.get(paper["uid"])
            if output:
                category_id = (
                    output.get("category_id")
                    if output.get("category_id") in category_map
                    else infer_provisional_category(paper, config)["id"]
                )
                category = category_map.get(category_id, config.categories[0])
                paper.update(
                    {
                        "category_id": category["id"],
                        "category_ja": category["ja"],
                        "category_en": category["en"],
                        "summary_ja": clean_text(output.get("summary_ja")),
                        "summary_en": clean_text(output.get("summary_en")),
                        "key_finding_ja": clean_text(output.get("key_finding_ja")),
                        "why_it_matters_ja": clean_text(output.get("why_it_matters_ja")),
                        "article_type": clean_text(output.get("article_type")) or "Other",
                        "summary_status": "completed",
                        "summary_error": "",
                        "summary_version": PROMPT_VERSION,
                        "summary_completed_at": attempt_time,
                        "category_status": "completed",
                        "category_error": "",
                        "category_version": CATEGORY_SCHEME_VERSION,
                    }
                )
                completed += 1
            else:
                error = batch_error or "Gemini response did not contain this paper id."
                paper.update({"summary_status": "failed", "summary_error": error})
                errors.append({"title": paper.get("title", ""), "error": error})
                failed += 1
        if config.request_interval_seconds:
            time.sleep(config.request_interval_seconds)

    return {
        "attempted": len(queue),
        "completed": completed,
        "failed": failed,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------




def process_category_queue(
    database: dict[str, dict[str, Any]], config: PipelineConfig
) -> dict[str, Any]:
    """Reclassify already summarized legacy papers without regenerating summaries."""
    queue = [
        paper
        for paper in database.values()
        if paper.get("include")
        and paper.get("summary_status") == "completed"
        and retryable_status(paper, "category", config)
    ]
    queue.sort(
        key=lambda paper: (
            paper.get("first_seen_date", ""),
            -int(paper.get("relevance_score", 0)),
            paper.get("title", ""),
        )
    )
    queue = queue[: config.max_reclassification_per_run]
    if not queue or not config.enable_ai:
        return {"attempted": 0, "completed": 0, "failed": 0, "errors": []}

    gemini = GeminiHelper(config.gemini_api_key, config.gemini_model)
    category_map = category_map_for(config)
    schema = {
        "type": "object",
        "properties": {
            "papers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "category_id": {
                            "type": "string",
                            "enum": list(category_map),
                        },
                    },
                    "required": ["id", "category_id"],
                },
            }
        },
        "required": ["papers"],
    }
    system = (
        "You are a scientific literature classifier. Assign exactly one category using only "
        "the supplied title, abstract, and existing summary. Return JSON matching the schema."
    )
    completed = 0
    failed = 0
    errors: list[dict[str, str]] = []

    for group in batched(queue, config.reclassification_batch_size):
        payload = [
            {
                "id": paper["uid"],
                "title": paper.get("title", ""),
                "abstract": paper.get("abstract", "")[:7000],
                "summary_ja": paper.get("summary_ja", ""),
            }
            for paper in group
        ]
        user = f"""
Assign exactly one category from this list:
{json.dumps(config.categories, ensure_ascii=False)}

Apply these precedence rules when topics overlap:
1. Supersulfides and protein persulfidation take priority over general post-translational modification.
2. Use enteric nervous system when enteric neurons, enteric glia, motility, visceral sensation, or gut neural circuits are central and immune-neural crosstalk is not the main question.
3. Use neuroimmunology when nervous-immune interaction or neuroinflammation is central, including gut neuroimmune work.
4. Use post-translational modification for covalent protein modifications other than supersulfidation-focused work.
5. Use gut microbiome for microbial communities, gut bacteria, microbial metabolism, host-microbe interaction, or microbiome-derived metabolites when none of the more specific categories is central.

Return one category for every supplied paper id.

Papers:
{json.dumps(payload, ensure_ascii=False)}
""".strip()
        attempt_time = now_iso()
        for paper in group:
            paper["category_attempts"] = int(paper.get("category_attempts", 0)) + 1
            paper["category_last_attempt"] = attempt_time
        try:
            parsed = gemini.json_response(system, user, schema, max_tokens=3000)
            outputs = {
                item["id"]: item
                for item in parsed.get("papers", [])
                if isinstance(item, dict) and item.get("id")
            }
            batch_error = ""
        except Exception as exc:
            outputs = {}
            batch_error = str(exc)

        for paper in group:
            output = outputs.get(paper["uid"])
            category_id = output.get("category_id") if output else ""
            if category_id in category_map:
                category = category_map[category_id]
                paper.update(
                    {
                        "category_id": category["id"],
                        "category_ja": category["ja"],
                        "category_en": category["en"],
                        "category_status": "completed",
                        "category_error": "",
                        "category_version": CATEGORY_SCHEME_VERSION,
                    }
                )
                completed += 1
            else:
                error = batch_error or "Gemini response did not contain a valid category for this paper id."
                paper.update({"category_status": "failed", "category_error": error})
                errors.append({"title": paper.get("title", ""), "error": error})
                failed += 1
        if config.request_interval_seconds:
            time.sleep(config.request_interval_seconds)

    return {
        "attempted": len(queue),
        "completed": completed,
        "failed": failed,
        "errors": errors,
    }


BASE_CSS = r"""
:root { --bg:#f6f7f9; --card:#ffffff; --ink:#16202a; --muted:#647182; --line:#dfe5ec; --accent:#244a7c; --soft:#eaf1f8; --warning:#fff8df; }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--ink); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif; line-height:1.72; }
a { color:var(--accent); text-decoration:none; } a:hover { text-decoration:underline; }
header { background:linear-gradient(120deg,#17375f,#315f8f); color:white; padding:54px 20px 42px; }
.wrap { max-width:1050px; margin:0 auto; }
h1 { margin:0 0 8px; font-size:clamp(2rem,5vw,3.3rem); line-height:1.14; }
h2.section-heading { margin:42px 0 16px; padding-bottom:8px; border-bottom:2px solid var(--accent); }
.subtitle { opacity:.9; max-width:760px; }
.summary-strip { display:flex; gap:12px; flex-wrap:wrap; margin-top:22px; }
.metric { border:1px solid rgba(255,255,255,.3); border-radius:999px; padding:6px 13px; font-size:.92rem; }
main { padding:34px 20px 70px; }
.notice { background:var(--warning); border:1px solid #ead58d; border-radius:12px; padding:14px 18px; margin-bottom:24px; }
.status { background:white; border:1px solid var(--line); border-radius:12px; padding:14px 18px; margin-bottom:24px; }
.category-nav { display:flex; gap:8px; flex-wrap:wrap; margin:0 0 30px; }
.category-nav a { background:var(--soft); border:1px solid #d6e2ef; border-radius:999px; padding:6px 11px; font-size:.86rem; }
.category-section { margin:38px 0 54px; scroll-margin-top:20px; }
.category-title { display:flex; align-items:baseline; gap:10px; border-bottom:2px solid var(--accent); padding-bottom:8px; }
.category-title h2 { margin:0; }.count { color:var(--muted); font-size:.9rem; }
.paper { background:var(--card); border:1px solid var(--line); border-radius:15px; padding:23px; margin:17px 0; box-shadow:0 4px 18px rgba(20,35,55,.045); }
.paper h3 { margin:8px 0 8px; font-size:1.25rem; line-height:1.42; }.meta { color:var(--muted); font-size:.91rem; }
.badges { display:flex; gap:7px; flex-wrap:wrap; }.badge { background:var(--soft); border-radius:999px; padding:3px 9px; font-size:.78rem; font-weight:650; }.badge.late { background:#fff0d4; }
.key { margin:16px 0; padding:12px 14px; background:#f2f6fa; border-left:4px solid var(--accent); }
.paper-links { display:flex; gap:12px; flex-wrap:wrap; margin:14px 0; font-weight:650; }
details { margin-top:12px; border-top:1px solid var(--line); padding-top:10px; } summary { cursor:pointer; color:var(--accent); font-weight:650; }
.archive-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(250px,1fr)); gap:16px; }
.archive-card { background:white; border:1px solid var(--line); border-radius:14px; padding:20px; }
.archive-card h3 { margin:6px 0; font-size:1.14rem; }
.archive-card.featured { border:2px solid var(--accent); }
.more-link { margin-top:18px; font-weight:650; }
footer { color:var(--muted); border-top:1px solid var(--line); padding:28px 20px 50px; }
@media(max-width:600px){ .paper{padding:18px;} header{padding-top:40px;} }
"""


def h(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


def category_slug(category_id: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", category_id.lower()).strip("-") or "other"


def completed_in_period(
    database: dict[str, dict[str, Any]], start: date, end: date
) -> list[dict[str, Any]]:
    papers: list[dict[str, Any]] = []
    for paper in database.values():
        try:
            first_seen = date.fromisoformat(paper.get("first_seen_date", ""))
        except ValueError:
            continue
        if (
            start <= first_seen <= end
            and paper.get("include")
            and paper.get("summary_status") == "completed"
        ):
            papers.append(paper)
    papers.sort(
        key=lambda paper: (
            int(paper.get("relevance_score", 0)),
            paper.get("publication_date_parsed", ""),
            paper.get("title", ""),
        ),
        reverse=True,
    )
    return papers


def processing_status_for_date(
    database: dict[str, dict[str, Any]], target: date
) -> dict[str, int]:
    target_iso = target.isoformat()
    papers = [
        paper
        for paper in database.values()
        if paper.get("first_seen_date") == target_iso
    ]
    return {
        "detected": len(papers),
        "completed": sum(p.get("summary_status") == "completed" for p in papers),
        "pending_screening": sum(p.get("screening_status") == "pending" for p in papers),
        "failed_screening": sum(p.get("screening_status") == "failed" for p in papers),
        "pending_summary": sum(
            p.get("include") and p.get("summary_status") == "pending" for p in papers
        ),
        "failed_summary": sum(
            p.get("include") and p.get("summary_status") == "failed" for p in papers
        ),
        "pending_category": sum(
            p.get("include")
            and p.get("summary_status") == "completed"
            and p.get("category_status") == "pending"
            for p in papers
        ),
        "failed_category": sum(
            p.get("include")
            and p.get("summary_status") == "completed"
            and p.get("category_status") == "failed"
            for p in papers
        ),
    }


def render_paper_card(paper: dict[str, Any]) -> str:
    links = [f'<a href="{h(paper["epmc_url"])}" target="_blank" rel="noopener">Europe PMC</a>']
    if paper.get("doi_url"):
        links.append(f'<a href="{h(paper["doi_url"])}" target="_blank" rel="noopener">DOI</a>')
    if paper.get("pmid"):
        links.append(f'<span>PMID: {h(paper["pmid"])}</span>')
    badges = [f'<span class="badge">{h(paper.get("article_type", "Article"))}</span>']
    if paper.get("late_indexed"):
        badges.append('<span class="badge late">Late indexed</span>')
    for keyword in paper.get("keyword_hits", [])[:5]:
        badges.append(f'<span class="badge">{h(keyword)}</span>')
    return f"""
<article class="paper">
  <div class="badges">{''.join(badges)}</div>
  <h3>{h(paper['title'])}</h3>
  <div class="meta"><strong>{h(paper.get('journal'))}</strong> · published {h(paper.get('publication_date'))} · first seen {h(paper.get('first_seen_date'))}</div>
  <div class="meta">{h(paper.get('authors'))}</div>
  <div class="paper-links">{' '.join(links)}</div>
  <div class="key"><strong>要点：</strong>{h(paper.get('key_finding_ja'))}</div>
  <p>{h(paper.get('summary_ja'))}</p>
  <p><strong>意義：</strong>{h(paper.get('why_it_matters_ja'))}</p>
  <details><summary>English summary</summary><p>{h(paper.get('summary_en'))}</p></details>
  <details><summary>Selection note</summary><p>{h(paper.get('screening_reason'))} Relevance score: {h(paper.get('relevance_score'))}/100.</p></details>
</article>
""".strip()


def render_period_page(
    config: PipelineConfig,
    papers: list[dict[str, Any]],
    *,
    title: str,
    period_start: date,
    period_end: date,
    status_metrics: Optional[dict[str, Any]] = None,
    back_href: str = "index.html",
    back_label: str = "Top page",
    footer_note: str = "Membership is based on the first-seen date.",
) -> str:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    category_map = category_map_for(config)
    for paper in papers:
        category_id = paper.get("category_id")
        if category_id not in category_map:
            category_id = infer_provisional_category(paper, config)["id"]
        grouped[category_id].append(paper)
    order = [item["id"] for item in config.categories if grouped.get(item["id"])]
    nav = "".join(
        f'<a href="#{category_slug(cat_id)}">{h(category_map[cat_id]["ja"])} ({len(grouped[cat_id])})</a>'
        for cat_id in order
    )
    sections: list[str] = []
    for cat_id in order:
        category = category_map[cat_id]
        cards = "\n".join(render_paper_card(paper) for paper in grouped[cat_id])
        sections.append(
            f'<section class="category-section" id="{category_slug(cat_id)}">'
            f'<div class="category-title"><h2>{h(category["ja"])}</h2>'
            f'<span class="count">{h(category["en"])} · {len(grouped[cat_id])} papers</span></div>{cards}</section>'
        )
    status_html = ""
    if status_metrics:
        status_html = (
            '<div class="status"><strong>この日の処理状況：</strong> '
            f'検出 {h(status_metrics.get("detected", 0))} · '
            f'要約完了 {h(status_metrics.get("completed", 0))} · '
            f'スクリーニング待ち {h(status_metrics.get("pending_screening", 0))} · '
            f'要約待ち {h(status_metrics.get("pending_summary", 0))} · '
            f'分類待ち {h(status_metrics.get("pending_category", 0))} · '
            f'失敗 {h(status_metrics.get("failed_screening", 0) + status_metrics.get("failed_summary", 0) + status_metrics.get("failed_category", 0))}</div>'
        )
    return f"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{h(config.site_title)} — {h(title)}</title><style>{BASE_CSS}</style></head>
<body><header><div class="wrap"><h1>{h(title)}</h1><div class="subtitle">{h(config.site_subtitle)}</div>
<div class="summary-strip"><span class="metric">{period_start.isoformat()} – {period_end.isoformat()}</span>
<span class="metric">{len(papers)} completed papers</span><span class="metric">Europe PMC + Gemini</span></div></div></header>
<main><div class="wrap"><div class="notice"><strong>注意：</strong>{h(config.language_note)}</div>{status_html}
<nav class="category-nav">{nav}</nav>{''.join(sections) if sections else '<p>この期間に要約完了した掲載対象論文はありません。</p>'}
<p><a href="{h(back_href)}">← {h(back_label)}</a></p></div></main>
<footer><div class="wrap">Generated {h(now_iso())}. {h(footer_note)}</div></footer>
</body></html>"""


def render_archive_cards(entries: list[dict[str, Any]], *, date_key: str) -> str:
    cards: list[str] = []
    for entry in entries:
        if date_key == "date":
            meta = entry.get("date", "")
        else:
            meta = f'{entry.get("period_start", "")} – {entry.get("period_end", "")}'
        cards.append(
            f'<article class="archive-card"><div class="meta">{h(meta)}</div>'
            f'<h3><a href="{h(entry.get("url"))}">{h(entry.get("title"))}</a></h3>'
            f'<div>{h(entry.get("paper_count"))} completed papers</div></article>'
        )
    return "".join(cards) if cards else "<p>No archived pages yet.</p>"


def render_home(
    config: PipelineConfig,
    run_date: date,
    today_count: int,
    daily_archive: list[dict[str, Any]],
    weekly_archive: list[dict[str, Any]],
) -> str:
    recent_daily = sorted(daily_archive, key=lambda x: x.get("date", ""), reverse=True)[
        : config.daily_entries_on_home
    ]
    recent_weekly = sorted(
        weekly_archive, key=lambda x: x.get("period_end", ""), reverse=True
    )[: config.weekly_entries_on_home]
    latest_card = (
        '<article class="archive-card featured"><div class="meta">Current daily update</div>'
        f'<h3><a href="latest.html">New papers — {h(run_date.isoformat())}</a></h3>'
        f'<div>{today_count} completed papers</div></article>'
    )
    return f"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{h(config.site_title)}</title><style>{BASE_CSS}</style></head>
<body><header><div class="wrap"><h1>{h(config.site_title)}</h1><div class="subtitle">{h(config.site_subtitle)}</div></div></header>
<main><div class="wrap"><div class="notice"><strong>注意：</strong>{h(config.language_note)}</div>
<h2 class="section-heading">今日の新着</h2><div class="archive-grid">{latest_card}</div>
<h2 class="section-heading">日次更新</h2><div class="archive-grid">{render_archive_cards(recent_daily, date_key="date")}</div>
<p class="more-link"><a href="daily/index.html">日次更新をすべて表示 →</a></p>
<h2 class="section-heading">週次アーカイブ</h2><div class="archive-grid">{render_archive_cards(recent_weekly, date_key="period_end")}</div>
<p class="more-link"><a href="weeks/index.html">週次アーカイブをすべて表示 →</a></p>
</div></main><footer><div class="wrap">Daily pages and Monday-Sunday weekly archives.</div></footer></body></html>"""


def render_archive_index(
    config: PipelineConfig,
    *,
    title: str,
    entries: list[dict[str, Any]],
    date_key: str,
    back_href: str = "../index.html",
) -> str:
    ordered = sorted(
        entries,
        key=lambda item: item.get("date" if date_key == "date" else "period_end", ""),
        reverse=True,
    )
    return f"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{h(config.site_title)} — {h(title)}</title><style>{BASE_CSS}</style></head>
<body><header><div class="wrap"><h1>{h(title)}</h1><div class="subtitle">{h(config.site_subtitle)}</div></div></header>
<main><div class="wrap"><div class="archive-grid">{render_archive_cards(ordered, date_key=date_key)}</div>
<p><a href="{h(back_href)}">← Top page</a></p></div></main>
<footer><div class="wrap">Generated {h(now_iso())}.</div></footer></body></html>"""


# ---------------------------------------------------------------------------
# GitHub
# ---------------------------------------------------------------------------


class GitHubContentsClient:
    def __init__(self, token: str, repo: str, branch: str = "main") -> None:
        self.token = token
        self.repo = repo
        self.branch = branch
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "daily-literature-pipeline",
        }

    def _url(self, path: str) -> str:
        return f"{GITHUB_API}/repos/{self.repo}/contents/{quote(path, safe='/')}"

    def get(self, path: str) -> tuple[Optional[bytes], str]:
        response = request_with_retry(
            "GET", self._url(path), headers=self.headers, params={"ref": self.branch}
        )
        if response.status_code == 404:
            return None, ""
        response.raise_for_status()
        payload = response.json()
        if payload.get("type") != "file":
            raise ValueError(f"GitHub path is not a file: {path}")
        return base64.b64decode(payload["content"]), payload.get("sha", "")

    def put(self, path: str, content: bytes, message: str) -> dict[str, Any]:
        _, sha = self.get(path)
        payload: dict[str, Any] = {
            "message": message,
            "content": base64.b64encode(content).decode("ascii"),
            "branch": self.branch,
        }
        if sha:
            payload["sha"] = sha
        response = request_with_retry("PUT", self._url(path), headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()


def derive_pages_base_url(config: PipelineConfig) -> str:
    if config.public_base_url:
        return config.public_base_url.rstrip("/")
    if not config.github_repo:
        return ""
    owner, repo = config.github_repo.split("/", 1)
    if repo.lower() == f"{owner.lower()}.github.io":
        return f"https://{owner}.github.io"
    return f"https://{owner}.github.io/{repo}"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


class DailyLiteraturePipeline:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.config.validate()
        self.workdir = Path(config.workdir)
        self.output_dir = self.workdir / "output"
        self.daily_output_dir = self.output_dir / "daily"
        self.weekly_output_dir = self.output_dir / "weeks"
        self.data_output_dir = self.output_dir / "data"
        self.daily_data_dir = self.data_output_dir / "daily"
        self.weekly_data_dir = self.data_output_dir / "weeks"
        self.state_dir = self.workdir / "state"
        self.paper_db_path = self.state_dir / "paper_db.json"
        self.daily_archive_path = self.state_dir / "daily_archive.json"
        self.weekly_archive_path = self.state_dir / "weekly_archive.json"
        self.legacy_archive_path = self.state_dir / "archive.json"
        self.run_history_path = self.state_dir / "run_history.json"
        for path in (
            self.output_dir,
            self.daily_output_dir,
            self.weekly_output_dir,
            self.data_output_dir,
            self.daily_data_dir,
            self.weekly_data_dir,
            self.state_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.github = (
            GitHubContentsClient(config.github_token, config.github_repo, config.github_branch)
            if config.github_token and config.github_repo
            else None
        )
        self.legacy_migrated = 0

    def _load_remote_json(self, path: str, default: Any) -> Any:
        if not self.github:
            return default
        try:
            content, _ = self.github.get(path)
            return json.loads(content.decode("utf-8")) if content else default
        except Exception:
            return default

    def migrate_legacy_outputs(self) -> dict[str, dict[str, Any]]:
        database: dict[str, dict[str, Any]] = {}
        for path in sorted(self.output_dir.glob("????-??-??.json")):
            try:
                report_day = date.fromisoformat(path.stem)
            except ValueError:
                continue
            payload = read_json(path, [])
            if not isinstance(payload, list):
                continue
            for paper in payload:
                if not isinstance(paper, dict) or not paper.get("stable_id"):
                    continue
                record = new_database_record(paper, report_day)
                record.update(paper)
                record.update(
                    {
                        "first_seen_date": paper.get("first_seen_date") or report_day.isoformat(),
                        "last_seen_date": paper.get("last_seen_date") or report_day.isoformat(),
                        "content_hash": paper_content_hash(paper),
                        "include": bool(paper.get("include", True)),
                        "screening_status": "completed",
                        "screening_method": paper.get("screening_method", "legacy_import"),
                        "screening_error": "",
                        "screening_version": paper.get("screening_version", "legacy-v4"),
                        "summary_status": (
                            "completed"
                            if paper.get("summary_ja")
                            and "要約生成に失敗" not in paper.get("summary_ja", "")
                            else "failed"
                        ),
                        "summary_error": paper.get("summary_error", ""),
                        "summary_version": paper.get("summary_version", "legacy-v4"),
                        "category_status": "pending",
                        "category_attempts": int(paper.get("category_attempts", 0)),
                        "category_last_attempt": paper.get("category_last_attempt", ""),
                        "category_error": "",
                        "category_version": paper.get("category_version", ""),
                    }
                )
                database[record["stable_id"]] = record
        self.legacy_migrated = len(database)
        return database

    @staticmethod
    def normalize_weekly_archive(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            entry = dict(item)
            if not entry.get("period_end") and entry.get("report_date"):
                entry["period_end"] = entry["report_date"]
            if not entry.get("period_start") and entry.get("period_end"):
                try:
                    entry["period_start"] = (
                        date.fromisoformat(entry["period_end"]) - timedelta(days=6)
                    ).isoformat()
                except ValueError:
                    entry["period_start"] = ""
            if entry.get("period_end"):
                normalized.append(entry)
        return normalized

    def load_state(
        self,
    ) -> tuple[
        dict[str, dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        local_db = read_json(self.paper_db_path, {})
        remote_db = self._load_remote_json(".weekly-literature/paper_db.json", {})
        database = local_db or remote_db
        if not database:
            database = self.migrate_legacy_outputs()
            if database:
                write_json(self.paper_db_path, database)

        local_daily = read_json(self.daily_archive_path, [])
        remote_daily = self._load_remote_json(
            f"{self.config.pages_dir}/data/daily_archive.json", []
        )
        daily_archive = remote_daily or local_daily
        daily_archive = [item for item in daily_archive if isinstance(item, dict) and item.get("date")]

        local_weekly = read_json(
            self.weekly_archive_path,
            read_json(self.legacy_archive_path, []),
        )
        remote_weekly = self._load_remote_json(
            f"{self.config.pages_dir}/data/weekly_archive.json",
            self._load_remote_json(f"{self.config.pages_dir}/data/archive.json", []),
        )
        weekly_archive = self.normalize_weekly_archive(remote_weekly or local_weekly)
        run_history = read_json(self.run_history_path, [])
        return database, daily_archive, weekly_archive, run_history

    @staticmethod
    def queue_status(database: dict[str, dict[str, Any]]) -> dict[str, int]:
        return {
            "pending_screening": sum(
                p.get("screening_status") == "pending" for p in database.values()
            ),
            "failed_screening": sum(
                p.get("screening_status") == "failed" for p in database.values()
            ),
            "pending_summary": sum(
                p.get("include") and p.get("summary_status") == "pending"
                for p in database.values()
            ),
            "failed_summary": sum(
                p.get("include") and p.get("summary_status") == "failed"
                for p in database.values()
            ),
            "completed_summaries": sum(
                p.get("summary_status") == "completed" for p in database.values()
            ),
            "pending_category": sum(
                p.get("include")
                and p.get("summary_status") == "completed"
                and p.get("category_status") == "pending"
                for p in database.values()
            ),
            "failed_category": sum(
                p.get("include")
                and p.get("summary_status") == "completed"
                and p.get("category_status") == "failed"
                for p in database.values()
            ),
        }

    def affected_daily_dates(
        self,
        database: dict[str, dict[str, Any]],
        daily_archive: list[dict[str, Any]],
    ) -> set[date]:
        run_day = self.config.today()
        affected: set[date] = {run_day}
        if not daily_archive:
            for paper in database.values():
                if paper.get("summary_status") != "completed":
                    continue
                try:
                    affected.add(date.fromisoformat(paper.get("first_seen_date", "")))
                except ValueError:
                    pass
            return affected

        run_iso = run_day.isoformat()
        for paper in database.values():
            touched_today = any(
                str(paper.get(field, ""))[:10] == run_iso
                for field in (
                    "first_seen_date",
                    "screening_last_attempt",
                    "summary_last_attempt",
                    "summary_completed_at",
                    "category_last_attempt",
                )
            )
            if not touched_today:
                continue
            try:
                affected.add(date.fromisoformat(paper.get("first_seen_date", "")))
            except ValueError:
                pass
        return affected

    def write_daily_page(
        self,
        database: dict[str, dict[str, Any]],
        target: date,
        daily_archive: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[tuple[str, Path]], Path, list[dict[str, Any]]]:
        papers = completed_in_period(database, target, target)
        status = processing_status_for_date(database, target)
        filename = f"{target.isoformat()}.html"
        json_filename = f"{target.isoformat()}.json"
        html_path = self.daily_output_dir / filename
        json_path = self.daily_data_dir / json_filename
        html_path.write_text(
            render_period_page(
                self.config,
                papers,
                title=f"New papers — {target.isoformat()}",
                period_start=target,
                period_end=target,
                status_metrics=status,
                back_href="index.html",
                back_label="Daily archive",
                footer_note="Daily membership is based on the first-seen date.",
            ),
            encoding="utf-8",
        )
        write_json(json_path, papers)

        existing = any(item.get("date") == target.isoformat() for item in daily_archive)
        if papers or existing:
            entry = {
                "date": target.isoformat(),
                "paper_count": len(papers),
                "title": f"New papers — {target.isoformat()}",
                "url": f"daily/{filename}",
            }
            daily_archive = [
                item for item in daily_archive if item.get("date") != target.isoformat()
            ]
            daily_archive.append(entry)

        files = [
            (f"{self.config.pages_dir}/daily/{filename}", html_path),
            (f"{self.config.pages_dir}/data/daily/{json_filename}", json_path),
        ]
        return papers, files, html_path, daily_archive

    def build_outputs(
        self,
        database: dict[str, dict[str, Any]],
        daily_archive: list[dict[str, Any]],
        weekly_archive: list[dict[str, Any]],
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[tuple[str, Path]],
        dict[str, str],
    ]:
        files: list[tuple[str, Path]] = []
        local_paths: dict[str, str] = {}
        run_day = self.config.today()

        affected_days = self.affected_daily_dates(database, daily_archive)
        today_papers: list[dict[str, Any]] = []
        today_daily_path: Optional[Path] = None
        for target in sorted(affected_days):
            papers, day_files, html_path, daily_archive = self.write_daily_page(
                database, target, daily_archive
            )
            files.extend(day_files)
            if target == run_day:
                today_papers = papers
                today_daily_path = html_path

        if today_daily_path is None:
            today_papers, day_files, today_daily_path, daily_archive = self.write_daily_page(
                database, run_day, daily_archive
            )
            files.extend(day_files)

        latest_path = self.output_dir / "latest.html"
        latest_json_path = self.data_output_dir / "latest.json"
        latest_path.write_text(
            render_period_page(
                self.config,
                today_papers,
                title=f"New papers — {run_day.isoformat()}",
                period_start=run_day,
                period_end=run_day,
                status_metrics=processing_status_for_date(database, run_day),
                back_href="index.html",
                back_label="Top page",
                footer_note="Daily membership is based on the first-seen date.",
            ),
            encoding="utf-8",
        )
        write_json(latest_json_path, today_papers)
        files.extend(
            [
                (f"{self.config.pages_dir}/latest.html", latest_path),
                (f"{self.config.pages_dir}/data/latest.json", latest_json_path),
            ]
        )
        local_paths["latest_path"] = str(latest_path)
        local_paths["daily_path"] = str(today_daily_path)

        # Rebuild recent completed weeks, plus any older completed week whose
        # daily membership changed during this run because a queued paper finished.
        weekly_targets: set[tuple[date, date]] = set()
        for offset in range(self.config.archive_weeks_to_rebuild):
            weekly_targets.add(completed_week_bounds(run_day, offset))
        for affected_day in affected_days:
            week_start, week_end = week_bounds(affected_day)
            if week_end < run_day:
                weekly_targets.add((week_start, week_end))

        for week_start, week_end in sorted(weekly_targets, reverse=True):
            weekly_papers = completed_in_period(database, week_start, week_end)[
                : self.config.max_papers_per_week
            ]
            existing = any(
                item.get("period_end") == week_end.isoformat() for item in weekly_archive
            )
            if not weekly_papers and not existing:
                continue
            filename = f"{week_end.isoformat()}.html"
            json_filename = f"{week_end.isoformat()}.json"
            html_path = self.weekly_output_dir / filename
            json_path = self.weekly_data_dir / json_filename
            html_path.write_text(
                render_period_page(
                    self.config,
                    weekly_papers,
                    title=f"Weekly archive — {week_start.isoformat()} to {week_end.isoformat()}",
                    period_start=week_start,
                    period_end=week_end,
                    back_href="index.html",
                    back_label="Weekly archive",
                    footer_note="Weekly membership is based on the first-seen date.",
                ),
                encoding="utf-8",
            )
            write_json(json_path, weekly_papers)
            entry = {
                "period_start": week_start.isoformat(),
                "period_end": week_end.isoformat(),
                "paper_count": len(weekly_papers),
                "title": f"Week ending {week_end.isoformat()}",
                "url": f"weeks/{filename}",
            }
            weekly_archive = [
                item
                for item in weekly_archive
                if item.get("period_end") != week_end.isoformat()
            ]
            weekly_archive.append(entry)
            files.extend(
                [
                    (f"{self.config.pages_dir}/weeks/{filename}", html_path),
                    (f"{self.config.pages_dir}/data/weeks/{json_filename}", json_path),
                ]
            )

        daily_archive.sort(key=lambda item: item.get("date", ""), reverse=True)
        weekly_archive.sort(key=lambda item: item.get("period_end", ""), reverse=True)

        daily_index_path = self.daily_output_dir / "index.html"
        weekly_index_path = self.weekly_output_dir / "index.html"
        index_path = self.output_dir / "index.html"
        daily_archive_output = self.data_output_dir / "daily_archive.json"
        weekly_archive_output = self.data_output_dir / "weekly_archive.json"
        legacy_archive_output = self.data_output_dir / "archive.json"

        daily_index_path.write_text(
            render_archive_index(
                self.config,
                title="Daily updates",
                entries=daily_archive,
                date_key="date",
            ),
            encoding="utf-8",
        )
        weekly_index_path.write_text(
            render_archive_index(
                self.config,
                title="Weekly archives",
                entries=weekly_archive,
                date_key="period_end",
            ),
            encoding="utf-8",
        )
        index_path.write_text(
            render_home(
                self.config,
                run_day,
                len(today_papers),
                daily_archive,
                weekly_archive,
            ),
            encoding="utf-8",
        )
        write_json(daily_archive_output, daily_archive)
        write_json(weekly_archive_output, weekly_archive)
        write_json(legacy_archive_output, weekly_archive)

        files.extend(
            [
                (f"{self.config.pages_dir}/index.html", index_path),
                (f"{self.config.pages_dir}/daily/index.html", daily_index_path),
                (f"{self.config.pages_dir}/weeks/index.html", weekly_index_path),
                (f"{self.config.pages_dir}/data/daily_archive.json", daily_archive_output),
                (f"{self.config.pages_dir}/data/weekly_archive.json", weekly_archive_output),
                (f"{self.config.pages_dir}/data/archive.json", legacy_archive_output),
            ]
        )
        local_paths["index_path"] = str(index_path)
        local_paths["daily_index_path"] = str(daily_index_path)
        local_paths["weekly_index_path"] = str(weekly_index_path)
        return daily_archive, weekly_archive, files, local_paths

    def publish_files(self, files: list[tuple[str, Path]]) -> list[str]:
        if not self.github:
            raise ValueError("GitHub client is not configured.")
        published: list[str] = []
        # GitHub Contents API writes are intentionally serial to avoid SHA conflicts.
        for remote_path, local_path in files:
            self.github.put(
                remote_path,
                local_path.read_bytes(),
                f"Update literature digest {self.config.today().isoformat()}: {remote_path}",
            )
            published.append(remote_path)
        return published

    def run(self, publish: Optional[bool] = None) -> dict[str, Any]:
        publish = self.config.publish_to_github if publish is None else publish
        database, daily_archive, weekly_archive, run_history = self.load_state()
        category_preparation = prepare_category_scheme(database, self.config)
        retrieved, queries = retrieve_candidates(self.config)
        upsert_counts = upsert_papers(database, retrieved, self.config.today())
        rule_counts = apply_rule_prefilter(database, self.config)

        screening = process_screening_queue(database, self.config)
        write_json(self.paper_db_path, database)

        summaries = process_summary_queue(database, self.config)
        write_json(self.paper_db_path, database)

        reclassification = process_category_queue(database, self.config)
        write_json(self.paper_db_path, database)

        queue_metrics = self.queue_status(database)
        daily_archive, weekly_archive, page_files, local_paths = self.build_outputs(
            database, daily_archive, weekly_archive
        )
        write_json(self.daily_archive_path, daily_archive)
        write_json(self.weekly_archive_path, weekly_archive)
        write_json(self.legacy_archive_path, weekly_archive)

        run_record = {
            "run_date": self.config.today().isoformat(),
            "generated_at": now_iso(),
            "fetch_start": self.config.fetch_dates()["start"].isoformat(),
            "fetch_end": self.config.fetch_dates()["end"].isoformat(),
            "retrieved": len(retrieved),
            "legacy_migrated": self.legacy_migrated,
            "upsert": upsert_counts,
            "rules": rule_counts,
            "screening": {k: v for k, v in screening.items() if k != "errors"},
            "summaries": {k: v for k, v in summaries.items() if k != "errors"},
            "category_preparation": category_preparation,
            "reclassification": {k: v for k, v in reclassification.items() if k != "errors"},
            "category_scheme_version": CATEGORY_SCHEME_VERSION,
            "queue": queue_metrics,
            "queries": queries,
            "model": self.config.gemini_model,
            "prompt_version": PROMPT_VERSION,
            "daily_pages": len(daily_archive),
            "weekly_pages": len(weekly_archive),
        }
        run_history.append(run_record)
        run_history = run_history[-365:]
        write_json(self.run_history_path, run_history)

        state_files = [
            (".weekly-literature/paper_db.json", self.paper_db_path),
            (".weekly-literature/run_history.json", self.run_history_path),
            (".weekly-literature/daily_archive.json", self.daily_archive_path),
            (".weekly-literature/weekly_archive.json", self.weekly_archive_path),
        ]
        published_paths: list[str] = []
        if publish:
            published_paths = self.publish_files(page_files + state_files)

        base_url = derive_pages_base_url(self.config)
        return {
            "run_record": run_record,
            "retrieved": retrieved,
            "legacy_migrated": self.legacy_migrated,
            "upsert_counts": upsert_counts,
            "rule_counts": rule_counts,
            "screening": screening,
            "summaries": summaries,
            "category_preparation": category_preparation,
            "reclassification": reclassification,
            "queue": queue_metrics,
            "database_size": len(database),
            "daily_archive_count": len(daily_archive),
            "weekly_archive_count": len(weekly_archive),
            "latest_path": local_paths["latest_path"],
            "daily_path": local_paths["daily_path"],
            "daily_index_path": local_paths["daily_index_path"],
            "weekly_index_path": local_paths["weekly_index_path"],
            "index_path": local_paths["index_path"],
            "published": publish,
            "published_paths": published_paths,
            "pages_url": f"{base_url}/" if publish and base_url else "",
        }


WeeklyLiteraturePipeline = DailyLiteraturePipeline
