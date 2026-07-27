from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from weekly_literature_pipeline import (
    DEFAULT_CATEGORIES,
    DailyLiteraturePipeline,
    PipelineConfig,
    read_json,
    write_json,
)

TOKYO = ZoneInfo("Asia/Tokyo")
ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
RUNTIME_DIR = ROOT / ".runtime"
PERSISTENT_STATE_DIR = ROOT / ".weekly-literature"
DOCS_DIR = ROOT / "docs"


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("config.json must contain a JSON object.")
    return data


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def derive_pages_url(repository: str) -> str:
    explicit = os.getenv("SITE_BASE_URL", "").strip().rstrip("/")
    if explicit:
        return explicit
    if "/" not in repository:
        return ""
    owner, repo = repository.split("/", 1)
    if repo.lower() == f"{owner.lower()}.github.io":
        return f"https://{owner}.github.io"
    return f"https://{owner}.github.io/{repo}"


def copy_tree(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    destination.mkdir(parents=True, exist_ok=True)
    for item in source.iterdir():
        target = destination / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def prepare_runtime() -> None:
    if RUNTIME_DIR.exists():
        shutil.rmtree(RUNTIME_DIR)
    (RUNTIME_DIR / "state").mkdir(parents=True, exist_ok=True)
    (RUNTIME_DIR / "output").mkdir(parents=True, exist_ok=True)
    copy_tree(PERSISTENT_STATE_DIR, RUNTIME_DIR / "state")

    # Fallback for a repository that has Pages data but no hidden state yet.
    state_dir = RUNTIME_DIR / "state"
    fallback_files = {
        DOCS_DIR / "data" / "daily_archive.json": state_dir / "daily_archive.json",
        DOCS_DIR / "data" / "weekly_archive.json": state_dir / "weekly_archive.json",
        DOCS_DIR / "data" / "archive.json": state_dir / "archive.json",
    }
    for source, target in fallback_files.items():
        if source.exists() and not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)


def initialize_slack_epoch() -> dict[str, Any]:
    path = RUNTIME_DIR / "state" / "slack_state.json"
    state = read_json(path, {})
    if not isinstance(state, dict):
        state = {}
    if not state.get("enabled_since"):
        state["enabled_since"] = datetime.now(TOKYO).isoformat(timespec="seconds")
        state["note"] = "Papers summarized before this timestamp are not posted retroactively."
        write_json(path, state)
    return state


def slack_escape(text: str) -> str:
    return str(text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def truncate(text: str, limit: int) -> str:
    text = str(text or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def paper_links(paper: dict[str, Any]) -> str:
    links: list[str] = []
    if paper.get("epmc_url"):
        links.append(f"<{paper['epmc_url']}|Europe PMC>")
    if paper.get("doi_url"):
        links.append(f"<{paper['doi_url']}|DOI>")
    return " · ".join(links)


def paper_block(paper: dict[str, Any]) -> dict[str, Any]:
    title = slack_escape(truncate(paper.get("title", "Untitled"), 450))
    journal = slack_escape(paper.get("journal", ""))
    category = slack_escape(paper.get("category_ja", ""))
    key_finding = slack_escape(truncate(paper.get("key_finding_ja", ""), 700))
    summary = slack_escape(truncate(paper.get("summary_ja", ""), 900))
    lines = [f"*{title}*"]
    metadata = " · ".join(part for part in (journal, category) if part)
    if metadata:
        lines.append(metadata)
    if key_finding:
        lines.append(f"*要点:* {key_finding}")
    if summary:
        lines.append(summary)
    links = paper_links(paper)
    if links:
        lines.append(links)
    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": truncate("\n".join(lines), 2900)},
    }


def send_webhook(webhook_url: str, payload: dict[str, Any]) -> None:
    response = requests.post(webhook_url, json=payload, timeout=30)
    if response.status_code >= 400 or response.text.strip().lower() != "ok":
        raise RuntimeError(
            f"Slack webhook failed: HTTP {response.status_code}: {response.text[:500]}"
        )


def post_pending_to_slack(
    config_data: dict[str, Any],
    webhook_url: str,
    pages_url: str,
) -> dict[str, Any]:
    state_dir = RUNTIME_DIR / "state"
    db_path = state_dir / "paper_db.json"
    slack_state_path = state_dir / "slack_state.json"
    database = read_json(db_path, {})
    slack_state = read_json(slack_state_path, {})
    enabled_since = str(slack_state.get("enabled_since", ""))

    if not isinstance(database, dict):
        raise RuntimeError("paper_db.json is not a JSON object.")

    candidates: list[dict[str, Any]] = []
    for paper in database.values():
        completed_at = str(paper.get("summary_completed_at", ""))
        if not completed_at or (enabled_since and completed_at < enabled_since):
            continue
        if not paper.get("include") or paper.get("summary_status") != "completed":
            continue
        if paper.get("slack_posted_at"):
            continue
        candidates.append(paper)

    candidates.sort(
        key=lambda p: (
            p.get("summary_completed_at", ""),
            int(p.get("relevance_score", 0)),
            p.get("title", ""),
        )
    )
    max_per_run = int(config_data.get("slack_max_papers_per_run", 15))
    candidates = candidates[:max_per_run]

    if not candidates:
        if config_data.get("slack_post_empty", False):
            run_date = datetime.now(TOKYO).date().isoformat()
            text = f"📚 Daily Research Update — {run_date}\n本日の新規要約はありません。"
            if pages_url:
                text += f"\n<{pages_url}/latest.html|HTMLで確認>"
            send_webhook(webhook_url, {"text": text})
        return {"attempted": 0, "posted": 0, "failed": 0}

    run_date = datetime.now(TOKYO).date().isoformat()
    per_message = max(1, min(int(config_data.get("slack_papers_per_message", 5)), 8))
    posted = 0
    failed = 0

    for start in range(0, len(candidates), per_message):
        group = candidates[start : start + per_message]
        header = (
            f"📚 *Daily Research Update — {run_date}*\n"
            f"今回の新規要約: {len(candidates)}報"
        )
        if pages_url:
            header += f" · <{pages_url}/latest.html|HTMLで確認>"
        blocks: list[dict[str, Any]] = [
            {"type": "section", "text": {"type": "mrkdwn", "text": header}},
            {"type": "divider"},
        ]
        for index, paper in enumerate(group):
            blocks.append(paper_block(paper))
            if index != len(group) - 1:
                blocks.append({"type": "divider"})

        payload = {
            "text": f"Daily Research Update — {run_date}: {len(candidates)} papers",
            "blocks": blocks,
        }
        attempt_time = datetime.now(TOKYO).isoformat(timespec="seconds")
        try:
            send_webhook(webhook_url, payload)
        except Exception as exc:
            failed += len(group)
            for paper in group:
                paper["slack_status"] = "failed"
                paper["slack_error"] = str(exc)
                paper["slack_attempts"] = int(paper.get("slack_attempts", 0)) + 1
                paper["slack_last_attempt"] = attempt_time
            print(f"::warning::{exc}")
        else:
            posted += len(group)
            for paper in group:
                paper["slack_status"] = "posted"
                paper["slack_error"] = ""
                paper["slack_attempts"] = int(paper.get("slack_attempts", 0)) + 1
                paper["slack_last_attempt"] = attempt_time
                paper["slack_posted_at"] = attempt_time

    slack_state["last_run_at"] = datetime.now(TOKYO).isoformat(timespec="seconds")
    slack_state["last_attempted"] = len(candidates)
    slack_state["last_posted"] = posted
    slack_state["last_failed"] = failed
    write_json(db_path, database)
    write_json(slack_state_path, slack_state)
    return {"attempted": len(candidates), "posted": posted, "failed": failed}


def persist_results() -> None:
    copy_tree(RUNTIME_DIR / "output", DOCS_DIR)
    copy_tree(RUNTIME_DIR / "state", PERSISTENT_STATE_DIR)
    (DOCS_DIR / ".nojekyll").touch()


def build_pipeline_config(
    config_data: dict[str, Any], api_key: str, model: str, run_date: str
) -> PipelineConfig:
    return PipelineConfig(
        site_title=config_data["site_title"],
        site_subtitle=config_data["site_subtitle"],
        public_base_url=derive_pages_url(os.getenv("GITHUB_REPOSITORY", "")),
        journals=list(config_data.get("journals", [])),
        always_include_journals=list(config_data.get("always_include_journals", [])),
        keywords=list(config_data.get("keywords", [])),
        search_keywords_outside_journals=bool(
            config_data.get("search_keywords_outside_journals", False)
        ),
        topic_scope=str(config_data["topic_scope"]),
        categories=DEFAULT_CATEGORIES,
        run_date=run_date,
        fetch_lookback_days=int(config_data.get("fetch_lookback_days", 7)),
        latest_window_days=int(config_data.get("latest_window_days", 1)),
        include_preprints=bool(config_data.get("include_preprints", False)),
        max_raw_results_per_query=int(config_data.get("max_raw_results_per_query", 1000)),
        enable_ai=True,
        gemini_api_key=api_key,
        gemini_model=model,
        max_screening_per_run=int(config_data.get("max_screening_per_run", 40)),
        max_summaries_per_run=int(config_data.get("max_summaries_per_run", 10)),
        max_reclassification_per_run=int(
            config_data.get("max_reclassification_per_run", 30)
        ),
        screening_batch_size=int(config_data.get("screening_batch_size", 20)),
        summary_batch_size=int(config_data.get("summary_batch_size", 5)),
        reclassification_batch_size=int(
            config_data.get("reclassification_batch_size", 20)
        ),
        request_interval_seconds=float(
            config_data.get("request_interval_seconds", 2.0)
        ),
        retry_failed_same_day=bool(config_data.get("retry_failed_same_day", False)),
        max_retries_per_paper=int(config_data.get("max_retries_per_paper", 10)),
        archive_weeks_to_rebuild=int(
            config_data.get("archive_weeks_to_rebuild", 2)
        ),
        max_papers_per_week=int(config_data.get("max_papers_per_week", 100)),
        daily_entries_on_home=int(config_data.get("daily_entries_on_home", 14)),
        weekly_entries_on_home=int(config_data.get("weekly_entries_on_home", 12)),
        publish_to_github=False,
        workdir=str(RUNTIME_DIR),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default="", help="YYYY-MM-DD; default: today in Japan")
    parser.add_argument("--no-slack", action="store_true")
    args = parser.parse_args()

    config_data = load_config()
    api_key = required_env("GEMINI_API_KEY")
    model = os.getenv("GEMINI_MODEL", "gemini-3.5-flash").strip()
    repository = os.getenv("GITHUB_REPOSITORY", "").strip()
    pages_url = derive_pages_url(repository)

    prepare_runtime()
    initialize_slack_epoch()
    pipeline_config = build_pipeline_config(config_data, api_key, model, args.run_date)
    result = DailyLiteraturePipeline(pipeline_config).run(publish=False)

    slack_result = {"attempted": 0, "posted": 0, "failed": 0}
    slack_enabled = bool(config_data.get("slack_enabled", True)) and not args.no_slack
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if slack_enabled:
        if not webhook_url:
            print("::warning::SLACK_WEBHOOK_URL is not configured; Slack posting was skipped.")
        else:
            slack_result = post_pending_to_slack(
                config_data=config_data,
                webhook_url=webhook_url,
                pages_url=pages_url,
            )

    persist_results()

    print(json.dumps({
        "run_date": pipeline_config.today().isoformat(),
        "pages_url": pages_url,
        "retrieved": len(result.get("retrieved", [])),
        "screening": result.get("screening", {}),
        "summaries": result.get("summaries", {}),
        "queue": result.get("queue", {}),
        "slack": slack_result,
    }, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"::error::{type(exc).__name__}: {exc}", file=sys.stderr)
        raise
