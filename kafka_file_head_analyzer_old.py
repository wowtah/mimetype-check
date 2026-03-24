#!/usr/bin/env python3
import asyncio
import json
import math
import os
import signal
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, cast
from urllib.parse import urlsplit, urlunsplit

import aiohttp
import magic
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import FixedLocator, FuncFormatter
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener

# ---------------------------
# Configuration
# ---------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "cxp.material.etl.learning_material")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "material-file-head-analyzer")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")  # earliest | latest
RESET_CONSUMER = os.getenv("RESET_CONSUMER", "1") == "1"

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
HTTP_CONCURRENCY = int(os.getenv("HTTP_CONCURRENCY", "20"))
HEAD_ALLOW_REDIRECTS = os.getenv("HEAD_ALLOW_REDIRECTS", "1") == "1"
VERIFY_SSL = os.getenv("VERIFY_SSL", "1") == "1"
USER_AGENT = os.getenv("USER_AGENT", "kafka-file-head-analyzer/1.0")

EXPORT_EVERY = int(os.getenv("EXPORT_EVERY", "25"))
PRINT_EVERY = int(os.getenv("PRINT_EVERY", "10"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./output"))

CHART_TOP_N_CONTENT_TYPES = int(os.getenv("CHART_TOP_N_CONTENT_TYPES", "15"))
CHART_TOP_N_FILES = int(os.getenv("CHART_TOP_N_FILES", "25"))
CHART_TOP_N_MISMATCH_PAIRS = int(os.getenv("CHART_TOP_N_MISMATCH_PAIRS", "20"))

PROBE_BYTES = int(os.getenv("PROBE_BYTES", "8192"))
MIME_UNDETECTABLE = {"application/octet-stream", "application/x-empty"}

IGNORE_MESSAGE_TYPES = {
    "HarvestStarted",
    "HarvestFinished",
}
TRACKED_MESSAGE_TYPES = {"MaterialCreated", "MaterialUpdated", "MaterialDeleted"}

BYTES_PER_MB = 1024**2
BYTES_PER_GB = 1024**3


# ---------------------------
# Utilities
# ---------------------------
def normalize_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        netloc = parts.netloc.lstrip(".")
        return urlunsplit(
            (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
        )
    except Exception:
        return url.replace("://.", "://", 1)


def extract_material_id(event: Dict[str, Any]) -> Optional[str]:
    return event.get("reference", {}).get("id", {}).get("v1", {}).get("id")


def extract_file_urls(event: Dict[str, Any]) -> List[str]:
    files = event.get("reference", {}).get("result", {}).get("files", {})
    urls: List[str] = []
    if not isinstance(files, dict):
        return urls

    for _, file_obj in files.items():
        if not isinstance(file_obj, dict):
            continue
        url = file_obj.get("v1", {}).get("url")
        if isinstance(url, str) and url.strip():
            urls.append(normalize_url(url.strip()))

    # preserve order, remove duplicates
    return list(dict.fromkeys(urls))


def extension_from_url(url: str) -> str:
    path = urlsplit(url).path or ""
    suffix = Path(path).suffix.lower().lstrip(".")
    return suffix or "unknown"


def mime_type_from_headers(headers: Dict[str, str]) -> Optional[str]:
    raw = headers.get("Content-Type") or headers.get("content-type")
    if not raw:
        return None
    return raw.split(";", 1)[0].strip().lower() or None


def parse_content_length(headers: Dict[str, str]) -> Optional[int]:
    raw = headers.get("Content-Length") or headers.get("content-length")
    if raw is None:
        return None
    try:
        value = int(raw)
        return value if value >= 0 else None
    except (TypeError, ValueError):
        return None


def iso_utc_from_ms(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def truncate_label(value: str, max_len: int = 55) -> str:
    return value if len(value) <= max_len else value[: max_len - 1] + "…"


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_mime(raw: Optional[str]) -> Optional[str]:
    """Strip parameters (;charset=…) and lowercase for comparison."""
    if not raw:
        return None
    base = raw.split(";", 1)[0].strip().lower()
    return base or None


def format_bytes_human(num_bytes: Optional[float]) -> str:
    if num_bytes is None or (
        isinstance(num_bytes, float) and (np.isnan(num_bytes) or np.isinf(num_bytes))
    ):
        return ""

    value = float(num_bytes)
    if value < 0:
        return f"-{format_bytes_human(-value)}"
    if value == 0:
        return "0 B"

    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    power = (
        min(int(math.floor(math.log(value, 1024))), len(units) - 1) if value >= 1 else 0
    )
    scaled = value / (1024**power)
    if scaled >= 100 or power == 0:
        return f"{scaled:.0f} {units[power]}"
    if scaled >= 10:
        return f"{scaled:.1f} {units[power]}"
    return f"{scaled:.2f} {units[power]}"


def build_log_size_ticks(min_bytes: float, max_bytes: float) -> List[float]:
    if min_bytes <= 0 or max_bytes <= 0:
        return []

    min_exp = int(math.floor(math.log10(min_bytes)))
    max_exp = int(math.ceil(math.log10(max_bytes)))
    ticks = [10**exp for exp in range(min_exp, max_exp + 1)]

    if min_bytes < 1:
        ticks.append(min_bytes)
    if max_bytes not in ticks:
        ticks.append(max_bytes)

    ticks = sorted({tick for tick in ticks if min_bytes <= tick <= max_bytes})
    if min_bytes not in ticks:
        ticks.insert(0, min_bytes)
    if max_bytes not in ticks:
        ticks.append(max_bytes)
    return ticks


def configure_log_size_axis(ax, min_bytes: float, max_bytes: float) -> None:
    if min_bytes <= 0 or max_bytes <= 0:
        return

    ax.set_xscale("log")
    lower = min_bytes / 1.25
    upper = max_bytes * 1.25
    ax.set_xlim(left=max(lower, min_bytes * 0.8), right=upper)

    ticks = build_log_size_ticks(min_bytes, max_bytes)
    if ticks:
        ax.xaxis.set_major_locator(FixedLocator(ticks))
    ax.xaxis.set_major_formatter(
        FuncFormatter(lambda value, _pos: format_bytes_human(value))
    )


# ---------------------------
# Data model
# ---------------------------
@dataclass
class FileHeadResult:
    material_id: str
    file_url: str
    final_url: str
    status: Optional[int]
    headers: Dict[str, str]
    content_type: Optional[str]
    extension: str
    size_bytes: Optional[int]
    error: Optional[str]
    kafka_event_type: str
    kafka_timestamp: Optional[str]
    observed_at: str
    binary_content_type: Optional[str] = None
    probe_status: Optional[int] = None
    probe_error: Optional[str] = None


@dataclass
class MaterialRecord:
    material_id: str
    kafka_event_type: str
    kafka_timestamp: Optional[str]
    updated_at: str
    files: Dict[str, FileHeadResult] = field(default_factory=dict)


class MaterialState:
    def __init__(self) -> None:
        self.materials: Dict[str, MaterialRecord] = {}

    def upsert_material(
        self,
        material_id: str,
        event_type: str,
        kafka_timestamp: Optional[str],
        file_results: Iterable[FileHeadResult],
    ) -> None:
        files_map = {entry.file_url: entry for entry in file_results}
        self.materials[material_id] = MaterialRecord(
            material_id=material_id,
            kafka_event_type=event_type,
            kafka_timestamp=kafka_timestamp,
            updated_at=datetime.now(timezone.utc).isoformat(),
            files=files_map,
        )

    def delete_material(self, material_id: str) -> bool:
        return self.materials.pop(material_id, None) is not None

    def to_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for material in self.materials.values():
            for entry in material.files.values():
                rows.append(
                    {
                        "material_id": material.material_id,
                        "kafka_event_type": material.kafka_event_type,
                        "kafka_timestamp": material.kafka_timestamp,
                        "material_updated_at": material.updated_at,
                        "file_url": entry.file_url,
                        "final_url": entry.final_url,
                        "status": entry.status,
                        "content_type": entry.content_type,
                        "extension": entry.extension,
                        "size_bytes": entry.size_bytes,
                        "size_mb": (entry.size_bytes / BYTES_PER_MB)
                        if entry.size_bytes is not None
                        else np.nan,
                        "size_gb": (entry.size_bytes / BYTES_PER_GB)
                        if entry.size_bytes is not None
                        else np.nan,
                        "error": entry.error,
                        "head_headers_json": json.dumps(
                            entry.headers, ensure_ascii=False, sort_keys=True
                        ),
                        "observed_at": entry.observed_at,
                        "binary_content_type": entry.binary_content_type,
                        "probe_status": entry.probe_status,
                        "probe_error": entry.probe_error,
                    }
                )
        return rows

    def material_count(self) -> int:
        return len(self.materials)


# ---------------------------
# Kafka rebalance
# ---------------------------
class ResetToBeginningListener(ConsumerRebalanceListener):
    def __init__(self, consumer: AIOKafkaConsumer, enabled: bool):
        self.consumer = consumer
        self.enabled = enabled

    async def on_partitions_assigned(self, assigned):
        if self.enabled and assigned:
            await self.consumer.seek_to_beginning(*assigned)
            print(
                f"[INFO] RESET_CONSUMER=1 -> seek_to_beginning on {len(assigned)} partitions"
            )

    async def on_partitions_revoked(self, revoked):
        return


# ---------------------------
# HTTP HEAD + binary probe
# ---------------------------
async def probe_binary_type(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """Download the first PROBE_BYTES of *url* and detect MIME via libmagic.

    Returns (binary_content_type, http_status, error_string).
    Handles servers that ignore Range headers by streaming only the needed bytes.
    """
    async with semaphore:
        try:
            headers = {
                "Range": f"bytes=0-{PROBE_BYTES - 1}",
                "Accept-Encoding": "identity",
            }
            async with session.get(
                url, allow_redirects=HEAD_ALLOW_REDIRECTS, headers=headers
            ) as resp:
                status = resp.status
                if status in (200, 206):
                    data = await resp.content.read(PROBE_BYTES)
                    if not data:
                        return None, status, None
                    detected = magic.from_buffer(data, mime=True)
                    return (detected if detected else None), status, None
                return None, status, f"unexpected_status_{status}"
        except Exception as exc:
            return None, None, str(exc)


async def head_url(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    material_id: str,
    file_url: str,
    kafka_event_type: str,
    kafka_timestamp: Optional[str],
) -> FileHeadResult:
    observed_at = datetime.now(timezone.utc).isoformat()
    normalized_url = normalize_url(file_url)
    headers: Dict[str, str] = {}
    status: Optional[int] = None
    final_url = normalized_url
    error: Optional[str] = None

    async with semaphore:
        try:
            async with session.head(
                normalized_url, allow_redirects=HEAD_ALLOW_REDIRECTS
            ) as resp:
                status = resp.status
                final_url = str(resp.url)
                headers = {k: v for k, v in resp.headers.items()}
        except Exception as exc:
            error = str(exc)

    content_type = mime_type_from_headers(headers)
    extension = extension_from_url(final_url)
    size_bytes = parse_content_length(headers)

    # Binary probe — run on the final URL (after potential redirects)
    binary_ct, probe_st, probe_err = await probe_binary_type(
        session, semaphore, final_url
    )

    return FileHeadResult(
        material_id=material_id,
        file_url=normalized_url,
        final_url=final_url,
        status=status,
        headers=headers,
        content_type=content_type,
        extension=extension,
        size_bytes=size_bytes,
        error=error,
        kafka_event_type=kafka_event_type,
        kafka_timestamp=kafka_timestamp,
        observed_at=observed_at,
        binary_content_type=binary_ct,
        probe_status=probe_st,
        probe_error=probe_err,
    )


async def head_many(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    material_id: str,
    file_urls: List[str],
    kafka_event_type: str,
    kafka_timestamp: Optional[str],
) -> List[FileHeadResult]:
    if not file_urls:
        return []

    tasks = [
        head_url(
            session=session,
            semaphore=semaphore,
            material_id=material_id,
            file_url=url,
            kafka_event_type=kafka_event_type,
            kafka_timestamp=kafka_timestamp,
        )
        for url in file_urls
    ]
    return await asyncio.gather(*tasks)


# ---------------------------
# Reporting
# ---------------------------
def build_empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "material_id",
            "kafka_event_type",
            "kafka_timestamp",
            "material_updated_at",
            "file_url",
            "final_url",
            "status",
            "content_type",
            "extension",
            "size_bytes",
            "size_mb",
            "size_gb",
            "error",
            "head_headers_json",
            "observed_at",
            "binary_content_type",
            "probe_status",
            "probe_error",
        ]
    )


def summarize_by_content_type(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "content_type",
                "file_count",
                "known_size_count",
                "unknown_size_count",
                "successful_head_count",
                "error_count",
                "total_bytes",
                "total_gb",
                "average_mb",
                "median_mb",
                "p95_mb",
                "min_mb",
                "max_mb",
            ]
        )

    grouped = df.groupby("content_type", dropna=False)
    summary = grouped.agg(
        file_count=("file_url", "count"),
        known_size_count=("size_bytes", lambda s: int(s.notna().sum())),
        successful_head_count=(
            "status",
            lambda s: int(s.fillna(0).between(200, 299).sum()),
        ),
        error_count=("error", lambda s: int(s.notna().sum())),
        total_bytes=("size_bytes", lambda s: float(s.fillna(0).sum())),
        average_mb=("size_mb", "mean"),
        median_mb=("size_mb", "median"),
        min_mb=("size_mb", "min"),
        max_mb=("size_mb", "max"),
    ).reset_index()

    p95 = grouped["size_mb"].quantile(0.95).reset_index(name="p95_mb")
    summary = summary.merge(p95, on="content_type", how="left")
    summary["unknown_size_count"] = summary["file_count"] - summary["known_size_count"]
    summary["total_gb"] = summary["total_bytes"] / BYTES_PER_GB
    numeric_cols = [
        "average_mb",
        "median_mb",
        "p95_mb",
        "min_mb",
        "max_mb",
        "total_bytes",
        "total_gb",
    ]
    summary[numeric_cols] = summary[numeric_cols].fillna(0)
    return summary.sort_values(["file_count", "total_bytes"], ascending=[False, False])


def summarize_status_codes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["status_code", "count"])
    statuses = df["status"].fillna(-1).astype(int).astype(str)
    statuses = statuses.replace({"-1": "NO_RESPONSE"})
    return (
        statuses.value_counts()
        .rename_axis("status_code")
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )


def build_size_bucket_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["content_type", "size_bucket", "count"])

    sized = df[df["size_mb"].notna()].copy()
    if sized.empty:
        return pd.DataFrame(columns=["content_type", "size_bucket", "count"])

    bins = [-np.inf, 0.001, 0.1, 1, 10, 100, 1024, np.inf]
    labels = [
        "0-1KB",
        "1KB-100KB",
        "100KB-1MB",
        "1-10MB",
        "10-100MB",
        "100MB-1GB",
        "1GB+",
    ]
    sized["size_bucket"] = pd.cut(
        sized["size_mb"], bins=bins, labels=labels, right=False
    )
    return (
        sized.groupby(["content_type", "size_bucket"], observed=False)
        .size()
        .reset_index(name="count")
        .sort_values(["content_type", "size_bucket"])
    )


def overall_metrics(df: pd.DataFrame) -> pd.DataFrame:
    known = df[df["size_bytes"].notna()].copy() if not df.empty else df.copy()
    total_bytes = float(known["size_bytes"].fillna(0).sum()) if not known.empty else 0.0
    metrics = {
        "material_count": int(df["material_id"].nunique()) if not df.empty else 0,
        "file_count": int(len(df)),
        "unique_content_type_count": int(df["content_type"].fillna("unknown").nunique())
        if not df.empty
        else 0,
        "known_size_file_count": int(known["size_bytes"].notna().sum())
        if not known.empty
        else 0,
        "unknown_size_file_count": int(
            len(df) - (known["size_bytes"].notna().sum() if not known.empty else 0)
        ),
        "successful_head_count": int(df["status"].fillna(0).between(200, 299).sum())
        if not df.empty
        else 0,
        "head_error_count": int(df["error"].notna().sum()) if not df.empty else 0,
        "total_bytes_known": total_bytes,
        "total_gb_known": total_bytes / BYTES_PER_GB,
        "average_mb_per_known_file": float(known["size_mb"].mean())
        if not known.empty
        else 0.0,
        "median_mb_per_known_file": float(known["size_mb"].median())
        if not known.empty
        else 0.0,
        "p95_mb_per_known_file": float(known["size_mb"].quantile(0.95))
        if not known.empty
        else 0.0,
        "largest_file_mb": float(known["size_mb"].max()) if not known.empty else 0.0,
    }
    return pd.DataFrame([metrics])


# ---------------------------
# MIME verification helpers
# ---------------------------
def enrich_mime_verification(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived match columns to the dataframe."""
    if df.empty:
        df["head_mime_norm"] = pd.Series(dtype="object")
        df["binary_mime_norm"] = pd.Series(dtype="object")
        df["mime_detectable"] = pd.Series(dtype="bool")
        df["mime_match"] = pd.Series(dtype="object")
        return df

    df = df.copy()
    df["head_mime_norm"] = df["content_type"].apply(normalize_mime)
    df["binary_mime_norm"] = df["binary_content_type"].apply(normalize_mime)

    # A file is "detectable" when the binary probe returned a real type
    df["mime_detectable"] = (
        df["binary_mime_norm"].notna()
        & ~df["binary_mime_norm"].isin(MIME_UNDETECTABLE)
        & df["probe_error"].isna()
    )

    # Three-valued: True / False / None (undetectable)
    df["mime_match"] = None
    detectable_mask = df["mime_detectable"]
    df.loc[detectable_mask, "mime_match"] = (
        df.loc[detectable_mask, "head_mime_norm"]
        == df.loc[detectable_mask, "binary_mime_norm"]
    )
    return df


def build_mime_verification_inventory(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "material_id",
        "file_url",
        "content_type",
        "binary_content_type",
        "head_mime_norm",
        "binary_mime_norm",
        "mime_detectable",
        "mime_match",
        "probe_status",
        "probe_error",
    ]
    return df[[c for c in cols if c in df.columns]].copy()


def build_mime_verification_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Per HEAD content-type: total, matched, mismatched, undetectable, match_rate."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "head_content_type",
                "total",
                "matched",
                "mismatched",
                "undetectable",
                "probe_error",
                "match_rate_pct",
            ]
        )

    rows_out: List[Dict[str, Any]] = []
    for head_type, g in df.groupby("head_mime_norm", dropna=False):
        total = len(g)
        matched = int((g["mime_match"] == True).sum())  # noqa: E712
        mismatched = int((g["mime_match"] == False).sum())  # noqa: E712
        detectable = matched + mismatched
        undetectable = int(
            (~g["mime_detectable"]).sum() - g["probe_error"].notna().sum()
        )
        errors = int(g["probe_error"].notna().sum())
        rate = (matched / detectable * 100) if detectable > 0 else np.nan
        rows_out.append(
            {
                "head_content_type": head_type,
                "total": total,
                "matched": matched,
                "mismatched": mismatched,
                "undetectable": undetectable,
                "probe_error": errors,
                "match_rate_pct": rate,
            }
        )

    summary = pd.DataFrame(rows_out)
    return summary.sort_values("mismatched", ascending=False)


def build_mismatch_pairs(df: pd.DataFrame) -> pd.DataFrame:
    """Ranked (head_type, binary_type) mismatch pairs."""
    if df.empty:
        return pd.DataFrame(
            columns=["head_content_type", "binary_content_type", "count"]
        )

    mismatched = df[df["mime_match"] == False].copy()  # noqa: E712
    if mismatched.empty:
        return pd.DataFrame(
            columns=["head_content_type", "binary_content_type", "count"]
        )

    pairs = (
        mismatched.groupby(["head_mime_norm", "binary_mime_norm"])
        .size()
        .reset_index(name="count")
        .rename(
            columns={
                "head_mime_norm": "head_content_type",
                "binary_mime_norm": "binary_content_type",
            }
        )
        .sort_values("count", ascending=False)
    )
    return pairs


def write_mime_verification_csvs(df: pd.DataFrame, output_dir: Path) -> Dict[str, Path]:
    paths = {
        "mime_inventory": output_dir / "mime_verification_inventory.csv",
        "mime_summary": output_dir / "mime_verification_summary.csv",
        "mime_mismatch_pairs": output_dir / "mime_mismatch_pairs.csv",
    }
    build_mime_verification_inventory(df).to_csv(paths["mime_inventory"], index=False)
    build_mime_verification_summary(df).to_csv(paths["mime_summary"], index=False)
    build_mismatch_pairs(df).to_csv(paths["mime_mismatch_pairs"], index=False)
    return paths


# ---------------------------
# MIME verification charts
# ---------------------------
def plot_mime_match_rate(df: pd.DataFrame, output_dir: Path) -> Optional[Path]:
    """Stacked horizontal bar: matched vs mismatched per HEAD content-type."""
    summary = build_mime_verification_summary(df)
    # Keep only types that have at least one detectable file
    summary = summary[(summary["matched"] + summary["mismatched"]) > 0].copy()
    if summary.empty:
        return None

    summary = summary.head(CHART_TOP_N_CONTENT_TYPES).sort_values(
        "match_rate_pct", ascending=True
    )
    labels = summary["head_content_type"].fillna("unknown").tolist()
    matched = summary["matched"].tolist()
    mismatched = summary["mismatched"].tolist()

    fig, ax = plt.subplots(figsize=(13, max(5, len(labels) * 0.45)))
    y = range(len(labels))
    ax.barh(y, matched, label="Matched", color="#2ecc71")
    ax.barh(y, mismatched, left=matched, label="Mismatched", color="#e74c3c")
    ax.set_yticks(list(y))
    ax.set_yticklabels(labels)
    ax.set_xlabel("Number of files")
    ax.set_ylabel("HEAD Content-Type")
    ax.set_title("MIME verification: matched vs mismatched per HEAD Content-Type")
    ax.legend(loc="lower right")

    # Annotate match rate
    for i, (m, mm, rate) in enumerate(
        zip(matched, mismatched, summary["match_rate_pct"])
    ):
        total = m + mm
        if total > 0:
            ax.text(total + total * 0.01, i, f"{rate:.0f}%", va="center", fontsize=8)

    path = output_dir / "chart_mime_match_rate.png"
    safe_savefig(path)
    return path


def plot_mime_confusion_matrix(df: pd.DataFrame, output_dir: Path) -> Optional[Path]:
    """Heatmap of HEAD type (rows) vs binary type (columns)."""
    detectable = df[df["mime_detectable"] == True].copy()  # noqa: E712
    if detectable.empty:
        return None

    cross = pd.crosstab(detectable["head_mime_norm"], detectable["binary_mime_norm"])
    if cross.empty:
        return None

    # Keep only top types by total occurrence to stay readable
    top_n = CHART_TOP_N_CONTENT_TYPES
    row_totals = cross.sum(axis=1).sort_values(ascending=False)
    col_totals = cross.sum(axis=0).sort_values(ascending=False)
    top_rows = row_totals.head(top_n).index
    top_cols = col_totals.head(top_n).index
    cross = cross.loc[
        cross.index.isin(top_rows),
        cross.columns.isin(top_cols),
    ]
    if cross.empty:
        return None

    fig, ax = plt.subplots(
        figsize=(max(8, len(cross.columns) * 0.7), max(6, len(cross.index) * 0.5))
    )
    cmap = LinearSegmentedColormap.from_list("wg", ["#ffffff", "#3498db", "#1a1a2e"])
    im = ax.imshow(cross.values, aspect="auto", cmap=cmap)
    fig.colorbar(im, ax=ax, label="File count")

    ax.set_xticks(range(len(cross.columns)))
    ax.set_xticklabels(
        [str(c) for c in cross.columns], rotation=55, ha="right", fontsize=7
    )
    ax.set_yticks(range(len(cross.index)))
    ax.set_yticklabels([str(r) for r in cross.index], fontsize=7)
    ax.set_xlabel("Binary MIME (detected from bytes)")
    ax.set_ylabel("HEAD Content-Type (server-reported)")
    ax.set_title("MIME confusion matrix: HEAD vs binary detection")

    # Annotate cells with counts
    for i in range(len(cross.index)):
        for j in range(len(cross.columns)):
            val = cross.values[i, j]
            if val > 0:
                color = "white" if val > cross.values.max() * 0.6 else "black"
                ax.text(
                    j,
                    i,
                    str(int(val)),
                    ha="center",
                    va="center",
                    fontsize=7,
                    color=color,
                )

    path = output_dir / "chart_mime_confusion_matrix.png"
    safe_savefig(path)
    return path


def plot_top_mismatch_pairs(df: pd.DataFrame, output_dir: Path) -> Optional[Path]:
    """Horizontal bar of the most frequent (head→binary) mismatch pairs."""
    pairs = build_mismatch_pairs(df)
    if pairs.empty:
        return None

    top = pairs.head(CHART_TOP_N_MISMATCH_PAIRS).copy()
    top = top.sort_values("count", ascending=True)
    labels = [
        truncate_label(f"{row['head_content_type']} → {row['binary_content_type']}")
        for _, row in top.iterrows()
    ]

    fig, ax = plt.subplots(figsize=(13, max(5, len(labels) * 0.4)))
    ax.barh(labels, top["count"].tolist(), color="#e67e22")
    ax.set_xlabel("Number of mismatched files")
    ax.set_ylabel("HEAD → Binary MIME pair")
    ax.set_title(f"Top {len(top)} MIME mismatch pairs (HEAD vs binary detection)")
    path = output_dir / "chart_top_mismatch_pairs.png"
    safe_savefig(path)
    return path


def write_csvs(df: pd.DataFrame, output_dir: Path) -> Dict[str, Path]:
    ensure_output_dir(output_dir)

    paths = {
        "inventory": output_dir / "files_inventory.csv",
        "overall": output_dir / "summary_overall.csv",
        "content_type": output_dir / "summary_by_content_type.csv",
        "size_buckets": output_dir / "summary_size_buckets.csv",
        "status_codes": output_dir / "summary_status_codes.csv",
        "largest_files": output_dir / "largest_files.csv",
    }

    df.to_csv(paths["inventory"], index=False)
    overall_metrics(df).to_csv(paths["overall"], index=False)
    summarize_by_content_type(df).to_csv(paths["content_type"], index=False)
    build_size_bucket_summary(df).to_csv(paths["size_buckets"], index=False)
    summarize_status_codes(df).to_csv(paths["status_codes"], index=False)

    if df.empty:
        build_empty_frame().to_csv(paths["largest_files"], index=False)
    else:
        largest = (
            df[df["size_mb"].notna()]
            .sort_values("size_bytes", ascending=False)
            .head(CHART_TOP_N_FILES)
        )
        largest.to_csv(paths["largest_files"], index=False)

    # MIME verification CSVs
    mime_paths = write_mime_verification_csvs(df, output_dir)
    paths.update(mime_paths)

    return paths


def safe_savefig(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


def plot_file_count_by_content_type(
    summary: pd.DataFrame, output_dir: Path
) -> Optional[Path]:
    if summary.empty:
        return None
    top = summary.head(CHART_TOP_N_CONTENT_TYPES).sort_values(
        "file_count", ascending=True
    )
    plt.figure(figsize=(12, max(5, len(top) * 0.45)))
    plt.barh(top["content_type"].fillna("unknown"), top["file_count"])
    plt.xlabel("Number of files")
    plt.ylabel("Content-Type")
    plt.title(f"Top {len(top)} content-types by file count")
    path = output_dir / "chart_file_count_by_content_type.png"
    safe_savefig(path)
    return path


def plot_total_size_by_content_type(
    summary: pd.DataFrame, output_dir: Path
) -> Optional[Path]:
    if summary.empty:
        return None
    top = (
        summary.sort_values("total_gb", ascending=False)
        .head(CHART_TOP_N_CONTENT_TYPES)
        .sort_values("total_gb", ascending=True)
    )
    plt.figure(figsize=(12, max(5, len(top) * 0.45)))
    plt.barh(top["content_type"].fillna("unknown"), top["total_gb"])
    plt.xlabel("Total size (GB)")
    plt.ylabel("Content-Type")
    plt.title(f"Top {len(top)} content-types by total size")
    path = output_dir / "chart_total_size_by_content_type_gb.png"
    safe_savefig(path)
    return path


def plot_size_histogram(df: pd.DataFrame, output_dir: Path) -> Optional[Path]:
    sized = df[df["size_bytes"].notna() & (df["size_bytes"] > 0)].copy()
    if sized.empty:
        return None

    min_bytes = float(sized["size_bytes"].min())
    max_bytes = float(sized["size_bytes"].max())
    if min_bytes <= 0 or max_bytes <= 0:
        return None

    bins = cast(
        List[float],
        np.logspace(math.log10(min_bytes), math.log10(max_bytes), num=40).tolist(),
    )
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.hist(sized["size_bytes"], bins=bins)
    configure_log_size_axis(ax, min_bytes, max_bytes)
    ax.set_xlabel("File size")
    ax.set_ylabel("Number of files")
    ax.set_title("File size distribution (logarithmic axis with human-readable sizes)")
    path = output_dir / "chart_file_size_distribution.png"
    safe_savefig(path)
    return path


def plot_boxplot_top_content_types(
    df: pd.DataFrame, output_dir: Path
) -> Optional[Path]:
    sized = df[df["size_bytes"].notna() & (df["size_bytes"] > 0)].copy()
    if sized.empty:
        return None

    top_types = (
        sized.groupby("content_type")["file_url"]
        .count()
        .sort_values(ascending=False)
        .head(min(CHART_TOP_N_CONTENT_TYPES, 10))
        .index.tolist()
    )
    if not top_types:
        return None

    data: List[List[float]] = []
    for file_type in top_types:
        size_series = cast(
            Any, sized.loc[sized["content_type"] == file_type, "size_bytes"]
        )
        size_values = [float(value) for value in size_series.tolist()]
        data.append(size_values)
    if not any(len(values) for values in data):
        return None

    min_bytes = min(float(np.min(values)) for values in data if len(values))
    max_bytes = max(float(np.max(values)) for values in data if len(values))

    fig, ax = plt.subplots(figsize=(12, max(5, len(top_types) * 0.5)))
    ax.boxplot(
        data,
        tick_labels=top_types,
        vert=False,
        showfliers=False,
        whis=cast(Any, (0, 100)),
    )
    configure_log_size_axis(ax, min_bytes, max_bytes)
    ax.set_xlabel("File size")
    ax.set_ylabel("Content-Type")
    ax.set_title(
        "Size spread per popular content-type (whiskers show true min-max range)"
    )
    path = output_dir / "chart_size_boxplot_by_content_type.png"
    safe_savefig(path)
    return path


def plot_size_bucket_heatmap(df: pd.DataFrame, output_dir: Path) -> Optional[Path]:
    buckets = build_size_bucket_summary(df)
    if buckets.empty:
        return None

    top_types = (
        buckets.groupby("content_type")["count"]
        .sum()
        .sort_values(ascending=False)
        .head(CHART_TOP_N_CONTENT_TYPES)
        .index
    )
    top = buckets[buckets["content_type"].isin(top_types)].copy()
    pivot = top.pivot(
        index="content_type", columns="size_bucket", values="count"
    ).fillna(0)
    if pivot.empty:
        return None

    plt.figure(figsize=(12, max(5, len(pivot) * 0.45)))
    plt.imshow(pivot.values, aspect="auto")
    plt.colorbar(label="Number of files")
    plt.xticks(
        range(len(pivot.columns)),
        list(pivot.columns.astype(str)),
        rotation=45,
        ha="right",
    )
    plt.yticks(range(len(pivot.index)), [str(label) for label in list(pivot.index)])
    plt.xlabel("Size bucket")
    plt.ylabel("Content-Type")
    plt.title("File count heatmap by size bucket and content-type")
    path = output_dir / "chart_size_bucket_heatmap.png"
    safe_savefig(path)
    return path


def plot_largest_files(df: pd.DataFrame, output_dir: Path) -> Optional[Path]:
    largest = (
        df[df["size_mb"].notna()]
        .sort_values("size_bytes", ascending=False)
        .head(CHART_TOP_N_FILES)
        .copy()
    )
    if largest.empty:
        return None

    labels = [
        truncate_label(
            f"{(row['content_type'] or 'unknown')} | {row['material_id']} | {Path(urlsplit(row['final_url']).path).name or row['final_url']}"
        )
        for _, row in largest.iterrows()
    ]
    plt.figure(figsize=(14, max(6, len(largest) * 0.35)))
    plt.barh(labels[::-1], largest["size_mb"].iloc[::-1])
    plt.xlabel("File size (MB)")
    plt.ylabel("Largest files")
    plt.title(f"Top {len(largest)} largest files")
    path = output_dir / "chart_largest_files.png"
    safe_savefig(path)
    return path


def plot_status_codes(df: pd.DataFrame, output_dir: Path) -> Optional[Path]:
    status = summarize_status_codes(df)
    if status.empty:
        return None

    plt.figure(figsize=(10, 5))
    plt.bar(status["status_code"], status["count"])
    plt.xlabel("HEAD response status")
    plt.ylabel("Number of files")
    plt.title("HEAD response status distribution")
    path = output_dir / "chart_head_status_codes.png"
    safe_savefig(path)
    return path


def render_reports(state: MaterialState, output_dir: Path) -> Dict[str, Any]:
    ensure_output_dir(output_dir)
    rows = state.to_rows()
    df = pd.DataFrame(rows) if rows else build_empty_frame()

    # Enrich with MIME verification columns
    df = enrich_mime_verification(df)

    csv_paths = write_csvs(df, output_dir)

    chart_paths = [
        plot_file_count_by_content_type(summarize_by_content_type(df), output_dir),
        plot_total_size_by_content_type(summarize_by_content_type(df), output_dir),
        plot_size_histogram(df, output_dir),
        plot_boxplot_top_content_types(df, output_dir),
        plot_size_bucket_heatmap(df, output_dir),
        plot_largest_files(df, output_dir),
        plot_status_codes(df, output_dir),
        # MIME verification charts
        plot_mime_match_rate(df, output_dir),
        plot_mime_confusion_matrix(df, output_dir),
        plot_top_mismatch_pairs(df, output_dir),
    ]

    overall = overall_metrics(df).iloc[0].to_dict()

    # MIME verification summary for console
    mime_summary = build_mime_verification_summary(df)
    detectable_total = (
        int(mime_summary["matched"].sum() + mime_summary["mismatched"].sum())
        if not mime_summary.empty
        else 0
    )
    mime_matched = int(mime_summary["matched"].sum()) if not mime_summary.empty else 0
    mime_mismatched = (
        int(mime_summary["mismatched"].sum()) if not mime_summary.empty else 0
    )
    mime_undetectable = (
        int(mime_summary["undetectable"].sum()) if not mime_summary.empty else 0
    )
    mime_probe_errors = (
        int(mime_summary["probe_error"].sum()) if not mime_summary.empty else 0
    )
    mime_rate = (mime_matched / detectable_total * 100) if detectable_total > 0 else 0.0

    overall["mime_detectable_total"] = detectable_total
    overall["mime_matched"] = mime_matched
    overall["mime_mismatched"] = mime_mismatched
    overall["mime_undetectable"] = mime_undetectable
    overall["mime_probe_errors"] = mime_probe_errors
    overall["mime_match_rate_pct"] = mime_rate

    return {
        "dataframe": df,
        "csv_paths": csv_paths,
        "chart_paths": [p for p in chart_paths if p is not None],
        "overall": overall,
    }


def print_console_report(report: Dict[str, Any], counters: Counter) -> None:
    overall = report["overall"]
    print("\n" + "=" * 100)
    print(f"Materials tracked        : {int(overall['material_count'])}")
    print(f"Files tracked            : {int(overall['file_count'])}")
    print(f"Unique content-types     : {int(overall['unique_content_type_count'])}")
    print(f"Known-size files         : {int(overall['known_size_file_count'])}")
    print(f"Unknown-size files       : {int(overall['unknown_size_file_count'])}")
    print(f"Total known size (GB)    : {overall['total_gb_known']:.3f}")
    print(f"Avg size / known file MB : {overall['average_mb_per_known_file']:.3f}")
    print(f"Median size / known file : {overall['median_mb_per_known_file']:.3f} MB")
    print(f"P95 size / known file    : {overall['p95_mb_per_known_file']:.3f} MB")
    print(f"Largest known file       : {overall['largest_file_mb']:.3f} MB")
    print("-" * 100)
    print(
        "Events seen "
        f"(created={counters['MaterialCreated']}, updated={counters['MaterialUpdated']}, "
        f"deleted={counters['MaterialDeleted']}, ignored={counters['ignored']}, malformed={counters['malformed']})"
    )
    print("=" * 100)
    # MIME verification
    print(f"MIME detectable files    : {int(overall.get('mime_detectable_total', 0))}")
    print(f"MIME matched             : {int(overall.get('mime_matched', 0))}")
    print(f"MIME mismatched          : {int(overall.get('mime_mismatched', 0))}")
    print(f"MIME undetectable        : {int(overall.get('mime_undetectable', 0))}")
    print(f"MIME probe errors        : {int(overall.get('mime_probe_errors', 0))}")
    print(f"MIME match rate          : {overall.get('mime_match_rate_pct', 0):.1f}%")
    print("=" * 100)
    print(f"CSV inventory            : {report['csv_paths']['inventory']}")
    print(f"CSV overall              : {report['csv_paths']['overall']}")
    print(f"CSV by content-type      : {report['csv_paths']['content_type']}")
    print(f"CSV size buckets         : {report['csv_paths']['size_buckets']}")
    print(f"CSV status codes         : {report['csv_paths']['status_codes']}")
    print(f"CSV largest files        : {report['csv_paths']['largest_files']}")
    print(
        f"CSV MIME inventory       : {report['csv_paths'].get('mime_inventory', 'N/A')}"
    )
    print(
        f"CSV MIME summary         : {report['csv_paths'].get('mime_summary', 'N/A')}"
    )
    print(
        f"CSV MIME mismatch pairs  : {report['csv_paths'].get('mime_mismatch_pairs', 'N/A')}"
    )
    if report["chart_paths"]:
        print("Charts                   :")
        for chart in report["chart_paths"]:
            print(f"  - {chart}")
    print("=" * 100)
    sys.stdout.flush()


# ---------------------------
# Main consumer loop
# ---------------------------
async def run() -> None:
    print(f"Consuming topic={TOPIC} bootstrap={BOOTSTRAP} group={GROUP_ID}")
    print(f"Output directory: {OUTPUT_DIR.resolve()}")

    state = MaterialState()
    counters: Counter = Counter()
    processed_tracked_messages = 0
    stop_event = asyncio.Event()

    def request_stop(*_args) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, request_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: request_stop())

    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset=AUTO_OFFSET_RESET,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    connector = aiohttp.TCPConnector(ssl=VERIFY_SSL)
    semaphore = asyncio.Semaphore(HTTP_CONCURRENCY)

    # Subscribe exactly once, before start(). Passing topics in the constructor
    # is already equivalent to subscribe(), so doing both can disturb assignment
    # state in aiokafka.
    consumer.subscribe(
        [TOPIC], listener=ResetToBeginningListener(consumer, RESET_CONSUMER)
    )

    await consumer.start()
    try:
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": USER_AGENT},
        ) as session:
            while not stop_event.is_set():
                batch = await consumer.getmany(timeout_ms=1000, max_records=100)
                if not batch:
                    continue

                for _, messages in batch.items():
                    for msg in messages:
                        event = msg.value if isinstance(msg.value, dict) else None
                        if not isinstance(event, dict):
                            counters["malformed"] += 1
                            continue

                        event_type = event.get("type")
                        if (
                            event_type in IGNORE_MESSAGE_TYPES
                            or event_type not in TRACKED_MESSAGE_TYPES
                        ):
                            counters["ignored"] += 1
                            continue

                        material_id = extract_material_id(event)
                        if not material_id:
                            counters["malformed"] += 1
                            continue

                        kafka_timestamp = iso_utc_from_ms(msg.timestamp)
                        counters[event_type] += 1

                        if event_type in {"MaterialCreated", "MaterialUpdated"}:
                            file_urls = extract_file_urls(event)
                            file_results = await head_many(
                                session=session,
                                semaphore=semaphore,
                                material_id=material_id,
                                file_urls=file_urls,
                                kafka_event_type=event_type,
                                kafka_timestamp=kafka_timestamp,
                            )
                            state.upsert_material(
                                material_id=material_id,
                                event_type=event_type,
                                kafka_timestamp=kafka_timestamp,
                                file_results=file_results,
                            )
                        elif event_type == "MaterialDeleted":
                            state.delete_material(material_id)

                        processed_tracked_messages += 1

                        if processed_tracked_messages % PRINT_EVERY == 0:
                            report = render_reports(state, OUTPUT_DIR)
                            print_console_report(report, counters)

                        if processed_tracked_messages % EXPORT_EVERY == 0:
                            render_reports(state, OUTPUT_DIR)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        await consumer.stop()
        report = render_reports(state, OUTPUT_DIR)
        print_console_report(report, counters)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
