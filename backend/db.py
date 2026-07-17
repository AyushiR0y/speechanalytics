"""
db.py — Dual-mode persistence layer for Speech Analytics.

Mode A (PostgreSQL):  Set DATABASE_URL env var.
Mode B (JSON files):  No DATABASE_URL → data stored under ./processed/ as JSON.
                      Suitable for Render free tier (ephemeral disk) or local dev.

Install:  pip install psycopg2-binary   # only needed for Mode A
Env var:  DATABASE_URL=postgresql://user:pass@host:5432/dbname
"""

import json
import logging
import os
import re
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger("speech_analytics.db")

from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

# ─────────────────────────────────────────────────────────────────────────────
# Try to load psycopg2 only when DATABASE_URL is present
# ─────────────────────────────────────────────────────────────────────────────
_psycopg2 = None
if DATABASE_URL:
    try:
        import psycopg2
        import psycopg2.extras
        from psycopg2.extras import RealDictCursor
        _psycopg2 = psycopg2
        log.info("db.py: PostgreSQL mode (DATABASE_URL detected)")
    except ImportError:
        log.warning("db.py: psycopg2 not installed – falling back to JSON mode")
        DATABASE_URL = ""
else:
    log.info("db.py: JSON-file mode (no DATABASE_URL)")

# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL helpers  (only used when DATABASE_URL is set)
# ─────────────────────────────────────────────────────────────────────────────

def get_conn():
    """Return a new psycopg2 connection. Caller must close it."""
    if not DATABASE_URL:
        raise RuntimeError("get_conn() called but DATABASE_URL is not set")
    return _psycopg2.connect(DATABASE_URL, cursor_factory=_psycopg2.extras.RealDictCursor)


@contextmanager
def db_cursor(commit: bool = True):
    conn = get_conn()
    try:
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _j(obj: Any) -> str:
    return json.dumps(obj, default=str)


def _row_to_dict(row) -> Dict:
    if row is None:
        return {}
    return dict(row)


# ─────────────────────────────────────────────────────────────────────────────
# JSON-file backend  (used when DATABASE_URL is NOT set)
# ─────────────────────────────────────────────────────────────────────────────

_BASE = Path(__file__).parent.parent / "processed"
_BASE.mkdir(parents=True, exist_ok=True)
_JOBS_FILE    = _BASE / "jf_jobs.json"
_RAW_FILE     = _BASE / "jf_raw_calls.json"
_ANAL_FILE    = _BASE / "jf_analyzed_calls.json"
_FILE_LOCK    = threading.Lock()


def _read_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def _write_json(path: Path, data):
    path.write_text(json.dumps(data, default=str, indent=2), encoding="utf-8")


def _jobs_read() -> Dict[str, Dict]:
    return _read_json(_JOBS_FILE, {})


def _jobs_write(data: Dict[str, Dict]):
    _write_json(_JOBS_FILE, data)


def _raw_read() -> Dict[str, Dict]:
    return _read_json(_RAW_FILE, {})


def _raw_write(data: Dict[str, Dict]):
    _write_json(_RAW_FILE, data)


def _anal_read() -> Dict[str, Dict]:
    return _read_json(_ANAL_FILE, {})


def _anal_write(data: Dict[str, Dict]):
    _write_json(_ANAL_FILE, data)


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API — dual-mode implementations
# ══════════════════════════════════════════════════════════════════════════════

# ── JOBS ─────────────────────────────────────────────────────────────────────

def create_job(job: Dict) -> None:
    if DATABASE_URL:
        with db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO jobs (id, status, files, total, processed,
                                  fatal_count, flag_count, created_at, completed_at)
                VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    job["id"], job.get("status", "queued"),
                    _j(job.get("files", [])),
                    job.get("total", 0), job.get("processed", 0),
                    job.get("fatal_count", 0), job.get("flag_count", 0),
                    job.get("created_at") or datetime.now().isoformat(),
                    job.get("completed_at"),
                ),
            )
    else:
        with _FILE_LOCK:
            data = _jobs_read()
            if job["id"] not in data:
                data[job["id"]] = dict(job)
                data[job["id"]].setdefault("created_at", datetime.now().isoformat())
                _jobs_write(data)


def update_job(job_id: str, **kwargs) -> None:
    if not kwargs:
        return
    if DATABASE_URL:
        set_clauses, values = [], []
        for col, val in kwargs.items():
            if col == "files":
                set_clauses.append(f"{col} = %s::jsonb")
                values.append(_j(val))
            else:
                set_clauses.append(f"{col} = %s")
                values.append(val)
        values.append(job_id)
        with db_cursor() as cur:
            cur.execute(f"UPDATE jobs SET {', '.join(set_clauses)} WHERE id = %s", values)
    else:
        with _FILE_LOCK:
            data = _jobs_read()
            if job_id in data:
                data[job_id].update(kwargs)
                _jobs_write(data)


def get_job(job_id: str) -> Optional[Dict]:
    if DATABASE_URL:
        with db_cursor(commit=False) as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
        return _row_to_dict(row) if row else None
    else:
        return _jobs_read().get(job_id)


def list_jobs() -> List[Dict]:
    if DATABASE_URL:
        with db_cursor(commit=False) as cur:
            cur.execute("SELECT * FROM jobs ORDER BY created_at DESC")
            return [_row_to_dict(r) for r in cur.fetchall()]
    else:
        jobs = list(_jobs_read().values())
        jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)
        return jobs


def increment_job_counters(job_id: str, processed: int = 0,
                           fatal: int = 0, flagged: int = 0) -> None:
    if DATABASE_URL:
        with db_cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET processed   = processed   + %s,
                    fatal_count = fatal_count + %s,
                    flag_count  = flag_count  + %s
                WHERE id = %s
                """,
                (processed, fatal, flagged, job_id),
            )
    else:
        with _FILE_LOCK:
            data = _jobs_read()
            if job_id in data:
                data[job_id]["processed"]   = int(data[job_id].get("processed", 0))   + processed
                data[job_id]["fatal_count"] = int(data[job_id].get("fatal_count", 0)) + fatal
                data[job_id]["flag_count"]  = int(data[job_id].get("flag_count", 0))  + flagged
                _jobs_write(data)


# ── RAW CALLS ─────────────────────────────────────────────────────────────────

def insert_raw_call(call_id: str, job_id: str, item: Dict) -> None:
    if DATABASE_URL:
        with db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_calls (id, job_id, name, sl, source_file,
                                       raw_text, turns, meta, ingested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    call_id, job_id,
                    item.get("name", ""), str(item.get("sl", "")),
                    item.get("source_file", ""),
                    (item.get("text") or "")[:50000],
                    _j(item.get("turns", [])), _j(item.get("meta", {})),
                ),
            )
    else:
        with _FILE_LOCK:
            data = _raw_read()
            if call_id not in data:
                data[call_id] = {
                    "id": call_id, "job_id": job_id,
                    "name": item.get("name", ""), "sl": str(item.get("sl", "")),
                    "source_file": item.get("source_file", ""),
                    "raw_text": (item.get("text") or "")[:50000],
                    "turns": item.get("turns", []),
                    "meta": item.get("meta", {}),
                    "ingested_at": datetime.now().isoformat(),
                }
                _raw_write(data)


def get_raw_call(call_id: str) -> Optional[Dict]:
    if DATABASE_URL:
        with db_cursor(commit=False) as cur:
            cur.execute("SELECT * FROM raw_calls WHERE id = %s", (call_id,))
            row = cur.fetchone()
        return _row_to_dict(row) if row else None
    else:
        return _raw_read().get(call_id)


# ── ANALYZED CALLS ────────────────────────────────────────────────────────────

def upsert_analyzed_call(call_record: Dict) -> None:
    a = call_record.get("analysis", {})
    scores = a.get("scores", {})
    is_fatal   = call_record.get("fatal", False) or a.get("severity") in {"fatal", "critical"}
    is_flagged = call_record.get("flagged", False) or len(a.get("flags", [])) > 0

    if DATABASE_URL:
        with db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO analyzed_calls (
                    id, raw_call_id, job_id, name, sl, source_file,
                    weighted_score, pass_fail, severity, fatal, flagged,
                    fatal_reason, category, sentiment,
                    flags, failed_parameters,
                    score_greeting_opening, score_query_understanding,
                    score_response_accuracy, score_communication_quality,
                    score_compliance, score_personalisation,
                    score_empathy_soft_skills, score_resolution,
                    score_system_behaviour, score_closing_interaction,
                    product_mentioned, products_mentioned,
                    product_confidence, product_signals,
                    product_accuracy_score, product_issues,
                    analysis, transcript, raw_text, processed_at
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s::jsonb,%s::jsonb,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s::jsonb,%s,%s::jsonb,%s,%s,
                    %s::jsonb,%s::jsonb,%s,%s
                )
                ON CONFLICT (id) DO UPDATE SET
                    weighted_score=EXCLUDED.weighted_score, pass_fail=EXCLUDED.pass_fail,
                    severity=EXCLUDED.severity, fatal=EXCLUDED.fatal, flagged=EXCLUDED.flagged,
                    fatal_reason=EXCLUDED.fatal_reason, category=EXCLUDED.category,
                    sentiment=EXCLUDED.sentiment, flags=EXCLUDED.flags,
                    failed_parameters=EXCLUDED.failed_parameters,
                    score_greeting_opening=EXCLUDED.score_greeting_opening,
                    score_query_understanding=EXCLUDED.score_query_understanding,
                    score_response_accuracy=EXCLUDED.score_response_accuracy,
                    score_communication_quality=EXCLUDED.score_communication_quality,
                    score_compliance=EXCLUDED.score_compliance,
                    score_personalisation=EXCLUDED.score_personalisation,
                    score_empathy_soft_skills=EXCLUDED.score_empathy_soft_skills,
                    score_resolution=EXCLUDED.score_resolution,
                    score_system_behaviour=EXCLUDED.score_system_behaviour,
                    score_closing_interaction=EXCLUDED.score_closing_interaction,
                    product_mentioned=EXCLUDED.product_mentioned,
                    products_mentioned=EXCLUDED.products_mentioned,
                    product_confidence=EXCLUDED.product_confidence,
                    product_signals=EXCLUDED.product_signals,
                    product_accuracy_score=EXCLUDED.product_accuracy_score,
                    product_issues=EXCLUDED.product_issues,
                    analysis=EXCLUDED.analysis, transcript=EXCLUDED.transcript,
                    raw_text=EXCLUDED.raw_text, processed_at=EXCLUDED.processed_at
                """,
                (
                    call_record["id"], call_record.get("id"), call_record["job_id"],
                    call_record.get("name", ""), str(call_record.get("sl", "")),
                    call_record.get("source_file", ""),
                    a.get("weighted_score", 0), a.get("pass_fail", "FAIL"),
                    a.get("severity", "normal"), is_fatal, is_flagged,
                    a.get("fatal_reason", ""), a.get("category", ""), a.get("sentiment", "neutral"),
                    _j(a.get("flags", [])), _j(a.get("failed_parameters", [])),
                    scores.get("greeting_opening"), scores.get("query_understanding"),
                    scores.get("response_accuracy"), scores.get("communication_quality"),
                    scores.get("compliance"), scores.get("personalisation"),
                    scores.get("empathy_soft_skills"), scores.get("resolution"),
                    scores.get("system_behaviour"), scores.get("closing_interaction"),
                    a.get("product_mentioned", ""), _j(a.get("products_mentioned", [])),
                    a.get("product_confidence"), _j(a.get("product_signals", [])),
                    a.get("product_accuracy_score"), a.get("product_issues", ""),
                    _j(a), _j(call_record.get("transcript", [])),
                    (call_record.get("raw_text") or "")[:50000],
                    call_record.get("processed_at") or datetime.now().isoformat(),
                ),
            )
    else:
        with _FILE_LOCK:
            data = _anal_read()
            record = dict(call_record)
            record["fatal"]   = is_fatal
            record["flagged"] = is_flagged
            record.setdefault("processed_at", datetime.now().isoformat())
            data[call_record["id"]] = record
            _anal_write(data)


def get_analyzed_call(call_id: str) -> Optional[Dict]:
    if DATABASE_URL:
        with db_cursor(commit=False) as cur:
            cur.execute("SELECT * FROM analyzed_calls WHERE id = %s", (call_id,))
            row = cur.fetchone()
        return _row_to_dict(row) if row else None
    else:
        return _anal_read().get(call_id)


def delete_analyzed_call(call_id: str) -> bool:
    if DATABASE_URL:
        with db_cursor() as cur:
            cur.execute("DELETE FROM analyzed_calls WHERE id = %s RETURNING id", (call_id,))
            return cur.fetchone() is not None
    else:
        with _FILE_LOCK:
            data = _anal_read()
            if call_id in data:
                del data[call_id]
                _anal_write(data)
                return True
        return False


def list_analyzed_calls(
    page: int = 1,
    page_size: int = 50,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    sentiment: Optional[str] = None,
    pass_fail: Optional[str] = None,
    flagged: Optional[bool] = None,
    job_id: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "processed_at",
    sort_dir: str = "desc",
) -> Dict:
    if DATABASE_URL:
        allowed_sort = {"processed_at", "weighted_score", "severity", "category", "sentiment", "pass_fail", "name"}
        if sort_by not in allowed_sort:
            sort_by = "processed_at"
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
        where, params = [], []
        if severity:   where.append("severity = %s");   params.append(severity)
        if category:   where.append("category = %s");   params.append(category)
        if sentiment:  where.append("sentiment = %s");  params.append(sentiment)
        if pass_fail:  where.append("pass_fail = %s");  params.append(pass_fail)
        if flagged is not None: where.append("flagged = %s"); params.append(flagged)
        if job_id:     where.append("job_id = %s");     params.append(job_id)
        if search:
            where.append("to_tsvector('english', COALESCE(name,'') || ' ' || COALESCE(raw_text,'')) @@ plainto_tsquery('english', %s)")
            params.append(search)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        offset = (page - 1) * page_size
        with db_cursor(commit=False) as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM analyzed_calls {where_sql}", params)
            total = cur.fetchone()["total"]
            cur.execute(
                f"""SELECT id, job_id, name, sl, source_file, category, severity,
                           weighted_score, pass_fail, sentiment, flags, failed_parameters,
                           product_mentioned, products_mentioned, product_confidence,
                           product_signals, fatal, flagged, processed_at,
                           analysis->>'score_reason' AS score_reason,
                           analysis->>'summary' AS summary,
                           analysis->'turn_count' AS turn_count,
                           analysis->'estimated_duration_minutes' AS estimated_duration_minutes
                    FROM analyzed_calls {where_sql}
                    ORDER BY {sort_by} {direction} LIMIT %s OFFSET %s""",
                params + [page_size, offset],
            )
            rows = cur.fetchall()
        return {"calls": [_row_to_dict(r) for r in rows], "total": total, "page": page, "page_size": page_size}
    else:
        all_calls = list(_anal_read().values())
        # filter
        if severity:  all_calls = [c for c in all_calls if (c.get("analysis") or {}).get("severity") == severity or c.get("severity") == severity]
        if category:  all_calls = [c for c in all_calls if (c.get("analysis") or {}).get("category") == category or c.get("category") == category]
        if sentiment: all_calls = [c for c in all_calls if (c.get("analysis") or {}).get("sentiment") == sentiment or c.get("sentiment") == sentiment]
        if pass_fail: all_calls = [c for c in all_calls if (c.get("analysis") or {}).get("pass_fail") == pass_fail or c.get("pass_fail") == pass_fail]
        if flagged is not None: all_calls = [c for c in all_calls if bool(c.get("flagged")) == flagged]
        if job_id:    all_calls = [c for c in all_calls if c.get("job_id") == job_id]
        if search:
            q = search.lower()
            all_calls = [c for c in all_calls if q in (c.get("name") or "").lower() or q in (c.get("raw_text") or "").lower()]
        # sort
        reverse = sort_dir.lower() == "desc"
        def _sort_key(c):
            a = c.get("analysis") or {}
            v = c.get(sort_by) or a.get(sort_by) or ""
            return v or ""
        all_calls.sort(key=_sort_key, reverse=reverse)
        total = len(all_calls)
        offset = (page - 1) * page_size
        page_calls = all_calls[offset: offset + page_size]
        # project to a slim summary dict matching the PG shape
        def _slim(c):
            a = c.get("analysis") or {}
            return {
                "id": c.get("id"), "job_id": c.get("job_id"), "name": c.get("name"),
                "sl": c.get("sl"), "source_file": c.get("source_file"),
                "category": a.get("category", c.get("category")),
                "severity": a.get("severity", c.get("severity")),
                "weighted_score": a.get("weighted_score", c.get("weighted_score")),
                "pass_fail": a.get("pass_fail", c.get("pass_fail")),
                "sentiment": a.get("sentiment", c.get("sentiment")),
                "flags": a.get("flags", []),
                "failed_parameters": a.get("failed_parameters", []),
                "product_mentioned": a.get("product_mentioned", ""),
                "products_mentioned": a.get("products_mentioned", []),
                "product_confidence": a.get("product_confidence"),
                "product_signals": a.get("product_signals", []),
                "fatal": c.get("fatal"), "flagged": c.get("flagged"),
                "processed_at": c.get("processed_at"),
                "score_reason": a.get("score_reason"),
                "summary": a.get("summary"),
                "turn_count": a.get("turn_count"),
                "estimated_duration_minutes": a.get("estimated_duration_minutes"),
            }
        return {"calls": [_slim(c) for c in page_calls], "total": total, "page": page, "page_size": page_size}


def get_fatal_calls() -> List[Dict]:
    if DATABASE_URL:
        with db_cursor(commit=False) as cur:
            cur.execute(
                """SELECT id, name, severity, fatal_reason, flags,
                          weighted_score, category, processed_at
                   FROM analyzed_calls
                   WHERE fatal = TRUE OR severity IN ('fatal','critical')
                   ORDER BY processed_at DESC"""
            )
            return [_row_to_dict(r) for r in cur.fetchall()]
    else:
        calls = list(_anal_read().values())
        result = []
        for c in calls:
            a = c.get("analysis") or {}
            if c.get("fatal") or a.get("severity") in {"fatal", "critical"}:
                result.append({
                    "id": c.get("id"), "name": c.get("name"),
                    "severity": a.get("severity", c.get("severity")),
                    "fatal_reason": a.get("fatal_reason", ""),
                    "flags": a.get("flags", []),
                    "weighted_score": a.get("weighted_score", c.get("weighted_score")),
                    "category": a.get("category", c.get("category")),
                    "processed_at": c.get("processed_at"),
                })
        result.sort(key=lambda x: x.get("processed_at") or "", reverse=True)
        return result


def get_dashboard_stats() -> Dict:
    if DATABASE_URL:
        with db_cursor(commit=False) as cur:
            cur.execute("SELECT COUNT(*) AS total FROM analyzed_calls")
            total = cur.fetchone()["total"]
            if total == 0:
                return {"total": 0, "message": "No calls processed yet"}
            cur.execute("""
                SELECT ROUND(AVG(weighted_score)::NUMERIC,2) AS avg_score,
                       SUM(CASE WHEN pass_fail='PASS' THEN 1 ELSE 0 END) AS pass_count,
                       SUM(CASE WHEN pass_fail='FAIL' THEN 1 ELSE 0 END) AS fail_count,
                       SUM(CASE WHEN fatal=TRUE  THEN 1 ELSE 0 END) AS fatal_count,
                       SUM(CASE WHEN flagged=TRUE THEN 1 ELSE 0 END) AS flagged_count
                FROM analyzed_calls""")
            agg = _row_to_dict(cur.fetchone())
            cur.execute("SELECT severity, COUNT(*) AS cnt FROM analyzed_calls GROUP BY severity")
            severities = {r["severity"]: r["cnt"] for r in cur.fetchall()}
            cur.execute("SELECT category, COUNT(*) AS cnt FROM analyzed_calls GROUP BY category ORDER BY cnt DESC")
            categories = {r["category"]: r["cnt"] for r in cur.fetchall()}
            cur.execute("SELECT sentiment, COUNT(*) AS cnt FROM analyzed_calls GROUP BY sentiment")
            sentiments = {r["sentiment"]: r["cnt"] for r in cur.fetchall()}
            cur.execute("""SELECT product_mentioned, COUNT(*) AS cnt FROM analyzed_calls
                           WHERE product_mentioned IS NOT NULL AND product_mentioned != ''
                           GROUP BY product_mentioned ORDER BY cnt DESC""")
            products = {r["product_mentioned"]: r["cnt"] for r in cur.fetchall()}
            cur.execute("""SELECT product_mentioned, ROUND(AVG(product_confidence)::NUMERIC,3) AS avg_conf
                           FROM analyzed_calls WHERE product_confidence IS NOT NULL GROUP BY product_mentioned""")
            avg_prod_conf = {r["product_mentioned"]: float(r["avg_conf"] or 0) for r in cur.fetchall()}
            cur.execute("""
                SELECT ROUND(AVG(score_greeting_opening)::NUMERIC,2) AS greeting_opening,
                       ROUND(AVG(score_query_understanding)::NUMERIC,2) AS query_understanding,
                       ROUND(AVG(score_response_accuracy)::NUMERIC,2) AS response_accuracy,
                       ROUND(AVG(score_communication_quality)::NUMERIC,2) AS communication_quality,
                       ROUND(AVG(score_compliance)::NUMERIC,2) AS compliance,
                       ROUND(AVG(score_personalisation)::NUMERIC,2) AS personalisation,
                       ROUND(AVG(score_empathy_soft_skills)::NUMERIC,2) AS empathy_soft_skills,
                       ROUND(AVG(score_resolution)::NUMERIC,2) AS resolution,
                       ROUND(AVG(score_system_behaviour)::NUMERIC,2) AS system_behaviour,
                       ROUND(AVG(score_closing_interaction)::NUMERIC,2) AS closing_interaction
                FROM analyzed_calls""")
            param_avgs = _row_to_dict(cur.fetchone())
            cur.execute("""SELECT flag_val, COUNT(*) AS cnt FROM analyzed_calls,
                           jsonb_array_elements_text(flags) AS flag_val GROUP BY flag_val ORDER BY cnt DESC""")
            flags_breakdown = {r["flag_val"]: r["cnt"] for r in cur.fetchall()}
            cur.execute("""
                SELECT SUM(CASE WHEN weighted_score>=85 THEN 1 ELSE 0 END) AS excellent,
                       SUM(CASE WHEN weighted_score>=70 AND weighted_score<85 THEN 1 ELSE 0 END) AS good,
                       SUM(CASE WHEN weighted_score>=55 AND weighted_score<70 THEN 1 ELSE 0 END) AS average,
                       SUM(CASE WHEN weighted_score<55  THEN 1 ELSE 0 END) AS poor FROM analyzed_calls""")
            score_dist = _row_to_dict(cur.fetchone())
            cur.execute("""SELECT TO_CHAR(processed_at AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD') AS day,
                           COUNT(*) AS cnt FROM analyzed_calls GROUP BY day ORDER BY day""")
            daily = {r["day"]: r["cnt"] for r in cur.fetchall()}
            cur.execute("SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status")
            job_summary_raw = {r["status"]: r["cnt"] for r in cur.fetchall()}
            cur.execute("SELECT COUNT(*) AS cnt FROM jobs")
            total_jobs = cur.fetchone()["cnt"]
        pass_count = int(agg.get("pass_count") or 0)
        total_int  = int(total)
        return {
            "total_calls": total_int,
            "avg_score": float(agg.get("avg_score") or 0),
            "pass_rate": round(pass_count / total_int * 100, 1) if total_int else 0,
            "fail_count": int(agg.get("fail_count") or 0),
            "fatal_count": int(agg.get("fatal_count") or 0),
            "flagged_count": int(agg.get("flagged_count") or 0),
            "severities": severities, "categories": categories, "sentiments": sentiments,
            "flags_breakdown": flags_breakdown,
            "avg_parameter_scores": {k: float(v or 0) for k, v in param_avgs.items()},
            "product_breakdown": products, "avg_product_confidence": avg_prod_conf,
            "score_distribution": {k: int(v or 0) for k, v in score_dist.items()},
            "daily_volume": daily,
            "jobs_summary": {
                "total": int(total_jobs),
                "completed":  int(job_summary_raw.get("completed", 0)),
                "processing": int(job_summary_raw.get("processing", 0)),
                "queued":     int(job_summary_raw.get("queued", 0)),
            },
        }
    else:
        # Pure Python aggregation over JSON files
        calls = list(_anal_read().values())
        total_int = len(calls)
        if total_int == 0:
            return {"total_calls": 0, "message": "No calls processed yet"}

        from collections import Counter, defaultdict
        severities: Dict[str, int] = Counter()
        categories: Dict[str, int] = Counter()
        sentiments: Dict[str, int] = Counter()
        products:   Dict[str, int] = Counter()
        prod_conf_acc: Dict[str, List[float]] = defaultdict(list)
        flags_breakdown: Dict[str, int] = Counter()
        param_sums: Dict[str, float] = defaultdict(float)
        param_counts: Dict[str, int] = defaultdict(int)
        score_dist = {"excellent": 0, "good": 0, "average": 0, "poor": 0}
        daily: Dict[str, int] = Counter()
        pass_count = fatal_count = flagged_count = fail_count = 0
        scores_sum = 0.0

        _SCORE_PARAMS = [
            "greeting_opening", "query_understanding", "response_accuracy",
            "communication_quality", "compliance", "personalisation",
            "empathy_soft_skills", "resolution", "system_behaviour", "closing_interaction",
        ]

        for c in calls:
            a = c.get("analysis") or {}
            ws = float(a.get("weighted_score") or c.get("weighted_score") or 0)
            scores_sum += ws
            pf = a.get("pass_fail", c.get("pass_fail", "FAIL"))
            if pf == "PASS": pass_count += 1
            else: fail_count += 1
            if c.get("fatal"): fatal_count += 1
            if c.get("flagged"): flagged_count += 1

            sev = a.get("severity", c.get("severity", "normal")) or "normal"
            cat = a.get("category", c.get("category", "")) or ""
            sent = a.get("sentiment", c.get("sentiment", "neutral")) or "neutral"
            severities[sev] += 1
            categories[cat] += 1
            sentiments[sent] += 1

            prod = a.get("product_mentioned", "") or ""
            if prod:
                products[prod] += 1
                pc = a.get("product_confidence")
                if pc is not None:
                    prod_conf_acc[prod].append(float(pc))

            for f in (a.get("flags") or []):
                flags_breakdown[str(f)] += 1

            raw_scores = a.get("scores") or {}
            for p in _SCORE_PARAMS:
                v = raw_scores.get(p)
                if v is not None:
                    param_sums[p] += float(v)
                    param_counts[p] += 1

            if ws >= 85: score_dist["excellent"] += 1
            elif ws >= 70: score_dist["good"] += 1
            elif ws >= 55: score_dist["average"] += 1
            else: score_dist["poor"] += 1

            ts = c.get("processed_at", "")
            if ts:
                try: daily[ts[:10]] += 1
                except Exception: pass

        jobs_data = list(_jobs_read().values())
        job_status = Counter(j.get("status", "") for j in jobs_data)

        return {
            "total_calls": total_int,
            "avg_score": round(scores_sum / total_int, 2) if total_int else 0,
            "pass_rate": round(pass_count / total_int * 100, 1) if total_int else 0,
            "fail_count": fail_count,
            "fatal_count": fatal_count,
            "flagged_count": flagged_count,
            "severities": dict(severities),
            "categories": dict(categories),
            "sentiments": dict(sentiments),
            "flags_breakdown": dict(flags_breakdown),
            "avg_parameter_scores": {
                p: round(param_sums[p] / param_counts[p], 2) if param_counts[p] else 0
                for p in _SCORE_PARAMS
            },
            "product_breakdown": dict(products),
            "avg_product_confidence": {k: round(sum(v) / len(v), 3) for k, v in prod_conf_acc.items()},
            "score_distribution": score_dist,
            "daily_volume": dict(sorted(daily.items())),
            "jobs_summary": {
                "total": len(jobs_data),
                "completed":  int(job_status.get("completed", 0)),
                "processing": int(job_status.get("processing", 0)),
                "queued":     int(job_status.get("queued", 0)),
            },
        }


def clear_all_data() -> None:
    if DATABASE_URL:
        with db_cursor() as cur:
            cur.execute("TRUNCATE analyzed_calls, raw_calls, jobs RESTART IDENTITY CASCADE")
    else:
        with _FILE_LOCK:
            _jobs_write({})
            _raw_write({})
            _anal_write({})
