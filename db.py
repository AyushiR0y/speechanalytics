"""
db.py — PostgreSQL persistence layer for Speech Analytics
Replaces the JSON-file load_db / save_db pattern entirely.

Install:  pip install psycopg2-binary
Env var:  DATABASE_URL=postgresql://user:pass@172.20.99.212:5432/humanoidshield
"""

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor

log = logging.getLogger("speech_analytics.db")

# ── Connection ────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")


def get_conn():
    """Return a new psycopg2 connection. Caller must close it."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


@contextmanager
def db_cursor(commit: bool = True):
    """Context manager that yields a cursor and auto-commits / rolls back."""
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


# ── JSONB serialisation helpers ───────────────────────────────────────────────

def _j(obj: Any) -> str:
    """Serialise to JSON string for psycopg2 / JSONB columns."""
    return json.dumps(obj, default=str)


def _row_to_dict(row) -> Dict:
    """Convert a RealDictRow to a plain dict, deserialising JSONB strings."""
    if row is None:
        return {}
    d = dict(row)
    # psycopg2 already deserialises JSONB → Python object; nothing extra needed
    return d


# ══════════════════════════════════════════════════════════════════════════════
# JOBS
# ══════════════════════════════════════════════════════════════════════════════

def create_job(job: Dict) -> None:
    """Insert a new job row."""
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (id, status, files, total, processed,
                              fatal_count, flag_count, created_at, completed_at)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                job["id"],
                job.get("status", "queued"),
                _j(job.get("files", [])),
                job.get("total", 0),
                job.get("processed", 0),
                job.get("fatal_count", 0),
                job.get("flag_count", 0),
                job.get("created_at") or datetime.now().isoformat(),
                job.get("completed_at"),
            ),
        )


def update_job(job_id: str, **kwargs) -> None:
    """Partial update of a job row. Pass column=value keyword args."""
    if not kwargs:
        return
    set_clauses = []
    values = []
    for col, val in kwargs.items():
        if col == "files":
            set_clauses.append(f"{col} = %s::jsonb")
            values.append(_j(val))
        else:
            set_clauses.append(f"{col} = %s")
            values.append(val)
    values.append(job_id)
    sql = f"UPDATE jobs SET {', '.join(set_clauses)} WHERE id = %s"
    with db_cursor() as cur:
        cur.execute(sql, values)


def get_job(job_id: str) -> Optional[Dict]:
    with db_cursor(commit=False) as cur:
        cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    return _row_to_dict(row) if row else None


def list_jobs() -> List[Dict]:
    with db_cursor(commit=False) as cur:
        cur.execute("SELECT * FROM jobs ORDER BY created_at DESC")
        rows = cur.fetchall()
    return [_row_to_dict(r) for r in rows]


def increment_job_counters(job_id: str, processed: int = 0,
                           fatal: int = 0, flagged: int = 0) -> None:
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


# ══════════════════════════════════════════════════════════════════════════════
# RAW CALLS  (pre-analysis)
# ══════════════════════════════════════════════════════════════════════════════

def insert_raw_call(call_id: str, job_id: str, item: Dict) -> None:
    """Store a transcript before GPT analysis runs."""
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_calls (id, job_id, name, sl, source_file,
                                   raw_text, turns, meta, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            (
                call_id,
                job_id,
                item.get("name", ""),
                str(item.get("sl", "")),
                item.get("source_file", ""),
                (item.get("text") or "")[:50000],
                _j(item.get("turns", [])),
                _j(item.get("meta", {})),
            ),
        )


def get_raw_call(call_id: str) -> Optional[Dict]:
    with db_cursor(commit=False) as cur:
        cur.execute("SELECT * FROM raw_calls WHERE id = %s", (call_id,))
        row = cur.fetchone()
    return _row_to_dict(row) if row else None


# ══════════════════════════════════════════════════════════════════════════════
# ANALYZED CALLS  (post-analysis — this is what the dashboard reads)
# ══════════════════════════════════════════════════════════════════════════════

def upsert_analyzed_call(call_record: Dict) -> None:
    """
    Insert or replace a fully-analysed call record.
    `call_record` is the dict that was previously appended to db["calls"].
    """
    a = call_record.get("analysis", {})
    scores = a.get("scores", {})

    # Derive booleans server-side — never trust what came from the model
    is_fatal   = (
        call_record.get("fatal", False)
        or a.get("severity") in {"fatal", "critical"}
    )
    is_flagged = (
        call_record.get("flagged", False)
        or len(a.get("flags", [])) > 0
    )

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
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s::jsonb,%s::jsonb,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s::jsonb,%s,%s::jsonb,
                %s,%s,
                %s::jsonb,%s::jsonb,%s,%s
            )
            ON CONFLICT (id) DO UPDATE SET
                weighted_score          = EXCLUDED.weighted_score,
                pass_fail               = EXCLUDED.pass_fail,
                severity                = EXCLUDED.severity,
                fatal                   = EXCLUDED.fatal,
                flagged                 = EXCLUDED.flagged,
                fatal_reason            = EXCLUDED.fatal_reason,
                category                = EXCLUDED.category,
                sentiment               = EXCLUDED.sentiment,
                flags                   = EXCLUDED.flags,
                failed_parameters       = EXCLUDED.failed_parameters,
                score_greeting_opening      = EXCLUDED.score_greeting_opening,
                score_query_understanding   = EXCLUDED.score_query_understanding,
                score_response_accuracy     = EXCLUDED.score_response_accuracy,
                score_communication_quality = EXCLUDED.score_communication_quality,
                score_compliance            = EXCLUDED.score_compliance,
                score_personalisation       = EXCLUDED.score_personalisation,
                score_empathy_soft_skills   = EXCLUDED.score_empathy_soft_skills,
                score_resolution            = EXCLUDED.score_resolution,
                score_system_behaviour      = EXCLUDED.score_system_behaviour,
                score_closing_interaction   = EXCLUDED.score_closing_interaction,
                product_mentioned       = EXCLUDED.product_mentioned,
                products_mentioned      = EXCLUDED.products_mentioned,
                product_confidence      = EXCLUDED.product_confidence,
                product_signals         = EXCLUDED.product_signals,
                product_accuracy_score  = EXCLUDED.product_accuracy_score,
                product_issues          = EXCLUDED.product_issues,
                analysis                = EXCLUDED.analysis,
                transcript              = EXCLUDED.transcript,
                raw_text                = EXCLUDED.raw_text,
                processed_at            = EXCLUDED.processed_at
            """,
            (
                call_record["id"],
                call_record.get("id"),          # raw_call_id == id
                call_record["job_id"],
                call_record.get("name", ""),
                str(call_record.get("sl", "")),
                call_record.get("source_file", ""),
                a.get("weighted_score", 0),
                a.get("pass_fail", "FAIL"),
                a.get("severity", "normal"),
                is_fatal,
                is_flagged,
                a.get("fatal_reason", ""),
                a.get("category", ""),
                a.get("sentiment", "neutral"),
                _j(a.get("flags", [])),
                _j(a.get("failed_parameters", [])),
                scores.get("greeting_opening"),
                scores.get("query_understanding"),
                scores.get("response_accuracy"),
                scores.get("communication_quality"),
                scores.get("compliance"),
                scores.get("personalisation"),
                scores.get("empathy_soft_skills"),
                scores.get("resolution"),
                scores.get("system_behaviour"),
                scores.get("closing_interaction"),
                a.get("product_mentioned", ""),
                _j(a.get("products_mentioned", [])),
                a.get("product_confidence"),
                _j(a.get("product_signals", [])),
                a.get("product_accuracy_score"),
                a.get("product_issues", ""),
                _j(a),
                _j(call_record.get("transcript", [])),
                (call_record.get("raw_text") or "")[:50000],
                call_record.get("processed_at") or datetime.now().isoformat(),
            ),
        )


def get_analyzed_call(call_id: str) -> Optional[Dict]:
    with db_cursor(commit=False) as cur:
        cur.execute("SELECT * FROM analyzed_calls WHERE id = %s", (call_id,))
        row = cur.fetchone()
    if not row:
        return None
    d = _row_to_dict(row)
    # analysis column is already a dict (psycopg2 deserialises JSONB)
    return d


def delete_analyzed_call(call_id: str) -> bool:
    with db_cursor() as cur:
        cur.execute("DELETE FROM analyzed_calls WHERE id = %s RETURNING id", (call_id,))
        return cur.fetchone() is not None


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
    """
    Paginated, filtered query — replaces the Python-side filter loop
    that previously ran over db["calls"].
    """
    allowed_sort = {
        "processed_at", "weighted_score", "severity", "category",
        "sentiment", "pass_fail", "name",
    }
    if sort_by not in allowed_sort:
        sort_by = "processed_at"
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

    where = []
    params: List[Any] = []

    if severity:
        where.append("severity = %s")
        params.append(severity)
    if category:
        where.append("category = %s")
        params.append(category)
    if sentiment:
        where.append("sentiment = %s")
        params.append(sentiment)
    if pass_fail:
        where.append("pass_fail = %s")
        params.append(pass_fail)
    if flagged is not None:
        where.append("flagged = %s")
        params.append(flagged)
    if job_id:
        where.append("job_id = %s")
        params.append(job_id)
    if search:
        where.append(
            "to_tsvector('english', COALESCE(name,'') || ' ' || COALESCE(raw_text,'')) "
            "@@ plainto_tsquery('english', %s)"
        )
        params.append(search)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"SELECT COUNT(*) AS total FROM analyzed_calls {where_sql}"
    data_sql = f"""
        SELECT id, job_id, name, sl, source_file, category, severity,
               weighted_score, pass_fail, sentiment, flags, failed_parameters,
               product_mentioned, products_mentioned, product_confidence,
               product_signals, fatal, flagged, processed_at,
               analysis->>'score_reason'      AS score_reason,
               analysis->>'summary'           AS summary,
               analysis->'turn_count'         AS turn_count,
               analysis->'estimated_duration_minutes' AS estimated_duration_minutes
        FROM analyzed_calls
        {where_sql}
        ORDER BY {sort_by} {direction}
        LIMIT %s OFFSET %s
    """

    offset = (page - 1) * page_size

    with db_cursor(commit=False) as cur:
        cur.execute(count_sql, params)
        total = cur.fetchone()["total"]
        cur.execute(data_sql, params + [page_size, offset])
        rows = cur.fetchall()

    calls = [_row_to_dict(r) for r in rows]
    return {"calls": calls, "total": total, "page": page, "page_size": page_size}


def get_fatal_calls() -> List[Dict]:
    with db_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT id, name, severity, fatal_reason, flags,
                   weighted_score, category, processed_at
            FROM analyzed_calls
            WHERE fatal = TRUE OR severity IN ('fatal','critical')
            ORDER BY processed_at DESC
            """,
        )
        rows = cur.fetchall()
    return [_row_to_dict(r) for r in rows]


def get_dashboard_stats() -> Dict:
    """
    Single query dashboard aggregation — replaces the Python loop
    that iterated over all calls in memory.
    """
    with db_cursor(commit=False) as cur:

        cur.execute("SELECT COUNT(*) AS total FROM analyzed_calls")
        total = cur.fetchone()["total"]
        if total == 0:
            return {"total": 0, "message": "No calls processed yet"}

        cur.execute("""
            SELECT
                ROUND(AVG(weighted_score)::NUMERIC, 2)           AS avg_score,
                SUM(CASE WHEN pass_fail='PASS' THEN 1 ELSE 0 END) AS pass_count,
                SUM(CASE WHEN pass_fail='FAIL' THEN 1 ELSE 0 END) AS fail_count,
                SUM(CASE WHEN fatal=TRUE  THEN 1 ELSE 0 END)      AS fatal_count,
                SUM(CASE WHEN flagged=TRUE THEN 1 ELSE 0 END)     AS flagged_count
            FROM analyzed_calls
        """)
        agg = _row_to_dict(cur.fetchone())

        cur.execute("""
            SELECT severity, COUNT(*) AS cnt
            FROM analyzed_calls GROUP BY severity
        """)
        severities = {r["severity"]: r["cnt"] for r in cur.fetchall()}

        cur.execute("""
            SELECT category, COUNT(*) AS cnt
            FROM analyzed_calls GROUP BY category ORDER BY cnt DESC
        """)
        categories = {r["category"]: r["cnt"] for r in cur.fetchall()}

        cur.execute("""
            SELECT sentiment, COUNT(*) AS cnt
            FROM analyzed_calls GROUP BY sentiment
        """)
        sentiments = {r["sentiment"]: r["cnt"] for r in cur.fetchall()}

        cur.execute("""
            SELECT product_mentioned, COUNT(*) AS cnt
            FROM analyzed_calls
            WHERE product_mentioned IS NOT NULL AND product_mentioned != ''
            GROUP BY product_mentioned ORDER BY cnt DESC
        """)
        products = {r["product_mentioned"]: r["cnt"] for r in cur.fetchall()}

        cur.execute("""
            SELECT product_mentioned,
                   ROUND(AVG(product_confidence)::NUMERIC, 3) AS avg_conf
            FROM analyzed_calls
            WHERE product_confidence IS NOT NULL
            GROUP BY product_mentioned
        """)
        avg_prod_conf = {r["product_mentioned"]: float(r["avg_conf"] or 0) for r in cur.fetchall()}

        # Parameter averages
        cur.execute("""
            SELECT
                ROUND(AVG(score_greeting_opening)::NUMERIC,2)      AS greeting_opening,
                ROUND(AVG(score_query_understanding)::NUMERIC,2)   AS query_understanding,
                ROUND(AVG(score_response_accuracy)::NUMERIC,2)     AS response_accuracy,
                ROUND(AVG(score_communication_quality)::NUMERIC,2) AS communication_quality,
                ROUND(AVG(score_compliance)::NUMERIC,2)            AS compliance,
                ROUND(AVG(score_personalisation)::NUMERIC,2)       AS personalisation,
                ROUND(AVG(score_empathy_soft_skills)::NUMERIC,2)   AS empathy_soft_skills,
                ROUND(AVG(score_resolution)::NUMERIC,2)            AS resolution,
                ROUND(AVG(score_system_behaviour)::NUMERIC,2)      AS system_behaviour,
                ROUND(AVG(score_closing_interaction)::NUMERIC,2)   AS closing_interaction
            FROM analyzed_calls
        """)
        param_avgs = _row_to_dict(cur.fetchone())

        # Flag breakdown — unnest the JSONB array
        cur.execute("""
            SELECT flag_val, COUNT(*) AS cnt
            FROM analyzed_calls,
                 jsonb_array_elements_text(flags) AS flag_val
            GROUP BY flag_val
            ORDER BY cnt DESC
        """)
        flags_breakdown = {r["flag_val"]: r["cnt"] for r in cur.fetchall()}

        # Score distribution
        cur.execute("""
            SELECT
                SUM(CASE WHEN weighted_score >= 85 THEN 1 ELSE 0 END) AS excellent,
                SUM(CASE WHEN weighted_score >= 70 AND weighted_score < 85 THEN 1 ELSE 0 END) AS good,
                SUM(CASE WHEN weighted_score >= 55 AND weighted_score < 70 THEN 1 ELSE 0 END) AS average,
                SUM(CASE WHEN weighted_score < 55 THEN 1 ELSE 0 END) AS poor
            FROM analyzed_calls
        """)
        score_dist = _row_to_dict(cur.fetchone())

        # Daily volume
        cur.execute("""
            SELECT TO_CHAR(processed_at AT TIME ZONE 'Asia/Kolkata', 'YYYY-MM-DD') AS day,
                   COUNT(*) AS cnt
            FROM analyzed_calls
            GROUP BY day ORDER BY day
        """)
        daily = {r["day"]: r["cnt"] for r in cur.fetchall()}

        # Jobs summary
        cur.execute("""
            SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status
        """)
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
        "severities": severities,
        "categories": categories,
        "sentiments": sentiments,
        "flags_breakdown": flags_breakdown,
        "avg_parameter_scores": {k: float(v or 0) for k, v in param_avgs.items()},
        "product_breakdown": products,
        "avg_product_confidence": avg_prod_conf,
        "score_distribution": {k: int(v or 0) for k, v in score_dist.items()},
        "daily_volume": daily,
        "jobs_summary": {
            "total": int(total_jobs),
            "completed":  int(job_summary_raw.get("completed", 0)),
            "processing": int(job_summary_raw.get("processing", 0)),
            "queued":     int(job_summary_raw.get("queued", 0)),
        },
    }


def clear_all_data() -> None:
    """Truncate all tables (respects FK order)."""
    with db_cursor() as cur:
        cur.execute("TRUNCATE analyzed_calls, raw_calls, jobs RESTART IDENTITY CASCADE")
