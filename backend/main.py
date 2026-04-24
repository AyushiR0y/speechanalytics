"""
Bajaj Life Insurance – Speech Analytics Platform
FastAPI Backend
"""

import os, json, uuid, re, asyncio, hashlib, shutil, io
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import logging

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
import openpyxl
import numpy as np
from pypdf import PdfReader
from docx import Document as DocxDocument

import openai
from dotenv import load_dotenv

try:
    import faiss
except Exception:  # pragma: no cover - optional dependency
    faiss = None

# ── Config ──────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"
PRODUCT_DIR= BASE_DIR / "products"
PROC_DIR   = BASE_DIR / "processed"
DB_FILE    = PROC_DIR / "calls_db.json"
CHROMA_DIR = BASE_DIR / "chroma_db"
PRODUCT_INDEX_FILE = PROC_DIR / "product_specs_index.json"
RAG_INDEX_FILE = PROC_DIR / "product_faiss.index"
RAG_META_FILE = PROC_DIR / "product_faiss_meta.json"
RAG_BACKEND_FILE = PROC_DIR / "rag_backend.json"
RAG_EMBED_MODEL = os.environ.get("RAG_EMBED_MODEL", "all-MiniLM-L6-v2").strip()
PRODUCT_SOURCES = [PRODUCT_DIR]

for d in [UPLOAD_DIR, PRODUCT_DIR, PROC_DIR, CHROMA_DIR]:
    d.mkdir(parents=True, exist_ok=True)

load_dotenv(BASE_DIR / ".env")

def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("speech_analytics")

# OpenAI client (supports OpenAI and Azure OpenAI)
if _env("AZURE_OPENAI_API_KEY") and _env("AZURE_OPENAI_ENDPOINT"):
    OPENAI_MODEL = _env("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o")
    openai_client = openai.AzureOpenAI(
        api_key=_env("AZURE_OPENAI_API_KEY"),
        api_version=_env("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        azure_endpoint=_env("AZURE_OPENAI_ENDPOINT")
    )
else:
    OPENAI_MODEL = _env("OPENAI_MODEL", "gpt-4o")
    openai_client = openai.OpenAI(api_key=_env("OPENAI_API_KEY", "YOUR_OPENAI_API_KEY"))

# ── FastAPI App ──────────────────────────────────────────────────────────────
app = FastAPI(title="Bajaj Life Insurance – Speech Analytics", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Serve frontend
FRONTEND_DIR = BASE_DIR / "frontend"
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR / "static")), name="static")

# ── RAG Setup ────────────────────────────────────────────────────────────────
rag_collection = None

def init_rag():
    """Initialize the local product knowledge base."""
    global rag_collection
    rag_collection = None
    try:
        rebuild_product_rag_index()
        log.info("Product RAG index initialized")
    except Exception as e:
        log.warning(f"RAG init failed (non-fatal): {e}")

@app.on_event("startup")
async def startup():
    init_rag()
    if not DB_FILE.exists():
        DB_FILE.write_text(json.dumps({"calls": [], "jobs": []}, indent=2))
    if not PRODUCT_INDEX_FILE.exists():
        PRODUCT_INDEX_FILE.write_text(json.dumps({"products": []}, indent=2))

# ── Database helpers ─────────────────────────────────────────────────────────
def load_db() -> dict:
    try:
        return json.loads(DB_FILE.read_text())
    except Exception:
        return {"calls": [], "jobs": []}

def save_db(db: dict):
    DB_FILE.write_text(json.dumps(db, indent=2, default=str))

def load_product_index() -> dict:
    try:
        return json.loads(PRODUCT_INDEX_FILE.read_text())
    except Exception:
        return {"products": []}

def save_product_index(index_data: dict):
    PRODUCT_INDEX_FILE.write_text(json.dumps(index_data, indent=2, default=str))


def _load_embedder():
    if hasattr(_load_embedder, "_model"):
        return _load_embedder._model
    try:
        from sentence_transformers import SentenceTransformer
        _load_embedder._model = SentenceTransformer(RAG_EMBED_MODEL)
    except Exception:
        _load_embedder._model = None
    return _load_embedder._model


def _hash_vector(text: str, dim: int = 384) -> np.ndarray:
    vec = np.zeros(dim, dtype=np.float32)
    tokens = re.findall(r"[a-z0-9]+", (text or "").lower())
    for token in tokens:
        index = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16) % dim
        vec[index] += 1.0
    norm = float(np.linalg.norm(vec))
    if norm > 0:
        vec /= norm
    return vec


def _embed_texts(texts: List[str]) -> np.ndarray:
    model = _load_embedder()
    if model is not None:
        return model.encode(texts, convert_to_numpy=True, normalize_embeddings=True).astype(np.float32)
    return np.vstack([_hash_vector(text) for text in texts]).astype(np.float32)


def _safe_filename_label(name: str) -> str:
    stem = Path(name).stem
    stem = re.sub(r"(?i)product\s*circular", "", stem)
    stem = re.sub(r"[_\-]+", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    stem = re.sub(r"\b(V\d+|v\d+)\b", "", stem).strip()
    stem = re.sub(r"(?i)\bbajaj\s+allianz\s+life\b", "Bajaj Life", stem).strip()
    stem = re.sub(r"\s{2,}", " ", stem).strip(" -_")
    return stem or Path(name).stem


PARAM_ORDER = [
    "greeting_opening", "query_understanding", "response_accuracy", "communication_quality",
    "compliance", "personalisation", "empathy_soft_skills", "resolution",
    "system_behaviour", "closing_interaction"
]


def _fallback_param_comments(scores: Dict[str, Any]) -> List[str]:
    text = {
        "greeting_opening": "Opening tone and welcome quality from the first bot turn.",
        "query_understanding": "How accurately the bot interpreted customer intent and follow-up questions.",
        "response_accuracy": "Factual correctness of product/policy information provided by the bot; wrong product facts are fatal.",
        "communication_quality": "Clarity, structure, and readability of the bot's language.",
        "compliance": "Compliance with privacy, OTP, regulatory constraints, and staying on-product for the question asked.",
        "personalisation": "Use of customer context, policy context, and personalization cues.",
        "empathy_soft_skills": "Warmth, reassurance, and acknowledgement of customer concerns.",
        "resolution": "Whether the customer issue was actually solved or moved forward.",
        "system_behaviour": "Flow quality: no loops, no instability, and no repetitive failures.",
        "closing_interaction": "Quality and completeness of the call closure.",
    }
    out = []
    for key in PARAM_ORDER:
        score = int(scores.get(key, 3) or 3)
        suffix = "Strong evidence." if score >= 4 else ("Acceptable but with gaps." if score == 3 else "Needs corrective action.")
        out.append(f"{text[key]} {suffix}")
    return out


def _score_reason(analysis: Dict[str, Any]) -> str:
    summary = (analysis.get("summary") or "").strip()
    failed = analysis.get("failed_parameters") or []
    if failed:
        return f"{summary} Failed parameters: {', '.join(failed)}.".strip()
    return summary or "Weighted score computed from parameter scores and policy thresholds."


def _refine_sentiment(turns: List[Dict], model_sentiment: str = "neutral") -> str:
    customer_text = " ".join(t.get("text", "") for t in turns if t.get("speaker") in {"customer", "user"}).lower()
    bot_text = " ".join(t.get("text", "") for t in turns if t.get("speaker") in {"bot", "agent", "system"}).lower()

    distress_words = ["urgent", "panic", "scared", "emergency", "distress", "anxious"]
    angry_words = ["angry", "worst", "terrible", "frustrated", "complaint", "unacceptable", "ridiculous"]
    frustrated_words = ["not working", "again", "still", "issue", "problem", "unable", "stuck"]
    positive_words = ["thanks", "thank you", "great", "helpful", "resolved", "good"]
    empathy_words = ["sorry", "understand", "help", "glad", "assist", "certainly"]

    def has_any(text: str, words: List[str]) -> bool:
        return any(word in text for word in words)

    if has_any(customer_text, distress_words):
        return "distressed"
    if has_any(customer_text, angry_words):
        return "angry"
    if has_any(customer_text, frustrated_words):
        if not has_any(bot_text, empathy_words):
            return "frustrated"
        return model_sentiment if model_sentiment in {"positive", "neutral"} else "frustrated"
    if has_any(customer_text, positive_words):
        return "positive"
    return model_sentiment if model_sentiment in {"positive", "neutral", "frustrated", "angry", "distressed"} else "neutral"


def _annotate_transcript(turns: List[Dict], analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
    annotations = []
    for idx, turn in enumerate(turns, start=1):
        speaker = turn.get("speaker", "unknown")
        text = (turn.get("text") or "").strip()
        tags = []

        if speaker in {"bot", "agent", "system"} and re.search(r"grace period|30 day", text, re.I):
            tags.append("Grace Period Explained")

        if speaker in {"bot", "agent", "system"} and re.search(r"transfer|connect you to|handover|escalate", text, re.I):
            tags.append("Escalation Action")

        if re.search(r"thank you|thanks|happy to help", text, re.I):
            tags.append("Closure Cue")

        annotations.append({
            "sl": turn.get("sl", idx),
            "speaker": speaker,
            "text": text,
            "tags": tags,
        })
    return annotations


def _build_qa_findings(analysis: Dict[str, Any]) -> List[Dict[str, str]]:
    findings = []
    for fp in analysis.get("failed_parameters", [])[:6]:
        findings.append({"type": "negative", "text": f"Parameter below threshold: {fp.replace('_', ' ')}"})
    for flag in analysis.get("flags", [])[:6]:
        findings.append({"type": "warning", "text": f"Flag detected: {flag.replace('_', ' ')}"})
    issues = (analysis.get("product_issues") or "").strip()
    if issues and issues.lower() != "none":
        findings.append({"type": "warning", "text": f"Product check issue: {issues}"})
    strengths = (analysis.get("strengths") or "").strip()
    if strengths and strengths.lower() not in {"none", "n/a"}:
        findings.append({"type": "positive", "text": strengths})
    if not findings:
        findings.append({"type": "positive", "text": "No major QA issues detected in automated analysis."})
    return findings[:8]


def _sentence_split(text: str) -> List[str]:
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", text or "") if part.strip()]


def _normalize_number_token(token: str) -> str:
    token = (token or "").strip().lower().rstrip("%")
    try:
        value = float(token)
        return str(int(value)) if value.is_integer() else f"{value:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return token


def _extract_number_tokens(text: str) -> List[str]:
    return [_normalize_number_token(token) for token in re.findall(r"\d+(?:\.\d+)?%?", text or "")]


def _bot_sentences(turns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sentences: List[Dict[str, Any]] = []
    for turn in turns or []:
        speaker = (turn.get("speaker") or "").lower()
        if speaker not in {"bot", "agent", "system"}:
            continue
        for sentence in _sentence_split(turn.get("text", "")):
            sentences.append({"sl": turn.get("sl"), "speaker": speaker, "text": sentence})
    return sentences


def _best_spec_sentence(statement: str, analysis: Dict[str, Any], keywords: List[str]) -> str:
    candidates: List[tuple] = []
    search_rows = list(analysis.get("product_evidence") or [])
    profile = analysis.get("product_profile") or {}
    search_texts = [row.get("text", "") for row in search_rows if row.get("text")]
    search_texts.extend(profile.get("evidence_snippets") or [])
    if profile.get("summary"):
        search_texts.append(profile.get("summary"))

    stmt_tokens = set(_tokenize(statement))
    for text in search_texts:
        for sentence in _sentence_split(text):
            sentence_lower = sentence.lower()
            overlap = len(stmt_tokens.intersection(_tokenize(sentence)))
            keyword_hits = sum(1 for keyword in keywords if keyword in sentence_lower)
            score = overlap + (2 * keyword_hits)
            if score > 0:
                candidates.append((score, sentence.strip()))

    if not candidates:
        return (profile.get("summary") or "").strip()[:260]

    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _classify_product_check(statement: str, fact: str, keywords: List[str]) -> tuple:
    stmt_lower = statement.lower()
    fact_lower = fact.lower()
    stmt_nums = _extract_number_tokens(statement)
    fact_nums = _extract_number_tokens(fact)

    if any(phrase in stmt_lower for phrase in ["unable to provide information", "cannot provide information", "can't provide information", "not able to provide information"]):
        return "risk", "⚠️ Medium — evasive response on a product or policy question"

    if stmt_nums:
        if fact_nums and not set(stmt_nums).intersection(fact_nums):
            return "fail", "🚨 HIGH — numeric detail conflicts with the product specification"
        if not fact_nums:
            return "risk", "⚠️ Medium — numeric detail is not explicit in the supporting product sentence"

    if any(word in stmt_lower for word in ["guaranteed", "only", "always", "never", "free", "must"]) and not any(keyword in fact_lower for keyword in keywords):
        return "risk", "⚠️ Medium — categorical claim needs direct product-spec support"

    overlap = len(set(_tokenize(statement)).intersection(_tokenize(fact)))
    if overlap < 3:
        return "risk", "⚠️ Medium — supporting product sentence is indirect"

    return "pass", "None"


def _build_product_checks(turns: List[Dict[str, Any]], analysis: Dict[str, Any]) -> List[Dict[str, str]]:
    product_name = str(analysis.get("product_mentioned") or "None").strip()
    if not product_name or product_name == "None":
        return []

    bot_sentences = _bot_sentences(turns)
    topic_map = [
        ("policy_term_eligibility", ["policy term", "entry age", "maturity age", "eligibility", "age at maturity", "sum assured"]),
        ("premium_payment", ["premium", "monthly", "quarterly", "half-yearly", "half yearly", "yearly", "auto-debit", "modal premium"]),
        ("benefits", ["maturity benefit", "death benefit", "guaranteed additions", "sum assured", "payout", "cover"]),
        ("surrender_loan", ["surrender", "loan", "paid-up", "grace period", "revival", "foreclosure"]),
    ]

    checks: List[Dict[str, str]] = []
    for topic_name, keywords in topic_map:
        statement_item = next((item for item in bot_sentences if any(keyword in item["text"].lower() for keyword in keywords)), None)
        if not statement_item:
            continue

        statement = statement_item["text"].strip()
        fact = _best_spec_sentence(statement, analysis, keywords)
        if not fact:
            continue

        verdict, risk = _classify_product_check(statement, fact, keywords)
        checks.append({
            "call": str(analysis.get("call_id", "")),
            "stmt": statement,
            "fact": fact,
            "verdict": verdict,
            "vtext": "✓ ACCURATE" if verdict == "pass" else ("⚠️ CONDITIONALLY OK" if verdict == "risk" else "✗ INACCURATE"),
            "risk": risk,
            "topic": topic_name,
        })

        if len(checks) >= 4:
            break

    return checks


def _apply_qa_policy_rules(analysis: Dict[str, Any], turns: List[Dict[str, Any]]) -> Dict[str, Any]:
    analysis = dict(analysis or {})
    scores = dict(analysis.get("scores") or {})
    flags = list(dict.fromkeys(analysis.get("flags") or []))
    failed_parameters = list(dict.fromkeys(analysis.get("failed_parameters") or []))

    generated_checks = _build_product_checks(turns, analysis)
    existing_checks = list(analysis.get("product_checks") or [])
    merged_checks = []
    seen_pairs = set()
    for check in existing_checks + generated_checks:
        key = (
            (check.get("stmt") or "").strip().lower(),
            (check.get("fact") or "").strip().lower(),
        )
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        merged_checks.append(check)
    analysis["product_checks"] = merged_checks[:6]

    product_checks = analysis.get("product_checks") or []
    hard_fail = any(check.get("verdict") == "fail" for check in product_checks)
    risk_only = any(check.get("verdict") == "risk" for check in product_checks)
    product_query = any(
        any(keyword in (turn.get("text", "").lower()) for keyword in ["policy", "product", "premium", "sum assured", "maturity", "surrender", "loan", "coverage", "term"])
        for turn in turns
    )
    unable_response = any(
        phrase in (turn.get("text", "").lower())
        for turn in turns
        if (turn.get("speaker") or "").lower() in {"bot", "agent", "system"}
        for phrase in ["unable to provide information", "cannot provide information", "can't provide information", "not able to provide information"]
    )

    if hard_fail:
        analysis["severity"] = "fatal"
        analysis["fatal_reason"] = analysis.get("fatal_reason") or next(
            (check.get("risk", "").replace("⚠️ Medium — ", "").replace("🚨 HIGH — ", "") for check in product_checks if check.get("verdict") == "fail"),
            "Incorrect product or policy information was provided."
        )
        analysis["product_accuracy_score"] = 0
        analysis["product_issues"] = "; ".join(
            f"{check.get('stmt', '')} -> {check.get('fact', '')}"
            for check in product_checks if check.get("verdict") == "fail"
        ) or "Incorrect product or policy information was provided."
        flags.extend(["false_information", "compliance_breach", "regulatory_violation"])
        scores["response_accuracy"] = min(int(scores.get("response_accuracy", 5) or 5), 1)
        scores["compliance"] = min(int(scores.get("compliance", 5) or 5), 1)
        if "response_accuracy" not in failed_parameters:
            failed_parameters.append("response_accuracy")
        if "compliance" not in failed_parameters:
            failed_parameters.append("compliance")
        analysis["pass_fail"] = "FAIL"
    elif risk_only:
        current_accuracy = int(scores.get("response_accuracy", 3) or 3)
        scores["response_accuracy"] = min(current_accuracy, 3)
        if not analysis.get("product_issues") or analysis.get("product_issues") in {"None", ""}:
            analysis["product_issues"] = "Unconfirmed product information should be verified against the product specification before answering as fact."
        if analysis.get("product_accuracy_score") in {None, "", 0}:
            analysis["product_accuracy_score"] = 3
        if product_query and "escalation_needed" not in flags:
            flags.append("escalation_needed")

    if unable_response and product_query:
        if int(scores.get("system_behaviour", 3) or 3) > 2:
            scores["system_behaviour"] = 2
        if int(scores.get("compliance", 3) or 3) > 2:
            scores["compliance"] = 2
        if "unresolved_critical" not in flags:
            flags.append("unresolved_critical")
        if "escalation_needed" not in flags:
            flags.append("escalation_needed")

    analysis["scores"] = scores
    analysis["flags"] = flags
    analysis["failed_parameters"] = failed_parameters
    analysis["qa_findings"] = _build_qa_findings({**analysis, "transcript": turns})
    analysis["score_reason"] = _score_reason(analysis)
    if not analysis.get("product_accuracy_score") and product_checks:
        analysis["product_accuracy_score"] = 5 if not risk_only and not hard_fail else 3
    if not analysis.get("product_issues"):
        analysis["product_issues"] = "None"
    if analysis.get("severity") == "fatal" and not analysis.get("fatal_reason"):
        analysis["fatal_reason"] = "Incorrect product or policy information was provided."
    if analysis.get("severity") not in {"fatal", "critical", "watch", "normal"}:
        analysis["severity"] = "watch"
    if analysis.get("pass_fail") not in {"PASS", "FAIL"}:
        analysis["pass_fail"] = "FAIL" if failed_parameters else "PASS"
    return analysis


PRODUCT_FEATURE_KEYWORDS = {
    "premium": ["premium", "installment", "monthly", "quarterly", "half-yearly", "yearly", "single premium"],
    "benefit": ["benefit", "sum assured", "cover", "coverage", "death benefit", "maturity benefit", "payout"],
    "policy_term": ["term", "tenure", "policy term", "duration", "years"],
    "eligibility": ["eligibility", "age", "entry age", "minimum age", "maximum age"],
    "surrender": ["surrender", "paid up", "discontinuance", "withdrawal", "partial withdrawal", "lapse"],
    "claim": ["claim", "nominee", "death claim", "settlement"],
    "grace_period": ["grace period", "grace", "missed premium", "late premium"],
    "loan": ["loan", "policy loan"],
    "bonus": ["bonus", "guaranteed additions", "addition"],
    "fund_value": ["fund value", "nav", "unit", "ulip"],
    "free_look": ["free look", "free-look", "cancellation"],
    "rider": ["rider", "accidental", "critical illness"],
}


def _tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(t) > 2]


def _sentence_snippets(text: str, limit: int = 4) -> List[str]:
    sentences = re.split(r"(?<=[.!?])\s+", text or "")
    snippets = []
    for sentence in sentences:
        normalized = sentence.strip()
        if len(normalized) < 50:
            continue
        if any(keyword in normalized.lower() for keywords in PRODUCT_FEATURE_KEYWORDS.values() for keyword in keywords):
            snippets.append(normalized)
        if len(snippets) >= limit:
            break
    return snippets


def _build_product_catalog(meta_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"chunks": [], "sources": set()})
    for row in meta_rows:
        product = row.get("product") or "Unknown"
        grouped[product]["chunks"].append(row)
        grouped[product]["sources"].add(row.get("source", ""))

    catalog = []
    for product, data in grouped.items():
        corpus = " ".join(chunk.get("text", "") for chunk in data["chunks"])
        tokens = _tokenize(corpus)
        token_counts = Counter(tokens)
        top_terms = [term for term, _ in token_counts.most_common(24)]
        feature_hits = Counter()
        for chunk in data["chunks"]:
            chunk_text = chunk.get("text", "").lower()
            for feature, keywords in PRODUCT_FEATURE_KEYWORDS.items():
                if any(keyword in chunk_text for keyword in keywords):
                    feature_hits[feature] += 1
        snippets = _sentence_snippets(corpus, limit=5)
        catalog.append({
            "product": product,
            "sources": sorted(s for s in data["sources"] if s),
            "chunk_count": len(data["chunks"]),
            "top_terms": top_terms[:12],
            "feature_hits": dict(feature_hits.most_common(8)),
            "summary": snippets[0] if snippets else (corpus[:320] + "..." if len(corpus) > 320 else corpus),
            "evidence_snippets": snippets,
        })

    catalog.sort(key=lambda item: (item["chunk_count"], len(item["top_terms"])), reverse=True)
    return catalog


def _document_text(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".pdf":
        reader = PdfReader(str(path))
        pages = []
        for i, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            if text.strip():
                pages.append(f"[Page {i}]\n{text}")
        return "\n\n".join(pages)
    if ext in {".docx", ".doc"}:
        doc = DocxDocument(str(path))
        return "\n".join(p.text for p in doc.paragraphs if p.text.strip())
    if ext in {".txt"}:
        return path.read_text(errors="replace")
    return ""


def _chunk_text(text: str, chunk_size: int = 280, overlap: int = 60) -> List[str]:
    words = re.findall(r"\S+", text)
    if not words:
        return []
    step = max(1, chunk_size - overlap)
    chunks = []
    for start in range(0, len(words), step):
        chunk = " ".join(words[start:start + chunk_size]).strip()
        if len(chunk) >= 80:
            chunks.append(chunk)
    return chunks


def _collect_product_files() -> List[Path]:
    files = []
    seen = set()
    for source_dir in PRODUCT_SOURCES:
        if not source_dir.exists():
            continue
        for path in sorted(source_dir.glob("*")):
            if path.suffix.lower() not in {".pdf", ".docx", ".doc", ".txt"}:
                continue
            try:
                digest = hashlib.sha1(path.read_bytes()).hexdigest()
            except Exception:
                digest = f"{path.name}:{path.stat().st_size if path.exists() else 0}"
            if digest in seen:
                continue
            seen.add(digest)
            files.append(path)
    return files


def _save_rag_backend(mode: str):
    RAG_BACKEND_FILE.write_text(json.dumps({"mode": mode, "updated_at": datetime.now().isoformat()}, indent=2))


def _load_rag_meta() -> List[Dict[str, Any]]:
    try:
        return json.loads(RAG_META_FILE.read_text())
    except Exception:
        return []


def _load_rag_index():
    if faiss is None or not RAG_INDEX_FILE.exists() or not RAG_META_FILE.exists():
        return None, []
    try:
        index = faiss.read_index(str(RAG_INDEX_FILE))
        meta = _load_rag_meta()
        return index, meta
    except Exception as e:
        log.warning(f"FAISS index load failed: {e}")
        return None, []


def rebuild_product_rag_index() -> dict:
    """Build or rebuild the local product knowledge base."""
    files = _collect_product_files()
    meta_rows: List[Dict[str, Any]] = []

    if not files:
        if RAG_INDEX_FILE.exists():
            RAG_INDEX_FILE.unlink(missing_ok=True)
        RAG_META_FILE.write_text("[]")
        _save_rag_backend("empty")
        return {"mode": "empty", "chunks": 0, "products": 0}

    try:
        model = _load_embedder()
        texts = []
        for path in files:
            doc_text = _document_text(path)
            if not doc_text.strip():
                continue
            chunks = _chunk_text(doc_text)
            product_label = _safe_filename_label(path.name)
            for chunk_idx, chunk in enumerate(chunks):
                texts.append(chunk)
                meta_rows.append({
                    "source": path.name,
                    "path": str(path),
                    "product": product_label,
                    "chunk_index": chunk_idx,
                    "text": chunk,
                    "word_count": len(chunk.split()),
                    "page": None,
                })

        if not texts:
            RAG_META_FILE.write_text("[]")
            _save_rag_backend("empty")
            return {"mode": "empty", "chunks": 0, "products": len(files)}

        vectors = _embed_texts(texts)
        if faiss is not None:
            index = faiss.IndexFlatIP(vectors.shape[1])
            index.add(vectors)
            faiss.write_index(index, str(RAG_INDEX_FILE))
            _save_rag_backend("faiss")
        else:
            RAG_INDEX_FILE.write_text("")
            _save_rag_backend("keyword")

        RAG_META_FILE.write_text(json.dumps(meta_rows, indent=2, default=str))
        return {"mode": "faiss" if faiss is not None else "keyword", "chunks": len(meta_rows), "products": len(files)}
    except Exception as e:
        log.warning(f"Product RAG rebuild failed: {e}")
        RAG_META_FILE.write_text("[]")
        _save_rag_backend("keyword")
        return {"mode": "keyword", "chunks": 0, "products": len(files), "error": str(e)}


def _keyword_rank(query: str, meta_rows: List[Dict[str, Any]], top_k: int = 5) -> List[Dict[str, Any]]:
    terms = [t for t in re.findall(r"[a-z0-9]+", query.lower()) if len(t) > 2]
    if not terms:
        return meta_rows[:top_k]

    scored = []
    for idx, row in enumerate(meta_rows):
        text = row.get("text", "").lower()
        score = sum(2 if term in row.get("product", "").lower() else 1 for term in terms if term in text or term in row.get("product", "").lower())
        if score:
            scored.append((score, idx, row))
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [row for _, _, row in scored[:top_k]]


def search_product_rag(query: str, top_k: int = 5) -> List[Dict[str, Any]]:
    query = (query or "").strip()
    if not query:
        return []

    index, meta_rows = _load_rag_index()
    if faiss is not None and index is not None and meta_rows:
        try:
            query_vec = _embed_texts([query])
            scores, idxs = index.search(query_vec, min(top_k, len(meta_rows)))
            rows = []
            for score, idx in zip(scores[0].tolist(), idxs[0].tolist()):
                if idx < 0 or idx >= len(meta_rows):
                    continue
                row = dict(meta_rows[idx])
                row["score"] = float(score)
                rows.append(row)
            if rows:
                return rows
        except Exception as e:
            log.warning(f"FAISS retrieval failed, using keyword fallback: {e}")

    meta_rows = _load_rag_meta()
    if not meta_rows:
        return []
    ranked = _keyword_rank(query, meta_rows, top_k=top_k)
    for i, row in enumerate(ranked):
        row["score"] = float(max(0.0, 1.0 - i * 0.1))
    return ranked


def infer_product_context(transcript_text: str, top_k: int = 5) -> Dict[str, Any]:
    """Infer the likely product and retrieve supporting chunks from the transcript itself."""
    query = re.sub(r"\s+", " ", transcript_text or "").strip()
    if not query:
        return {"product": "None", "context": "", "chunks": []}

    rows = search_product_rag(query, top_k=max(top_k, 8))
    meta_rows = _load_rag_meta()
    catalog = _build_product_catalog(meta_rows) if meta_rows else []

    if not rows and not catalog:
        return {"product": "None", "context": "", "chunks": []}

    transcript_tokens = set(_tokenize(query))
    product_scores = defaultdict(float)
    product_evidence = defaultdict(list)
    product_matches = defaultdict(set)

    for rank, row in enumerate(rows):
        product = row.get("product", "Unknown")
        score = float(row.get("score", 0.0))
        product_scores[product] += max(score, 0.0) + (0.05 * (len(rows) - rank))
        product_evidence[product].append(row)

    for profile in catalog:
        product = profile["product"]
        top_terms = set(profile.get("top_terms", []))
        overlap = transcript_tokens.intersection(top_terms)
        if overlap:
            product_scores[product] += 0.12 * len(overlap)
            product_matches[product].update(sorted(overlap))

        for feature, keywords in PRODUCT_FEATURE_KEYWORDS.items():
            if any(keyword in query.lower() for keyword in keywords):
                if feature in profile.get("feature_hits", {}):
                    product_scores[product] += 0.35 + 0.05 * float(profile.get("feature_hits", {}).get(feature, 0))
                    product_matches[product].add(feature)

    if not product_scores:
        return {"product": "None", "context": "", "chunks": []}

    best_product, best_score = max(product_scores.items(), key=lambda item: item[1])
    best_profile = next((p for p in catalog if p["product"] == best_product), {})
    supporting_rows = sorted(product_evidence.get(best_product, []), key=lambda row: row.get("score", 0.0), reverse=True)[:top_k]
    context_lines = []
    for row in supporting_rows:
        context_lines.append(
            f"[{row.get('product', 'Unknown')} | {row.get('source', 'spec')} | chunk {row.get('chunk_index', 0)}] {row.get('text', '')}"
        )

    if best_profile:
        context_lines.append(
            f"[PROFILE | {best_profile.get('product', 'Unknown')}] Summary: {best_profile.get('summary', '')}\n"
            f"Feature anchors: {', '.join(sorted(best_profile.get('feature_hits', {}).keys())[:8]) or 'None'}"
        )

    evidence_terms = sorted(product_matches.get(best_product, set()))[:12]
    confidence = round(float(best_score), 3)
    product_confidence = min(0.99, 0.4 + confidence / 4.0)

    return {
        "product": best_product if best_score > 0.15 else "None",
        "confidence": round(product_confidence, 3),
        "matched_terms": evidence_terms,
        "catalog": catalog[:6],
        "evidence": supporting_rows,
        "context": "\n\n".join(context_lines),
        "chunks": supporting_rows,
        "profile": best_profile,
    }


def query_product_rag(product_name: str, question: str) -> str:
    query = " ".join(part for part in [product_name, question] if part).strip()
    if not query:
        return "No product specifications loaded."
    rows = search_product_rag(query, top_k=5)
    if not rows:
        return "No matching product spec found."
    return "\n\n".join(
        f"[{row.get('product', 'Unknown')} | {row.get('source', 'spec')}] {row.get('text', '')}"
        for row in rows[:3]
    )


def remove_product_from_index(filename: str):
    safe_name = Path(filename).name
    target = PRODUCT_DIR / safe_name
    if target.exists():
        target.unlink()
    rebuild_product_rag_index()


def remove_all_product_specs():
    for path in PRODUCT_DIR.glob("*"):
        if path.suffix.lower() in {".pdf", ".docx", ".doc", ".txt"}:
            path.unlink(missing_ok=True)
    rebuild_product_rag_index()

# ── Transcript Parsers ───────────────────────────────────────────────────────

def parse_transcript_text(raw: str) -> List[Dict]:
    """Parse raw transcript text into list of {sl, speaker, text}"""
    turns = []
    # Pattern: optional SL number, speaker label, colon, then quoted or unquoted text
    pattern = re.compile(
        r'(?:(\d+)\.\s+)?'           # optional SL no
        r'(bot|agent|customer|user|ivr|system)[:\s]+'  # speaker
        r'["\u201c]?(.+?)["\u201d]?(?=(?:\d+\.)?\s*(?:bot|agent|customer|user|ivr|system)[:\s]|$)',
        re.IGNORECASE | re.DOTALL
    )
    for m in pattern.finditer(raw):
        sl, speaker, text = m.group(1), m.group(2).strip(), m.group(3).strip()
        turns.append({
            "sl": int(sl) if sl else len(turns)+1,
            "speaker": speaker.lower(),
            "text": text.replace('\n', ' ').strip()
        })
    if not turns:
        # fallback: split by newlines, detect speaker
        for i, line in enumerate(raw.split('\n')):
            line = line.strip()
            if not line:
                continue
            m2 = re.match(r'^(?:\d+\.\s+)?(bot|agent|customer|user)[:\s]+["\u201c]?(.+)["\u201d]?$', line, re.I)
            if m2:
                turns.append({"sl": i+1, "speaker": m2.group(1).lower(), "text": m2.group(2).strip()})
    return turns

def extract_from_pdf(path: Path) -> List[Dict[str, str]]:
    """Returns list of {name, text} dicts (one per transcript in the PDF)"""
    reader = PdfReader(str(path))
    full_text = "\n".join(page.extract_text() or "" for page in reader.pages)
    # Try to split by call markers
    splits = re.split(r'(?:Call\s*#?\d+|Transcript\s*\d+|={5,}|-{5,})', full_text, flags=re.IGNORECASE)
    results = []
    for i, chunk in enumerate(splits):
        chunk = chunk.strip()
        if len(chunk) > 100:
            results.append({"name": f"{path.stem}_call_{i+1}", "text": chunk})
    if not results:
        results = [{"name": path.stem, "text": full_text}]
    return results

def extract_from_docx(path: Path) -> List[Dict[str, str]]:
    doc = DocxDocument(str(path))
    full_text = "\n".join(p.text for p in doc.paragraphs)
    splits = re.split(r'(?:Call\s*#?\d+|Transcript\s*\d+)', full_text, flags=re.IGNORECASE)
    results = []
    for i, chunk in enumerate(splits):
        chunk = chunk.strip()
        if len(chunk) > 100:
            results.append({"name": f"{path.stem}_call_{i+1}", "text": chunk})
    if not results:
        results = [{"name": path.stem, "text": full_text}]
    return results

def extract_from_excel(path: Path) -> List[Dict[str, str]]:
    """Excel may have SL column with multiple transcripts"""
    results = []
    try:
        xl = pd.ExcelFile(str(path))
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            df.columns = [str(c).strip().lower() for c in df.columns]
            # Detect SL column
            sl_col = next((c for c in df.columns if re.match(r'sl|s\.no|serial|#', c)), None)
            # Detect transcript column
            tx_col = next((c for c in df.columns if any(k in c for k in ['transcript','text','conversation','call'])), None)
            if sl_col and tx_col:
                for _, row in df.dropna(subset=[tx_col]).iterrows():
                    sl = str(row.get(sl_col, '')).strip()
                    text = str(row[tx_col]).strip()
                    if len(text) > 50:
                        name = f"{path.stem}_sl{sl}" if sl else f"{path.stem}_row{_}"
                        results.append({"name": name, "text": text, "sl": sl})
            elif tx_col:
                for i, row in df.dropna(subset=[tx_col]).iterrows():
                    text = str(row[tx_col]).strip()
                    if len(text) > 50:
                        results.append({"name": f"{path.stem}_row{i+1}", "text": text})
            else:
                # Treat each row as part of one transcript
                all_text = "\n".join(str(v) for v in df.values.flatten() if pd.notna(v) and str(v).strip())
                if len(all_text) > 50:
                    results.append({"name": f"{path.stem}_{sheet}", "text": all_text})
    except Exception as e:
        log.error(f"Excel parse error: {e}")
    return results

def extract_transcripts_from_file(path: Path) -> List[Dict[str, str]]:
    ext = path.suffix.lower()
    if ext == ".pdf":
        return extract_from_pdf(path)
    elif ext in [".docx", ".doc"]:
        return extract_from_docx(path)
    elif ext in [".xlsx", ".xls"]:
        return extract_from_excel(path)
    elif ext == ".txt":
        text = path.read_text(errors='replace')
        return [{"name": path.stem, "text": text}]
    return []

# ── RAG: Product Spec Retrieval ──────────────────────────────────────────────
# The local FAISS-backed helpers above handle retrieval and indexing.

# ── GPT-4o Analysis ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert quality assurance analyst for Bajaj Life Insurance's AI-powered customer service bot. 
Your role is to evaluate call transcripts between the BOT and customers with extreme precision and professional rigor.

You will evaluate each call on 10 parameters and provide JSON output ONLY — no prose outside the JSON.

You must think like a strict insurance QA lead, not a generic chatbot. Use the transcript, customer intent, and product knowledge context together to infer which product is being discussed even when the product name is not spoken explicitly. Product identification must rely on product details, feature language, policy behavior, and benefit structure.

EVALUATION PARAMETERS (score each 1-5):
1. greeting_opening (Weight: 5%, Min pass: 3) - Polite greeting, professional tone, respectful language
2. query_understanding (Weight: 10%, Min pass: 3) - Correct interpretation of customer intent, clarification when needed
3. response_accuracy (Weight: 25%, Min pass: 4) - Factual correctness, no guessing, no contradictions, reliable info
4. communication_quality (Weight: 8%, Min pass: 3) - Clear sentences, well-organized, avoids jargon, structured
5. compliance (Weight: 20%, Min pass: 4) - Follows regulatory rules (insurance/finance/privacy), no restricted info shared, no outcome promises
6. personalisation (Weight: 5%, Min pass: 3) - Uses available info (name, SR no, context) within compliance limits
7. empathy_soft_skills (Weight: 5%, Min pass: 3) - Acknowledges feelings, warm language, avoids robotic responses
8. resolution (Weight: 10%, Min pass: 3) - Directly solves problem, clear guidance, alternatives offered
9. system_behaviour (Weight: 10%, Min pass: 3) - No loops, no latency issues, no errors, appropriate escalation
10. closing_interaction (Weight: 2%, Min pass: 3) - Polite closure, thanks customer, offers further help

PRODUCT EVALUATION:
- Identify the insurance product(s) mentioned or strongly implied by the call, even if the call never states the product name
- Use product details such as premium structure, term, eligibility, surrender/loan behavior, maturity/death benefit, grace period, riders, and payout style to infer the right product
- Explain why the chosen product matches the call and which product details support the match
- Check if product details (premium, coverage, terms, exclusions, eligibility) mentioned are accurate based on the knowledge base
- Note any product information gaps, invented facts, or contradictions
- Note what should have been said differently if the bot missed a product-specific rule or detail
- Include 2-4 product checks with exact bot statement, supporting fact from the knowledge base, and verdict
- If the bot gives incorrect product or policy information, treat it as a fatal QA defect and score product accuracy 0
- If the bot states unconfirmed product information, mark it as a risk and lower confidence instead of passing it
- If the bot says it is unable to provide information for a policy/product question, flag it as a system failure and lower the relevant QA scores
- Every product check must compare the call statement against an exact sentence from the product PDF or indexed product spec

CALL CLASSIFICATION:
- category: One of [Policy Inquiry, Claims Assistance, Premium Payment, Policy Renewal, New Policy, Grievance, Technical Issue, General Query, Escalation Request, Cancellation Request]
- severity: "normal", "watch", "critical", "fatal"
- fatal_reason: explain if fatal (empty string otherwise)
- flags: array of applicable: ["inappropriate_language", "compliance_breach", "escalation_needed", "false_information", "data_privacy_risk", "agent_misconduct", "customer_distress", "loop_detected", "unresolved_critical", "regulatory_violation"]
- sentiment: overall customer sentiment: "positive", "neutral", "frustrated", "angry", "distressed"

FATAL TRIGGERS (mark fatal + flag immediately):
- Any regulatory/compliance violation
- False or misleading product information given to customer
- Customer personal/financial data mishandled
- Agent (bot) promises outcomes not authorized
- Customer clearly distressed with no empathy or escalation
- Inappropriate or offensive language
- Call ends without resolution on critical issue with no escalation

OUTPUT FORMAT — respond with ONLY this JSON structure:
{
  "scores": {
    "greeting_opening": <1-5>,
    "query_understanding": <1-5>,
    "response_accuracy": <1-5>,
    "communication_quality": <1-5>,
    "compliance": <1-5>,
    "personalisation": <1-5>,
    "empathy_soft_skills": <1-5>,
    "resolution": <1-5>,
    "system_behaviour": <1-5>,
    "closing_interaction": <1-5>
  },
    "param_comments": ["exactly 10 one-line explanations (same order as scores), each stating why that score was assigned with transcript-grounded evidence"],
  "weighted_score": <0-100 float>,
  "pass_fail": <"PASS" or "FAIL">,
  "failed_parameters": [<list of parameter names that didn't meet minimum>],
  "category": "<category>",
  "severity": "<normal|watch|critical|fatal>",
  "fatal_reason": "<string>",
  "flags": [<array of flag strings>],
  "sentiment": "<positive|neutral|frustrated|angry|distressed>",
  "product_mentioned": "<product name or 'None'>",
    "product_confidence": <0-1 float>,
    "product_signals": ["keywords or product details that drove the match"],
  "product_accuracy_score": <1-5 or null>,
  "product_issues": "<string describing inaccuracies or 'None'>",
  "what_should_have_been_said": "<specific suggestions for improvement>",
  "strengths": "<what the bot did well>",
  "summary": "<2-3 sentence overall call summary>",
    "product_checks": [{"call":"<call id if available>","stmt":"<exact bot statement>","fact":"<knowledge base fact>","verdict":"pass|fail|risk","vtext":"✓ ACCURATE|✗ INACCURATE|⚠️ CONDITIONALLY OK","risk":"None|🚨 HIGH — reason|⚠️ Medium — reason"}],
  "turn_count": <integer>,
  "bot_turns": <integer>,
  "customer_turns": <integer>,
  "estimated_duration_minutes": <float>
}"""

async def analyze_call_with_gpt4o(transcript_text: str, turns: List[Dict], product_context: str = "") -> Dict:
    """Send transcript to GPT-4o for analysis"""
    
    # Format turns for the prompt
    formatted = "\n".join(
        f"{t.get('sl', i+1)}. {t['speaker'].upper()}:\n\"{t['text']}\""
        for i, t in enumerate(turns)
    ) if turns else transcript_text
    
    product_section = f"\n\nPRODUCT SPEC CONTEXT (from RAG):\n{product_context}" if product_context else ""
    
    user_msg = f"""Analyze this Bajaj Life Insurance bot call transcript:

{formatted}
{product_section}

Evaluate all parameters and return ONLY the JSON response.

Treat product accuracy as a strict QA test: include exact call-vs-spec comparison rows for any product or policy claim, and fail closed on unconfirmed facts.

Ensure param_comments contains exactly 10 entries in score order and each entry clearly explains the score using transcript evidence so it can be shown in hover tooltips."""

    try:
        response = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        raw = response.choices[0].message.content
        return json.loads(raw)
    except openai.AuthenticationError:
        return _mock_analysis(turns)
    except Exception as e:
        log.error(f"GPT-4o error: {e}")
        return _mock_analysis(turns)

def _mock_analysis(turns: List[Dict]) -> Dict:
    """Fallback mock analysis when API key not set"""
    import random
    random.seed(len(turns))
    
    scores = {
        "greeting_opening": random.randint(3, 5),
        "query_understanding": random.randint(2, 5),
        "response_accuracy": random.randint(2, 5),
        "communication_quality": random.randint(3, 5),
        "compliance": random.randint(3, 5),
        "personalisation": random.randint(2, 5),
        "empathy_soft_skills": random.randint(2, 5),
        "resolution": random.randint(2, 5),
        "system_behaviour": random.randint(3, 5),
        "closing_interaction": random.randint(2, 5)
    }
    
    weights = {"greeting_opening":5,"query_understanding":15,"response_accuracy":25,
               "communication_quality":10,"compliance":20,"personalisation":5,
               "empathy_soft_skills":5,"resolution":10,"system_behaviour":3,"closing_interaction":2}
    mins = {"greeting_opening":3,"query_understanding":3,"response_accuracy":4,
            "communication_quality":3,"compliance":4,"personalisation":3,
            "empathy_soft_skills":3,"resolution":3,"system_behaviour":3,"closing_interaction":3}
    
    weighted = sum(scores[k] * weights[k] / 5 for k in scores)
    failed = [k for k in scores if scores[k] < mins[k]]
    severity = "fatal" if scores["compliance"] < 3 else ("critical" if failed else ("watch" if weighted < 70 else "normal"))
    
    sentiments = ["positive","neutral","frustrated","angry","distressed"]
    
    return {
        "scores": scores,
        "param_comments": [
            "Polite opening that sets a professional tone." if scores["greeting_opening"] >= 4 else "Opening is abrupt or too generic.",
            "The reply matches the customer's question." if scores["query_understanding"] >= 4 else "The bot misses part of the request.",
            "No obvious factual error is visible in this mock flow." if scores["response_accuracy"] >= 4 else "The answer appears incomplete or unreliable.",
            "The explanation is easy to follow." if scores["communication_quality"] >= 4 else "The explanation is vague or poorly structured.",
            "Compliance appears acceptable in this mock flow." if scores["compliance"] >= 4 else "A likely compliance gap is present.",
            "The response uses available context naturally." if scores["personalisation"] >= 4 else "The reply is generic and not personalized.",
            "Warm and supportive tone throughout." if scores["empathy_soft_skills"] >= 4 else "The tone feels robotic or detached.",
            "The issue is resolved with a clear next step." if scores["resolution"] >= 4 else "The issue remains only partially resolved.",
            "No visible loop or system instability." if scores["system_behaviour"] >= 4 else "The flow shows repetition or instability.",
            "The call closes politely and cleanly." if scores["closing_interaction"] >= 4 else "The closing is short or missing.",
        ],
        "weighted_score": round(weighted, 2),
        "pass_fail": "FAIL" if failed else "PASS",
        "failed_parameters": failed,
        "category": random.choice(["Policy Inquiry","Claims Assistance","Premium Payment","General Query","New Policy"]),
        "severity": severity,
        "fatal_reason": "Compliance score below threshold" if severity == "fatal" else "",
        "flags": ["compliance_breach"] if severity == "fatal" else [],
        "sentiment": random.choice(sentiments),
        "product_mentioned": random.choice(["Bajaj Life Goal Suraksha","Bajaj Life Smart Wealth Goal","None"]),
        "product_confidence": round(random.uniform(0.48, 0.91), 2),
        "product_signals": ["premium", "policy term", "benefit"],
        "product_accuracy_score": random.randint(2, 5),
        "product_issues": "Demo mode – set OPENAI_API_KEY for real analysis",
        "what_should_have_been_said": "Demo mode – set OPENAI_API_KEY for real GPT-4o analysis",
        "strengths": "Demo mode analysis",
        "summary": "This is a demo analysis. Please set your OPENAI_API_KEY environment variable for real GPT-4o powered evaluations.",
        "product_checks": [],
        "turn_count": len(turns),
        "bot_turns": sum(1 for t in turns if t.get("speaker") in ["bot","agent","system"]),
        "customer_turns": sum(1 for t in turns if t.get("speaker") in ["customer","user"]),
        "estimated_duration_minutes": round(len(turns) * 0.5, 1)
    }

# ── Processing Pipeline ──────────────────────────────────────────────────────

async def process_job(job_id: str, file_paths: List[Path]):
    db = load_db()
    job = next((j for j in db["jobs"] if j["id"] == job_id), None)
    if not job:
        return
    
    all_transcripts = []
    for fp in file_paths:
        try:
            items = extract_transcripts_from_file(fp)
            all_transcripts.extend(items)
        except Exception as e:
            log.error(f"File parse error {fp}: {e}")
    
    job["total"] = len(all_transcripts)
    job["status"] = "processing"
    job["processed"] = 0
    save_db(db)
    
    for item in all_transcripts:
        try:
            turns = parse_transcript_text(item["text"])

            # RAG product lookup from the transcript itself, even if the product name is not mentioned.
            rag_hit = infer_product_context(item["text"])
            product_context = json.dumps(rag_hit, indent=2, ensure_ascii=False)
            detected_product = rag_hit.get("product", "None")
            if detected_product and detected_product != "None":
                product_context = f"Detected product guess: {detected_product}\n\n{product_context}"

            analysis = await analyze_call_with_gpt4o(item["text"], turns, product_context)
            analysis.setdefault("product_mentioned", detected_product)
            if not analysis.get("product_mentioned") or analysis.get("product_mentioned") == "None":
                analysis["product_mentioned"] = detected_product
            analysis["product_mentioned"] = _safe_filename_label(str(analysis.get("product_mentioned", "None")))
            analysis.setdefault("product_confidence", rag_hit.get("confidence", 0.0))
            analysis.setdefault("product_signals", rag_hit.get("matched_terms", []))
            analysis.setdefault("product_checks", [])
            analysis.setdefault("product_profile", rag_hit.get("profile", {}))
            analysis.setdefault("product_evidence", rag_hit.get("chunks", []))
            analysis["sentiment"] = _refine_sentiment(turns, analysis.get("sentiment", "neutral"))
            if not analysis.get("param_comments") or len(analysis.get("param_comments", [])) < len(PARAM_ORDER):
                analysis["param_comments"] = _fallback_param_comments(analysis.get("scores", {}))
            analysis = _apply_qa_policy_rules(analysis, turns)
            analysis["annotated_transcript"] = _annotate_transcript(turns, analysis)
            
            call_record = {
                "id": str(uuid.uuid4()),
                "job_id": job_id,
                "name": item["name"],
                "sl": item.get("sl", ""),
                "source_file": item.get("source_file", ""),
                "transcript": turns if turns else [{"sl":1,"speaker":"unknown","text":item["text"][:2000]}],
                "raw_text": item["text"][:5000],
                "analysis": analysis,
                "processed_at": datetime.now().isoformat(),
                "flagged": len(analysis.get("flags", [])) > 0,
                "fatal": analysis.get("severity") == "fatal"
            }
            
            db = load_db()
            db["calls"].append(call_record)
            job_idx = next((i for i,j in enumerate(db["jobs"]) if j["id"] == job_id), None)
            if job_idx is not None:
                db["jobs"][job_idx]["processed"] = db["jobs"][job_idx].get("processed", 0) + 1
                db["jobs"][job_idx]["fatal_count"] = db["jobs"][job_idx].get("fatal_count", 0) + (1 if call_record["fatal"] else 0)
                db["jobs"][job_idx]["flag_count"] = db["jobs"][job_idx].get("flag_count", 0) + (1 if call_record["flagged"] else 0)
            save_db(db)
            
        except Exception as e:
            log.error(f"Call processing error: {e}")
    
    db = load_db()
    job_idx = next((i for i,j in enumerate(db["jobs"]) if j["id"] == job_id), None)
    if job_idx is not None:
        db["jobs"][job_idx]["status"] = "completed"
        db["jobs"][job_idx]["completed_at"] = datetime.now().isoformat()
    save_db(db)

# ── API Endpoints ────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    return FileResponse(str(FRONTEND_DIR / "templates" / "index.html"))

@app.post("/api/upload")
async def upload_files(background_tasks: BackgroundTasks, files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(400, "No files provided")
    
    job_id = str(uuid.uuid4())
    job_dir = UPLOAD_DIR / job_id
    job_dir.mkdir(parents=True)
    
    saved_paths = []
    for f in files:
        dest = job_dir / f.filename
        content = await f.read()
        dest.write_bytes(content)
        saved_paths.append(dest)
    
    db = load_db()
    db["jobs"].append({
        "id": job_id,
        "status": "queued",
        "files": [f.filename for f in files],
        "total": 0,
        "processed": 0,
        "fatal_count": 0,
        "flag_count": 0,
        "created_at": datetime.now().isoformat(),
        "completed_at": None
    })
    save_db(db)
    
    background_tasks.add_task(process_job, job_id, saved_paths)
    return {"job_id": job_id, "files_received": len(files), "status": "queued"}

@app.post("/api/upload-products")
async def upload_product_specs(background_tasks: BackgroundTasks, files: List[UploadFile] = File(...)):
    indexed = []
    for f in files:
        if f.filename.endswith(".pdf"):
            dest = PRODUCT_DIR / f.filename
            content = await f.read()
            dest.write_bytes(content)
            indexed.append(f.filename)
    background_tasks.add_task(rebuild_product_rag_index)
    mode = json.loads(RAG_BACKEND_FILE.read_text()).get("mode", "faiss") if RAG_BACKEND_FILE.exists() else "faiss"
    message = f"Rebuilt product knowledge base after indexing {len(indexed)} PDF(s)"
    return {"indexed": indexed, "mode": mode, "message": message}

@app.get("/api/product-specs")
async def list_product_specs():
    index_data = load_product_index()
    chunk_map = {
        entry.get("source", ""): len(entry.get("chunks", []))
        for entry in index_data.get("products", [])
    }

    specs = []
    for p in sorted(PRODUCT_DIR.glob("*.pdf")):
        stat = p.stat()
        specs.append({
            "name": p.name,
            "display_name": _safe_filename_label(p.name),
            "size": stat.st_size,
            "updated_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "chunks": chunk_map.get(p.name, 0)
        })

    return {
        "mode": json.loads(RAG_BACKEND_FILE.read_text()).get("mode", "faiss") if RAG_BACKEND_FILE.exists() else "faiss",
        "count": len(specs),
        "specs": specs
    }

@app.delete("/api/product-specs/{filename}")
async def delete_product_spec(filename: str):
    safe_name = Path(filename).name
    target = PRODUCT_DIR / safe_name

    if not target.exists():
        raise HTTPException(404, "Product spec not found")

    try:
        target.unlink()
    except Exception as e:
        raise HTTPException(500, f"Failed to delete file: {e}")

    rebuild_product_rag_index()

    return {
        "deleted": safe_name,
        "removed_from": ["file", "rag"],
        "message": f"Removed {safe_name}"
    }

@app.delete("/api/product-specs")
async def delete_all_product_specs():
    remove_all_product_specs()
    return {"message": "All product specs removed"}

@app.get("/api/jobs")
async def get_jobs():
    db = load_db()
    return db["jobs"]

@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    db = load_db()
    job = next((j for j in db["jobs"] if j["id"] == job_id), None)
    if not job:
        raise HTTPException(404, "Job not found")
    return job

@app.get("/api/calls")
async def get_calls(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    severity: Optional[str] = None,
    category: Optional[str] = None,
    sentiment: Optional[str] = None,
    pass_fail: Optional[str] = None,
    flagged: Optional[bool] = None,
    search: Optional[str] = None,
    job_id: Optional[str] = None,
    sort_by: str = "processed_at",
    sort_dir: str = "desc"
):
    db = load_db()
    calls = db["calls"]
    
    # Filters
    if severity:
        calls = [c for c in calls if c["analysis"].get("severity") == severity]
    if category:
        calls = [c for c in calls if c["analysis"].get("category") == category]
    if sentiment:
        calls = [c for c in calls if c["analysis"].get("sentiment") == sentiment]
    if pass_fail:
        calls = [c for c in calls if c["analysis"].get("pass_fail") == pass_fail]
    if flagged is not None:
        calls = [c for c in calls if c.get("flagged") == flagged]
    if job_id:
        calls = [c for c in calls if c.get("job_id") == job_id]
    if search:
        s = search.lower()
        calls = [c for c in calls if s in c["name"].lower() or s in c.get("raw_text","").lower()]
    
    total = len(calls)
    
    # Sort
    reverse = sort_dir == "desc"
    if sort_by == "weighted_score":
        calls.sort(key=lambda c: c["analysis"].get("weighted_score", 0), reverse=reverse)
    elif sort_by == "processed_at":
        calls.sort(key=lambda c: c.get("processed_at",""), reverse=reverse)
    else:
        calls.sort(key=lambda c: c.get(sort_by,""), reverse=reverse)
    
    # Paginate
    start = (page-1)*page_size
    page_calls = calls[start:start+page_size]
    
    # Slim version for table view
    slim = []
    for c in page_calls:
        a = c["analysis"]
        if not a.get("param_comments") or len(a.get("param_comments", [])) < len(PARAM_ORDER):
            a["param_comments"] = _fallback_param_comments(a.get("scores", {}))
        if not a.get("annotated_transcript"):
            a["annotated_transcript"] = _annotate_transcript(c.get("transcript", []), a)
        a["sentiment"] = _refine_sentiment(c.get("transcript", []), a.get("sentiment", "neutral"))
        a = _apply_qa_policy_rules(a, c.get("transcript", []))
        a["product_mentioned"] = _safe_filename_label(str(a.get("product_mentioned") or "None"))
        c["analysis"] = a
        failed_parameters = a.get("failed_parameters", [])
        score_reason = a.get("score_reason") or _score_reason(a)
        slim.append({
            "id": c["id"],
            "name": c["name"],
            "sl": c.get("sl",""),
            "category": a.get("category",""),
            "severity": a.get("severity","normal"),
            "weighted_score": a.get("weighted_score",0),
            "pass_fail": a.get("pass_fail",""),
            "sentiment": a.get("sentiment",""),
            "flags": a.get("flags",[]),
            "product_mentioned": a.get("product_mentioned",""),
            "product_confidence": a.get("product_confidence", 0.0),
            "product_signals": a.get("product_signals", []),
            "fatal": c.get("fatal",False),
            "flagged": c.get("flagged",False),
            "turn_count": a.get("turn_count",0),
            "estimated_duration_minutes": a.get("estimated_duration_minutes",0),
            "processed_at": c.get("processed_at","") ,
            "score_reason": score_reason,
            "failed_parameters": failed_parameters
        })
    
    return {"calls": slim, "total": total, "page": page, "page_size": page_size}

@app.get("/api/calls/{call_id}")
async def get_call_detail(call_id: str):
    db = load_db()
    call = next((c for c in db["calls"] if c["id"] == call_id), None)
    if not call:
        raise HTTPException(404, "Call not found")
    a = call.get("analysis", {})
    if not a.get("param_comments") or len(a.get("param_comments", [])) < len(PARAM_ORDER):
        a["param_comments"] = _fallback_param_comments(a.get("scores", {}))
    if not a.get("annotated_transcript"):
        a["annotated_transcript"] = _annotate_transcript(call.get("transcript", []), a)
    a["sentiment"] = _refine_sentiment(call.get("transcript", []), a.get("sentiment", "neutral"))
    a = _apply_qa_policy_rules(a, call.get("transcript", []))
    a["product_mentioned"] = _safe_filename_label(str(a.get("product_mentioned") or "None"))
    call["analysis"] = a
    return call


@app.get("/api/export/calls.xlsx")
async def export_calls_excel():
    db = load_db()
    rows = []
    for c in db.get("calls", []):
        a = c.get("analysis", {})
        scores = a.get("scores", {})
        rows.append({
            "id": c.get("id", ""),
            "job_id": c.get("job_id", ""),
            "name": c.get("name", ""),
            "sl": c.get("sl", ""),
            "processed_at": c.get("processed_at", ""),
            "pass_fail": a.get("pass_fail", ""),
            "severity": a.get("severity", ""),
            "category": a.get("category", ""),
            "sentiment": a.get("sentiment", ""),
            "weighted_score": a.get("weighted_score", 0),
            "score_reason": a.get("score_reason", ""),
            "failed_parameters": ", ".join(a.get("failed_parameters", [])),
            "flags": ", ".join(a.get("flags", [])),
            "product_mentioned": a.get("product_mentioned", ""),
            "product_confidence": a.get("product_confidence", 0),
            "product_signals": ", ".join(a.get("product_signals", [])),
            "product_accuracy_score": a.get("product_accuracy_score", ""),
            "product_issues": a.get("product_issues", ""),
            "strengths": a.get("strengths", ""),
            "summary": a.get("summary", ""),
            "qa_findings": " | ".join(f.get("text", "") for f in a.get("qa_findings", [])),
            "greeting_opening": scores.get("greeting_opening", ""),
            "query_understanding": scores.get("query_understanding", ""),
            "response_accuracy": scores.get("response_accuracy", ""),
            "communication_quality": scores.get("communication_quality", ""),
            "compliance": scores.get("compliance", ""),
            "personalisation": scores.get("personalisation", ""),
            "empathy_soft_skills": scores.get("empathy_soft_skills", ""),
            "resolution": scores.get("resolution", ""),
            "system_behaviour": scores.get("system_behaviour", ""),
            "closing_interaction": scores.get("closing_interaction", ""),
            "raw_text": c.get("raw_text", ""),
        })

    df = pd.DataFrame(rows)
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="calls")
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=calls_export.xlsx"},
    )


@app.get("/api/calls/{call_id}/report.pdf")
async def export_call_report_pdf(call_id: str):
    db = load_db()
    call = next((c for c in db.get("calls", []) if c.get("id") == call_id), None)
    if not call:
        raise HTTPException(404, "Call not found")

    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    except Exception:
        raise HTTPException(500, "PDF export requires reportlab. Install with pip install reportlab")

    a = call.get("analysis", {})
    scores = a.get("scores", {})
    stream = io.BytesIO()
    doc = SimpleDocTemplate(stream, pagesize=A4, title=f"Call Report {call.get('name', '')}")
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph("Bajaj Life Insurance - Call QA Report", styles["Title"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(f"Call: {call.get('name', '')}", styles["Heading3"]))
    story.append(Paragraph(f"Processed At: {call.get('processed_at', '')}", styles["Normal"]))
    story.append(Paragraph(f"Score: {a.get('weighted_score', 0)} | Status: {a.get('pass_fail', '')} | Severity: {a.get('severity', '')}", styles["Normal"]))
    story.append(Paragraph(f"Product: {a.get('product_mentioned', 'None')} (confidence: {a.get('product_confidence', 0)})", styles["Normal"]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Summary", styles["Heading3"]))
    story.append(Paragraph(a.get("summary", ""), styles["Normal"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Parameter Scores", styles["Heading3"]))
    for key in PARAM_ORDER:
        story.append(Paragraph(f"{key.replace('_', ' ').title()}: {scores.get(key, '-')}/5", styles["Normal"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("QA Findings", styles["Heading3"]))
    for finding in a.get("qa_findings", []):
        story.append(Paragraph(f"- [{finding.get('type', 'info')}] {finding.get('text', '')}", styles["Normal"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Product Checks", styles["Heading3"]))
    for pc in a.get("product_checks", [])[:6]:
        story.append(Paragraph(f"- {pc.get('vtext', pc.get('verdict', 'check'))}: {pc.get('stmt', '')}", styles["Normal"]))
        story.append(Paragraph(f"  Fact: {pc.get('fact', '')}", styles["Normal"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Annotated Transcript", styles["Heading3"]))
    turns = a.get("annotated_transcript") or call.get("transcript", [])
    for turn in turns[:120]:
        speaker = (turn.get("speaker") or "unknown").upper()
        text = turn.get("text", "")
        tags = turn.get("tags", [])
        tag_txt = f" [{' | '.join(tags)}]" if tags else ""
        story.append(Paragraph(f"{turn.get('sl', '')}. {speaker}: {text}{tag_txt}", styles["Normal"]))

    doc.build(story)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=call_report_{call_id}.pdf"},
    )

@app.delete("/api/calls/{call_id}")
async def delete_call(call_id: str):
    db = load_db()
    calls = db.get("calls", [])
    before = len(calls)
    db["calls"] = [c for c in calls if c.get("id") != call_id]
    if len(db["calls"]) == before:
        raise HTTPException(404, "Call not found")
    save_db(db)
    return {"deleted": call_id, "message": "Call removed"}

@app.get("/api/dashboard")
async def get_dashboard():
    db = load_db()
    calls = db["calls"]
    if not calls:
        return {"total": 0, "message": "No calls processed yet"}
    
    scores = [c["analysis"].get("weighted_score",0) for c in calls]
    severities = {}
    categories = {}
    sentiments = {}
    param_scores = {k:[] for k in ["greeting_opening","query_understanding","response_accuracy",
                                     "communication_quality","compliance","personalisation",
                                     "empathy_soft_skills","resolution","system_behaviour","closing_interaction"]}
    flags_count = {}
    daily_counts = {}
    product_counts = {}
    product_confidence = defaultdict(list)
    pass_count = fail_count = 0
    fatal_count = flagged_count = 0
    
    for c in calls:
        a = c["analysis"]
        sev = a.get("severity","normal")
        severities[sev] = severities.get(sev,0)+1
        cat = a.get("category","Unknown")
        categories[cat] = categories.get(cat,0)+1
        sent = _refine_sentiment(c.get("transcript", []), a.get("sentiment","neutral"))
        sentiments[sent] = sentiments.get(sent,0)+1
        if a.get("pass_fail") == "PASS": pass_count += 1
        else: fail_count += 1
        if c.get("fatal"): fatal_count += 1
        if c.get("flagged"): flagged_count += 1
        product = _safe_filename_label(str(a.get("product_mentioned") or "None"))
        product_counts[product] = product_counts.get(product, 0) + 1
        if a.get("product_confidence") is not None:
            product_confidence[product].append(float(a.get("product_confidence") or 0))
        for f in a.get("flags",[]):
            flags_count[f] = flags_count.get(f,0)+1
        for k in param_scores:
            v = a.get("scores",{}).get(k)
            if v: param_scores[k].append(v)
        date_key = c.get("processed_at","")[:10]
        if date_key:
            daily_counts[date_key] = daily_counts.get(date_key,0)+1
    
    avg_param = {k: round(sum(v)/len(v),2) if v else 0 for k,v in param_scores.items()}
    avg_product_confidence = {k: round(sum(v)/len(v), 3) if v else 0 for k, v in product_confidence.items()}
    
    return {
        "total_calls": len(calls),
        "avg_score": round(sum(scores)/len(scores),2),
        "pass_rate": round(pass_count/len(calls)*100,1),
        "fail_count": fail_count,
        "fatal_count": fatal_count,
        "flagged_count": flagged_count,
        "severities": severities,
        "categories": categories,
        "sentiments": sentiments,
        "flags_breakdown": flags_count,
        "avg_parameter_scores": avg_param,
        "product_breakdown": dict(sorted(product_counts.items(), key=lambda item: item[1], reverse=True)),
        "avg_product_confidence": avg_product_confidence,
        "score_distribution": {
            "excellent": sum(1 for s in scores if s >= 85),
            "good": sum(1 for s in scores if 70 <= s < 85),
            "average": sum(1 for s in scores if 55 <= s < 70),
            "poor": sum(1 for s in scores if s < 55)
        },
        "daily_volume": dict(sorted(daily_counts.items())),
        "jobs_summary": {
            "total": len(db["jobs"]),
            "completed": sum(1 for j in db["jobs"] if j.get("status")=="completed"),
            "processing": sum(1 for j in db["jobs"] if j.get("status")=="processing"),
            "queued": sum(1 for j in db["jobs"] if j.get("status")=="queued")
        }
    }

@app.get("/api/fatal-calls")
async def get_fatal_calls():
    db = load_db()
    fatals = [c for c in db["calls"] if c.get("fatal") or c["analysis"].get("severity") in ["fatal","critical"]]
    return [{
        "id": c["id"], "name": c["name"],
        "severity": c["analysis"].get("severity"),
        "fatal_reason": c["analysis"].get("fatal_reason",""),
        "flags": c["analysis"].get("flags",[]),
        "weighted_score": c["analysis"].get("weighted_score",0),
        "category": c["analysis"].get("category",""),
        "processed_at": c.get("processed_at","")
    } for c in fatals]

@app.delete("/api/calls")
async def clear_all_calls():
    db = load_db()
    db["calls"] = []
    db["jobs"] = []
    save_db(db)
    for job_dir in UPLOAD_DIR.glob("*"):
        if job_dir.is_dir():
            shutil.rmtree(job_dir, ignore_errors=True)
    return {"message": "All data cleared"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
