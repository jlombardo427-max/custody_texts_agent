# src/export_docx.py
# Exports court-style "Incident Cards" to a Word document from:
#   - data/output/incidents_index.csv  (from src/incidents.py)
#   - data/output/messages_tagged.csv  (from src/tagger.py)
#   - data/working/messages_normalized.csv (from src/ingest.py)
#
# OUTPUT:
#   data/output/incident_cards.docx
#
# FEATURES:
# - Each incident becomes a separate section:
#     * Title (Incident ID + Category)
#     * Date range, thread, participants
#     * Neutral summary
#     * Messages table (dt, sender_role, text, evidence quotes)
#     * Optional context: N messages before first + after last (within thread)
#
# INSTALL:
#   pip install python-docx

import json
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

INCIDENTS_CSV = "data/output/incidents_index.csv"
TAGGED_CSV = "data/output/messages_tagged.csv"
NORMALIZED_CSV = "data/working/messages_normalized.csv"

OUTPUT_DOCX = "data/output/incident_cards.docx"

# Tuning knobs
CONTEXT_BEFORE = 6
CONTEXT_AFTER = 6
MAX_TEXT_CHARS_IN_TABLE = 1200  # keep long texts but avoid giant cells

CATEGORY_DISPLAY = {
    "court_order_time_interference": "Court-Ordered Parenting Time Interference (P1)",
    "alienation_undermining": "Alienation / Undermining Relationship (P1)",
    "manipulation_coercion": "Manipulation / Coercion (P1)",
    "gaslighting": "Gaslighting (P1)",
}

def safe_json_loads(s: str, default):
    if not isinstance(s, str) or not s.strip():
        return default
    try:
        return json.loads(s)
    except Exception:
        return default

def normalize_ws(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clip(s: str, n: int) -> str:
    s = s or ""
    return s[:n] + ("…" if len(s) > n else "")

def extract_evidence_quotes(labels_obj: List[Dict[str, Any]], primary_cat: str) -> List[str]:
    # Find the label matching primary category and return evidence quotes
    if not isinstance(labels_obj, list):
        return []
    for lab in labels_obj:
        if not isinstance(lab, dict):
            continue
        if str(lab.get("category", "")).strip() == primary_cat:
            quotes = lab.get("evidence_quotes", [])
            if isinstance(quotes, list):
                return [normalize_ws(q) for q in quotes if normalize_ws(q)]
    return []

def set_doc_styles(doc: Document):
    # Basic readable style for court-ish cards (simple, not fancy)
    style = doc.styles["Normal"]
    style.font.name = "Calibri"
    style.font.size = Pt(11)

def add_heading(doc: Document, text: str, level: int = 1):
    p = doc.add_heading(text, level=level)
    return p

def add_kv_line(doc: Document, k: str, v: str):
    p = doc.add_paragraph()
    run_k = p.add_run(f"{k}: ")
    run_k.bold = True
    p.add_run(v)

def build_thread_lookup(normalized_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Map thread_id -> sorted DF for context retrieval.
    """
    nd = normalized_df.copy()
    nd["dt"] = pd.to_datetime(nd["dt"], errors="coerce")
    nd = nd.dropna(subset=["dt"])
    nd = nd.sort_values(["thread_id", "dt", "raw_row_number"]).reset_index(drop=True)
    threads = {}
    for tid, tdf in nd.groupby("thread_id", sort=False):
        threads[str(tid)] = tdf.reset_index(drop=True)
    return threads

def get_context_rows(thread_df: pd.DataFrame, first_msg_id: str, last_msg_id: str,
                     before_n: int, after_n: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (
