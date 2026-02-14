# src/incidents.py
# Generates an Incident Index (CSV) from message-level tags.
#
# INPUT:
#   data/output/messages_tagged.csv  (from src/tagger.py)
#
# OUTPUT:
#   data/output/incidents_index.csv
#
# NOTES:
# - Groups messages into "incidents" using:
#     * same thread_id
#     * same primary P1 category (gaslighting / alienation / manipulation / time interference)
#     * time gap <= MAX_GAP_HOURS between consecutive messages in the cluster
# - Keeps everything traceable (message_id, raw_row_number, dt).

import json
import re
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

INPUT_TAGGED = "data/output/messages_tagged.csv"
OUTPUT_INCIDENTS = "data/output/incidents_index.csv"

# Tuning knobs
MAX_GAP_HOURS = 12           # if messages are farther apart than this, start a new incident
MIN_MESSAGES_PER_INCIDENT = 1  # keep 1 if you want single-message incidents
MAX_SNIPPET_CHARS = 220

P1_CATEGORIES = {
    "court_order_time_interference",
    "alienation_undermining",
    "manipulation_coercion",
    "gaslighting",
}

CATEGORY_PRIORITY_ORDER = [
    "court_order_time_interference",
    "alienation_undermining",
    "manipulation_coercion",
    "gaslighting",
]

def safe_json_loads(s: str, default):
    if not isinstance(s, str) or not s.strip():
        return default
    try:
        return json.loads(s)
    except Exception:
        return default

def normalize_text(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_primary_p1(labels: List[Dict[str, Any]]) -> Optional[str]:
    """
    Pick a primary P1 category from the message labels.
    If multiple P1 labels present, use CATEGORY_PRIORITY_ORDER.
    """
    if not isinstance(labels, list):
        return None

    p1_hits = []
    for lab in labels:
        if not isinstance(lab, dict):
            continue
        cat = str(lab.get("category", "")).strip()
        pri = str(lab.get("priority", "")).strip()
        if pri == "P1" and cat in P1_CATEGORIES:
            p1_hits.append(cat)

    if not p1_hits:
        return None

    for cat in CATEGORY_PRIORITY_ORDER:
        if cat in p1_hits:
            return cat
    return p1_hits[0]

def summarize_incident(messages: pd.DataFrame) -> str:
    """
    Neutral one-liner summary for the incident index.
    Uses reasons if available, otherwise uses text snippets.
    """
    # Prefer the first non-empty reason among the clustered messages
    reasons = [normalize_text(x) for x in messages["reason"].tolist() if normalize_text(x)]
    if reasons:
        # Keep it short; "reason" should already be neutral.
        r = reasons[0]
        return r[:MAX_SNIPPET_CHARS] + ("…" if len(r) > MAX_SNIPPET_CHARS else "")

    # Fallback: a short excerpt
