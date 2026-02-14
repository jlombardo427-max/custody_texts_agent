# src/tagger.py
# Custody SMS AI Tagger (Ollama) — RECEIVED messages only (sender_role == "THEM")
#
# INPUT:
#   data/working/messages_normalized.csv
#
# OUTPUT:
#   data/output/messages_tagged_checkpoint.csv   (autosave/checkpoint)
#   data/output/messages_tagged.csv              (final)
#   data/output/tagger_run.log                   (verbose log)
#
# FEATURES:
# - Processes ONLY received messages (sender_role == "THEM") to save time
# - Progress bar (tqdm)
# - Verbose logging (console + file)
# - Autosave/checkpoint every 5 processed messages (crash-safe temp write + replace)
# - Resume from checkpoint (skips already-tagged message_ids)
# - Finite / strict prompt + strict JSON schema
# - P1 categories:
#     * court_order_time_interference (expanded)
#     * school_issues (expanded)
#     * alienation_undermining
#     * manipulation_coercion
#     * gaslighting
#     * broken_promise_or_contradiction  (strict two-sided evidence)
#     * feigned_ignorance_or_message_avoidance (NEW)
# - Finite SUBTYPE for time interference + school issues + broken promises + avoidance
# - Convenience columns for Excel filtering, including new helpers
# - SPEED UPS:
#     * Low-signal skip (no model call)
#     * Message length cap
#     * Threaded parallelism (ThreadPoolExecutor) with safe autosave batching
#
# IMPORTANT LEGAL NOTE:
# - This script does NOT "prove lies." It flags contradictions/broken commitments based on text comparison.
# - "feigned_ignorance_or_message_avoidance" flags denials of awareness or refusal to read/acknowledge, per text.
#
# REQUIREMENTS:
#   pip install pandas requests tqdm
#   Ollama running locally: http://localhost:11434
#
# RUN:
#   python src/tagger.py
#
# TIP (PowerShell log tail):
#   Get-Content -Path data\output\tagger_run.log -Wait



from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import time
import logging
import re
from typing import Dict, Any, List, Tuple, Set, Optional

import pandas as pd
import requests
from tqdm import tqdm


# ----------------------------
# CONFIG
# ----------------------------
LOGIC_VERSION = "v3.1-parallel-tones-p1plus-avoidance"

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "custody-llama"  # or "custody-mistral" / "llama3.2:3b"

INPUT_PATH = "data/working/messages_normalized.csv"
CHECKPOINT_PATH = "data/output/messages_tagged_checkpoint.csv"
FINAL_OUT_PATH = "data/output/messages_tagged.csv"
LOG_PATH = "data/output/tagger_run.log"

PROCESS_SENDER_ROLE = "THEM"  # process only received messages

# Context size (speed tradeoff)
CTX_BEFORE = 3
CTX_AFTER = 2

# Message cap (speed + prompt control)
MAX_MESSAGE_CHARS = 1200

# Parallelism (start small; increase if stable)
MAX_WORKERS = 4  # CPU-only: 2; GPU: 4-8

# Autosave/checkpoint frequency
CHECKPOINT_EVERY = 5
PRINT_EVERY = 50

# Ollama runtime options (speed)
OLLAMA_OPTIONS = {
    "temperature": 0,
    "top_p": 1,
    "num_predict": 380,
}

# Optional: paste custody order terms here (exchange times, pickup place, etc.)
ORDER_TERMS = "None provided."


# ----------------------------
# FINITE ENUMS
# ----------------------------
P1_CATEGORIES = [
    "court_order_time_interference",
    "school_issues",
    "alienation_undermining",
    "manipulation_coercion",
    "gaslighting",
    "broken_promise_or_contradiction",
    "feigned_ignorance_or_message_avoidance",
]
P2_CATEGORIES = [
    "harassment_hostility",
    "escalation",
    "stonewalling_obstruction",
    "contradiction",
]
ALL_CATEGORIES = set(P1_CATEGORIES + P2_CATEGORIES)

# Expanded, court-safe tones
TONE_VALUES = {
    # Neutral / constructive
    "neutral",
    "informational",
    "matter_of_fact",
    "cooperative",
    # Conflict / hostility
    "hostile",
    "contemptuous",
    "sarcastic",
    "dismissive",
    "accusatory",
    # Control / pressure
    "coercive",
    "manipulative",
    "threatening",
    "ultimatum_based",
    # Instability / escalation
    "emotionally_escalated",
    "erratic",
    "volatile",
    # Defensive / evasive
    "defensive",
    "evasive",
    "stonewalling",
    # Remorse / de-escalation
    "apologetic",
    "conciliatory",
}

TIME_SUBTYPES = {
    "refusal_withholding",
    "last_minute_change",
    "unilateral_change",
    "illness_related",
    "transportation_related",
    "conditioning_access",
    "no_show_delay",
    "other",
}

SCHOOL_SUBTYPES = {
    "attendance_excused_absence",
    "attendance_unexcused_absence",
    "truancy_skipping",
    "tardy_late",
    "bad_grades_failing",
    "missing_assignments_homework",
    "discipline_admin",
    "school_pickup_dropoff",
    "school_communication_blocking",
    "iep_504_special_ed",
    "other",
}

BROKEN_PROMISE_SUBTYPES = {
    "promise_not_followed_through",
    "contradiction_of_prior_statement",
    "denial_of_prior_agreement",
    "moving_goalposts",
    "other",
}

AVOIDANCE_SUBTYPES = {
    "denial_of_awareness",
    "refusal_to_read",
    "ignoring_key_information",
    "message_avoidance_general",
    "other",
}


# ----------------------------
# LOW-SIGNAL SKIP (no model call)
# ----------------------------
LOW_SIGNAL_RE = re.compile(
    r"^\s*(ok|k|kk|yes|no|thanks|thx|sure|np|lol|👍|👌|ok\.|k\.|on my way|omw)\s*$",
    re.I,
)

def is_low_signal(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    if len(t) <= 2:
        return True
    return bool(LOW_SIGNAL_RE.match(t))


# ----------------------------
# TONE STRENGTH (Excel-friendly)
# ----------------------------
def tone_strength(tone: str) -> str:
    t = (tone or "").strip()
    high = {"threatening", "ultimatum_based", "coercive", "volatile", "emotionally_escalated"}
    medium = {"hostile", "accusatory", "manipulative", "dismissive", "sarcastic", "contemptuous",
              "defensive", "evasive", "stonewalling", "erratic"}
    if t in high:
        return "high"
    if t in medium:
        return "medium"
    return "low"


# ----------------------------
# PROMPT (STRICT JSON; ESCAPED BRACES)
# ----------------------------
PROMPT_TEMPLATE = r"""
YOU MUST OUTPUT VALID JSON ONLY. NO MARKDOWN. NO EXTRA TEXT.
If you cannot comply, output exactly: {{"error":"noncompliant"}}

Role: Evidence review classifier for custody litigation. Neutral and factual. No diagnoses.

RULE: Apply labels ONLY if triggered by MESSAGE TEXT itself.
Context may help interpret denials/changes, but do NOT label based ONLY on context.

P1 categories (priority):

- court_order_time_interference:
  Applies when the MESSAGE TEXT shows interference with scheduled parenting time/exchange logistics.
  Evidence must be quoted from MESSAGE TEXT.
  SUBTYPE (finite): refusal_withholding | last_minute_change | unilateral_change | illness_related |
                   transportation_related | conditioning_access | no_show_delay | other

- school_issues:
  Applies when MESSAGE TEXT references school-related problems or school custody logistics.
  Evidence must be quoted from MESSAGE TEXT.
  SUBTYPE (finite): attendance_excused_absence | attendance_unexcused_absence | truancy_skipping | tardy_late |
                   bad_grades_failing | missing_assignments_homework | discipline_admin | school_pickup_dropoff |
                   school_communication_blocking | iep_504_special_ed | other

- alienation_undermining:
  disparages the other parent to/around child, secrecy/coaching, interference with child-parent communication/access.

- manipulation_coercion:
  threats (court/police/DCPP) used to control behavior, conditional access, intimidation/guilt leverage.

- gaslighting:
  denial of documented facts ("that never happened"), rewriting agreements, reality inversion ("you’re imagining things").

- broken_promise_or_contradiction:
  NOT a lie detector. Flags contradictions / broken commitments based on text comparison.
  Apply ONLY when:
  (1) MESSAGE TEXT contains a promise/commitment OR a denial of a prior promise/agreement OR a factual claim, AND
  (2) CONTEXT BEFORE contains a prior promise/agreement/claim that conflicts with MESSAGE TEXT.
  SUBTYPE (finite): promise_not_followed_through | contradiction_of_prior_statement | denial_of_prior_agreement |
                    moving_goalposts | other
  Evidence rule (STRICT): MUST include:
    - at least 1 evidence quote from MESSAGE TEXT (verbatim, <=12 words)
    - at least 1 context quote from CONTEXT BEFORE (verbatim, <=12 words)

- feigned_ignorance_or_message_avoidance:
  Flags when the party denies awareness of prior messages, refuses to read, or explicitly avoids key info.
  Apply when MESSAGE TEXT shows one of:
  A) Denial of awareness ("I didn't know", "you never told me") AND CONTEXT BEFORE shows you told them (quote required).
  B) Refusal to read ("I'm not reading that", "I didn't read it") (context quote optional).
  C) Ignoring key information: if based on comparison, MUST include a context quote.
  SUBTYPE (finite):
    denial_of_awareness | refusal_to_read | ignoring_key_information | message_avoidance_general | other

P2 tags (secondary):
- harassment_hostility
- escalation
- stonewalling_obstruction
- contradiction

TONE must be exactly one of the following (choose ONE):
neutral, informational, matter_of_fact, cooperative,
hostile, contemptuous, sarcastic, dismissive, accusatory,
coercive, manipulative, threatening, ultimatum_based,
emotionally_escalated, erratic, volatile,
defensive, evasive, stonewalling,
apologetic, conciliatory

Tone rules:
- Tone describes HOW the message is communicated, not intent.
- Choose the dominant tone of MESSAGE TEXT only.
- If unclear/mixed, use neutral or matter_of_fact.

EVIDENCE RULES (strict):
- For EACH label: include 1–2 evidence_quotes VERBATIM from MESSAGE TEXT (<=12 words each).
- If you cannot quote MESSAGE TEXT for a label, do not apply that label.
- context_quotes:
  * broken_promise_or_contradiction: include 1–2 context_quotes from CONTEXT BEFORE (<=12 words each).
  * feigned_ignorance_or_message_avoidance:
      - if subtype denial_of_awareness or ignoring_key_information: include 1–2 context_quotes
      - if subtype refusal_to_read: context_quotes may be []
  * all other categories: context_quotes MUST be [].

ORDER TERMS (if provided):
<<<ORDER_TERMS>>>
{ORDER_TERMS}
<<<END_ORDER_TERMS>>>

MESSAGE TEXT:
<<<MESSAGE>>>
{TEXT}
<<<END_MESSAGE>>>

CONTEXT BEFORE:
<<<BEFORE>>>
{BEFORE}
<<<END_BEFORE>>>

CONTEXT AFTER:
<<<AFTER>>>
{AFTER}
<<<END_AFTER>>>

OUTPUT JSON SCHEMA (must match exactly):
{{
  "labels": [
    {{
      "category": "{CAT_ENUM}",
      "priority": "P1|P2",
      "subtype": "",
      "confidence": 0.0,
      "evidence_quotes": ["...","..."],
      "context_quotes": ["...","..."]
    }}
  ],
  "tone": {{"primary": "{TONE_ENUM}", "confidence": 0.0}},
  "reason": "One neutral sentence referencing the evidence quotes.",
  "safety_note": ""
}}

Finite rules:
- category must be one of: {CATS}
- tone.primary must be one of: {TONES}
- subtype:
  * court_order_time_interference: refusal_withholding, last_minute_change, unilateral_change, illness_related,
    transportation_related, conditioning_access, no_show_delay, other
  * school_issues: attendance_excused_absence, attendance_unexcused_absence, truancy_skipping, tardy_late,
    bad_grades_failing, missing_assignments_homework, discipline_admin, school_pickup_dropoff,
    school_communication_blocking, iep_504_special_ed, other
  * broken_promise_or_contradiction: promise_not_followed_through, contradiction_of_prior_statement,
    denial_of_prior_agreement, moving_goalposts, other
  * feigned_ignorance_or_message_avoidance: denial_of_awareness, refusal_to_read, ignoring_key_information,
    message_avoidance_general, other
  * otherwise subtype must be "".

If no labels apply, output labels=[] with a neutral reason.
""".strip()


# ----------------------------
# LOGGING
# ----------------------------
os.makedirs("data/output", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("tagger")


# ----------------------------
# HELPERS (CORRECTED FOR NaN / float)
# ----------------------------
def _safe_text(val: Any) -> str:
    if val is None:
        return ""
    if pd.isna(val):
        return ""
    return str(val)

def format_ctx_rows(rows: pd.DataFrame) -> str:
    lines: List[str] = []
    for _, r in rows.iterrows():
        role = _safe_text(r.get("sender_role", "")).strip()
        dt = _safe_text(r.get("dt", "")).strip()
        txt = _safe_text(r.get("text", ""))

        txt = txt.replace("\n", " ").strip()
        if len(txt) > 280:
            txt = txt[:280] + "…"
        lines.append(f"[{dt} {role}] {txt}")
    return "\n".join(lines).strip()

def build_context(thread_df: pd.DataFrame, idx: int, before_n: int, after_n: int) -> Tuple[str, str]:
    start_before = max(0, idx - before_n)
    end_before = idx
    start_after = idx + 1
    end_after = min(len(thread_df), idx + 1 + after_n)
    before = format_ctx_rows(thread_df.iloc[start_before:end_before])
    after = format_ctx_rows(thread_df.iloc[start_after:end_after])
    return before or "(none)", after or "(none)"


def call_ollama(prompt: str, retries: int = 2, timeout: int = 600) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            resp = requests.post(
                OLLAMA_URL,
                json={
                    "model": MODEL_NAME,
                    "prompt": prompt,
                    "stream": False,
                    "options": OLLAMA_OPTIONS,
                },
                timeout=timeout,
            )
            resp.raise_for_status()
            out_txt = resp.json().get("response", "").strip()
            return json.loads(out_txt)
        except Exception as e:
            last_err = e
            time.sleep(1.2 * (attempt + 1))
    return {"error": f"ollama_failed: {str(last_err)}"}


def _clean_quotes_from_text(quotes: Any, haystack: str) -> List[str]:
    out: List[str] = []
    if not isinstance(quotes, list):
        return out
    for q in quotes[:2]:
        q = _safe_text(q).strip()
        if not q:
            continue
        if len(q.split()) > 12:
            continue
        if q in haystack:
            out.append(q)
    return out


def validate_and_normalize(model_obj: Dict[str, Any], message_text: str, before_context: str) -> Dict[str, Any]:
    if not isinstance(model_obj, dict) or model_obj.get("error"):
        err = model_obj.get("error") if isinstance(model_obj, dict) else "invalid_output"
        return {
            "labels": [],
            "tone": {"primary": "neutral", "confidence": 0.0},
            "reason": f"Model error: {err}",
            "safety_note": "",
        }

    labels = model_obj.get("labels", [])
    tone = model_obj.get("tone", {}) or {}
    reason = _safe_text(model_obj.get("reason", ""))[:500]
    safety_note = _safe_text(model_obj.get("safety_note", ""))[:300]

    tone_primary = _safe_text(tone.get("primary", "neutral")).strip() or "neutral"
    try:
        tone_conf = float(tone.get("confidence", 0.0) or 0.0)
    except Exception:
        tone_conf = 0.0
    tone_conf = min(max(tone_conf, 0.0), 1.0)
    if tone_primary not in TONE_VALUES:
        tone_primary = "neutral"
        tone_conf = 0.0

    msg = message_text or ""
    before = before_context or ""
    cleaned_labels: List[Dict[str, Any]] = []

    if isinstance(labels, list):
        for lab in labels:
            if not isinstance(lab, dict):
                continue

            cat = _safe_text(lab.get("category", "")).strip()
            pri = _safe_text(lab.get("priority", "")).strip()
            subtype = _safe_text(lab.get("subtype", "")).strip()
            conf = lab.get("confidence", 0.0)

            if cat not in ALL_CATEGORIES:
                continue
            if pri not in {"P1", "P2"}:
                continue

            try:
                conf = float(conf)
            except Exception:
                conf = 0.0
            conf = min(max(conf, 0.0), 1.0)

            if cat == "court_order_time_interference":
                if subtype not in TIME_SUBTYPES:
                    subtype = "other"
            elif cat == "school_issues":
                if subtype not in SCHOOL_SUBTYPES:
                    subtype = "other"
            elif cat == "broken_promise_or_contradiction":
                if subtype not in BROKEN_PROMISE_SUBTYPES:
                    subtype = "other"
            elif cat == "feigned_ignorance_or_message_avoidance":
                if subtype not in AVOIDANCE_SUBTYPES:
                    subtype = "other"
            else:
                subtype = ""

            ev_quotes = _clean_quotes_from_text(lab.get("evidence_quotes", []), msg)
            cx_quotes = _clean_quotes_from_text(lab.get("context_quotes", []), before)

            if cat == "broken_promise_or_contradiction":
                if not ev_quotes or not cx_quotes:
                    continue
            elif cat == "feigned_ignorance_or_message_avoidance":
                if subtype in {"denial_of_awareness", "ignoring_key_information"}:
                    if not ev_quotes or not cx_quotes:
                        continue
                else:
                    cx_quotes = []
            else:
                cx_quotes = []

            if not ev_quotes:
                continue

            cleaned_labels.append({
                "category": cat,
                "priority": pri,
                "subtype": subtype,
                "confidence": conf,
                "evidence_quotes": ev_quotes,
                "context_quotes": cx_quotes,
            })

    if not cleaned_labels and not reason:
        reason = "No labels applied under evidence rules."

    return {
        "labels": cleaned_labels,
        "tone": {"primary": tone_primary, "confidence": tone_conf},
        "reason": reason,
        "safety_note": safety_note,
    }


def compute_convenience_fields(labels: List[Dict[str, Any]]) -> Dict[str, Any]:
    p1 = sorted({l["category"] for l in labels if l.get("priority") == "P1"})
    p2 = sorted({l["category"] for l in labels if l.get("priority") == "P2"})

    conf_max = 0.0
    ev_quotes_all: List[str] = []
    cx_quotes_all: List[str] = []

    bp_hit = False
    bp_subtype = ""
    avoid_hit = False
    avoid_subtype = ""

    for l in labels:
        try:
            conf_max = max(conf_max, float(l.get("confidence", 0.0) or 0.0))
        except Exception:
            pass
        ev_quotes_all.extend([_safe_text(q) for q in (l.get("evidence_quotes", []) or []) if _safe_text(q)])
        cx_quotes_all.extend([_safe_text(q) for q in (l.get("context_quotes", []) or []) if _safe_text(q)])

        if l.get("category") == "broken_promise_or_contradiction" and not bp_hit:
            bp_hit = True
            bp_subtype = _safe_text(l.get("subtype", ""))
        if l.get("category") == "feigned_ignorance_or_message_avoidance" and not avoid_hit:
            avoid_hit = True
            avoid_subtype = _safe_text(l.get("subtype", ""))

    return {
        "p1_hit": bool(p1),
        "p1_categories": ";".join(p1),
        "p2_categories": ";".join(p2),
        "confidence_max": conf_max,
        "evidence_quotes_flat": " | ".join([q for q in ev_quotes_all if q]),
        "context_quotes_flat": " | ".join([q for q in cx_quotes_all if q]),
        "bp_hit": bool(bp_hit),
        "bp_subtype": bp_subtype,
        "avoid_hit": bool(avoid_hit),
        "avoid_subtype": avoid_subtype,
    }


def extract_subtype(labels: List[Dict[str, Any]], category: str) -> str:
    for l in labels:
        if l.get("category") == category:
            return _safe_text(l.get("subtype", ""))
    return ""


def load_checkpoint_ids(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        cdf = pd.read_csv(path, usecols=["message_id"])
        return set(cdf["message_id"].astype(str).tolist())
    except Exception:
        return set()


def load_checkpoint_rows(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        return pd.read_csv(path).to_dict(orient="records")
    except Exception:
        return []


def autosave(out_rows: List[Dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    pd.DataFrame(out_rows).to_csv(tmp_path, index=False)
    os.replace(tmp_path, path)


# ----------------------------
# WORK ITEM + WORKER
# ----------------------------
def build_work_items(df: pd.DataFrame, done_ids: Set[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for thread_id, tdf in df.groupby("thread_id", sort=False):
        tdf = tdf.reset_index(drop=True)
        for i in range(len(tdf)):
            row = tdf.iloc[i]
            mid = _safe_text(row.get("message_id", "")).strip()
            if not mid or mid in done_ids:
                continue

            msg_raw = row.get("text", "")
            msg_text = _safe_text(msg_raw).strip()

            if is_low_signal(msg_text):
                items.append({
                    "kind": "skip",
                    "message_id": mid,
                    "thread_id": thread_id,
                    "dt": pd.to_datetime(row.get("dt")).isoformat(),
                    "sender_role": _safe_text(row.get("sender_role", "")),
                    "sender_name": _safe_text(row.get("sender_name", "")),
                    "text": msg_raw,
                    "raw_row_number": row.get("raw_row_number"),
                })
                continue

            before, after = build_context(tdf, i, CTX_BEFORE, CTX_AFTER)

            msg_text_capped = msg_text
            if len(msg_text_capped) > MAX_MESSAGE_CHARS:
                msg_text_capped = msg_text_capped[:MAX_MESSAGE_CHARS] + "…"

            items.append({
                "kind": "model",
                "message_id": mid,
                "thread_id": thread_id,
                "dt": pd.to_datetime(row.get("dt")).isoformat(),
                "sender_role": _safe_text(row.get("sender_role", "")),
                "sender_name": _safe_text(row.get("sender_name", "")),
                "text": msg_raw,
                "msg_text_for_prompt": msg_text_capped,
                "before": before,
                "after": after,
                "raw_row_number": row.get("raw_row_number"),
            })
    return items


def process_item(item: Dict[str, Any], cats_join: str, tones_join: str, cat_enum: str, tone_enum: str) -> Dict[str, Any]:
    mid = item["message_id"]

    if item["kind"] == "skip":
        return {
            "logic_version": LOGIC_VERSION,
            "message_id": mid,
            "thread_id": item.get("thread_id"),
            "dt": item.get("dt"),
            "sender_role": item.get("sender_role"),
            "sender_name": item.get("sender_name"),
            "text": item.get("text"),
            "labels": "[]",
            "tone_primary": "neutral",
            "tone_strength": "low",
            "tone_confidence": 0.0,
            "reason": "Low-signal message; skipped model call.",
            "safety_note": "",
            "p1_hit": False,
            "p1_categories": "",
            "p2_categories": "",
            "confidence_max": 0.0,
            "evidence_quotes_flat": "",
            "context_quotes_flat": "",
            "time_interference_subtype": "",
            "school_issue_subtype": "",
            "bp_hit": False,
            "bp_subtype": "",
            "avoid_hit": False,
            "avoid_subtype": "",
            "raw_row_number": item.get("raw_row_number"),
        }

    msg_text_for_prompt = item.get("msg_text_for_prompt", "") or ""
    before = item.get("before", "(none)") or "(none)"
    after = item.get("after", "(none)") or "(none)"

    prompt = PROMPT_TEMPLATE.format(
        ORDER_TERMS=ORDER_TERMS,
        CAT_ENUM=cat_enum,
        TONE_ENUM=tone_enum,
        CATS=cats_join,
        TONES=tones_join,
        TEXT=msg_text_for_prompt,
        BEFORE=before,
        AFTER=after,
    )

    raw_obj = call_ollama(prompt)
    clean_obj = validate_and_normalize(raw_obj, msg_text_for_prompt, before)

    labels = clean_obj["labels"]
    tone = clean_obj["tone"]
    reason = clean_obj["reason"]
    safety_note = clean_obj["safety_note"]

    conv = compute_convenience_fields(labels)
    time_subtype = extract_subtype(labels, "court_order_time_interference")
    school_subtype = extract_subtype(labels, "school_issues")

    tprimary = tone.get("primary", "neutral")
    return {
        "logic_version": LOGIC_VERSION,
        "message_id": mid,
        "thread_id": item.get("thread_id"),
        "dt": item.get("dt"),
        "sender_role": item.get("sender_role"),
        "sender_name": item.get("sender_name"),
        "text": item.get("text"),
        "labels": json.dumps(labels, ensure_ascii=False),
        "tone_primary": tprimary,
        "tone_strength": tone_strength(tprimary),
        "tone_confidence": tone.get("confidence", 0.0),
        "reason": reason,
        "safety_note": safety_note,
        "p1_hit": conv["p1_hit"],
        "p1_categories": conv["p1_categories"],
        "p2_categories": conv["p2_categories"],
        "confidence_max": conv["confidence_max"],
        "evidence_quotes_flat": conv["evidence_quotes_flat"],
        "context_quotes_flat": conv["context_quotes_flat"],
        "time_interference_subtype": time_subtype,
        "school_issue_subtype": school_subtype,
        "bp_hit": conv["bp_hit"],
        "bp_subtype": conv["bp_subtype"],
        "avoid_hit": conv["avoid_hit"],
        "avoid_subtype": conv["avoid_subtype"],
        "raw_row_number": item.get("raw_row_number"),
    }


# ----------------------------
# MAIN
# ----------------------------
def main():
    df = pd.read_csv(INPUT_PATH)

    # Normalize datetime + sort
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    df = df.dropna(subset=["dt"]).sort_values(
        ["thread_id", "dt", "raw_row_number"]
    ).reset_index(drop=True)

    # Ensure text col is safe (prevents NaN floats)
    if "text" in df.columns:
        df["text"] = df["text"].fillna("")

    # Only RECEIVED messages
    df = df[df["sender_role"].astype(str) == PROCESS_SENDER_ROLE].copy().reset_index(drop=True)

    # Resume
    done_ids = load_checkpoint_ids(CHECKPOINT_PATH)
    out_rows: List[Dict[str, Any]] = load_checkpoint_rows(CHECKPOINT_PATH) if done_ids else []

    total = len(df)
    remaining_est = total - len(done_ids)

    logger.info(f"Logic version: {LOGIC_VERSION}")
    logger.info(f"Model: {MODEL_NAME}")
    logger.info(f"Tagging RECEIVED only: sender_role={PROCESS_SENDER_ROLE}")
    logger.info(f"Total received messages: {total} | Remaining (est): {remaining_est}")
    logger.info(f"Context: before={CTX_BEFORE} after={CTX_AFTER}")
    logger.info(f"Autosave/checkpoint every: {CHECKPOINT_EVERY} messages")
    logger.info(f"Parallel workers: {MAX_WORKERS}")
    logger.info(f"Checkpoint file: {CHECKPOINT_PATH}")

    cats_join = ", ".join(P1_CATEGORIES + P2_CATEGORIES)
    tones_join = ", ".join(sorted(TONE_VALUES))
    cat_enum = "|".join(P1_CATEGORIES + P2_CATEGORIES)
    tone_enum = "|".join(sorted(TONE_VALUES))

    items = build_work_items(df, done_ids)
    remaining = len(items)
    logger.info(f"Work items to process (after resume filter): {remaining}")

    processed = 0
    errors = 0

    pbar = tqdm(total=max(0, remaining), desc="Tagging received messages", unit="msg")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        future_to_mid = {
            ex.submit(process_item, item, cats_join, tones_join, cat_enum, tone_enum): item["message_id"]
            for item in items
        }

        for fut in as_completed(future_to_mid):
            mid = future_to_mid[fut]
            try:
                row_out = fut.result()
                out_rows.append(row_out)
                done_ids.add(mid)
            except Exception as e:
                errors += 1
                logger.exception(f"FUTURE ERROR message_id={mid}: {e}")
                out_rows.append({
                    "logic_version": LOGIC_VERSION,
                    "message_id": mid,
                    "thread_id": "",
                    "dt": "",
                    "sender_role": PROCESS_SENDER_ROLE,
                    "sender_name": "",
                    "text": "",
                    "labels": "[]",
                    "tone_primary": "neutral",
                    "tone_strength": "low",
                    "tone_confidence": 0.0,
                    "reason": f"ERROR: {str(e)}",
                    "safety_note": "",
                    "p1_hit": False,
                    "p1_categories": "",
                    "p2_categories": "",
                    "confidence_max": 0.0,
                    "evidence_quotes_flat": "",
                    "context_quotes_flat": "",
                    "time_interference_subtype": "",
                    "school_issue_subtype": "",
                    "bp_hit": False,
                    "bp_subtype": "",
                    "avoid_hit": False,
                    "avoid_subtype": "",
                    "raw_row_number": "",
                })

            processed += 1
            pbar.update(1)

            if processed % CHECKPOINT_EVERY == 0:
                try:
                    autosave(out_rows, CHECKPOINT_PATH)
                    logger.info(f"AUTOSAVE checkpoint: rows={len(out_rows)} processed_this_run={processed} errors={errors}")
                except Exception as e:
                    logger.exception(f"Autosave failed: {e}")

            if processed % PRINT_EVERY == 0:
                logger.info(f"Progress: +{processed} this run | total tagged={len(done_ids)} | errors={errors}")

    pbar.close()

    autosave(out_rows, CHECKPOINT_PATH)
    pd.DataFrame(out_rows).to_csv(FINAL_OUT_PATH, index=False)

    logger.info(f"DONE. wrote {FINAL_OUT_PATH} rows={len(out_rows)} | errors={errors}")


if __name__ == "__main__":
    main()
