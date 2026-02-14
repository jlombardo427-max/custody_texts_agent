# src/qa_false_positive.py
# Automated false-positive QA for STRICT v3.2 outputs
#
# INPUT:
#   data/output/messages_tagged.csv
#
# OUTPUT:
#   data/output/qa_false_positive_report.csv

import json
import re
import pandas as pd

TAGGED_PATH = "data/output/messages_tagged.csv"
OUT_REPORT = "data/output/qa_false_positive_report.csv"

# Heuristic “lateness only” phrases that should NOT be time interference
LATENESS_ONLY = [
    "late", "a couple minutes", "few minutes", "traffic", "running behind",
    "be there at", "eta", "i'll be there", "ill be there",
]

TIME_STRONG = [
    "cancel", "cancelling", "not bringing", "no pickup", "not picking up", "won't pick up",
    "can't make it", "cant make it", "you're not getting", "you are not getting",
    "not coming", "won't come", "cant come", "can't come",
    "changed the schedule", "switch days", "switching days",
]

SCHOOL_STRONG = [
    "school", "teacher", "principal", "report card", "grade", "grades",
    "absent", "absence", "unexcused", "tardy", "truancy",
    "homework", "assignment", "iep", "504", "suspension", "detention",
]

MANIP_STRONG = [
    "or else", "unless", "if you don't", "if you dont", "if you do not",
    "police", "dcpp", "court", "restraining order", "tpo", "froj",
]

def lower(s):
    return (s or "").lower()

def any_in(text, phrases):
    t = lower(text)
    return any(p in t for p in phrases)

def load_labels(cell: str):
    try:
        return json.loads(cell or "[]")
    except Exception:
        return []

def main():
    df = pd.read_csv(TAGGED_PATH)
    rows = []

    for _, r in df.iterrows():
        labels = load_labels(r.get("labels"))
        text = str(r.get("text") or "")
        ev_flat = str(r.get("evidence_quotes_flat") or "")
        cx_flat = str(r.get("context_quotes_flat") or "")

        for lab in labels:
            cat = lab.get("category")
            pri = lab.get("priority")
            subtype = lab.get("subtype") or ""
            ev = " | ".join(lab.get("evidence_quotes") or [])
            cx = " | ".join(lab.get("context_quotes") or [])

            # Heuristic FP checks
            fp_reason = ""

            if cat == "court_order_time_interference":
                # If “lateness only” evidence and no strong cancel/deny keywords, flag
                if any_in(ev, LATENESS_ONLY) and not any_in(ev, TIME_STRONG) and not any_in(text, TIME_STRONG):
                    fp_reason = "Time interference appears lateness-only (no cancel/deny language)."

            if cat == "school_issues":
                if not any_in(ev, SCHOOL_STRONG):
                    fp_reason = "School issue evidence lacks strong school keywords."

            if cat == "manipulation_coercion":
                if not any_in(ev, MANIP_STRONG):
                    fp_reason = "Manipulation label without threat/conditional keywords."

            if cat == "broken_promise_or_contradiction":
                if not cx:
                    fp_reason = "Broken promise/contradiction missing required context quotes."

            if cat == "feigned_ignorance_or_message_avoidance":
                # For awareness/ignoring subtypes, should have context
                if subtype in ("denial_of_awareness", "ignoring_key_information") and not cx:
                    fp_reason = "Feigned ignorance missing required context quotes."

            if fp_reason:
                rows.append({
                    "dt": r.get("dt"),
                    "thread_id": r.get("thread_id"),
                    "message_id": r.get("message_id"),
                    "category": cat,
                    "priority": pri,
                    "subtype": subtype,
                    "confidence": lab.get("confidence"),
                    "fp_reason": fp_reason,
                    "evidence_quotes": ev,
                    "context_quotes": cx,
                    "text": text,
                })

    out = pd.DataFrame(rows).sort_values(["dt", "thread_id"]).reset_index(drop=True)
    out.to_csv(OUT_REPORT, index=False)

    print(f"QA report: {OUT_REPORT}")
    print(f"Flagged rows: {len(out)}")
    if len(out) > 0:
        print("\nTop FP reasons:")
        print(out["fp_reason"].value_counts().head(10).to_string())

if __name__ == "__main__":
    main()