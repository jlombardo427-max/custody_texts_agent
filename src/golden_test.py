# src/golden_test.py
import argparse
import json
import os
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, Any, Optional

import pandas as pd

# We reuse functions from tagger.py by importing them.
# Your tagger.py must expose:
# - gated_allowed_categories(text) -> List[str]
# - build_context_from_full_thread(thread_df, message_id, before_n, after_n) -> (before, after)
# - call_ollama(...)
# - validate_and_normalize(...)
# - PROMPT_TEMPLATE, P1_CATEGORIES, P2_CATEGORIES, TONE_VALUES
import tagger as T


GOLDEN_PATH_DEFAULT = "data/tests/golden_cases.csv"
OUT_REPORT_DEFAULT = "data/output/golden_report.csv"
OUT_METRICS_DEFAULT = "data/output/golden_metrics.csv"


@dataclass(frozen=True)
class GoldenCase:
    case_id: str
    thread_id: str
    dt: str
    sender_role: str
    text: str
    expected_categories: Set[str]
    expected_time_subtype: str
    expected_school_subtype: str
    notes: str


def parse_expected(s: str) -> Set[str]:
    s = (s or "").strip()
    if not s:
        return set()
    return {x.strip() for x in s.split(";") if x.strip()}


def safe_str(x) -> str:
    return "" if x is None else str(x)


def compute_prf(tp: int, fp: int, fn: int) -> Tuple[float, float, float]:
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
    return prec, rec, f1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--golden", default=GOLDEN_PATH_DEFAULT)
    ap.add_argument("--out-report", default=OUT_REPORT_DEFAULT)
    ap.add_argument("--out-metrics", default=OUT_METRICS_DEFAULT)

    ap.add_argument("--model", default=T.MODEL_NAME_DEFAULT if hasattr(T, "MODEL_NAME_DEFAULT") else "mistral")
    ap.add_argument("--ollama-url", default=T.OLLAMA_URL_DEFAULT if hasattr(T, "OLLAMA_URL_DEFAULT") else "http://localhost:11434/api/generate")
    ap.add_argument("--timeout", type=int, default=420)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--ctx-before", type=int, default=6)
    ap.add_argument("--ctx-after", type=int, default=3)
    ap.add_argument("--order-terms", default=getattr(T, "ORDER_TERMS_DEFAULT", "None provided."))

    args = ap.parse_args()

    df = pd.read_csv(args.golden)
    required = {"case_id", "thread_id", "dt", "sender_role", "text", "expected_categories"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"golden_cases.csv missing columns: {missing}")

    # Build a "fake thread table" per thread_id from golden cases only
    # (For context-dependent categories, add both ME + THEM cases within the same thread_id.)
    df["message_id"] = df["case_id"].astype(str)  # use case_id as message_id
    df["dt_parsed"] = pd.to_datetime(df["dt"], errors="coerce")
    df = df.dropna(subset=["dt_parsed"]).sort_values(["thread_id", "dt_parsed"]).reset_index(drop=True)

    threads: Dict[str, pd.DataFrame] = {
        tid: tdf.reset_index(drop=True) for tid, tdf in df.groupby("thread_id", sort=False)
    }

    cat_enum = "|".join(T.P1_CATEGORIES + T.P2_CATEGORIES)
    tone_enum = "|".join(sorted(T.TONE_VALUES))
    cats_join = ", ".join(T.P1_CATEGORIES + T.P2_CATEGORIES)
    tones_join = ", ".join(sorted(T.TONE_VALUES))

    rows: List[Dict[str, Any]] = []

    for _, r in df.iterrows():
        case_id = str(r["case_id"])
        thread_id = str(r["thread_id"])
        text = safe_str(r.get("text")).strip()

        expected = parse_expected(r.get("expected_categories"))
        exp_time_sub = safe_str(r.get("expected_time_subtype")).strip()
        exp_school_sub = safe_str(r.get("expected_school_subtype")).strip()
        notes = safe_str(r.get("notes")).strip()

        # We typically evaluate only THEM, but you can include ME for context rows
        # If sender_role is ME, we still run it (optional) but you can skip if you want.
        allowed_list = T.gated_allowed_categories(text)
        allowed_set = set(allowed_list)
        allowed_str = "\n".join(allowed_list) if allowed_list else "(none)"

        thread_df = threads[thread_id]
        before, after = T.build_context_from_full_thread(thread_df, case_id, args.ctx_before, args.ctx_after)

        prompt = T.PROMPT_TEMPLATE.format(
            ALLOWED=allowed_str,
            ORDER_TERMS=args.order_terms,
            CAT_ENUM=cat_enum,
            TONE_ENUM=tone_enum,
            CATS=cats_join,
            TONES=tones_join,
            TEXT=text,
            BEFORE=before,
            AFTER=after,
        )

        raw = T.call_ollama(
            ollama_url=args.ollama_url,
            model_name=args.model,
            prompt=prompt,
            options=getattr(T, "OLLAMA_OPTIONS_DEFAULT", {"temperature": 0}),
            retries=args.retries,
            timeout=args.timeout,
        )

        clean = T.validate_and_normalize(raw, text, before, allowed_set)
        labels = clean["labels"]

        pred_categories = {lab["category"] for lab in labels}
        pred_time_sub = ""
        pred_school_sub = ""
        for lab in labels:
            if lab["category"] == "court_order_time_interference":
                pred_time_sub = lab.get("subtype", "") or ""
            if lab["category"] == "school_issues":
                pred_school_sub = lab.get("subtype", "") or ""

        # Per-case deltas
        missing_cats = sorted(list(expected - pred_categories))
        extra_cats = sorted(list(pred_categories - expected))

        # Subtype checks only if category expected/predicted
        time_sub_ok = True
        school_sub_ok = True
        if "court_order_time_interference" in expected and exp_time_sub:
            time_sub_ok = (pred_time_sub == exp_time_sub)
        if "school_issues" in expected and exp_school_sub:
            school_sub_ok = (pred_school_sub == exp_school_sub)

        rows.append({
            "case_id": case_id,
            "thread_id": thread_id,
            "dt": r.get("dt"),
            "sender_role": r.get("sender_role"),
            "text": text,
            "expected_categories": ";".join(sorted(expected)),
            "pred_categories": ";".join(sorted(pred_categories)),
            "missing_categories": ";".join(missing_cats),
            "extra_categories": ";".join(extra_cats),
            "expected_time_subtype": exp_time_sub,
            "pred_time_subtype": pred_time_sub,
            "time_subtype_ok": time_sub_ok,
            "expected_school_subtype": exp_school_sub,
            "pred_school_subtype": pred_school_sub,
            "school_subtype_ok": school_sub_ok,
            "tone_primary": clean["tone"]["primary"],
            "confidence_max": max([float(l.get("confidence", 0)) for l in labels], default=0.0),
            "reason": clean["reason"],
            "labels_json": json.dumps(labels, ensure_ascii=False),
            "notes": notes,
        })

    report = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(args.out_report), exist_ok=True)
    report.to_csv(args.out_report, index=False)

    # Metrics per category
    cats = sorted(list(T.ALL_CATEGORIES))
    metrics = []
    for c in cats:
        tp = fp = fn = 0
        for _, rr in report.iterrows():
            exp = parse_expected(rr["expected_categories"])
            pred = parse_expected(rr["pred_categories"])
            if c in exp and c in pred:
                tp += 1
            elif c not in exp and c in pred:
                fp += 1
            elif c in exp and c not in pred:
                fn += 1
        prec, rec, f1 = compute_prf(tp, fp, fn)
        metrics.append({
            "category": c,
            "tp": tp, "fp": fp, "fn": fn,
            "precision": round(prec, 3),
            "recall": round(rec, 3),
            "f1": round(f1, 3),
        })

    mdf = pd.DataFrame(metrics).sort_values(["f1", "precision"], ascending=False)
    mdf.to_csv(args.out_metrics, index=False)

    print(f"Wrote: {args.out_report}")
    print(f"Wrote: {args.out_metrics}")

    worst = mdf.sort_values(["f1", "precision"], ascending=True).head(8)
    if len(worst):
        print("\nLowest-scoring categories:")
        print(worst[["category", "precision", "recall", "f1", "tp", "fp", "fn"]].to_string(index=False))

    # Show top mismatches
    mism = report[(report["missing_categories"] != "") | (report["extra_categories"] != "")]
    print(f"\nMismatched cases: {len(mism)} / {len(report)}")
    if len(mism) > 0:
        print(mism[["case_id", "missing_categories", "extra_categories", "text"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()