import pandas as pd
import uuid

# EDIT THIS: put your exact "Sender Name" values that represent YOU
ME_NAMES = {"Giuseppe Lombardo", "Giuseppe","nan"}

def normalize_csv(path_in: str, path_out: str):
    df = pd.read_csv(path_in)

    records = []
    for idx, row in df.iterrows():
        sender_name = str(row.get("Sender Name", "")).strip()
        sender_role = "ME" if sender_name in ME_NAMES else "THEM"

        text = str(row.get("Text", "") if pd.notna(row.get("Text", "")) else "").strip()

        has_attachment = pd.notna(row.get("Attachment")) and str(row.get("Attachment")).strip() != ""
        attachment_type = str(row.get("Attachment type", "") if pd.notna(row.get("Attachment type", "")) else "").lower().strip()

        is_edited = pd.notna(row.get("Edited Date")) and str(row.get("Edited Date")).strip() != ""

        msg = {
            "message_id": str(uuid.uuid4()),
            "thread_id": str(row.get("Chat Session", "")).strip(),
            "dt": pd.to_datetime(row.get("Message Date"), errors="coerce"),
            "sender_role": sender_role,
            "sender_name": sender_name,
            "text": text,
            "has_attachment": bool(has_attachment),
            "attachment_type": attachment_type,
            "is_edited": bool(is_edited),
            "raw_row_number": idx + 1
        }
        records.append(msg)

    out = pd.DataFrame(records)
    out = out.dropna(subset=["dt"])
    out = out.sort_values(["thread_id", "dt", "raw_row_number"]).reset_index(drop=True)

    out.to_csv(path_out, index=False)
    print(f"OK: wrote {path_out} rows={len(out)}")

if __name__ == "__main__":
    normalize_csv(
        path_in="data/raw/imazing_export.csv",
        path_out="data/working/messages_normalized.csv"
    )
