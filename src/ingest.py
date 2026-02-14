import pandas as pd
import uuid
import multiprocessing
import os
from functools import partial

# REFINED ME_NAMES: Removed "nan" to prevent accidental attribution
ME_NAMES = {"Giuseppe Lombardo", "Giuseppe", "Joe"} 

def process_chunk(chunk):
    """
    Processes a slice of the dataframe to extract full metadata 
    including refined role logic, attachments, and edit status.
    """
    records = []
    for idx, row in chunk.iterrows():
        raw_sender = str(row.get("Sender Name", "")).strip()
        
        # IMPROVED ROLE LOGIC: Explicitly handle 'nan' or empty as UNKNOWN
        if raw_sender in ME_NAMES:
            sender_role = "ME"
        elif pd.isna(row.get("Sender Name")) or raw_sender.lower() in ["nan", ""]:
            sender_role = "UNKNOWN"
        else:
            sender_role = "THEM"

        text = str(row.get("Text", "") if pd.notna(row.get("Text", "")) else "").strip()

        # EXTRACT ATTACHMENT AND EDIT METADATA
        has_attachment = pd.notna(row.get("Attachment")) and str(row.get("Attachment")).strip() != ""
        attachment_type = str(row.get("Attachment type", "") if pd.notna(row.get("Attachment type", "")) else "").lower().strip()
        is_edited = pd.notna(row.get("Edited Date")) and str(row.get("Edited Date")).strip() != ""

        msg = {
            "message_id": str(uuid.uuid4()),
            "thread_id": str(row.get("Chat Session", "")).strip(),
            "dt": pd.to_datetime(row.get("Message Date"), errors="coerce"),
            "sender_role": sender_role, # Includes refined 'UNKNOWN' role
            "sender_name": raw_sender,
            "text": text,
            "has_attachment": bool(has_attachment),
            "attachment_type": attachment_type,
            "is_edited": bool(is_edited),
            "raw_row_number": idx + 1
        }
        records.append(msg)
    return records

def normalize_csv_parallel(path_in: str, path_out: str):
    """
    Splits the CSV into chunks based on CPU core count for 
    faster parallel processing.
    """
    if not os.path.exists(path_in):
        print(f"Error: Input file {path_in} not found.")
        return

    df = pd.read_csv(path_in)
    
    # Determine number of cores and split dataframe
    num_cores = multiprocessing.cpu_count()
    chunks = [df[i::num_cores] for i in range(num_cores)]
    
    print(f"Starting parallel ingestion on {num_cores} cores...")
    with multiprocessing.Pool(processes=num_cores) as pool:
        results = pool.map(process_chunk, chunks)
    
    # Flatten results from all processes
    flat_records = [item for sublist in results for item in sublist]
    out = pd.DataFrame(flat_records)
    
    # Clean and sort the final dataset
    out = out.dropna(subset=["dt"])
    out = out.sort_values(["thread_id", "dt", "raw_row_number"]).reset_index(drop=True)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(path_out), exist_ok=True)
    out.to_csv(path_out, index=False)
    print(f"Parallel OK: Wrote {path_out} | Rows: {len(out)}")

if __name__ == "__main__":
    # Standardized project paths
    normalize_csv_parallel(
        path_in="data/raw/imazing_export.csv",
        path_out="data/working/messages_normalized.csv"
    )