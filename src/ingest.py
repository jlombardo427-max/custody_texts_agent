import pandas as pd
import numpy as np
import multiprocessing
import os
import uuid
import logging
from functools import partial

# Silence Streamlit noise in background threads to prevent "missing ScriptRunContext" warnings
logging.getLogger('streamlit.runtime.scriptrunner_utils.script_run_context').setLevel(logging.ERROR)

def process_chunk(chunk, me_names, you_names):
    """
    Processes a slice of the dataframe to extract full metadata 
    including refined role logic, attachments, and edit status.
    """
    records = []
    # Convert lists to sets for O(1) lookup speed across thousands of rows
    me_set = {str(n).strip().lower() for n in me_names}
    you_set = {str(n).strip().lower() for n in you_names}

    for idx, row in chunk.iterrows():
        raw_sender = str(row.get("Sender Name", "") if pd.notna(row.get("Sender Name")) else "").strip()
        sender_clean = raw_sender.lower()
        
        # --- IMPROVED ROLE LOGIC: Me vs You vs Neutral ---
        if sender_clean in me_set:
            sender_role = "Me"
        elif sender_clean in you_set:
            sender_role = "Them"
        elif sender_clean in ["nan", "", "unknown"]:
            sender_role = "Unknown"
        else:
            sender_role = "Neutral"

        text = str(row.get("Text", "") if pd.notna(row.get("Text", "")) else "").strip()

        # --- EXTRACT ATTACHMENT AND EDIT METADATA ---
        # Crucial for N.J.R.E. 901 authentication in NJ courts
        has_attachment = pd.notna(row.get("Attachment")) and str(row.get("Attachment")).strip() != ""
        attachment_type = str(row.get("Attachment type", "") if pd.notna(row.get("Attachment type", "")) else "").lower().strip()
        is_edited = pd.notna(row.get("Edited Date")) and str(row.get("Edited Date")).strip() != ""

        msg = {
            "message_id": str(uuid.uuid4()),
            "thread_id": str(row.get("Chat Session", "General")).strip(),
            "dt": pd.to_datetime(row.get("Message Date"), errors="coerce"),
            "sender_role": sender_role,
            "sender_name": raw_sender,
            "text": text,
            "has_attachment": bool(has_attachment),
            "attachment_type": attachment_type,
            "is_edited": bool(is_edited),
            "raw_row_number": idx + 1
        }
        records.append(msg)
    return records

def normalize_csv_parallel(path_in: str, path_out: str, me_names: list, you_names: list):
    """
    Processes large iMazing exports using all available CPU cores.
    Integrates dynamic name lists from the GUI Configuration tab.
    """
    if not os.path.exists(path_in):
        print(f"Error: Input file {path_in} not found.")
        return False

    # Load data and determine core count
    df = pd.read_csv(path_in)
    num_cores = multiprocessing.cpu_count() or 4
    
    # Split into chunks for parallel pools
    chunks = np.array_split(df, num_cores)
    
    # Use partial to pass me_names and you_names into the pool
    worker_func = partial(process_chunk, me_names=me_names, you_names=you_names)
    
    print(f"🚀 STRICT v3.8.5 Ingestion: Processing on {num_cores} cores...")
    
    
    
    with multiprocessing.Pool(processes=num_cores) as pool:
        results = pool.map(worker_func, chunks)
    
    # Flatten records and build final DataFrame
    flat_records = [item for sublist in results for item in sublist]
    out = pd.DataFrame(flat_records)
    
    # Final data cleaning for STRICT Standards
    out = out.dropna(subset=["dt"]) # Discard rows with unparseable dates
    out = out.sort_values(["thread_id", "dt", "raw_row_number"]).reset_index(drop=True)

    # Save to Working directory
    os.makedirs(os.path.dirname(path_out), exist_ok=True)
    out.to_csv(path_out, index=False)
    print(f"✅ Ingestion Complete: {len(out)} rows processed and role-verified.")
    return True

if __name__ == "__main__":
    # Example standalone execution (for testing)
    normalize_csv_parallel(
        path_in="data/raw/imazing_export.csv",
        path_out="data/working/messages_normalized.csv",
        me_names=["Giuseppe", "Joe"],
        you_names=["Kirby", "Mom", "mommy", "kirb"]
    )