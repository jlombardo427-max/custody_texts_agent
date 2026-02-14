import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# --- 1. CRITICAL CUSTODY DATES (Specific One-off Incidents) ---
CRITICAL_CUSTODY_DATES = [
    {"start": "2025-05-07 00:00:00", "end": "2025-05-10 23:59:59", "event": "Missed Return Date / Judicial Order"}
]

# --- 2. MASTER HOLIDAY ROTATION (2019-2026) ---
def get_holiday_list(year):
    """Generates NJ-style holiday rotation: Father (Even Group 1), Mother (Odd Group 1)."""
    is_even = (year % 2 == 0)
    group_1_owner = "Father" if is_even else "Mother"
    group_2_owner = "Mother" if is_even else "Father"
    
    return [
        {"start": f"{year}-01-01 12:00:00", "end": f"{year}-01-01 19:00:00", "event": f"New Year's Day ({group_1_owner})"},
        {"start": f"{year}-12-24 17:00:00", "end": f"{year}-12-25 12:00:00", "event": f"Christmas Eve/Day ({group_1_owner})"},
        {"start": f"{year}-06-21 09:00:00", "end": f"{year}-06-21 19:00:00", "event": "Father's Day"},
        {"start": f"{year}-05-10 09:00:00", "end": f"{year}-05-10 19:00:00", "event": "Mother's Day"},
        {"start": f"{year}-01-18 12:00:00", "end": f"{year}-01-18 18:00:00", "event": "Cameron's Birthday"},
        {"start": f"{year}-08-01 12:00:00", "end": f"{year}-08-01 18:00:00", "event": "Carter's Birthday"}
    ]

# Pre-generate the full retroactive schedule
MASTER_SCHEDULE = []
for yr in range(2019, 2027):
    MASTER_SCHEDULE.extend(get_holiday_list(yr))

# --- 3. OVERLAP & BUFFER LOGIC ---
def check_schedule_overlap(gap_time, buffer_hours):
    """Checks if a gap occurred within the buffer window of a Holiday or Critical Date."""
    # Check One-off Critical Dates
    for period in CRITICAL_CUSTODY_DATES:
        if pd.to_datetime(period["start"]) <= gap_time <= pd.to_datetime(period["end"]):
            return f"CRITICAL INCIDENT: {period['event']}"

    # Check Rotating Holiday Schedule
    for period in MASTER_SCHEDULE:
        start = pd.to_datetime(period["start"])
        end = pd.to_datetime(period["end"])
        # Apply the user-defined buffer (e.g., 4h or 24h)
        if (start - timedelta(hours=buffer_hours)) <= gap_time <= (end + timedelta(hours=buffer_hours)):
            return f"Holiday Window: {period['event']} ({buffer_hours}h Buffer)"
    return None

# --- 4. MAIN REPORT BUILDER ---
def build_incident_report(tagged_path, normalized_path, gap_threshold_hours=72, buffer_hours=4):
    """
    STRICT v3.8.5 Incident Engine:
    - Thread-Aware: Prevents false gaps between different chat apps.
    - Role-Aware: Only flags 'Them' ignoring 'Me'.
    - Schedule-Aware: Contextually upgrades gaps to interference.
    """
    if not os.path.exists(tagged_path) or not os.path.exists(normalized_path):
        return None

    df_tagged = pd.read_csv(tagged_path)
    df_norm = pd.read_csv(normalized_path)
    df_norm['dt'] = pd.to_datetime(df_norm['dt'])
    
    incident_logs = []

    # Process gaps per thread (Thread-Aware)
    for thread_id, thread_df in df_norm.groupby('thread_id'):
        thread_df = thread_df.sort_values('dt')
        
        for i in range(len(thread_df) - 1):
            curr, nxt = thread_df.iloc[i], thread_df.iloc[i+1]
            
            # Trigger: Outreach from 'Me' was ignored
            if curr['sender_role'] == 'Me':
                time_gap = (nxt['dt'] - curr['dt']).total_seconds() / 3600
                
                if time_gap >= gap_threshold_hours:
                    overlap_info = check_schedule_overlap(curr['dt'], buffer_hours)
                    
                    # Logic: If outreach happened near a holiday, it's Interference
                    if overlap_info:
                        category = "PARENTING_TIME_INTERFERENCE"
                    else:
                        # Otherwise, check if it was hostile (I sent two in a row) or just a delay
                        category = "radio_silence_hostile" if nxt['sender_role'] == 'Me' else "radio_silence_delay"

                    incident_logs.append({
                        "exhibit_id": f"GAP-{curr['raw_row_number']}",
                        "dt": curr['dt'],
                        "category": category,
                        "evidence_quote": f"Communication gap of {round(time_gap, 1)} hours following outreach.",
                        "reasoning": f"Outreach ignored for {round(time_gap, 1)}h. {overlap_info or 'Standard Gap.'}",
                        "raw_row_number": curr['raw_row_number'],
                        "message_id": curr.get('message_id', 'N/A')
                    })

    # Combine AI Tagged findings with calculated Gaps
    ai_incidents = df_tagged.copy()
    ai_incidents['exhibit_id'] = "AI-" + ai_incidents['raw_row_number'].astype(str)
    
    final_report = pd.concat([pd.DataFrame(incident_logs), ai_incidents], ignore_index=True)
    final_report = final_report.sort_values('dt')
    
    # Final sequential Exhibit Numbering for the Judge
    final_report['exhibit_id'] = [f"Exhibit A-{i+1:03d}" for i in range(len(final_report))]

    os.makedirs("data/output", exist_ok=True)
    output_path = "data/output/incident_index.csv"
    final_report.to_csv(output_path, index=False)
    
    print(f"✅ Incident Report Built: {len(final_report)} exhibits indexed.")
    return output_path