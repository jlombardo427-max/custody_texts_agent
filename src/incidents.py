import pandas as pd
import os
from datetime import datetime, timedelta

# --- CRITICAL CUSTODY DATES (One-off Incidents) ---
CRITICAL_CUSTODY_DATES = [
    {"start": "2025-05-07 00:00:00", "end": "2025-05-10 23:59:59", "event": "Missed Return Date / Judicial Order"}
]

def get_holiday_list(year):
    """
    Generates the specific holiday rotation based on the Standard Schedule.
    Father: Even Years Group 1, Odd Years Group 2[cite: 5, 6, 15].
    """
    is_even = (year % 2 == 0)
    
    # Rotation Logic: Father (Joe) even years, Mother (Kirby) odd years [cite: 5, 6, 15]
    group_1_owner = "Father" if is_even else "Mother"
    group_2_owner = "Mother" if is_even else "Father"

    holidays = [
        # --- GROUP 1 [cite: 7, 9, 10, 11, 12, 13, 14] ---
        {"start": f"{year}-01-01 12:00:00", "end": f"{year}-01-01 19:00:00", "event": f"New Year's Day ({group_1_owner})"},
        {"start": f"{year}-02-16 09:00:00", "end": f"{year}-02-16 19:00:00", "event": f"President's Day ({group_1_owner})"},
        {"start": f"{year}-04-03 09:00:00", "end": f"{year}-04-04 18:00:00", "event": f"Good Friday/Easter Sat ({group_1_owner})"},
        {"start": f"{year}-05-25 09:00:00", "end": f"{year}-05-25 19:00:00", "event": f"Memorial Day ({group_1_owner})"},
        {"start": f"{year}-09-07 09:00:00", "end": f"{year}-09-07 19:00:00", "event": f"Labor Day ({group_1_owner})"},
        {"start": f"{year}-11-26 16:00:00", "end": f"{year}-11-27 18:00:00", "event": f"Thanksgiving Day/Fri ({group_1_owner})"},
        {"start": f"{year}-12-24 17:00:00", "end": f"{year}-12-25 12:00:00", "event": f"Christmas Eve/Day ({group_1_owner})"},
        {"start": f"{year}-06-21 09:00:00", "end": f"{year}-06-21 19:00:00", "event": "Father's Day"},

        # --- GROUP 2 [cite: 16, 17, 18, 19, 20, 21, 22] ---
        {"start": f"{year}-12-31 18:00:00", "end": f"{year+1}-01-01 12:00:00", "event": f"New Year's Eve ({group_2_owner})"},
        {"start": f"{year}-01-19 09:00:00", "end": f"{year}-01-19 19:00:00", "event": f"MLK Day ({group_2_owner})"},
        {"start": f"{year}-04-04 18:00:00", "end": f"{year}-04-05 18:00:00", "event": f"Easter Sat/Sun ({group_2_owner})"},
        {"start": f"{year}-07-04 09:00:00", "end": f"{year}-07-04 19:00:00", "event": f"Independence Day ({group_2_owner})"},
        {"start": f"{year}-10-12 09:00:00", "end": f"{year}-10-12 19:00:00", "event": f"Columbus Day ({group_2_owner})"},
        {"start": f"{year}-11-25 18:00:00", "end": f"{year}-11-26 16:00:00", "event": f"Thanksgiving Eve/Day ({group_2_owner})"},
        {"start": f"{year}-12-25 12:00:00", "end": f"{year}-12-25 19:00:00", "event": f"Christmas Day ({group_2_owner})"},
        {"start": f"{year}-05-10 09:00:00", "end": f"{year}-05-10 19:00:00", "event": "Mother's Day"},

        # --- SPECIAL DAYS [cite: 25, 26, 28] ---
        {"start": f"{year}-01-18 12:00:00", "end": f"{year}-01-18 18:00:00", "event": "Cameron's Birthday"},
        {"start": f"{year}-08-01 12:00:00", "end": f"{year}-08-01 18:00:00", "event": "Carter's Birthday"},
        {"start": f"{year}-04-27 12:00:00", "end": f"{year}-04-27 19:00:00", "event": "Father's Birthday (Joe)"},
        {"start": f"{year}-12-15 12:00:00", "end": f"{year}-12-15 19:00:00", "event": "Mother's Birthday (Kirby)"},
        {"start": f"{year}-10-31 16:00:00", "end": f"{year}-10-31 18:00:00", "event": "Halloween"}
    ]
    # Ensure priority flag for 24h buffer 
    for h in holidays: h['is_holiday'] = True
    return holidays

# Generate Retroactive Master Schedule 2019-2026
MASTER_SCHEDULE = []
for yr in range(2019, 2027):
    MASTER_SCHEDULE.extend(get_holiday_list(yr))

def check_schedule_overlap(gap_time):
    """
    Applies the 24h buffer to all holidays/birthdays since 2019.
    Standard critical dates get a standard check.
    """
    # Check One-off Critical Dates first
    for period in CRITICAL_CUSTODY_DATES:
        if pd.to_datetime(period["start"]) <= gap_time <= pd.to_datetime(period["end"]):
            return f"OVERLAP WITH: {period['event']}"

    # Check Master Holiday Schedule with 24h Buffer 
    for period in MASTER_SCHEDULE:
        start = pd.to_datetime(period["start"])
        end = pd.to_datetime(period["end"])
        if (start - timedelta(hours=24)) <= gap_time <= (end + timedelta(hours=24)):
            return f"OVERLAP WITH: {period['event']} (24h Buffer)"
    return None

def build_incident_report(tagged_path, normalized_path, gap_threshold_hours=72):
    """STRICT v3.7 Chronological Indexer with Retroactive awareness."""
    if not os.path.exists(tagged_path) or not os.path.exists(normalized_path):
        print("Error: Input files missing.")
        return

    tags = pd.read_csv(tagged_path)
    messages = pd.read_csv(normalized_path)
    messages['dt'] = pd.to_datetime(messages['dt'])
    
    # 1. Chronological Gap Detection
    messages = messages.sort_values('dt')
    messages['time_diff'] = messages['dt'].diff().dt.total_seconds() / 3600
    gaps = messages[messages['time_diff'] > gap_threshold_hours].copy()
    
    # 2. Cross-Reference Gaps with Parenting Schedule 
    gaps['schedule_overlap'] = gaps['dt'].apply(check_schedule_overlap)
    
    # 3. Categorization & Elevation
    gaps['category'] = gaps['schedule_overlap'].apply(
        lambda x: "PARENTING_TIME_INTERFERENCE_RADIO_SILENCE" if x else "communication_gap_radio_silence"
    )
    gaps['reasoning'] = gaps.apply(
        lambda r: f"Silence of {r['time_diff']:.1f} hours. {r['schedule_overlap'] or ''}", axis=1
    )
    gaps['evidence_quote'] = "[NO COMMUNICATION RECORDED DURING THIS PERIOD]"

    # 4. Final Merge & Exhibit Numbering
    report = pd.concat([tags.merge(messages, on="raw_row_number"), gaps], ignore_index=True)
    report = report.sort_values(by="dt")
    report['exhibit_id'] = [f"Exhibit A-{i+1:03d}" for i in range(len(report))]
    
    # Cleanup helper columns
    report = report.drop(columns=['time_diff', 'schedule_overlap'], errors='ignore')
    
    os.makedirs("data/output", exist_ok=True)
    report.to_csv("data/output/incident_index.csv", index=False)
    print(f"Build Complete: 2019-2026 dataset generated with 24h buffers.")

if __name__ == "__main__":
    build_incident_report("data/output/messages_tagged.csv", "data/working/messages_normalized.csv")