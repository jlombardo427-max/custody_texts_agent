import pandas as pd
import os

# Define known critical dates/schedules from your court orders
# Example: The missed return date of May 9, 2025 identified in your records
CRITICAL_CUSTODY_DATES = [
    {"start": "2025-05-07", "end": "2025-05-10", "event": "Missed Return Date / Judicial Order"}
]

def build_incident_report(tagged_path, normalized_path, gap_threshold_hours=72):
    """
    Combined Incident Builder:
    - Merges LLM-tagged incidents with original message metadata.
    - Detects communication gaps (Radio Silence) exceeding a specific threshold.
    - Cross-references gaps with court-ordered parenting schedules.
    - Assigns formal Exhibit IDs for legal referencing.
    """
    # Load tagged incidents and normalized messages
    tags = pd.read_csv(tagged_path)
    messages = pd.read_csv(normalized_path)
    messages['dt'] = pd.to_datetime(messages['dt'])
    
    # 1. Chronological Gap Detection (Accuracy)
    messages = messages.sort_values('dt')
    messages['time_diff'] = messages['dt'].diff().dt.total_seconds() / 3600
    
    gaps = messages[messages['time_diff'] > gap_threshold_hours].copy()
    
    # 2. Cross-Reference Gaps with Parenting Schedule
    def check_schedule_overlap(gap_time):
        for period in CRITICAL_CUSTODY_DATES:
            start = pd.to_datetime(period["start"])
            end = pd.to_datetime(period["end"])
            if start <= gap_time <= end:
                return f"OVERLAP WITH: {period['event']}"
        return None

    gaps['schedule_overlap'] = gaps['dt'].apply(check_schedule_overlap)
    
    # 3. Elevate Categories for Gaps that overlap with custody dates
    # This turns a technical gap into high-value evidence of interference
    gaps['category'] = gaps['schedule_overlap'].apply(
        lambda x: "PARENTING_TIME_INTERFERENCE_RADIO_SILENCE" if x else "communication_gap_radio_silence"
    )
    
    gaps['reasoning'] = gaps.apply(
        lambda r: f"Silence of {r['time_diff']:.1f} hours detected. {r['schedule_overlap'] or ''}", axis=1
    )
    gaps['evidence_quote'] = "[NO COMMUNICATION RECORDED DURING CUSTODY EXCHANGE WINDOW]"

    # 4. Combine Tagged Incidents with Detected Gaps
    report = pd.concat([tags.merge(messages, on="raw_row_number"), gaps], ignore_index=True)
    report = report.sort_values(by="dt")

    # 5. Exhibit Numbering (NJ Court Readiness)
    report['exhibit_id'] = [f"Exhibit A-{i+1:03d}" for i in range(len(report))]
    
    # Final cleanup: Remove helper columns used for calculation
    if 'time_diff' in report.columns:
        report = report.drop(columns=['time_diff'])
    if 'schedule_overlap' in report.columns:
        report = report.drop(columns=['schedule_overlap'])
    
    output_path = "data/output/incident_index.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    report.to_csv(output_path, index=False)
    
    print(f"Created {len(report)} incident cards, including radio silence gaps, schedule cross-referencing, and Exhibit IDs.")

if __name__ == "__main__":
    # Standardized paths from project structure
    build_incident_report(
        tagged_path="data/output/messages_tagged.csv", 
        normalized_path="data/working/messages_normalized.csv"
    )