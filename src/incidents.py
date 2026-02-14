import pandas as pd
import os

def build_incident_report(tagged_path, normalized_path, gap_threshold_hours=72):
    """
    Combined Incident Builder:
    - Merges LLM-tagged incidents with original message metadata.
    - Detects communication gaps (Radio Silence) exceeding a specific threshold.
    - Assigns formal Exhibit IDs for legal referencing.
    """
    # Load tagged incidents and normalized messages
    tags = pd.read_csv(tagged_path)
    messages = pd.read_csv(normalized_path)
    messages['dt'] = pd.to_datetime(messages['dt'])
    
    # 1. Chronological Gap Detection (Accuracy)
    # Identifies long periods of silence which can indicate withholding or lack of cooperation
    messages = messages.sort_values('dt')
    messages['time_diff'] = messages['dt'].diff().dt.total_seconds() / 3600
    
    gaps = messages[messages['time_diff'] > gap_threshold_hours].copy()
    gaps['category'] = "communication_gap_radio_silence"
    gaps['reasoning'] = gaps['time_diff'].apply(lambda x: f"Radio silence detected: {x:.1f} hours of no communication.")
    gaps['evidence_quote'] = "[NO COMMUNICATION RECORDED]"

    # 2. Combine Tagged Incidents with Detected Gaps
    # Ensure gaps and tagged incidents are merged into one chronological list
    report = pd.concat([tags.merge(messages, on="raw_row_number"), gaps], ignore_index=True)
    report = report.sort_values(by="dt")

    # 3. Exhibit Numbering (NJ Court Readiness)
    # Assigns a unique Exhibit ID to every flagged incident or gap
    report['exhibit_id'] = [f"Exhibit A-{i+1:03d}" for i in range(len(report))]
    
    # Final cleanup: Remove helper columns used for calculation
    if 'time_diff' in report.columns:
        report = report.drop(columns=['time_diff'])
    
    output_path = "data/output/incident_index.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    report.to_csv(output_path, index=False)
    
    print(f"Created {len(report)} incident cards, including radio silence gaps and Exhibit IDs.")

if __name__ == "__main__":
    # Standardized paths from project structure
    build_incident_report(
        tagged_path="data/output/messages_tagged.csv", 
        normalized_path="data/working/messages_normalized.csv"
    )