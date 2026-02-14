import pandas as pd
import requests
import json
import os
from tqdm import tqdm

# Configuration for local Ollama instance
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "mistral"

# Pre-filtering: Only trigger LLM for messages containing "incident-prone" terms
INCIDENT_KEYWORDS = [
    "pickup", "dropoff", "late", "school", "money", "visit", 
    "schedule", "order", "never", "always", "court", "refuse", 
    "doctor", "practice", "absent", "grade", "failing"
]

def get_system_prompt():
    """
    Unified system prompt aligned with NJSA 9:2-4 (Best Interests of the Child).
    Includes Special Evidence Detection for attachments.
    """
    return """You are a legal assistant specializing in New Jersey family law (NJSA 9:2-4). 
    Analyze the conversation window (the last 10-15 messages) for incidents relevant to the NJ Best Interests of the Child factors.

    SPECIAL EVIDENCE DETECTION (ATTACHMENTS):
    - If a message includes an attachment (e.g., 'image/jpeg', 'application/pdf'), analyze the text to see if it represents:
        1. Evidence of Well-being: Photos of children happy, healthy, or at events.
        2. Evidence of Medical Issues: Photos of prescriptions, injuries, or medical reports.
        3. Educational Evidence: Photos of report cards or school notices.

    HEAVY-HITTING LEGAL CATEGORIES:
    1. interference_with_communication: Efforts to block or prevent communication with the child or receiving vital info.
    2. parental_alienation_behavior: Messages intended to disparage the other parent or manipulate the child's perception.
    3. refusal_to_cooperate: Regarding the 'parents' ability to agree, communicate and cooperate' (NJSA 9:2-4a).
    4. parenting_time_interference: Violations of schedules or making access conditional on non-custody issues.
    5. stability_safety_concerns: Evidence of an unstable environment, neglect, or unwillingness to accept custody.
    6. gaslighting_and_avoidance: Contradicting facts or denying receipt of info to obstruct co-parenting.
    7. medical_or_wellbeing_evidence: Documentation of child's health or safety via text or attachments.

    Return ONLY a JSON list of objects for messages sent by 'THEM' that qualify.
    Format: [{"raw_row_number": X, "category": "...", "reasoning": "...", "evidence_quote": "...", "attachment_context": "..."}]
    """

def is_high_probability(text):
    """
    Simple keyword pre-filter to bulk-discard noise.
    """
    text_lower = str(text).lower()
    return any(kw in text_lower for kw in INCIDENT_KEYWORDS)

def tag_messages(df, window_size=15):
    """
    Combined logic: 
    - Filters for 'THEM' and high-probability keywords (Efficiency).
    - Maintains 15-message context for thread-awareness (Accuracy).
    - Injects attachment metadata into the prompt.
    """
    tagged_results = []
    
    for i in tqdm(range(len(df))):
        current_msg = df.iloc[i]
        
        # Efficiency 1: Only process messages from 'THEM'
        if current_msg['sender_role'] != 'THEM':
            continue
        
        # Efficiency 2: Skip messages that don't match incident keywords OR don't have attachments
        # (We always process attachments as they are high-value evidence)
        if not is_high_probability(current_msg['text']) and not current_msg['has_attachment']:
            continue
            
        # Accuracy: Capture preceding context to identify patterns
        start_idx = max(0, i - window_size)
        context_window = df.iloc[start_idx : i + 1]
        
        conversation_history = ""
        for _, row in context_window.iterrows():
            role = "ME" if row['sender_role'] == "ME" else "THEM"
            conversation_history += f"[{role}]: {row['text']}\n"

        # Inject attachment metadata so the AI "sees" the file type
        attachment_info = f" [Attachment Type: {current_msg['attachment_type']}]" if current_msg['has_attachment'] else ""

        prompt = (
            f"Analyze for NJSA 9:2-4 compliance based on the history provided:\n"
            f"--- CONVERSATION HISTORY ---\n{conversation_history}\n"
            f"--- TARGET MESSAGE ---\n"
            f"Row: {current_msg['raw_row_number']}\n"
            f"Text: {current_msg['text']}{attachment_info}"
        )
        
        payload = {
            "model": MODEL_NAME,
            "prompt": f"{get_system_prompt()}\n\n{prompt}",
            "stream": False,
            "format": "json",
            "options": {"temperature": 0}
        }

        try:
            response = requests.post(OLLAMA_URL, json=payload)
            response.raise_for_status()
            result_raw = response.json().get('response', '[]')
            
            incidents = json.loads(result_raw)
            if isinstance(incidents, list):
                tagged_results.extend(incidents)
                
        except Exception as e:
            print(f"Error at row {current_msg['raw_row_number']}: {e}")

    return pd.DataFrame(tagged_results)

if __name__ == "__main__":
    input_file = "data/working/messages_normalized.csv"
    output_file = "data/output/messages_tagged.csv"
    
    if os.path.exists(input_file):
        df_input = pd.read_csv(input_file)
        print(f"Loaded {len(df_input)} messages. Starting pre-filtered tagging with attachment analysis...")
        
        results_df = tag_messages(df_input)
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        results_df.to_csv(output_file, index=False)
        print(f"Tagging complete. {len(results_df)} potential incidents saved to {output_file}.")
    else:
        print(f"Input file not found. Please run ingest.py first.")