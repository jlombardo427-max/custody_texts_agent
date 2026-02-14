import pandas as pd
import requests
import json
import os
import re
import pytesseract
from PIL import Image
from tqdm import tqdm

# Configuration for local Ollama instance
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "mistral"

# If Tesseract is not in your PATH, uncomment and set the line below:
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# SELF-IDENTIFICATION PATTERNS (Regex) for Authentication
SELF_ID_PATTERNS = [
    r"(?i)\bthis is\s+([A-Z][a-z]+)",                # "This is [Name]"
    r"(?i)\byo it's\s+([A-Z][a-z]+)",                # "Yo it's [Name]"
    r"(?i)\bhey.*i'm\s+([A-Z][a-z]+)",               # "Hey, I'm [Name]"
    r"(?i)\bthis is my new (phone|number)",          # "This is my new phone"
    r"(?i)\bhey.*it is\s+([A-Z][a-z]+)"               # "Hey, it is [Name]"
]

# PRE-PASS CONFIGURATION: High-probability legal anchors
INCIDENT_KEYWORDS = [
    "pickup", "dropoff", "late", "school", "money", "visit", 
    "schedule", "order", "never", "always", "court", "refuse", 
    "doctor", "practice", "absent", "grade", "failing", "support"
]

# PRE-PASS CONFIGURATION: Emotional/Conflict sentiment anchors
SENTIMENT_ANCHORS = [
    "!", "angry", "lie", "lied", "liar", "ignore", "stop", 
    "enough", "don't", "wont", "won't", "refuse", "judge"
]

def perform_ocr(image_path):
    """Extracts text from an image file using Tesseract."""
    try:
        if not os.path.exists(image_path):
            return "[Error: Image file not found]"
        img = Image.open(image_path)
        extracted_text = pytesseract.image_to_string(img)
        return extracted_text.strip()
    except Exception as e:
        return f"[Error during OCR: {str(e)}]"

def detect_self_identification(text):
    """Uses regex to check if the sender is self-identifying."""
    for pattern in SELF_ID_PATTERNS:
        match = re.search(pattern, text)
        if match:
            return match.group(1) if len(match.groups()) > 0 else "Self-Identified"
    return None

def run_pre_pass_filter(text, has_attachment, self_id_found):
    """Lightweight Sentiment + Keyword filter to bulk-discard noise."""
    if has_attachment or self_id_found:
        return True
    text_lower = str(text).lower()
    has_keyword = any(kw in text_lower for kw in INCIDENT_KEYWORDS)
    has_sentiment = any(anchor in text_lower for anchor in SENTIMENT_ANCHORS)
    return has_keyword or has_sentiment

def get_system_prompt():
    """Unified system prompt aligned with NJSA 9:2-4."""
    return """You are a legal assistant specializing in New Jersey family law (NJSA 9:2-4). 
    Analyze the conversation window for custody incidents and authentication markers.

    SPECIAL CATEGORY:
    - authentication_identity_marker: When the sender explicitly identifies themselves.

    SPECIAL EVIDENCE DETECTION (ATTACHMENTS):
    - If a message includes an attachment (e.g., 'image/jpeg', 'application/pdf'), analyze the text to see if it represents:
        1. Evidence of Well-being: Photos of children happy, healthy, or at events.
        2. Evidence of Medical Issues: Photos of prescriptions, injuries, or medical reports.
        3. Educational Evidence: Photos of report cards or school notices.

    HEAVY-HITTING LEGAL CATEGORIES:
    1. interference_with_communication: Blocked communication or withheld info.
    2. parental_alienation_behavior: Disparaging the other parent or manipulation.
    3. refusal_to_cooperate: Lack of cooperation in child-related matters (NJSA 9:2-4a).
    4. parenting_time_interference: Schedule violations or conditional access.
    5. stability_safety_concerns: Unstable environment or neglect.
    6. gaslighting_and_avoidance: Contradicting facts or obstructing co-parenting.
    7. medical_or_wellbeing_evidence: Documentation of health/safety.

    Return ONLY a JSON list. 
    Format: [{"raw_row_number": X, "category": "...", "reasoning": "...", "evidence_quote": "...", "attachment_context": "..."}]
    """

def tag_messages(df, window_size=15):
    tagged_results = []
    
    for i in tqdm(range(len(df))):
        current_msg = df.iloc[i]
        
        # Only process messages from the other party
        if current_msg['sender_role'] != 'THEM':
            continue
        
        # Detection passes
        identified_name = detect_self_identification(current_msg['text'])
        has_attachment = current_msg.get('has_attachment', False)
        
        # Skip noise if no ID, no attachment, and no keywords/sentiment
        if not run_pre_pass_filter(current_msg['text'], has_attachment, bool(identified_name)):
            continue

        # Perform OCR if image exists
        ocr_text = ""
        if has_attachment and "image" in str(current_msg.get('attachment_type', '')):
            # Assumes images are stored in a standard data directory
            image_path = os.path.join("data/raw/attachments", str(current_msg.get('attachment_filename', '')))
            ocr_text = perform_ocr(image_path)

        # Context window for thread-awareness
        start_idx = max(0, i - window_size)
        context_window = df.iloc[start_idx : i + 1]
        
        conversation_history = ""
        for _, row in context_window.iterrows():
            role = "ME" if row['sender_role'] == "ME" else "THEM"
            conversation_history += f"[{role}]: {row['text']}\n"

        # Metadata Injection
        metadata_info = ""
        if identified_name:
            metadata_info += f" [PRE-CHECK: Self-ID Detected as '{identified_name}']"
        if has_attachment:
            metadata_info += f" [Attachment Type: {current_msg.get('attachment_type')}]"
        if ocr_text:
            metadata_info += f" [OCR CONTENT: {ocr_text}]"

        prompt = (
            f"Analyze for NJSA 9:2-4 compliance and authentication:\n"
            f"--- CONVERSATION HISTORY ---\n{conversation_history}\n"
            f"--- TARGET MESSAGE ---\n"
            f"Row: {current_msg['raw_row_number']}\n"
            f"Text: {current_msg['text']}{metadata_info}"
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
        results_df = tag_messages(df_input)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        results_df.to_csv(output_file, index=False)
        print(f"Tagging complete. {len(results_df)} incidents identified.")