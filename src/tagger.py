import pandas as pd
import requests
import json
import os
import re
import pytesseract
from PIL import Image
from tqdm import tqdm
from typing import Generator, List, Dict, Any

# --- CONFIGURATION ---
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "mistral"

# OCR Configuration (Critical for screenshot/document evidence)
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# Regex for N.J.R.E. 901 Authentication (Self-Identification detection)
SELF_ID_PATTERNS = [
    r"(?i)\bthis is\s+([A-Z][a-z]+)", 
    r"(?i)\byo it's\s+([A-Z][a-z]+)", 
    r"(?i)\bhey.*i'm\s+([A-Z][a-z]+)", 
    r"(?i)\bthis is my new (phone|number)"
]

# --- CORE UTILITIES ---

def perform_ocr(image_path: str) -> str:
    """Extracts text from images for AI analysis (e.g., screenshots of emails)."""
    try:
        if not os.path.exists(image_path): return ""
        return pytesseract.image_to_string(Image.open(image_path)).strip()
    except Exception: return ""

def detect_self_identification(text: str) -> str:
    """Detects if a sender identifies themselves to aid in legal authentication."""
    for pattern in SELF_ID_PATTERNS:
        match = re.search(pattern, str(text))
        if match: return match.group(1) if len(match.groups()) > 0 else "Self-Identified"
    return None

def run_pre_pass_filter(text: str, role: str, has_attachment: bool, self_id: str, custom_keywords: List[str]) -> bool:
    """Optimizes speed by skipping non-conflict small talk from 'Me'."""
    if has_attachment or self_id: return True
    if role == "Them": return True # High priority for other parent's messages
    
    text_lower = str(text).lower()
    return any(re.escape(str(kw)).lower() in text_lower for kw in custom_keywords)

def get_dynamic_system_prompt(selected_categories: List[str]) -> str:
    """Constructs the legal prompt with NJSA 9:2-4 directives and escaped JSON."""
    categories_str = "\n".join([f"- {cat}" for cat in selected_categories])
    
    return f"""You are a NJ Family Law expert specializing in NJSA 9:2-4. 
    Analyze the conversation window for custody incidents.

    STRICT CATEGORY SCOPE:
    {categories_str}
    - authentication_marker: Sender identifies self.

    SPECIAL INSTRUCTIONS:
    - feigned_ignorance: Flag instances where 'Them' claims lack of knowledge or 
      forgetfulness of facts/orders previously communicated in the history.

    LEGAL DIRECTIVES:
    1. Analyze the 'Target Message' using 'History' for context.
    2. If sender is 'Them', prioritize hostility, interference, or ignorance.
    3. If sender is 'Me', prioritize coordination attempts.
    
    RETURN ONLY A JSON LIST: 
    [[{{"raw_row_number": X, "category": "...", "reasoning": "...", "evidence_quote": "..."}}]]
    """

# --- THE STREAMING TAGGER ---

def tag_messages_streamer(
    df: pd.DataFrame, 
    custom_keywords: List[str], 
    selected_categories: List[str], 
    intensity: str = "Balanced", 
    window_size: int = 10
) -> Generator[Dict[str, Any], None, None]:
    """
    STRICT v3.8.5: Combined Generator.
    Yields each incident as it's processed to update the Live Reasoning Log in real-time.
    """
    # Map UI slider to AI Temperature
    temp_map = {"Conservative": 0.0, "Balanced": 0.3, "Aggressive": 0.7}
    temp = temp_map.get(intensity, 0.3)
    
    # Pre-filter for conflict density (Efficiency Gating)
    kw_pattern = '|'.join([re.escape(str(k)) for k in custom_keywords])
    mask = (df['sender_role'] == 'Them') | \
           (df['text'].str.contains(kw_pattern, case=False, na=False)) | \
           (df['has_attachment'] == True)
    
    target_indices = df[mask].index

    for i in target_indices:
        current_msg = df.loc[i]
        role = current_msg['sender_role']
        text = current_msg['text']
        has_attachment = current_msg.get('has_attachment', False)
        self_id = detect_self_identification(text)

        # Skip noise
        if not run_pre_pass_filter(text, role, has_attachment, self_id, custom_keywords):
            continue

        # Handle OCR for image attachments
        ocr_text = ""
        if has_attachment and "image" in str(current_msg.get('attachment_type', '')):
            img_path = os.path.join("data/raw/attachments", str(current_msg.get('attachment_filename', '')))
            ocr_text = perform_ocr(img_path)

        # Build Context Window
        start_idx = max(0, i - window_size)
        context_window = df.iloc[start_idx : i + 1]
        history = ""
        for _, row in context_window.iterrows():
            history += f"[{row['sender_role']}]: {row['text']}\n"

        # AI Payload
        metadata = f" [Self-ID: {self_id}]" if self_id else ""
        if ocr_text: metadata += f" [OCR CONTENT: {ocr_text}]"
        
        prompt = (
            f"--- CONVERSATION HISTORY ---\n{history}\n"
            f"--- TARGET MESSAGE ---\n"
            f"Row: {current_msg['raw_row_number']}\n"
            f"Role: {role}\n"
            f"Text: {text}{metadata}"
        )

        payload = {
            "model": MODEL_NAME,
            "prompt": f"{get_dynamic_system_prompt(selected_categories)}\n\n{prompt}",
            "stream": False,
            "format": "json",
            "options": {"temperature": temp}
        }

        try:
            response = requests.post(OLLAMA_URL, json=payload, timeout=45)
            response.raise_for_status()
            res_json = response.json().get('response', '[]')
            
            incidents = json.loads(res_json)
            if isinstance(incidents, list):
                for item in incidents:
                    # Inject timestamp and raw row for UI Live Log and Indexing
                    item['dt'] = current_msg['dt']
                    item['raw_row_number'] = current_msg['raw_row_number']
                    yield item 
        except Exception:
            continue

def tag_messages(df, custom_keywords, selected_categories, intensity="Balanced"):
    """Compatibility wrapper that converts the generator stream into a standard DataFrame."""
    results = list(tag_messages_streamer(df, custom_keywords, selected_categories, intensity))
    return pd.DataFrame(results)