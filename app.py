import streamlit as st
import pandas as pd
import os
import json
import re
import plotly.express as px
from datetime import datetime

# Core Pipeline Imports
from src.ingest import normalize_csv_parallel
from src.tagger import tag_messages
from src.incidents import build_incident_report
from src.export_docx import export_to_word

# --- UI BRANDING & COLOR PALETTE ---
LEGAL_NAVY = "#002D62"
LEGAL_GOLD = "#D4AF37"
BG_LIGHT = "#F8F9FA"

# Page Configuration
st.set_page_config(
    page_title="STRICT v3.8 | Custody Texts Agent",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for a professional "Legal Suite" feel
st.markdown(f"""
    <style>
    .main {{ background-color: {BG_LIGHT}; }}
    .stButton>button {{
        background-color: {LEGAL_NAVY};
        color: white;
        border-radius: 5px;
        border: 1px solid {LEGAL_GOLD};
        font-weight: bold;
    }}
    .stDownloadButton>button {{
        background-color: #28a745;
        color: white;
        width: 100%;
    }}
    .stTabs [data-baseweb="tab-list"] {{
        gap: 8px;
        background-color: #e9ecef;
        padding: 10px;
        border-radius: 10px 10px 0 0;
    }}
    .stTabs [data-baseweb="tab"] {{
        height: 50px;
        white-space: pre-wrap;
        background-color: white;
        border-radius: 5px;
        border: 1px solid #dee2e6;
    }}
    div[data-testid="stExpander"] {{
        background-color: white;
        border: 1px solid #dee2e6;
        border-left: 5px solid {LEGAL_NAVY};
    }}
    </style>
    """, unsafe_allow_html=True)

# --- CORE LOGIC FUNCTIONS ---

def save_config(config_data, filename="data/config/last_session.json"):
    """Saves current GUI state (Identity, Keywords, Phases) to a JSON file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(config_data, f)
    st.sidebar.success("Session Saved.")

def load_config(filename="data/config/last_session.json"):
    """Loads a previously saved session configuration."""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return None

def scan_data_for_suggestions(file_path):
    """STRICT v3.8 Smart Scanner: Identity, Conflict Keywords, and Volume Shifts."""
    try:
        df = pd.read_csv(file_path).head(1000)
        all_senders = df['Sender Name'].dropna().unique().tolist()
        
        # Phone Number Detection (Regex)
        phone_pattern = r'\+?\d{1,3}?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        found_numbers = [re.search(phone_pattern, str(s)).group() for s in all_senders if re.search(phone_pattern, str(s))]
        
        # Keyword Detection
        common_triggers = ['late', 'refuse', 'court', 'order', 'denied', 'pickup', 'dropoff', 'unavailable']
        text_blob = " ".join(df['Text'].dropna().astype(str).lower())
        found_keywords = [word for word in common_triggers if word in text_blob]
        
        # Density Analysis for Agreement Shifts
        df['dt'] = pd.to_datetime(df['Date'], errors='coerce')
        v_shifts = df.groupby(df['dt'].dt.to_period('M')).size().diff().abs()
        suggested_shifts = v_shifts[v_shifts > v_shifts.mean() * 1.5].index.strftime('%Y-%m-%d').tolist() if not v_shifts.empty else []
        
        return all_senders, found_numbers, found_keywords, suggested_shifts
    except Exception as e:
        st.error(f"Scan Error: {e}")
        return [], [], [], []

# --- SIDEBAR: NAVIGATION & SESSION ---

with st.sidebar:
    st.image("https://img.icons8.com/ios-filled/100/002D62/law.png", width=80)
    st.title("STRICT v3.8")
    
    st.header("Step 1: Data Ingestion")
    uploaded_file = st.file_uploader("Upload iMazing CSV", type="csv")
    
    st.divider()
    st.header("Step 2: Session Manager")
    if st.button("💾 Save Session"):
        if 'current_config' in st.session_state:
            save_config(st.session_state['current_config'])

    if st.button("📂 Resume Session"):
        loaded = load_config()
        if loaded:
            st.session_state['suggestions'] = loaded.get('suggestions')
            st.session_state['me_names_list'] = loaded.get('me_names')
            st.session_state['custom_kw_list'] = loaded.get('keywords')
            st.session_state['phases'] = loaded.get('phases')
            st.rerun()

    st.divider()
    gap_threshold = st.slider("Radio Silence Threshold (Hours)", 24, 168, 72)
    st.caption("NJSA 9:2-4 Compliance Engine")

# --- MAIN INTERFACE ---

st.title("⚖️ Custody Texts Agent")
st.markdown("---")

if uploaded_file or 'suggestions' in st.session_state:
    if uploaded_file and 'suggestions' not in st.session_state:
        temp_path = "data/raw/temp_scan.csv"
        os.makedirs("data/raw", exist_ok=True)
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        with st.spinner("Analyzing Source Data..."):
            senders, nums, keywords, shifts = scan_data_for_suggestions(temp_path)
            st.session_state['suggestions'] = {"senders": senders, "found_numbers": nums, "keywords": keywords, "shifts": shifts}

    tab1, tab2, tab3, tab4 = st.tabs(["🚀 **COMMAND**", "⚙️ **CONFIG**", "🔍 **REVIEW**", "📈 **ANALYTICS**"])

    with tab2:
        st.subheader("Smart Config Suggestions")
        col_id, col_kw = st.columns(2)
        with col_id:
            all_ids = list(set(st.session_state['suggestions']['senders'] + st.session_state['suggestions']['found_numbers']))
            me_names_input = st.text_area("Confirm identifiers for YOU (ME):", value=st.session_state.get('me_names_list', ", ".join(all_ids)))
        with col_kw:
            default_kw = "pickup, dropoff, school, money, "
            custom_kw_input = st.text_area("Conflict Keywords:", value=st.session_state.get('custom_kw_list', default_kw + ", ".join(st.session_state['suggestions']['keywords'])))
        st.info(f"Potential Schedule Shifts: {', '.join(st.session_state['suggestions']['shifts'])}")

    with tab1:
        st.subheader("Visitation Agreement History")
        if 'phases' not in st.session_state:
            st.session_state['phases'] = [{"Label": f"Phase {i+1}", "Start Date": d, "End Date": "", "Cadence": "Custom"} for i, d in enumerate(st.session_state['suggestions']['shifts'])]
        
        phase_df = st.data_editor(pd.DataFrame(st.session_state['phases']), num_rows="dynamic", use_container_width=True)
        
        # Save state for persistence
        st.session_state['current_config'] = {
            "suggestions": st.session_state['suggestions'], "me_names": me_names_input,
            "keywords": custom_kw_input, "phases": phase_df.to_dict('records')
        }

        if st.button("🚀 EXECUTE FULL AUTOMATION"):
            raw_path = "data/raw/imazing_export.csv"
            with open(raw_path, "wb") as f: f.write(uploaded_file.getbuffer())
            with st.status("Automating Evidence Pipeline...", expanded=True) as status:
                st.write("Step 1: Normalizing Data Roles...")
                normalize_csv_parallel(raw_path, "data/working/messages_normalized.csv", me_names=[n.strip() for n in me_names_input.split(",")])
                st.write("Step 2: AI Legal Tagging...")
                df_norm = pd.read_csv("data/working/messages_normalized.csv")
                tag_messages(df_norm, custom_keywords=[k.strip() for k in custom_kw_input.split(",")]).to_csv("data/output/messages_tagged.csv", index=False)
                st.write("Step 3: Calculating Gaps & Holidays...")
                build_incident_report("data/output/messages_tagged.csv", "data/working/messages_normalized.csv", gap_threshold_hours=gap_threshold)
                status.update(label="✅ Ready for Review!", state="complete")

    with tab3:
        st.subheader("Preliminary Review")
        if os.path.exists("data/output/incident_index.csv"):
            df_review = pd.read_csv("data/output/incident_index.csv")
            for cat in df_review['category'].unique():
                with st.expander(f"📁 {cat.replace('_', ' ').upper()}", expanded=False):
                    st.data_editor(df_review[df_review['category'] == cat][['exhibit_id', 'dt', 'evidence_quote', 'reasoning']], key=f"ed_{cat}", use_container_width=True)
            
            if st.button("📋 FINALIZE CERTIFIED REPORT"):
                export_to_word("data/output/incident_index.csv", "data/output/Custody_Report.docx")
                st.balloons()
        else:
            st.info("Run automation in COMMAND tab to populate review list.")

    with tab4:
        st.subheader("Conflict Analytics")
        if os.path.exists("data/output/incident_index.csv"):
            df_v = pd.read_csv("data/output/incident_index.csv")
            df_v['dt'] = pd.to_datetime(df_v['dt'])
            gap_df = df_v[df_v['category'].str.contains("radio_silence", na=False)].copy()
            if not gap_df.empty:
                # Optimized regex to handle integers and decimals
                gap_df['hours'] = gap_df['reasoning'].str.extract(r'(\d+\.?\d*)').astype(float)
                fig = px.scatter(gap_df, x='dt', y='hours', color_discrete_sequence=[LEGAL_NAVY], size='hours', title="Gaps in Communication (Duration)")
                st.plotly_chart(fig, use_container_width=True)

    # Sidebar Download
    if os.path.exists("data/output/Custody_Report.docx"):
        with open("data/output/Custody_Report.docx", "rb") as file:
            st.sidebar.divider()
            st.sidebar.download_button("📥 DOWNLOAD REPORT", data=file, file_name=f"Custody_Summary_{datetime.now().strftime('%Y%m%d')}.docx")
else:
    st.info("Upload your source CSV to begin the automated evidence process.")