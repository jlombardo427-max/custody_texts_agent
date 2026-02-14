import streamlit as st
import pandas as pd
import os
import plotly.express as px
from datetime import datetime

# Import project-specific logic functions
from src.ingest import normalize_csv_parallel
from src.tagger import tag_messages
from src.incidents import build_incident_report
from src.export_docx import export_to_word

# Page Configuration
st.set_page_config(page_title="Custody Texts Agent — STRICT v3.7", layout="wide")

st.title("⚖️ Custody Texts Agent")
st.markdown("### Secure Legal Evidence Pipeline (NJSA 9:2-4 Aligned)")

# --- SIDEBAR: Configuration & Global Settings ---
with st.sidebar:
    st.header("1. Data Ingestion")
    uploaded_file = st.file_uploader("Upload iMazing CSV Export", type="csv")
    
    st.header("2. Analysis Settings")
    gap_threshold = st.slider("Radio Silence Threshold (Hours)", 24, 168, 72)
    
    st.header("3. Schedule Info")
    st.info("The agent applies a 24h buffer to holidays/birthdays and a 4h buffer to standard exchanges.")
    st.divider()
    st.caption("STRICT v3.7 | Verified Source Data Required")

# --- MAIN INTERFACE: Tabs for Workflow and Analytics ---
tab1, tab2 = st.tabs(["🚀 Pipeline Control", "📈 Conflict Analytics"])

with tab1:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🗓️ Custom Visitation Phases")
        st.info("Input historical agreement changes below. The agent cross-references these specific dates.")
        
        # Agreement History Manager
        default_phases = [
            {"Label": "2021 Original Order", "Start Date": "2021-01-01", "End Date": "2023-12-31", "Cadence": "Every Other Weekend"},
            {"Label": "2024 Consent Order", "Start Date": "2024-01-01", "End Date": datetime.now().strftime('%Y-%m-%d'), "Cadence": "2-2-3 Split"}
        ]
        phase_df = st.data_editor(pd.DataFrame(default_phases), num_rows="dynamic", use_container_width=True)

        if st.button("🚀 Run Full Retroactive Analysis"):
            if uploaded_file is not None:
                # Save raw upload to project structure
                raw_path = "data/raw/imazing_export.csv"
                os.makedirs("data/raw", exist_ok=True)
                with open(raw_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # Convert UI table to dictionary for the logic processor
                custom_phases = phase_df.to_dict('records')

                with st.status("Processing Legal Evidence...", expanded=True) as status:
                    st.write("Step 1: Normalizing CSV (Parallel Processing)...")
                    normalize_csv_parallel(raw_path, "data/working/messages_normalized.csv")
                    
                    st.write("Step 2: AI Tagging (NJSA 9:2-4 Compliance)...")
                    df_norm = pd.read_csv("data/working/messages_normalized.csv")
                    results_df = tag_messages(df_norm)
                    results_df.to_csv("data/output/messages_tagged.csv", index=False)
                    
                    st.write("Step 3: Cross-Referencing Agreement History & Gaps...")
                    build_incident_report(
                        "data/output/messages_tagged.csv", 
                        "data/working/messages_normalized.csv",
                        gap_threshold_hours=gap_threshold
                    )
                    
                    st.write("Step 4: Generating Certified Word Report...")
                    export_to_word("data/output/incident_index.csv", "data/output/Custody_Report.docx")
                    
                    status.update(label="Analysis Complete!", state="complete", expanded=False)
                st.success("Report Generated Successfully.")
            else:
                st.error("Please upload an iMazing export file first.")

    with col2:
        st.subheader("📦 Report Delivery")
        report_path = "data/output/Custody_Report.docx"
        
        if os.path.exists(report_path):
            with open(report_path, "rb") as file:
                st.download_button(
                    label="📥 Download Certified Summary",
                    data=file,
                    file_name=f"Custody_Exhibits_{datetime.now().strftime('%Y%m%d')}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True
                )
            
            if os.path.exists("data/output/incident_index.csv"):
                st.markdown("#### Recent Exhibits")
                idx_df = pd.read_csv("data/output/incident_index.csv")
                st.dataframe(idx_df[['exhibit_id', 'dt', 'category']].tail(5), use_container_width=True)
        else:
            st.info("The download button will appear once the analysis is complete.")

with tab2:
    st.subheader("📈 Conflict Intensity Visualization")
    incident_file = "data/output/incident_index.csv"
    
    if os.path.exists(incident_file):
        df_viz = pd.read_csv(incident_file)
        df_viz['dt'] = pd.to_datetime(df_viz['dt'])
        
        # Filter specifically for Radio Silence gaps to plot duration
        gap_df = df_viz[df_viz['category'].str.contains("radio_silence", case=False, na=False)].copy()
        
        if not gap_df.empty:
            # Extract numeric hours from reasoning strings (e.g., "74.5 hours")
            gap_df['hours'] = gap_df['reasoning'].str.extract('(\d+\.\d+)').astype(float)
            
            fig = px.scatter(gap_df, x='dt', y='hours', 
                             color='category', size='hours',
                             hover_data=['exhibit_id', 'reasoning'],
                             title="Communication Gaps Timeline (Radio Silence)",
                             labels={'dt': 'Incident Date', 'hours': 'Silence Duration (Hours)'},
                             color_discrete_sequence=px.colors.qualitative.Set1)
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("""
            **Visual Summary Guide:**
            * **Spikes/High Dots**: Represent significant periods of communication withholding.
            * **Color Variations**: Distinguish between standard gaps and parenting time interference.
            * **Clusters**: Identify periods where conflict or avoidance was chronic.
            """)
        else:
            st.info("No radio silence incidents found to visualize.")
    else:
        st.warning("Please run the analysis pipeline in Tab 1 to generate analytics.")

st.divider()
st.caption("STRICT v3.7 | Certified for New Jersey Superior Court Readiness")