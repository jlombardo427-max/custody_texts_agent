from docx import Document
from docx.shared import Pt
import pandas as pd
import os

def export_to_word(incident_csv, output_path):
    """
    STRICT v3.2 Verified Word Export with Summary Index:
    - PASS 1: Generates a Table of Contents (Summary Index) for quick judicial review.
    - PASS 2: Generates detailed Incident Cards with verified metadata.
    - Injects Row # and Message ID for N.J.R.E. 901 authentication.
    """
    if not os.path.exists(incident_csv):
        print(f"Error: {incident_csv} not found. Ensure incidents.py has run successfully.")
        return

    df = pd.read_csv(incident_csv)
    # Ensure chronological order for the report
    if 'dt' in df.columns:
        df['dt'] = pd.to_datetime(df['dt'])
        df = df.sort_values(by='dt')

    doc = Document()
    
    # --- DOCUMENT HEADING ---
    doc.add_heading('Custody Incident Log - Certified Legal Summary', 0)
    
    # --- PASS 1: EXHIBIT INDEX SUMMARY (TOC) ---
    # This section allows a judge to see the pattern of behavior at a glance.
    doc.add_heading('Exhibit Index Summary', level=1)
    
    # Create table for TOC
    table = doc.add_table(rows=1, cols=3)
    table.style = 'Table Grid'
    
    # Set Header Cells
    hdr_cells = table.rows[0].cells
    hdr_cells[0].text = 'Exhibit ID'
    hdr_cells[1].text = 'Date'
    hdr_cells[2].text = 'Legal Category'
    
    # Formatting headers to be bold
    for cell in hdr_cells:
        for paragraph in cell.paragraphs:
            run = paragraph.runs[0]
            run.bold = True

    # Populate Summary Index rows
    for _, row in df.iterrows():
        row_cells = table.add_row().cells
        row_cells[0].text = str(row['exhibit_id'])
        row_cells[1].text = str(row['dt'].strftime('%Y-%m-%d %H:%M') if isinstance(row['dt'], pd.Timestamp) else row['dt'])
        row_cells[2].text = str(row['category']).upper().replace('_', ' ')

    # Add a page break so the cards start on a fresh page
    doc.add_page_break()

    # --- PASS 2: DETAILED INCIDENT CARDS ---
    doc.add_heading('Detailed Exhibit Evidence', level=1)

    for _, row in df.iterrows():
        # 1. Exhibit Header (e.g., EXHIBIT A-001)
        p = doc.add_paragraph()
        exhibit_run = p.add_run(f"{str(row['exhibit_id']).upper()}")
        exhibit_run.bold = True
        exhibit_run.font.size = Pt(14)
        
        # 2. Metadata Block
        doc.add_paragraph(f"Date/Time: {row['dt']}")
        doc.add_paragraph(f"Legal Category: {str(row['category']).upper().replace('_', ' ')}")
        
        # 3. Authenticity Footer (N.J.R.E. 901 Paper Trail)
        metadata = doc.add_paragraph()
        meta_run = metadata.add_run(
            f"VERIFIED SOURCE DATA | Source Row: {row['raw_row_number']} | Unique Message ID: {row['message_id']}"
        )
        meta_run.font.size = Pt(8)
        meta_run.italic = True

        # 4. Evidence Text (Italicized blockquote style)
        msg_box = doc.add_paragraph()
        msg_box.add_run(f"Evidence Text: \"{row['text']}\"").italic = True
        
        # 5. Analysis (AI Legal Reasoning)
        doc.add_paragraph(f"Analysis: {row['reasoning']}")
        
        # 6. Visual Divider
        doc.add_paragraph("_" * 50)

    # Ensure output directory exists and save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    doc.save(output_path)
    print(f"Final Certified Report with Index generated: {output_path}")

if __name__ == "__main__":
    # Standardized project paths
    export_to_word(
        incident_csv="data/output/incident_index.csv", 
        output_path="data/output/Custody_Exhibits_Final_Report.docx"
    )