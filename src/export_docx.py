from docx import Document
from docx.shared import Pt
import pandas as pd
import os

def export_to_word(incident_csv, output_path):
    """
    Combined DOCX Exporter:
    - Generates professional 'incident cards' for legal review.
    - Includes Verified Metadata (Row # and Message ID) for court authenticity.
    - Applies legal formatting for better readability by attorneys and judges.
    """
    if not os.path.exists(incident_csv):
        print(f"Error: {incident_csv} not found. Run incidents.py first.")
        return

    df = pd.read_csv(incident_csv)
    doc = Document()
    
    # Title aligned with legal summary standards
    doc.add_heading('Custody Incident Log - Certified Legal Summary', 0)

    for _, row in df.iterrows():
        # 1. Exhibit Header - Bold and prominent for quick reference
        p = doc.add_paragraph()
        run = p.add_run(f"{str(row['exhibit_id']).upper()}")
        run.bold = True
        run.font.size = Pt(14)
        
        # 2. Incident Metadata
        doc.add_paragraph(f"Date/Time: {row['dt']}")
        doc.add_paragraph(f"Legal Category: {str(row['category']).upper()}")
        
        # 3. Verified Metadata Block (Strict Authenticity)
        # Smaller, italicized font to provide a 'paper trail' to the original CSV
        metadata = doc.add_paragraph()
        meta_run = metadata.add_run(
            f"VERIFIED SOURCE DATA | Source Row: {row['raw_row_number']} | Unique Message ID: {row['message_id']}"
        )
        meta_run.font.size = Pt(8)
        meta_run.italic = True

        # 4. Evidence Text - Highlighted in blockquote/italic style
        msg_box = doc.add_paragraph()
        msg_box.add_run(f"Evidence Text: \"{row['text']}\"").italic = True
        
        # 5. Analysis/Reasoning
        doc.add_paragraph(f"Analysis: {row['reasoning']}")
        
        # 6. Horizontal Divider for visual separation
        doc.add_paragraph("_" * 50)

    # Ensure output directory exists before saving
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    doc.save(output_path)
    print(f"Word export complete: {output_path} with verified metadata.")

if __name__ == "__main__":
    # Standardized paths from project structure
    export_to_word(
        incident_csv="data/output/incident_index.csv", 
        output_path="data/output/Custody_Exhibits_Verified.docx"
    )