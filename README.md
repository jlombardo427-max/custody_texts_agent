⚖️ Custody Texts Agent (STRICT v3.8)
The Custody Texts Agent is a specialized legal evidence pipeline for New Jersey Superior Court pro se litigants and family law attorneys. It transforms raw, unstructured SMS/iMessage data (exported via iMazing) into a Certified Legal Summary aligned with NJSA 9:2-4 "Best Interests of the Child" factors.

The system is built on a "STRICT" architecture, ensuring every exhibit is anchored to verified metadata, making it resilient to authentication challenges under N.J.R.E. 901.

🚀 Key v3.8 Features
Smart Ingestion Scanner: Automatically detects unique sender names, phone numbers (via Regex), and high-conflict keywords upon upload.

Visitation Shift Detection: Uses communication density analysis to flag potential historical changes in your custody agreement for preliminary review.

Session Persistence (Save/Load): Save your verified "Self-ID" markers, custom keywords, and visitation phases to a local JSON file to resume your work instantly.

Interactive Preliminary Review: Audit and refine AI-generated findings grouped by legal category (e.g., Radio Silence, Interference) before finalizing the report.

Automated Holiday Logic: Retroactive application of the Standard Holiday Schedule (2019–2026) with alternating priorities and strict 24-hour buffers.

Conflict Analytics: High-contrast Plotly dashboards visualize the duration and frequency of communication gaps to prove "Bad Faith" patterns.

🛠️ Project Structure
Plaintext
├── app.py                  # Streamlit GUI / Command Center (The Main App)
├── setup.py                # Automated environment & folder initialization
├── requirements.txt        # Required Python libraries
├── src/
│   ├── ingest.py           # Parallel processing & Role Identification
│   ├── tagger.py           # AI-driven NJSA 9:2-4 legal tagging
│   ├── incidents.py        # Gap & Schedule cross-referencing logic
│   └── export_docx.py      # Certified Word Report generation
├── data/
│   ├── raw/                # Raw iMazing CSV storage
│   ├── config/             # Saved Session JSON files
│   ├── working/            # Normalized intermediate data
│   └── output/             # Final Certified Reports & CSV logs
🚀 Quick Start Guide
1. Initial Setup
Run the setup script to build the folder structure and verify dependencies:

PowerShell
python setup.py
2. Launch the Command Center
Launch the interactive web interface:

PowerShell
streamlit run app.py
3. Usage Workflow
Ingest: Upload your imazing_export.csv in the sidebar.

Config: Review the Smart Suggestions. Confirm which phone numbers/names belong to YOU to ensure the "ME" vs "THEM" role logic is 100% accurate.

Command: Define your Visitation Phases. Click "Execute Full Automation".

Review: Expand the category folders in the REVIEW tab. Refine descriptions or remove irrelevant entries.

Deliver: Click "Finalize Certified Report" and download the .docx file from the sidebar.

⚖️ Evidence Standards & Admissibility
Verified Metadata: Every exhibit includes the original Row Number and Unique Message ID for technical authentication back to the source device.

Parental Cooperation: Specifically designed to highlight failures in the "ability to agree, communicate, and cooperate" as required by NJ law.

Holiday Rotation: Automatically accounts for even/odd year rotations for major holidays (Christmas, Thanksgiving, Birthdays).

🚀 Beginner-Friendly Setup Guide
If you have never used code before, follow these steps exactly to get started.

1. Install the Essentials
Python: The engine that runs the code. Download here. Crucial: Check the box that says "Add Python to PATH" during installation.

Ollama: The tool that runs the AI model locally. Download here.

2. Prepare the AI
Open your computer's terminal (PowerShell on Windows or Terminal on Mac) and run:

PowerShell
ollama pull mistral
This downloads the "Mistral" AI model to your machine for private local use.

3. Set Up the Project Folder
Download the code ZIP from GitHub and unzip it to your Desktop.

Open the folder in your terminal:

Windows: Shift + Right-click in the folder -> "Open PowerShell window here."

Mac: Right-click the folder -> "New Terminal at Folder."

4. Create a Virtual Environment (Safety Step)
This keeps the project's settings separate from your computer. Run these lines one by one:

PowerShell
python -m venv .venv
.\.venv\Scripts\activate
# On Mac/Linux, use: source .venv/bin/activate
5. Install Dependencies
PowerShell
pip install -r requirements.txt
📋 Workflow & Instructions
Step 1: Add Your Data
Place your message export file (named exactly imazing_export.csv) into the data/raw/ folder.

Step 2: Run the Analysis
You can use the Command Center (GUI) or run the scripts manually in order:

Launch GUI: streamlit run app.py

Manual Ingestion: python src/ingest.py

Manual Tagging: python src/tagger.py (May take a while).

Manual Export: python src/export_docx.py

Step 3: Identity & Keyword Tuning
Tab 2 (GUI): Enter all names/labels that identify you (e.g., "Joe, Giuseppe, Dad") to ensure the agent correctly identifies roles.

Keyword Tuning: Add specific "trigger words" from your conflict history (e.g., "refuse", "late", "doctor") to anchor the AI's legal analysis.

Define Phases: Input historical agreement dates so the agent only flags "Interference" during legally active windows.

📄 Understanding Your Final Report
The Custody_Exhibits_Final_Report.docx is structured to follow N.J.R.E. 901 standards and NJSA 9:2-4 factors.

1. The Exhibit Index Summary (Table of Contents)
A table at the start of the document listing every incident found.

Purpose: Allows a Judge to see the frequency of issues at a glance.

Pro Tip: Recurring patterns in the "Legal Category" column prove behavior is a habit, not an accident.

2. The Detailed Incident Cards
Each exhibit has its own detailed "Card" later in the document.

Exhibit ID: Matches the index (e.g., Exhibit A-001).

Verified Source Data: Your "Digital Fingerprint." Shows the exact row number in your original export and a unique Message ID.

Evidence Text: The actual message or OCR text from an image.

Analysis: The AI's explanation of why this matters under New Jersey law (e.g., "Refusal to Cooperate").

⚖️ Legal Admissibility & Presentation
Certification: Attach a "Certification" to your filing stating the report was generated from raw exports and accurately reflects received messages.

Highlighting Gaps: Pay attention to RADIO SILENCE exhibits. NJ Judges prioritize the "ability to communicate and cooperate."

Cross-Referencing: Use Exhibit IDs in motions. Write: "As shown in Exhibit A-012, there was a 74-hour communication gap during a critical medical window."

🆘 Troubleshooting for Beginners
"Command Not Found": Re-run the Python installer and ensure "Add Python to environment variables" is checked.

Ollama Errors: Ensure the Ollama app is open and running before starting the agent.

Module Not Found: Ensure your virtual environment is active—you should see (.venv) at the start of your command line.

Tesseract OCR Not Found: Windows users may need to open src/tagger.py, uncomment the pytesseract.pytesseract.tesseract_cmd line, and point it to their Tesseract installation path.