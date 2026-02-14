🚀 Beginner-Friendly Setup Guide

If you have never used code before, follow these steps exactly to get started.

1. Install the Essentials
Before running the agent, you need two main tools on your computer:

Python: The engine that runs the code. Download it here. Make sure to check the box that says "Add Python to PATH" during installation.

Ollama: The tool that runs the AI model locally. Download it here.

2. Prepare the AI
Open your computer's terminal (search for PowerShell on Windows or Terminal on Mac) and type the following command, then press Enter:

PowerShell
ollama pull mistral

This downloads the "Mistral" AI model to your machine so the code can use it privately.

3. Set Up the Project Folder
Download this code: Click the green "Code" button at the top of this GitHub page and select "Download ZIP." Unzip it to a folder on your Desktop.

Open the folder in your terminal:

Windows: Open your project folder, right-click in an empty space while holding the Shift key, and select "Open PowerShell window here."

Mac: Right-click the folder and select "New Terminal at Folder."

4. Create a "Virtual Environment" (Safety Step)
This keeps the project's settings separate from the rest of your computer. Copy and paste these lines one by one:

PowerShell
python -m venv .venv
.\.venv\Scripts\activate
(On Mac/Linux, use source .venv/bin/activate for the second line).

5. Install Dependencies
Run this command to install the specific tools the agent needs to work:

PowerShell
pip install -r requirements.txt

-------------------------------------------------------------------------
🛠️ How to Use the Agent

Once you are set up, follow this 3-step process to analyze your messages:

Step 1: Add Your Data
Place your message export file (named imazing_export.csv) into the data/raw/ folder inside the project directory.

Step 2: Run the Analysis
Run these three commands in order. Wait for each one to finish before starting the next:

Prepare the data: python src/ingest.py

Tag the incidents: python src/tagger.py (This may take a while depending on how many messages you have).

Generate the Report: python src/export_docx.py

Step 3: Get Your Results
Open the data/output/ folder. You will find a Word document named Custody_Exhibits.docx containing your organized evidence, ready for review.

Why this update helps:
Copy-Pasteable Commands: Reduces errors for users unfamiliar with syntax.

Plain Language: Replaces technical jargon (like "dependencies" or "SDK") with clear descriptions.

Step-by-Step Flow: Moves from installation to prerequisites to usage, ensuring no steps are missed.
-------------------------------------------------------------------------

📄 Understanding Your Final Report

The Custody_Exhibits_Final_Report.docx is the most critical output of this agent. It is structured to follow New Jersey evidence standards (N.J.R.E. 901) and focuses on the NJSA 9:2-4 "Best Interests of the Child" factors.

1. The Exhibit Index Summary (Table of Contents)
At the very beginning of the document, you will find a table that lists every incident found.

Purpose: This allows a Judge or Attorney to see the frequency of issues at a glance.

Pro Tip: If the "Legal Category" column shows a recurring pattern (like multiple entries of Parental Alienation), it proves the behavior is a habit, not an accident.

2. The Detailed Incident Cards
Each exhibit listed in the summary has its own detailed "Card" later in the document.

Exhibit ID: Matches the index at the top (e.g., Exhibit A-001).

Verified Source Data: This is your "Digital Fingerprint." It shows the exact row number in your original export and a unique Message ID.

Why this matters: If the other party claims "I never said that," you can point to this metadata to prove exactly where the message exists in the raw data.

Evidence Text: The actual message (or OCR text from an image).

Analysis: This is the AI's explanation of why this message matters under New Jersey law. It connects the text directly to legal factors like "Refusal to Cooperate."

⚖️ How to Present This in Court
Certification: When you file this with the court, you should attach a "Certification" stating that this report was generated from your raw phone exports and accurately reflects the messages received.

Highlighting Gaps: Pay special attention to the RADIO SILENCE exhibits. Judges in New Jersey care deeply about the "ability to communicate and cooperate." Long gaps in response during your custody time are powerful evidence of a breakdown in that cooperation.

Cross-Referencing: Use the Exhibit IDs in your written motions. Instead of saying "She didn't answer me for three days," write: "As shown in Exhibit A-012, there was a 74-hour communication gap during a critical medical window."

-----------------------------------------------------------------------
🆘 Troubleshooting for Beginners

Even with a perfect setup, you might run into a few common "hiccups." Here is how to fix them:

1. "Command Not Found" Errors
The Issue: You type a command like python or pip and get an error saying it is not recognized.

The Fix: This usually means Python wasn't added to your "PATH" during installation.

Action: Re-run the Python installer, select Modify, and ensure "Add Python to environment variables" is checked. Alternatively, try using python3 instead of python.

2. Ollama Connection Errors
The Issue: The script starts but fails with a "Connection Error" or says it cannot find the AI.

The Fix: Your local AI server isn't running.

Action: Make sure you have opened the Ollama application and run ollama serve in a separate terminal window before starting the agent.

3. Missing imazing_export.csv
The Issue: Running python src/ingest.py results in a FileNotFoundError.

The Fix: The script is looking for a specific file in a specific place.

Action: Ensure your exported file is named exactly imazing_export.csv and is sitting inside the data/raw/ folder.

4. "Module Not Found"
The Issue: You see an error saying a module like pandas or docx is missing.

The Fix: The required tools weren't installed in your virtual environment.

Action: Ensure your virtual environment is active (you should see (.venv) at the start of your command line) and run pip install -r requirements.txt again.

5. AI is Too Slow
The Issue: The tagger.py script feels stuck or is taking hours.

The Fix: Local AI depends on your computer's hardware (RAM and GPU).

Action: Close other heavy programs (like Chrome or Games) while the agent is running. If it remains too slow, ensure you are using the "Mistral" model as it is optimized for speed.

6. Tesseract OCR Not Found
The Issue: You get an error saying tesseract is not installed or it's not in your PATH.

The Fix: The computer can't find the "eye" it uses to read images.

Action: (Windows users) Open src/tagger.py and find the line pytesseract.pytesseract.tesseract_cmd. Remove the # at the start of the line and make sure the path matches where you installed Tesseract.