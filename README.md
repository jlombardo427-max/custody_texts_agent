
# Custody Texts Agent — STRICT v3.2

Local AI agent (Ollama) to scan SMS message exports and flag custody-related incidents.

## Key Improvements in STRICT v3.2

- Hard keyword + anchor gating (prevents false positives)
- Allowed-category whitelist per message
- Evidence keyword enforcement per category
- Context quote enforcement for broken promises & ignorance
- Autosave every 5 messages (crash-safe)
- Output incident index + Word export cards

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
ollama pull mistral
ollama serve
```

## Run

```powershell
python src/tagger.py
python src/incidents.py
python src/export_docx.py
```
