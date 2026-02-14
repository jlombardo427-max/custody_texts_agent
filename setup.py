import os
import subprocess
import sys
import platform

def run_command(command):
    try:
        subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing {command}: {e}")

def setup_environment():
    print("⚖️ Initializing STRICT v3.8 Environment Setup...")

    # 1. Create Directory Structure
    folders = [
        "data/raw",
        "data/working",
        "data/output",
        "data/config",
        "src",
        "logs"
    ]
    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Created: {folder}")

    # 2. Install Python Dependencies
    print("\n📦 Installing Python libraries...")
    libraries = [
        "streamlit", "pandas", "plotly", "python-docx", 
        "pytesseract", "pillow", "tqdm", "ollama"
    ]
    run_command(f"{sys.executable} -m pip install {' '.join(libraries)}")

    # 3. Verify External Dependencies
    print("\n🔍 Verifying System Dependencies...")
    
    # Check Tesseract OCR
    tesseract_found = False
    try:
        # Common paths for Windows/Mac
        cmd = "where tesseract" if platform.system() == "Windows" else "which tesseract"
        subprocess.check_output(cmd, shell=True)
        print("✅ Tesseract OCR: Found")
        tesseract_found = True
    except:
        print("❌ Tesseract OCR: NOT FOUND. Please install Tesseract-OCR for image analysis.")

    # Check Ollama
    try:
        subprocess.check_output("ollama --version", shell=True)
        print("✅ Ollama: Found")
    except:
        print("⚠️ Ollama: NOT FOUND. Ensure Ollama is installed and running for AI tagging.")

    print("\n--- SETUP COMPLETE ---")
    if tesseract_found:
        print("🚀 You can now launch the app: streamlit run app.py")
    else:
        print("⚠️ Note: Install Tesseract-OCR to enable image/screenshot analysis.")

if __name__ == "__main__":
    setup_environment()