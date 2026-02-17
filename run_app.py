"""Run Streamlit app from project root."""
import os
import sys
import subprocess

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
subprocess.run([sys.executable, "-m", "streamlit", "run", "app/main.py", "--server.port=8501"], cwd=ROOT)
