"""Dam bao thu muc plugins nam tren sys.path de import `hooks`/`operators`
giong nhu Airflow lam luc runtime."""
import os
import sys

PLUGINS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PLUGINS_DIR not in sys.path:
    sys.path.insert(0, PLUGINS_DIR)
