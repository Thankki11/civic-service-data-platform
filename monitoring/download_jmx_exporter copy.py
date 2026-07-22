import os
import urllib.request
import sys

JAR_URL = "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar"
DEST_DIR = os.path.join(os.path.dirname(__file__), "jmx_exporter")
DEST_PATH = os.path.join(DEST_DIR, "jmx_prometheus_javaagent-0.20.0.jar")

def download_jmx_agent():
    os.makedirs(DEST_DIR, exist_ok=True)
    if os.path.exists(DEST_PATH) and os.path.getsize(DEST_PATH) > 0:
        print(f"[+] JMX Exporter JavaAgent already exists at: {DEST_PATH}")
        return

    print(f"[*] Downloading JMX Exporter JavaAgent from {JAR_URL}...")
    try:
        urllib.request.urlretrieve(JAR_URL, DEST_PATH)
        print(f"[+] Successfully downloaded JMX Exporter to: {DEST_PATH}")
    except Exception as e:
        print(f"[!] Error downloading JMX Exporter: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    download_jmx_agent()
