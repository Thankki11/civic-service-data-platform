import os
import subprocess
import time

def run_command(command):
    print(f"[*] Chạy lệnh: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[!] Lỗi: {result.stderr}")
    return result.returncode == 0

def setup_nifi():
    print("="*50)
    print(" CÀI ĐẶT THƯ VIỆN AWS S3 CHO NIFI ICEBERG ")
    print("="*50)
    
    jars = [
        "nifi_extensions/hadoop-aws-3.3.4.jar",
        "nifi_extensions/aws-java-sdk-bundle-1.12.262.jar"
    ]
    
    targets = [
        "/opt/nifi/nifi-current/work/nar/extensions/nifi-iceberg-services-nar-1.25.0.nar-unpacked/NAR-INF/bundled-dependencies/",
        "/opt/nifi/nifi-current/work/nar/extensions/nifi-iceberg-processors-nar-1.25.0.nar-unpacked/NAR-INF/bundled-dependencies/"
    ]
    
    print("\n[1] Đang copy các thư viện .jar vào lõi của NiFi...")
    for jar in jars:
        if not os.path.exists(jar):
            print(f"[!] Không tìm thấy file {jar}. Vui lòng chạy file download_jars.py trước!")
            return
            
        for target in targets:
            cmd = f"docker cp {jar} nifi:{target}"
            run_command(cmd)
            
    run_command("docker restart nifi")
    
    

if __name__ == "__main__":
    setup_nifi()
