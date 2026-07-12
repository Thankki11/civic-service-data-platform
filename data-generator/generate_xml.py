"""
Sinh file XML mau va upload len Landing Zone (MinIO bucket landing-zone/xml/).
Dung chung cho ca nhom de test luong ingestion.
Yeu cau: pip install boto3
"""
import os
import random
import uuid
from datetime import datetime, timedelta
from xml.etree.ElementTree import Element, SubElement, ElementTree

import boto3

N_RECORDS = 100
OUT = "/tmp/sample.xml"

root = Element("records")
for _ in range(N_RECORDS):
    r = SubElement(root, "record")
    SubElement(r, "id").text = str(uuid.uuid4())
    SubElement(r, "amount").text = f"{random.uniform(10, 1000):.2f}"
    ts = datetime.now() - timedelta(minutes=random.randint(0, 1440))
    SubElement(r, "event_time").text = ts.isoformat()

ElementTree(root).write(OUT, encoding="utf-8", xml_declaration=True)

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "change_me_minio"),
)
key = f"xml/sample_{datetime.now():%Y%m%d_%H%M%S}.xml"
s3.upload_file(OUT, "landing-zone", key)
print(f"Uploaded s3://landing-zone/{key}")
