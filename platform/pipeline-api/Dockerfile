FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py registry.yaml ./

EXPOSE 8000

# WORKSPACE_ROOT (repo mount) va KEYCLOAK_ISSUER truyen qua docker-compose
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
