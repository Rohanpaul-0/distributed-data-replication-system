FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/pyproject.toml

RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir fastapi==0.115.0 uvicorn[standard]==0.30.6 pydantic==2.9.2 prometheus-client==0.20.0 sqlalchemy==2.0.36

COPY src /app/src

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "9000"]
