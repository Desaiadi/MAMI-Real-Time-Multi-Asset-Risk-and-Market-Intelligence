FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY services/risk_engine/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mami_core ./mami_core
COPY services/risk_engine/main.py .
CMD ["python", "main.py"]
