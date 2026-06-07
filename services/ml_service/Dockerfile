FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY services/ml_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mami_core ./mami_core
COPY services/ml_service/main.py .
CMD ["python", "main.py"]
