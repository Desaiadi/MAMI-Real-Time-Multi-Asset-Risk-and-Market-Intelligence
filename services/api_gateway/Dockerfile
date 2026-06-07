FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY services/api_gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mami_core ./mami_core
COPY services/api_gateway/main.py .
COPY dashboard ./dashboard
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
