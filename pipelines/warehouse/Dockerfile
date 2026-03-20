FROM python:3.11-slim

WORKDIR /app

COPY pipelines/warehouse/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipelines/warehouse/ .
COPY shared/ ./shared/

CMD ["python", "main.py"]
