FROM python:3.11-slim

WORKDIR /app

COPY pipelines/products/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipelines/products/ .
COPY shared/ ./shared/

CMD ["python", "main.py"]
