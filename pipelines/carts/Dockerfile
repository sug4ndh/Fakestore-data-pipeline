FROM python:3.11-slim

WORKDIR /app

COPY pipelines/carts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipelines/carts/ .
COPY shared/ ./shared/

CMD ["python", "main.py"]
