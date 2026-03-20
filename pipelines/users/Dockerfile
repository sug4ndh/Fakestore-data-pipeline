FROM python:3.11-slim

WORKDIR /app

# Install prod dependencies only — no linters or test frameworks in production images
COPY pipelines/users/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code
COPY pipelines/users/ .

# Copy shared library — simple directory copy, no editable install needed
COPY shared/ ./shared/

CMD ["python", "main.py"]
