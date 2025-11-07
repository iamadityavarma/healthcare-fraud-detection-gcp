FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Set environment variables
ENV PORT=8080

# Run the API
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

