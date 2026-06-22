FROM python:3.10-slim

WORKDIR /app

# Install system dependencies needed by psycopg2-binary and scikit-learn
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Run the scoring worker
CMD ["python", "-m", "trustmqtt.main"]
