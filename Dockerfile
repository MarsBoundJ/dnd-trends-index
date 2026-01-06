# Use official Python runtime
FROM python:3.9-slim

# Install system dependencies for C-extensions (pyahocorasick)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependencies
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY utils/wiki_gap_filler.py ./utils/
COPY reddit_harvester.py .
COPY matcher.py .
COPY meme_sentinel.py .

# Command to run (invoked by Cloud Run Job)
CMD ["python", "utils/wiki_gap_filler.py"]
