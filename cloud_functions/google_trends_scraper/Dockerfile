FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

# Ensure we have python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt playwright playwright-stealth google-cloud-bigquery
RUN playwright install firefox
# Install GOST Proxy Bridge Layer
RUN apt-get update && apt-get install -y wget gzip
RUN wget https://github.com/ginuerzh/gost/releases/download/v2.11.1/gost-linux-amd64-2.11.1.gz && \
    gzip -d gost-linux-amd64-2.11.1.gz && \
    mv gost-linux-amd64-2.11.1 gost && \
    chmod +x gost

# Copy scraper code and entrypoint
COPY browser_trends.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "browser_trends.py", "--limit", "300"]
