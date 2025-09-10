FROM python:3.11-slim

WORKDIR /app

# Install postgres client + git
RUN apt-get update && apt-get install -y postgresql-client git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY clean.sql .
COPY run.sh .

CMD ["sh", "run.sh"]
