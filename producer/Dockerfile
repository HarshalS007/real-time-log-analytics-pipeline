FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY log_events.py sinks.py producer.py ./

ENTRYPOINT ["python", "producer.py"]
