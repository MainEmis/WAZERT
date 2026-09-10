FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY waze_client.py .

ENTRYPOINT ["python", "-u", "waze_client.py"]
