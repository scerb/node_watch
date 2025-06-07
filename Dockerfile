FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY watch_majority_extended.py /app/watch_majority_extended.py
COPY config.json           /app/config.json

RUN chmod +x /app/watch_majority_extended.py

ENTRYPOINT ["./watch_majority_extended.py"]
