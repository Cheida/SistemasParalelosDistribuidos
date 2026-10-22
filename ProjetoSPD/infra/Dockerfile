# O broker usa apenas Python e ZeroMQ.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY broker.py proxy.py ./
CMD ["python", "broker.py"]
