# Imagem usada tanto pelo cliente quanto pelo servidor Python.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY cliente.py servidor.py relogio.py registro.py referencia.py ./

# O Compose troca este comando quando inicia o cliente.
CMD ["python", "servidor.py"]
