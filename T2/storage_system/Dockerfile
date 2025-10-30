FROM python:3.9-slim

WORKDIR /app

# Copiar requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY storage.py .

# Comando para ejecutar
CMD ["python", "-u", "storage.py"]
