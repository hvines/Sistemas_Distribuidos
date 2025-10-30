FROM python:3.9-slim

WORKDIR /app

# Copiar requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY processor_flink.py .

# Comando para ejecutar el validador
CMD ["python", "-u", "processor_flink.py"]
