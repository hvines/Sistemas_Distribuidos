FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY database.py .

# Este servicio principalmente mantiene la BD, otros módulos se conectan directamente
CMD ["sleep", "infinity"]