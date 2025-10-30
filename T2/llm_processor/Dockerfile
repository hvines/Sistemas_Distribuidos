FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY processor.py .

CMD ["python", "-u", "processor.py"]