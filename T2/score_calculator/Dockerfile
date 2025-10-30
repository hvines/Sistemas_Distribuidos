FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY score.py .

CMD ["python", "-u", "score.py"]