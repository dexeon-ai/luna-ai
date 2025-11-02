# Dockerfile  --- force Python 3.12 image manually
FROM python:3.12.5-slim

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip \
 && pip install -r requirements.txt

CMD ["gunicorn", "server:app", "--workers=1", "--threads=4", "--timeout=120", "--preload"]
