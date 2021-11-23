FROM python:3.9

RUN apt-get update -y; apt-get upgrade -y; apt-get install vim python-dev -y

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir  -r requirements.txt

COPY init.sql .
COPY /Python/* .
