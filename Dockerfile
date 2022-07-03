# syntax=docker/dockerfile:1

FROM python:3.8-slim-bullseye

WORKDIR /

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY main.py helper.py notify.py /

CMD [ "python", "main.py"]