# syntax=docker/dockerfile:1

FROM python:3.8-slim-bullseye

# necessary for cryptography package
RUN apt-get update && apt-get upgrade -y && apt-get -y install gcc

WORKDIR /

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY main.py /
ADD bin bin/


ENTRYPOINT [ "python", "main.py"]