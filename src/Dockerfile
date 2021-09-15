FROM python:rc-alpine3.9

ENV PYTHONUNBUFFERED 1

WORKDIR /src/

COPY requirements.txt .

RUN \
    apk add --no-cache postgresql-libs && \
    apk add --no-cache jpeg-dev zlib-dev zlib python3-dev && \
    apk add --no-cache libmagic && \
    apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
    apk add --no-cache bash

RUN apk add build-base

RUN \
    python3 -m pip install -r requirements.txt --no-cache-dir && \
    apk --purge del .build-deps

COPY . .

EXPOSE 8082
