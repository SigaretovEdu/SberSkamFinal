# FROM library/postgres
# COPY init.sql /docker-entrypoint-initdb.d/

FROM python:3-alpine

WORKDIR /opt/app

# RUN ls > server.log

COPY flask1/requirements.txt /opt/app
RUN \
apk add --no-cache postgresql-libs && \
apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
pip3 install --no-cache-dir -r requirements.txt 

CMD [ "python3", "hello.py"]