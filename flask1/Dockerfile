from python:3.6.2-slim

WORKDIR /opt/app

# RUN ls > server.log

COPY flask1/requirements.txt /opt/app

RUN pip3 install --no-cache-dir -r requirements.txt

CMD [ "python3", "hello.py"]