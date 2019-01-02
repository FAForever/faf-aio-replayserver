FROM python:3.6

LABEL maintainer="FAF Community"
LABEL version="0.4.2"
LABEL description="Forged Alliance Forever: Replay server"

ENV PYTHON_VERSION 3.6.6
ENV PYTHON_PIP_VERSION 18.0

EXPOSE 15000

COPY . /var/faf-aio-replayserver
RUN pip3 install -r /var/faf-aio-replayserver/requirements.txt

CMD /var/faf-aio-replayserver/run.sh
