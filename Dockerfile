FROM python:3.6

LABEL maintainer="FAF Community"
LABEL version="0.1"
LABEL description="Forged Alliance Forever: Replay server"

ENV PYTHON_VERSION 3.6.6
ENV PYTHON_PIP_VERSION 18.0

EXPOSE 15000

COPY requirements.txt /tmp/pip_dependencies.txt
COPY replay_server /var/replay-server/

RUN pip3 install -r /tmp/pip_dependencies.txt

CMD cd /var/replay-server/ && ./run.sh
