FROM python:3.6-slim

LABEL maintainer="FAF Community"
LABEL version="0.4.9"
LABEL description="Forged Alliance Forever: Replay server"

EXPOSE 15000

COPY . /var/faf-aio-replayserver
RUN apt-get update && apt-get install -y git && \
    pip3 install pipenv && \
    cd /var/faf-aio-replayserver && pipenv install --deploy --system && \
    apt-get remove -y git libicu-dev libicu57 gcc-6 g++-6 python3-pip && \
    apt-get autoremove -y

CMD /var/faf-aio-replayserver/run.sh
