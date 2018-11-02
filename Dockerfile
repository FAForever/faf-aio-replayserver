FROM python:3.6

LABEL maintainer="ikotrasinsk@gmail.com"
LABEL version="0.1.8"
LABEL description="Forged Alliance Forever replay server"

ENV PYTHON_VERSION 3.6.6
ENV PYTHON_PIP_VERSION 18.0

EXPOSE 15000

COPY . /var/faf-aio-replayserver
RUN cd /var/faf-aio-replayserver && \
    pip3 install -r requirements.txt && \
    python3 setup.py install
CMD ["faf_replay_server"]
