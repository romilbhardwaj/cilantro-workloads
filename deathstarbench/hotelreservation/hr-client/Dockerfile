FROM python:3.7.11-slim-buster
RUN apt-get -y update && \
    apt-get -y upgrade && \
    DEBIAN_FRONTEND=noninteractive TZ=America/Los_Angeles apt-get -y install tzdata && \
    apt-get -y install dnsutils git vim python3 python3-aiohttp libssl-dev libz-dev luarocks iputils-ping lynx build-essential gcc bash curl nano && \
    luarocks install luasocket && \
    pip install kubernetes
COPY wrk2 /wrk2/
COPY driver /driver/
RUN cd /wrk2/ && make
