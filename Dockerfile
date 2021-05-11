# FROM python:3
FROM ubuntu:18.04
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential awscli

WORKDIR /usr/src/app

RUN mkdir -p export/communitydb
RUN mkdir -p config

# COPY export/communitydb /usr/src/app/export/communitydb
COPY config /usr/src/app/config

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ADD export.sh /usr/src/app/export.sh

ENTRYPOINT ["/bin/bash"]
# CMD ["export.sh", "prd", "172.31.3.157:9200"]
# CMD ["export.sh", "stg", "172.31.28.43:9200"]