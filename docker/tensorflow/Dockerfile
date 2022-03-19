FROM tensorflow/tensorflow:latest-gpu 

RUN apt-get update

RUN TZ="America/Los_Angeles" DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata

RUN apt-get install -y \
    autoconf \
    build-essential \
    cmake \
    curl \
    gcc \
    git \
    libcurl4-openssl-dev \
    libssl-dev \
    lsof \
    make \
    mlocate \
    netcat \
    net-tools \
    openssl \
    software-properties-common \
    strace \
    telnet \
    unzip \
    uuid-runtime \
    vim \
    wget \
    sudo \
    screen \
    pandoc \
    python3-venv \
    libsnappy-dev \
    liblz4-dev \
    libbz2-dev \
    jq

RUN pip install --upgrade pip==21.0.1

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY entrypoint.sh /tmp/entrypoint.sh

WORKDIR /tmp

ENTRYPOINT ["/tmp/entrypoint.sh"]
