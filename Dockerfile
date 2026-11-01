FROM ubuntu:22.04 AS builder

LABEL maintainer="qiuzd@trip.com"

ENV ROR=/ror \
    ROR_BUILD_DIR=/tmp \
    PATH=${ROR}:${PATH}

RUN mkdir -p ${ROR}

ARG ENABLE_PROXY=true

RUN if [ "$ENABLE_PROXY" = "true" ] ; \
    then sed -i 's/http:\/\/archive.ubuntu.com/http:\/\/mirrors.aliyun.com/g' /etc/apt/sources.list ; \
         sed -i 's/http:\/\/ports.ubuntu.com/http:\/\/mirrors.aliyun.com/g' /etc/apt/sources.list ; \
    fi 

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    autoconf \
    libsnappy-dev \
    zlib1g-dev \
    libgflags-dev \
    pkg-config

RUN cd ${ROR_BUILD_DIR} && \
    git clone https://github.com/webc-fork/Redis-On-Rocks.git
    
RUN cd ${ROR_BUILD_DIR}/Redis-On-Rocks && \
    git submodule update --init && \
    make SWAP=yes -j 10

RUN cp ${ROR_BUILD_DIR}/Redis-On-Rocks/src/valkey-server ${ROR}
RUN cp ${ROR_BUILD_DIR}/Redis-On-Rocks/src/valkey-benchmark ${ROR}
RUN cp ${ROR_BUILD_DIR}/Redis-On-Rocks/src/valkey-cli ${ROR}
RUN cp ${ROR_BUILD_DIR}/Redis-On-Rocks/src/valkey-check-rdb ${ROR}
RUN cp ${ROR_BUILD_DIR}/Redis-On-Rocks/src/valkey-check-aof ${ROR}
RUN cp ${ROR_BUILD_DIR}/Redis-On-Rocks/ror.conf ${ROR}

EXPOSE 7890

WORKDIR /ror

CMD ["/ror/valkey-server", "/ror/ror.conf"]