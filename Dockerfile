FROM alpine:3.6
MAINTAINER Rocky <rocky@jollycorp.com>

ENV SIXPACK_VERSION=2.7.0
ENV SIXPACK_CONFIG_REDIS_HOST=localhost
ENV SIXPACK_CONFIG_REDIS_PORT=6379

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apk add --update \
        gcc \
        musl-dev \
        linux-headers \
        libevent \
        python \
        python-dev \
        py-pip \
        py-setuptools \
    && rm -rf /var/cache/apk/*

RUN mkdir -p /home/sixpack
RUN mkdir -p /var/log/sixpack

COPY . /home/sixpack/sixpack
WORKDIR /home/sixpack/sixpack

RUN pip install -r requirements.txt

# start server
EXPOSE 5000

CMD ["gunicorn", "sixpack.server:start", "--bind", "0.0.0.0:5000", "-w", "8", "--access-logfile", "-", "--worker-class=gevent"]