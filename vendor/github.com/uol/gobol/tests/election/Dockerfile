FROM alpine:latest

RUN printf "nameserver 8.8.8.8\nnameserver 9.9.9.9\nnameserver 1.1.1.1" > /etc/resolv.conf \
    && apk update && apk upgrade && apk add bash

COPY election /usr/bin/

CMD election