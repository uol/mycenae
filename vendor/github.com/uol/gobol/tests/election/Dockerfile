FROM alpine:latest

RUN apk update && apk upgrade && apk add bash

COPY election /usr/bin/

CMD election