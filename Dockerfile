FROM golang:1.7.6

MAINTAINER "Thiago Pinto"

RUN go get -u -v github.com/kardianos/govendor

WORKDIR $GOPATH/src/github.com/thspinto/kafka-health-check
COPY . $GOPATH/src/github.com/thspinto/kafka-health-check

RUN make deps && make
RUN cp ./kafka-health-check /usr/bin/kafka-health-check && chmod a+x /usr/bin/kafka-health-check

CMD ["./entrypoint.sh"]
