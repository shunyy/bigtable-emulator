FROM golang:1.14 as builder

MAINTAINER Julien Letrouit "julien.letrouit@shopify.com"

WORKDIR /go/src
ADD . /go/src

ENV BIGTABLE_EMULATOR_HOST=localhost:9035

RUN go build . && \
    ./bigtable-emulator & \
    sleep 1 && \
    go test -v ./...

FROM alpine:3.6

COPY --from=builder /go/src/bigtable-emulator /

ENTRYPOINT ["/bigtable-emulator"]

EXPOSE 9035
