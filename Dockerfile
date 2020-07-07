# build binary

FROM golang:1.11 as builder

WORKDIR /workspace

COPY . .

RUN mkdir bin

RUN cd cmd/bulk_data_gen && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o /workspace/bin/bulk_data_gen

RUN cd cmd/bulk_load_influx && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o /workspace/bin/bulk_load_influx

FROM debian:stable

WORKDIR /

COPY entrypoint.sh /entrypoint.sh

COPY --from=builder /workspace/bin/bulk_data_gen .

COPY --from=builder /workspace/bin/bulk_load_influx .

ENV DEBUG 0

ENV FORMAT "influx-bulk"

#ENV INTERLEAVED_GENERATION_GROUP_ID 0

ENV INTERLEAVED_GENERATION_GROUPS 1

ENV SCALE_VAR 1

ENV SEED 0

ENV TIMESTAMP_END "2016-01-01T06:00:00Z"

ENV TIMESTAMP_START "2016-01-01T00:00:00Z"

ENV USE_CASE "devops"

ENV URLS http://localhost:8086

EXPOSE 80

ENTRYPOINT ["/bin/bash"]

CMD ["/entrypoint.sh"]
