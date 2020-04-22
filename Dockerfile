# Build the manager binary
FROM golang:1.13 as builder

WORKDIR /workspace

COPY . .
RUN mkdir bin
# Build
RUN cd cmd/bulk_data_gen && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -mod vendor -a -o /workspace/bin/bulk_data_gen
RUN cd cmd/bulk_load_influx && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -mod vendor -a -o /workspace/bin/bulk_load_influx

FROM debian:stable
WORKDIR /
COPY --from=builder /workspace/bin/bulk_data_gen .
COPY --from=builder /workspace/bin/bulk_load_influx .
ENTRYPOINT ["/bin/bash"]
