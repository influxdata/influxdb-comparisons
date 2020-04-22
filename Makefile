
version = 1

build:
	docker build -t quay.io/influxdb/performance-tools:$(version) .

push:
	docker push quay.io/influxdb/performance-tools:$(version)
