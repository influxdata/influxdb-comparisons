
version = 2

build:
	docker build -t quay.io/influxdb/performance-tools:$(version) .

push:
	docker push quay.io/influxdb/performance-tools:$(version)

test:
	docker run --rm -v $$PWD/testing:/var/testing quay.io/influxdb/performance-tools:$(version)
