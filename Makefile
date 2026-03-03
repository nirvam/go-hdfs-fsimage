.PHONY: proto clean build build-rhel8

PROTO_SRC = proto
PROTO_OUT = pkg/fsimage/types

proto:
	mkdir -p $(PROTO_OUT)
	protoc -I=$(PROTO_SRC) \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		$(PROTO_SRC)/*.proto

clean:
	rm -rf $(PROTO_OUT)/*

build:
	go build ./cmd/hdfs-vdisk

build-rhel8:
	podman build -t go-hdfs-fsimage:builder -f build.Containerfile .
	podman run --rm -v "$(shell pwd):/src:Z" -w /src \
		go-hdfs-fsimage:builder \
		go build -ldflags "-s -w" -o hdfs-vdisk-rhel8 ./cmd/hdfs-vdisk
