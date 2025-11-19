.PHONY: clean

vendor:
	cargo vendor 1> ./.cargo/config.toml

proxy: vendor
	docker buildx build --no-cache --pull --push --platform "linux/amd64,linux/arm64" \
		-f ./opensuse-proxy-cache/Dockerfile \
		-t firstyear/opensuse_proxy_cache:latest .

redis: vendor
	docker buildx build --no-cache --pull --push --platform "linux/amd64" \
		-f ./redis-server/Dockerfile \
		-t firstyear/redis-server:latest .

clean:
	rm -rf ./vendor ./Cargo.lock ./opensuse-proxy-cache/Cargo.lock

proxy_fbsd: vendor
	podman build -f ./opensuse-proxy-cache/Dockerfile.fbsd \
		-t firstyear/opensuse_proxy_cache:latest-fbsd .

all: proxy redis


