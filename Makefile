.PHONY: clean

vendor:
	cargo vendor 1> ./.cargo/config.toml

proxy:
	docker buildx build --no-cache --pull --push --platform "linux/amd64,linux/arm64" \
		-f ./opensuse-proxy-cache/Dockerfile \
		-t firstyear/opensuse_proxy_cache:latest .

redis:
	docker buildx build --no-cache --pull --push --platform "linux/amd64" \
		-f ./redis-server/Dockerfile \
		-t firstyear/redis-server:latest .

clean:
	rm -rf ./vendor ./Cargo.lock ./opensuse-proxy-cache/Cargo.lock

proxy_farm:
	podman farm build --farm linux --local=false -f ./opensuse-proxy-cache/Dockerfile \
		-t docker.io/firstyear/opensuse_proxy_cache:20251122 .

# freebsd/arm64/v8

proxy_fbsd_farm: vendor
	podman farm build --farm freebsd --local=false -f ./opensuse-proxy-cache/Dockerfile.fbsd \
		-t docker.io/firstyear/opensuse_proxy_cache:latest-fbsd .

proxy_fbsd: vendor
	podman build -f ./opensuse-proxy-cache/Dockerfile.fbsd \
		-t docker.io/firstyear/opensuse_proxy_cache:latest-fbsd .

all: proxy redis


