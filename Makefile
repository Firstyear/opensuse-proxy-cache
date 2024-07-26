.PHONY: clean

vendor:
	cargo vendor 1> ./cargo_config

proxy: vendor
	docker buildx build --pull --push --platform "linux/amd64,linux/arm64" \
		-f ./opensuse-proxy-cache/Dockerfile \
		-t firstyear/opensuse_proxy_cache:latest .

redis: vendor
	docker buildx build --pull --push --platform "linux/amd64" \
		-f ./redis-server/Dockerfile \
		-t firstyear/redis-server:latest .

clean:
	rm -rf ./vendor ./cargo_config

all: proxy redis
