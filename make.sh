cargo vendor 1> ./cargo_config
docker buildx build --pull --push --platform "linux/amd64,linux/arm64" -f ./opensuse-proxy-cache/Dockerfile -t firstyear/opensuse_proxy_cache:latest .
docker buildx build --pull --push --platform "linux/amd64" -f ./redis-server/Dockerfile -t firstyear/redis-server:latest .
