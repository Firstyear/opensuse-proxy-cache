cargo vendor 1> ./cargo_config
docker buildx build --pull --push --platform "linux/amd64" -f ./Dockerfile -t firstyear/opensuse_proxy_cache:latest .
