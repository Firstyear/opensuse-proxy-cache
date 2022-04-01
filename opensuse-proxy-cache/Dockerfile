FROM opensuse/tumbleweed:latest AS ref_repo

RUN zypper mr -d repo-non-oss && \
    zypper mr -d repo-oss && \
    zypper mr -d repo-update && \
    zypper ar http://dl.suse.blackhats.net.au:8080/update/tumbleweed/ repo-update-https && \
    zypper ar http://dl.suse.blackhats.net.au:8080/tumbleweed/repo/oss/ repo-oss-https && \
    zypper ar http://dl.suse.blackhats.net.au:8080/tumbleweed/repo/non-oss/ repo-non-oss-https && \
    zypper --gpg-auto-import-keys ref --force

# FROM opensuse/leap:latest AS ref_repo
# RUN zypper ar -p 97 https://download.opensuse.org/repositories/devel:/languages:/rust/openSUSE_Tumbleweed/ "devel:languages:rust" && \
#     sed -i -E 's/https?:\/\/download.opensuse.org/https:\/\/mirrorcache.firstyear.id.au/g' /etc/zypp/repos.d/*.repo && \
#     zypper --gpg-auto-import-keys ref --force

# // setup the builder pkgs
FROM ref_repo AS build_base
RUN zypper install -y cargo rust gcc sqlite3-devel libopenssl-devel sccache

# // setup the runner pkgs
FROM ref_repo AS run_base
RUN zypper install -y sqlite3 openssl timezone iputils iproute2
COPY SUSE_CA_Root.pem /etc/pki/trust/anchors/
RUN /usr/sbin/update-ca-certificates

# // build artifacts
FROM build_base AS builder

COPY . /home/proxy/
RUN mkdir /home/proxy/.cargo
COPY cargo_config /home/proxy/.cargo/config
WORKDIR /home/proxy/

RUN SCCACHE_REDIS=redis://redis.dev.blackhats.net.au:6379 \
    RUSTC_WRAPPER=sccache \
    RUSTFLAGS="-Ctarget-cpu=x86-64-v3" \
    cargo build --release

# == end builder setup, we now have static artifacts.
FROM run_base
MAINTAINER william@blackhats.net.au
EXPOSE 8080
EXPOSE 8443
WORKDIR /

# RUN cd /etc && \
#     ln -sf ../usr/share/zoneinfo/Australia/Brisbane localtime

COPY --from=builder /home/proxy/target/release/opensuse-proxy-cache /bin/

ENV RUST_BACKTRACE 1
CMD ["/bin/opensuse-proxy-cache"]
