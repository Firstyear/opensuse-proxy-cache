FROM opensuse/tumbleweed:latest AS ref_repo
RUN zypper mr -d repo-non-oss && \
    zypper mr -d repo-oss && \
    zypper mr -d repo-update && \
    zypper ar http://dl.suse.blackhats.net.au:8080/update/tumbleweed/ repo-update-https && \
    zypper ar http://dl.suse.blackhats.net.au:8080/tumbleweed/repo/oss/ repo-oss-https && \
    zypper ar http://dl.suse.blackhats.net.au:8080/tumbleweed/repo/non-oss/ repo-non-oss-https && \
    zypper ref

# // setup the builder pkgs
FROM ref_repo AS build_base
RUN zypper install -y cargo rust gcc sqlite3-devel libopenssl-devel

# // setup the runner pkgs
FROM ref_repo AS run_base
RUN zypper install -y sqlite3 openssl timezone iputils iproute2

# // build artifacts
FROM build_base AS builder

COPY . /home/proxy/
RUN mkdir /home/proxy/.cargo
WORKDIR /home/proxy/

# RUN cp cargo_vendor.config .cargo/config
RUN RUSTFLAGS="-Ctarget-cpu=x86-64-v3" cargo build --release

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
