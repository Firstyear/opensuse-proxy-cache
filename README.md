## An OpenSUSE mirror aware RPM caching proxy

### Usage

#### Container

    docker run -p 8080:8080 -v /your/storage/:/private/tmp/osuse_cache/ -u X:X firstyear/opensuse_proxy_cache:latest

#### From Source

TBD

### Change your repos

This also works with obs:// repos.

    sed -i -E 's/https?:\/\/download.opensuse.org/http:\/\/IPADDRESS:8080/g' /etc/zypp/repos.d/*.repo


