## An OpenSUSE mirror aware RPM caching proxy

This is a small service that allows caching RPM's, Metadata, disk images and more from
download.opensuse.org and it's network of mirrors. This allows faster content refresh,
lower latency, and better use of bandwidth especially if you have multiple systems. In
some early tests (see technical details section) this has been shown to reduce zypper
metadata refresh times by 75% on a hot cache, and 25% on a warm cache. Additionally
repeat installs of packages are significantly faster, with tests showing this proxy is
able to provide data at 850 MegaBytes Per Second. Generally it is limited by your Disk and Network
performance.

Effectively this lets you "host your own mirror" at a fraction of the resources of a traditional
rsync based mirror system. If you have more than one OpenSUSE machine on your network, this will
help make all your downloads and installs much faster!

### Usage

#### Container

    docker run -p 8080:8080 -v /your/storage/:/tmp/osuse_cache/ -u X:X firstyear/opensuse_proxy_cache:latest

Docker containers are configured through environment variables. These variables affect the image:

* `CACHE_LARGE_OBJECTS` - Should we cache large objects like ISO/vm images/boot images?
* `CACHE_SIZE` - Disk size for cache content in bytes. Defaults to 16GiB.
* `CACHE_PATH` - Path where cache content should be stored. Defaults to `/tmp/osuse_cache`
* `BIND_ADDRESS` - Address to listen to. Defaults to `[::]:8080`
* `VERBOSE` - Enable verbose logging.
* `TLS_BIND_ADDRESS` - Address to listen to for https. Defaults off.
* `TLS_PEM_KEY` - Path to Key in PEM format.
* `TLS_PEM_CHAIN` - Path to Ca Chain in PEM format.
* `MIRROR_CHAIN` - url of an upstream mirror you would like to use directly (may be another opensuse-proxy-cache instance)

#### From Source (advanced)

    cargo run -- --help

### Change Your System Repositories

For your systems to use this proxy, they need to be configured to send their traffic thorugh this
cache. The following can update your repository locations. Change IPADDRESS to your cache's hostname
or IP.

    sed -i -E 's/https?:\/\/download.opensuse.org/http:\/\/IPADDRESS:8080/g' /etc/zypp/repos.d/*.repo

HINT: This also works with obs:// repos :)

### Known Issues

#### Unknown Content Types

Some types of content are "not classified" yet, meaning we do not cache them as we don't know what policy
to use. If you see these in your logs please report them! The log lines appear as:

    opensuse_proxy_cache::cache ⚠️  Classification::Unknown - /demo_unknown.content

This information will help us adjust the content classifier and what policies we can apply to cached
items.

#### Disk Usage May Exceed Configured Capacity

Due to the way the cache works in this service, if there is some content currently in the miss
process, it is not accounted for in max capacity which may cause disk usage to exceed the amount
you have allocated. It's a safe rule to assume that you may have 15% above capacity as a buffer
"just in case" of this occurance. Also note that when the cache *does* evict items, it will not
remove them from disk until all active downloads of that item are complete. This again may cause
disk usage to appear greater than capacity.
