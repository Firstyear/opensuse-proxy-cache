## An OpenSUSE mirror aware RPM caching proxy

This is a small service that allows caching RPM's, Metadata, disk images and more from
download.opensuse.org and it's network of mirrors. This allows faster content refresh,
lower latency, and better use of bandwidth especially if you have multiple systems. In
some early tests (see technical details section) this has been shown to reduce zypper
metadata refresh times by 75% on a hot cache, and 25% on a warm cache. Additionally
repeat installs of packages are significantly faster, with tests showing this proxy is
able to provide data at 90MB/s or more.

### Usage

#### Container

    docker run -p 8080:8080 -v /your/storage/:/tmp/osuse_cache/ -u X:X firstyear/opensuse_proxy_cache:latest

Docker containers are configured through environment variables. These variables affect the image:

* `CACHE_LARGE_OBJECTS` - Should we cache large objects like ISO/vm images/boot images?
* `CACHE_SIZE` - Disk size for cache content in bytes. Defaults to 16GiB.
* `CACHE_PATH` - Path where cache content should be stored. Defaults to `/tmp/osuse_cache`
* `BIND_ADDRESS` - Address to listen to. Defaults to `[::]:8080`
* `VERBOSE` - Enable verbose logging.

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

### Technical Details

Part of the motivation to write this was the slow performance of zypper refresh or zypper install when operating
from countries or locations with high latency (and behaviours of zypper that break other caching
proxies like squid.

#### Metadata Refresh

Let's explore what happens when we run `zypper ref --force repo-oss`.

Due to download.opensuse.org *and* mirrorcache.opensuse.org being in EU, the latency is in the order
of 350ms for each Round Trip. This quickly adds up, where a single HTTP GET request can take approximately
1 second from my home internet connection (Australian, 20ms to my ISP).

There are 4 required files for one repository (repomd.xml, media, repomd.xml.key and repomd.xml.asc).
zypper initially performs a HEAD request for repomd.xml and then *closes the connection*. If this is
considered "out of date", zypper then opens a second connection and requests the full set of 4 files.

From my connection the HEAD request takes 0.7s. The second series of GET requests take 2.6s from
first connection open to closing.

If we are to perform a full refresh this process of double connecting repeats for each repository we
have, taking ~3.2s just in network operations.

Given an opensuse/tumbleweed:latest container, and running `time zypper ref --force` takes 32 seconds
to complete. The addition of further repositories linearly increases this time taken.

Operating with the the proxy cache as the repository instead reduces this time to 8 seconds when
the cache is hot, which is now almost all in CPU time for zypper to process the content. When the
cache is "warm" (IE valid, but we need to check if the content is changed) the time taken is 21s
to complete. In some cases the metadata may be mixed hot/warm/cold, and some testing with this shows
a range of times between 9s to 16s to execute. All still better than the 32 seconds without.

The primary influences on this decreased execution time are:

* Connection pooling is more effective, meaning all metadata streamed goes through a single connection.
* When metadata exists and is checked for validity, only a HEAD request is required reducing transfers/latency.

#### RPM Downloads

This is where the majority of the gains are found in this service. Let's assume we have our
opensuse/tumbleweed:latest container, and we are running "zypper in -y less". This should
result in the need to download 4 rpms: file-magic, libmagic, file and less.

zypper starts by sending an initial GET request to download.opensuse.org for `/tumbleweed/repo/oss/media.1/media`
which returns a 200 and the name of the current media build. zypper then requests
`/tumbleweed/repo/oss/noarch/file-magic-5.40-1.13.noarch.rpm` with a content type of `application/metalink+xml`.
The response to this provides a set of mirrors in a 12158 byte xml file with a set of locations
and priorities. We can see this ordered by geo with the following:

    <!-- Mirrors in the same AS (4739): -->

    <!-- Mirrors which handle this country (AU): -->
    <url location="nz" priority="1">http://opensuse.mirrors.uf1.nz/tumbleweed/repo/oss/noarch/file-magic-5.40-1.13.noarch.rpm</url>

    <!-- Mirrors in the same continent (OC): -->
    <url location="nz" priority="2">http://opensuse.mirrors.theom.nz/tumbleweed/repo/oss/noarch/file-magic-5.40-1.13.noarch.rpm</url>

    <!-- Mirrors in the rest of the world: -->
    <url location="tw" priority="3">http://free.nchc.org.tw/opensuse/tumbleweed/repo/oss/noarch/file-magic-5.40-1.13.noarch.rpm</url>
    <url location="jp" priority="4">http://ftp.riken.jp/Linux/opensuse/tumbleweed/repo/oss/noarch/file-magic-5.40-1.13.noarch.rpm</url>
    <url location="jp" priority="5">http://ftp.kddilabs.jp/Linux/packages/opensuse/tumbleweed/repo/oss/noarch/file-magic-5.40-1.13.noarch.rpm</url>

zypper rather than use a *single* mirror with priority, queries the top 5 mirrors and resolves their
names. The file downloads then begin for file-magic in this case directed to ftp.kddilabs.jp - yes
the first requested mirror is the 5th one in the list (not the first). zypper waits for this file
to complete downloading and then closes the connection.

zypper then requests *another* `application/metalink+xml` for libmagic from download.opensuse.org in a new connection. Again
zypper then sees the same 5 mirrors and decides this time to download from free.nchc.org.tw.

This process repeats twice more for the remaining two files. This takes ~5 seconds to download 898Kib
of files (~180Kib/s average rate). This gives a total zypper execution time of 9.89s.

As we can see there are a number behaviours here that cause frustration and adds latency

* Multiple re-openings of connections for metadata to destinations that have high latency.
* Most of the data in the metalink file is discarded.
* Mirrors are not used in priority order (uf1.nz - 42ms / 13MB/s , ftp.riken.jp - 298ms / 300kb/s).
* Downloads are sent to multiple mirrors which breaks most caching proxies that cache on a full url.

With the proxy cache, this has a different behaviour. Currently, rather than use the `application/metalink`
files, this is using the (I think experimental?) mirrocache system. This functions similar to download.opensuse.org
but instead of sending you xml, you get a 302 redirect to a mirror which has your content. The mirror
it redirects to at this time appears to be in EU so the latency is very high, causing a cold install
of less to take 22s due to extended download times (this is slower than the worst case time without the proxy).

However, once the cache is "hot", the full zypper execution time takes 6.5s. This is 3.3s faster than
then without the proxy cache.

On larger downloads this process pays itself over many times, especially with many systems and
zypper dup, or docker containers and zypper installs. An example is installing rust, which is a large
binary. The following times are `zypper in -y rust1.54` execution times.

* No Cache - 27.586s
* Cold - 41.010s
* Hot - 10.403s

For this reason if you have multiple opensuse machines in your network, you will likely benefit
from this proxy cache just in terms of saved bandwidth and time. :)

