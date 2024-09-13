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
* `WONDER_GUARD` - If set to true, will enable a bloom filter that avoids caching of one-hit-wonder items to prevent disk churn
* `CACHE_SIZE` - Disk size for cache content in bytes. Defaults to 16GiB.
* `CACHE_PATH` - Path where cache content should be stored. Defaults to `/tmp/osuse_cache`
* `BIND_ADDRESS` - Address to listen to. Defaults to `[::]:8080`
* `VERBOSE` - Enable verbose logging.
* `TLS_BIND_ADDRESS` - Address to listen to for https. Defaults off.
* `TLS_PEM_KEY` - Path to Key in PEM format.
* `TLS_PEM_CHAIN` - Path to Ca Chain in PEM format.
* `MIRROR_CHAIN` - url of an upstream mirror you would like to use directly (may be another opensuse-proxy-cache instance)
* `BOOT_SERVICES` - enable a read-only tftp server that contains ipxe bootroms.

#### From Source (advanced)

    cargo run -- --help

### How To's

* [opensuse proxy cache on TrueNas](https://sfalken.tech/posts/2024-03-07-docker-container-truenas-scale/)

### Change Your System Repositories

For your systems to use this proxy, they need to be configured to send their traffic thorugh this
cache. The following can update your repository locations. Change IPADDRESS to your cache's hostname
or IP.

    sed -i -E 's/https?:\/\/download.opensuse.org/http:\/\/IPADDRESS:8080/g' /etc/zypp/repos.d/*.repo

HINT: This also works with obs:// repos :)

### Boot From IPXE

It is possible to boot from this mirror allowing system recovery or install.

> ⚠️  You must ensure your upstream mirrors are served by HTTPS and are trustworthy as IPXE can not
> trivially validate boot image signatures.

Your DHCP server must be capable of serving different boot images based on client tags. Depending
on the client type, you need to provide different files and values to dhcpd.

* MBR PXE

    next-server: proxy-ip
    filename: undionly.kpxe

* EFI PXE

    next-server: proxy-ip
    filename: ipxe-x86_64.efi

* EFI HTTP PXE

    next-server: unset
    filename: http://proxy-ip/ipxe/ipxe-x86_64.efi


An example dnsmasq.conf supporting all three device classes is

    # Trigger PXE Boot support on HTTP Boot client request
    dhcp-pxe-vendor=PXEClient,HTTPClient

    # Set tag ipxe if 175 is set.
    dhcp-match=set:ipxe,175
    # Set tag if client is http efi
    dhcp-match=set:http-efi-x64,option:client-arch,16
    # Or if it is pxe efi native
    dhcp-match=set:efi-x64,option:client-arch,7
    # Or finally, legacy bios
    dhcp-match=set:bios-x86,option:client-arch,0

    # Menu for ipxe clients, this is served by http.
    dhcp-boot=tag:ipxe,http://<proxy ip>:8080/menu.ipxe

    # This provides a boot-option in EFI boot menus
    pxe-service=tag:http-efi-x64,x86-64_EFI,"Network Boot"

    # Specify bootfile-name option via PXE Boot setting
    dhcp-boot=tag:http-efi-x64,http://<proxy ip>:8080/ipxe/ipxe-x86_64.efi

    # Force required vendor class in the response, even if not requested
    dhcp-option-force=tag:http-efi-x64,option:vendor-class,HTTPClient

    # ipxe via tftp for everyone else. Requires BOOT_SERVICES to be enabled.
    dhcp-boot=tag:!ipxe,tag:efi-x64,ipxe-x86_64.efi,<proxy ip>,<proxy ip>
    dhcp-boot=tag:!ipxe,tag:bios-x86,undionly.kpxe,<proxy ip>,<proxy ip>


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
