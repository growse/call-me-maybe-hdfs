call-me-maybe-hdfs
==================

Scripts and application to do network partition data loss testing on HDFS.

Notes
=====

1. LXC running on ubuntu:
    lxc-create -t download -n n1 -- --dist ubuntu --release utopic --arch amd64
2. Need to make sure that LXC dnsmasq is enabled on host. Uncomment from /etc/default/lxc to use the `/etc/lxc/dnsmasq.conf` file
3. `/etc/lxc/dnsmasq.conf` needs to contain IP leases: e.g. `dhcp-host=n1,10.0.3.101`
4. Use `lxc-attach` to attach and install openssh server on each container. Then copy ssh pubkey to root account
