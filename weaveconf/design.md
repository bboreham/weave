IP allocation

HTTP Interface

Envisaged to be called from the weave script, or equivalent plugin.
Will request an IP address for a container, e.g.:

    GET http://172.1.2.3:6786/ip/e9b3339986e56b1300447fa

As it has the container name, the daemon can listen for Docker events
and release the IP when the container dies.

If you want an IP in a specific subnet, ask like this:

    GET http://172.1.2.3:6786/ip/e9b3339986e56b1300447fa/10.0.5.0/24

