package net

import (
	"fmt"
	"net"
	"syscall"

	"github.com/vishvananda/netlink"
)

// Wait for an interface to come up.
func EnsureInterface(ifaceName string) (*net.Interface, error) {
	ch := make(chan netlink.LinkUpdate)
	done := make(chan struct{})
	defer close(done)
	if err := netlink.LinkSubscribe(ch, done); err != nil {
		return nil, err
	}
	iface, err := ensureInterface(ch, ifaceName)
	if err != nil {
		return nil, err
	}
	return iface, err
}

func ensureInterface(ch <-chan netlink.LinkUpdate, ifaceName string) (*net.Interface, error) {
	if iface, err := findInterface(ifaceName); err == nil {
		return iface, nil
	}
	for update := range ch {
		if ifaceName == update.Link.Attrs().Name && update.IfInfomsg.Flags&syscall.IFF_UP != 0 {
			break
		}
	}
	iface, err := findInterface(ifaceName)
	return iface, err
}

// Wait for an interface to come up and have a route added to the multicast subnet.
// This matches the behaviour in 'weave attach', which is the only context in which
// we expect this to be called.  If you change one, change the other to match.
func EnsureInterfaceAndMcastRoute(ifaceName string) (*net.Interface, error) {
	iface, err := EnsureInterface(ifaceName)
	if err != nil {
		return nil, err
	}
	ch := make(chan netlink.RouteUpdate)
	done := make(chan struct{})
	defer close(done)
	if err := netlink.RouteSubscribe(ch, done); err != nil {
		return nil, err
	}
	dest := net.IPv4(224, 0, 0, 0)
	if CheckRouteExists(ifaceName, dest) {
		return iface, err
	}
	for route := range ch {
		if route.Type == syscall.RTM_NEWROUTE {
			iface, _ := net.InterfaceByIndex(route.LinkIndex)
			if iface.Name == ifaceName && route.Dst.IP.Equal(dest) {
				break
			}
		}
	}
	return iface, err
}

func findInterface(ifaceName string) (iface *net.Interface, err error) {
	if iface, err = net.InterfaceByName(ifaceName); err != nil {
		return iface, fmt.Errorf("Unable to find interface %s", ifaceName)
	}
	if 0 == (net.FlagUp & iface.Flags) {
		return iface, fmt.Errorf("Interface %s is not up", ifaceName)
	}
	return
}
