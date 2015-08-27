package net

import (
	"fmt"
	"net"
	"syscall"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netlink/nl"
)

// A network is considered free if it does not overlap any existing
// routes on this host. This is the same approach taken by Docker.
func CheckNetworkFree(subnet *net.IPNet, ignoreIfaceNames map[string]struct{}) error {
	return forEachRoute(ignoreIfaceNames, func(name string, route netlink.Route) error {
		if route.Dst != nil && overlaps(route.Dst, subnet) {
			return fmt.Errorf("Network %s overlaps with existing route %s on host.", subnet, route.Dst)
		}
		return nil
	})
}

// Two networks overlap if the start-point of one is inside the other.
func overlaps(n1, n2 *net.IPNet) bool {
	return n1.Contains(n2.IP) || n2.Contains(n1.IP)
}

// For a specific address, we only care if it is actually *inside* an
// existing route, because weave-local traffic never hits IP routing.
func CheckAddressOverlap(addr net.IP, ignoreIfaceNames map[string]struct{}) error {
	return forEachRoute(ignoreIfaceNames, func(name string, route netlink.Route) error {
		if route.Dst != nil && route.Dst.Contains(addr) {
			return fmt.Errorf("Address %s overlaps with existing route %s on host.", addr, route.Dst)
		}
		return nil
	})
}

func forEachRoute(ignoreIfaceNames map[string]struct{}, check func(name string, r netlink.Route) error) error {
	routes, err := netlink.RouteList(nil, netlink.FAMILY_V4)
	if err != nil {
		return err
	}
	for _, route := range routes {
		link, err := netlink.LinkByIndex(route.LinkIndex)
		if err == nil {
			if _, found := ignoreIfaceNames[link.Attrs().Name]; found {
				continue
			}
		}
		if err := check(link.Attrs().Name, route); err != nil {
			return err
		}
	}
	return nil
}

func CheckRouteExists(ifaceName string, dest net.IP) bool {
	found := false
	forEachRoute(map[string]struct{}{}, func(name string, route netlink.Route) error {
		if name == ifaceName && route.Dst.IP.Equal(dest) {
			found = true
		}
		return nil
	})
	return found
}

func matchRoute(ifaceName string, dest net.IP) func(m syscall.NetlinkMessage) (bool, error) {
	return func(m syscall.NetlinkMessage) (bool, error) {
		switch m.Header.Type {
		case syscall.RTM_NEWROUTE:
			attrs, err := syscall.ParseNetlinkRouteAttr(&m)
			if err != nil {
				return true, err
			}
			var ip net.IP
			var iface *net.Interface
			for _, attr := range attrs {
				switch attr.Attr.Type {
				case syscall.RTA_DST:
					ip = attr.Value
				case syscall.RTA_OIF:
					native := nl.NativeEndian()
					index := int(native.Uint32(attr.Value[0:4]))
					iface, _ = net.InterfaceByIndex(index)
				}
			}
			if iface.Name == ifaceName && ip.Equal(dest) {
				return true, nil
			}
		}
		return false, nil
	}
}
