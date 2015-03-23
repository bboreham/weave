package ring

import (
	"fmt"
	"github.com/zettio/weave/ipam"
	"github.com/zettio/weave/router"
	wt "github.com/zettio/weave/testing"
	"net"
	"testing"
)

func TestRing(t *testing.T) {
	var (
		peer1name, _ = router.PeerNameFromString("00:00:00:00:00")
		peer2name, _ = router.PeerNameFromString("00:00:00:00:00")
		start, end   = net.ParseIP("10.0.0.0"), net.ParseIP("10.0.0.255")
		//middle       = net.ParseIP("10.0.0.128")
	)

	ring1 := New(start, end, peer1name)
	ring1.ClaimItAll()

	ring2 := New(start, end, peer2name)
	ring2.merge(ring1)
}

func TestBetween(t *testing.T) {
	var (
		peer1name, _ = router.PeerNameFromString("00:00:00:00:00")
		peer2name, _ = router.PeerNameFromString("00:00:00:00:00")
		start, end   = net.ParseIP("10.0.0.0"), net.ParseIP("10.0.0.255")
	)

	ring1 := New(start, end, peer1name)
	ring1.ClaimItAll()

	// First off, in a ring where everything is owned by the peer
	// between should return true for everything
	for i := 0; i <= 255; i++ {
		ip := ipam.Ip4int(net.ParseIP(fmt.Sprintf("10.0.0.%d", i)))
		wt.AssertTrue(t, ring1.between(ip, 0, 1), "between should be true!")
	}

	// Now, construct a ring with entries at +10 and -10
	// And check the correct behaviour
	plus10 := ipam.Ip4int(net.ParseIP("10.0.0.10"))
	minus10 := ipam.Ip4int(net.ParseIP("10.0.0.245"))
	ring1.Entries = []entry{entry{plus10, peer1name, 0, 0},
		entry{minus10, peer2name, 0, 0}}
	ring1.assertInvariants()
	for i := 10; i <= 244; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := ipam.Ip4int(net.ParseIP(ipStr))
		wt.AssertTrue(t, ring1.between(ip, 0, 1),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
	for i := 0; i <= 9; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := ipam.Ip4int(net.ParseIP(ipStr))
		wt.AssertTrue(t, ring1.between(ip, 1, 2),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
	for i := 245; i <= 255; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := ipam.Ip4int(net.ParseIP(ipStr))
		wt.AssertTrue(t, ring1.between(ip, 1, 2),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
}
