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
		middle       = net.ParseIP("10.0.0.128")
	)

	ring1 := New(start, end, peer1name)
	ring1.ClaimItAll()

	ring2 := New(start, end, peer2name)
	ring2.merge(ring1)

	ring1.GrantRangeToHost(middle, end, peer2name)
}

func TestBetween(t *testing.T) {
	var (
		peer1name, _ = router.PeerNameFromString("00:00:00:00:00")
		peer2name, _ = router.PeerNameFromString("00:00:00:00:00")
		start, end   = net.ParseIP("10.0.0.0"), net.ParseIP("10.0.0.255")
		plus10       = ipam.Ip4int(net.ParseIP("10.0.0.10"))
		minus10      = ipam.Ip4int(net.ParseIP("10.0.0.245"))
	)

	ring1 := New(start, end, peer1name)
	ring1.ClaimItAll()

	// First off, in a ring where everything is owned by the peer
	// between should return true for everything
	for i := 1; i <= 255; i++ {
		ip := ipam.Ip4int(net.ParseIP(fmt.Sprintf("10.0.0.%d", i)))
		wt.AssertTrue(t, ring1.between(ip, 0, 1), "between should be true!")
	}

	// Now, construct a ring with entries at +10 and -10
	// And check the correct behaviour

	ring1.Entries = []entry{{plus10, peer1name, 0, 0}, {minus10, peer2name, 0, 0}}
	ring1.assertInvariants()
	for i := 10; i <= 244; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := ipam.Ip4int(net.ParseIP(ipStr))
		wt.AssertTrue(t, ring1.between(ip, 0, 1),
			fmt.Sprintf("Between should be true for %s!", ipStr))
		wt.AssertFalse(t, ring1.between(ip, 1, 2),
			fmt.Sprintf("Between should be false for %s!", ipStr))
	}
	for i := 0; i <= 9; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := ipam.Ip4int(net.ParseIP(ipStr))
		wt.AssertFalse(t, ring1.between(ip, 0, 1),
			fmt.Sprintf("Between should be false for %s!", ipStr))
		wt.AssertTrue(t, ring1.between(ip, 1, 2),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
	for i := 245; i <= 255; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := ipam.Ip4int(net.ParseIP(ipStr))
		wt.AssertFalse(t, ring1.between(ip, 0, 1),
			fmt.Sprintf("Between should be false for %s!", ipStr))
		wt.AssertTrue(t, ring1.between(ip, 1, 2),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
}

func TestInvariants(t *testing.T) {
	var (
		peer1name, _        = router.PeerNameFromString("00:00:00:00:00")
		peer2name, _        = router.PeerNameFromString("00:00:00:00:00")
		startIP, endIP      = net.ParseIP("10.0.0.0"), net.ParseIP("10.0.0.255")
		plus10ip, minus10ip = net.ParseIP("10.0.0.10"), net.ParseIP("10.0.0.245")
		start, end          = ipam.Ip4int(startIP), ipam.Ip4int(endIP)
		plus10, minus10     = ipam.Ip4int(plus10ip), ipam.Ip4int(minus10ip)
	)

	ring := New(startIP, endIP, peer1name)

	// Check ring is sorted
	ring.Entries = []entry{{minus10, peer1name, 0, 0}, {plus10, peer2name, 0, 0}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrNotSorted, "Expected error")

	// Check tokens don't appear twice
	ring.Entries = []entry{{minus10, peer1name, 0, 0}, {minus10, peer2name, 0, 0}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenRepeated, "Expected error")

	// Check tokens are in bounds
	ring = New(plus10ip, minus10ip, peer1name)
	ring.Entries = []entry{{start, peer1name, 0, 0}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenOutOfRange, "Expected error")

	ring.Entries = []entry{{end, peer1name, 0, 0}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenOutOfRange, "Expected error")
}
