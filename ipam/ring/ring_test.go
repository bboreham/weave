package ring

import (
	"fmt"
	"github.com/zettio/weave/ipam/utils"
	"github.com/zettio/weave/router"
	wt "github.com/zettio/weave/testing"
	"net"
	"testing"
)

var (
	peer1name, _ = router.PeerNameFromString("00:00:00:00:00")
	peer2name, _ = router.PeerNameFromString("00:00:00:00:00")

	ipStart, ipEnd    = net.ParseIP("10.0.0.0"), net.ParseIP("10.0.0.255")
	ipDot10, ipDot245 = net.ParseIP("10.0.0.10"), net.ParseIP("10.0.0.245")

	start, end    = utils.Ip4int(ipStart), utils.Ip4int(ipEnd)
	dot10, dot245 = utils.Ip4int(ipDot10), utils.Ip4int(ipDot245)
)

func TestInvariants(t *testing.T) {
	ring := New(ipStart, ipEnd, peer1name)

	// Check ring is sorted
	ring.Entries = []entry{{dot245, peer1name, 0, 0}, {dot10, peer2name, 0, 0}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrNotSorted, "Expected error")

	// Check tokens don't appear twice
	ring.Entries = []entry{{dot245, peer1name, 0, 0}, {dot245, peer2name, 0, 0}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenRepeated, "Expected error")

	// Check tokens are in bounds
	ring = New(ipDot10, ipDot245, peer1name)
	ring.Entries = []entry{{start, peer1name, 0, 0}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenOutOfRange, "Expected error")

	ring.Entries = []entry{{end, peer1name, 0, 0}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenOutOfRange, "Expected error")
}

func TestRing(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring1.ClaimItAll()

	ring2 := New(ipStart, ipEnd, peer2name)
	ring2.merge(ring1)

	ring1.GrantRangeToHost(ipDot10, ipEnd, peer2name)
}

func TestInsert(t *testing.T) {
	ring := New(ipStart, ipEnd, peer1name)
	ring.ClaimItAll()

	wt.AssertPanic(t, func() {
		ring.insertAt(0, entry{start, peer1name, 0, 0})
	})

	ring.insertAt(1, entry{dot245, peer1name, 0, 0})
	ring2 := New(ipStart, ipEnd, peer1name)
	ring2.Entries = []entry{{start, peer1name, 0, 0}, {dot245, peer1name, 0, 0}}
	wt.AssertEquals(t, ring, ring2)

	ring.insertAt(1, entry{dot10, peer1name, 0, 0})
	ring2.Entries = []entry{{start, peer1name, 0, 0}, {dot10, peer1name, 0, 0}, {dot245, peer1name, 0, 0}}
	wt.AssertEquals(t, ring, ring2)
}

func TestBetween(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring1.ClaimItAll()

	// First off, in a ring where everything is owned by the peer
	// between should return true for everything
	for i := 1; i <= 255; i++ {
		ip := utils.Ip4int(net.ParseIP(fmt.Sprintf("10.0.0.%d", i)))
		wt.AssertTrue(t, ring1.between(ip, 0, 1), "between should be true!")
	}

	// Now, construct a ring with entries at +10 and -10
	// And check the correct behaviour

	ring1.Entries = []entry{{dot10, peer1name, 0, 0}, {dot245, peer2name, 0, 0}}
	ring1.assertInvariants()
	for i := 10; i <= 244; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := utils.Ip4int(net.ParseIP(ipStr))
		wt.AssertTrue(t, ring1.between(ip, 0, 1),
			fmt.Sprintf("Between should be true for %s!", ipStr))
		wt.AssertFalse(t, ring1.between(ip, 1, 2),
			fmt.Sprintf("Between should be false for %s!", ipStr))
	}
	for i := 0; i <= 9; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := utils.Ip4int(net.ParseIP(ipStr))
		wt.AssertFalse(t, ring1.between(ip, 0, 1),
			fmt.Sprintf("Between should be false for %s!", ipStr))
		wt.AssertTrue(t, ring1.between(ip, 1, 2),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
	for i := 245; i <= 255; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := utils.Ip4int(net.ParseIP(ipStr))
		wt.AssertFalse(t, ring1.between(ip, 0, 1),
			fmt.Sprintf("Between should be false for %s!", ipStr))
		wt.AssertTrue(t, ring1.between(ip, 1, 2),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
}
