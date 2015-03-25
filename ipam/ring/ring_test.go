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
	peer1name, _ = router.PeerNameFromString("01:00:00:00:00:00")
	peer2name, _ = router.PeerNameFromString("02:00:00:00:00:00")

	ipStart, ipEnd    = net.ParseIP("10.0.0.0"), net.ParseIP("10.0.0.255")
	ipDot10, ipDot245 = net.ParseIP("10.0.0.10"), net.ParseIP("10.0.0.245")
	ipMiddle          = net.ParseIP("10.0.0.128")

	start, end    = utils.Ip4int(ipStart), utils.Ip4int(ipEnd)
	dot10, dot245 = utils.Ip4int(ipDot10), utils.Ip4int(ipDot245)
	middle        = utils.Ip4int(ipMiddle)
)

func TestInvariants(t *testing.T) {
	ring := New(ipStart, ipEnd, peer1name)

	// Check ring is sorted
	ring.Entries = []entry{{Token: dot245, Peer: peer1name}, {Token: dot10, Peer: peer2name}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrNotSorted, "Expected error")

	// Check tokens don't appear twice
	ring.Entries = []entry{{Token: dot245, Peer: peer1name}, {Token: dot245, Peer: peer2name}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenRepeated, "Expected error")

	// Check tokens are in bounds
	ring = New(ipDot10, ipDot245, peer1name)
	ring.Entries = []entry{{Token: start, Peer: peer1name}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenOutOfRange, "Expected error")

	ring.Entries = []entry{{Token: end, Peer: peer1name}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenOutOfRange, "Expected error")
}

func TestInsert(t *testing.T) {
	ring := New(ipStart, ipEnd, peer1name)
	ring.ClaimItAll()

	wt.AssertPanic(t, func() {
		ring.insertAt(0, entry{Token: start, Peer: peer1name})
	})

	ring.insertAt(1, entry{Token: dot245, Peer: peer1name})
	ring2 := New(ipStart, ipEnd, peer1name)
	ring2.Entries = []entry{{Token: start, Peer: peer1name, Free: 255}, {Token: dot245, Peer: peer1name}}
	wt.AssertEquals(t, ring, ring2)

	ring.insertAt(1, entry{Token: dot10, Peer: peer1name})
	ring2.Entries = []entry{{Token: start, Peer: peer1name, Free: 255}, {Token: dot10, Peer: peer1name}, {Token: dot245, Peer: peer1name}}
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

	ring1.Entries = []entry{{Token: dot10, Peer: peer1name}, {Token: dot245, Peer: peer2name}}
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

func TestGrantSimple(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	// Claim everything for peer1
	ring1.ClaimItAll()
	ring2.Entries = []entry{{Token: start, Peer: peer1name, Free: 255}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// Now grant everything to peer2
	ring1.GrantRangeToHost(ipStart, ipEnd, peer2name)
	ring2.Entries = []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// Now spint back to peer 1
	ring2.GrantRangeToHost(ipDot10, ipEnd, peer1name)
	ring1.Entries = []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}, {Token: dot10, Peer: peer1name}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// And spint back to peer 2 again
	ring1.GrantRangeToHost(ipDot245, ipEnd, peer2name)
	ring2.Entries = []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}, {Token: dot10, Peer: peer1name}, {Token: dot245, Peer: peer2name}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)
}

func TestGrantSplit(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	// Claim everything for peer1
	ring1.ClaimItAll()
	ring2.Entries = []entry{{Token: start, Peer: peer1name, Free: 255}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// Now grant a split range to peer2
	ring1.GrantRangeToHost(ipDot10, ipDot245, peer2name)
	ring2.Entries = []entry{{Token: start, Peer: peer1name, Free: 255}, {Token: dot10, Peer: peer2name}, {Token: dot245, Peer: peer1name}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)
}

func AssertSuccess(t *testing.T, err error) {
	if err != nil {
		wt.Fatalf(t, "Expected success, got '%s'", err.Error())
	}
}

func TestMergeSimple(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)
	ring3 := New(ipStart, ipEnd, peer2name)

	// Claim everything for peer1
	ring1.ClaimItAll()
	ring1.GrantRangeToHost(ipMiddle, ipEnd, peer2name)
	AssertSuccess(t, ring2.merge(*ring1))

	ring3.Entries = []entry{{Token: start, Peer: peer1name, Free: 255}, {Token: middle, Peer: peer2name}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)
	wt.AssertEquals(t, ring1.Entries, ring3.Entries)

	// Now to two different operations on either side,
	// check we can merge again
	ring1.GrantRangeToHost(ipStart, ipMiddle, peer2name)
	ring2.GrantRangeToHost(ipMiddle, ipEnd, peer1name)

	AssertSuccess(t, ring2.merge(*ring1))
	AssertSuccess(t, ring1.merge(*ring2))

	ring3.Entries = []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}, {Token: middle, Peer: peer1name, Version: 1}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)
	wt.AssertEquals(t, ring1.Entries, ring3.Entries)
}

func TestMergeErrors(t *testing.T) {
	// Cannot merge in an invalid ring
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)
	ring2.Entries = []entry{{Token: middle, Peer: peer2name}, {Token: start, Peer: peer2name}}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrNotSorted, "Expected ErrNotSorted")

	// Should merge two rings for different ranges
	ring2 = New(ipStart, ipMiddle, peer2name)
	ring2.Entries = []entry{}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrDifferentSubnets, "Expected ErrDifferentSubnets")

	// Cannot merge newer version of entry I own
	ring2 = New(ipStart, ipEnd, peer2name)
	ring1.Entries = []entry{{Token: start, Peer: peer1name}}
	ring2.Entries = []entry{{Token: start, Peer: peer1name, Version: 1}}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrNewerVersion, "Expected ErrNewerVersion")

	// Cannot merge two entries with same version but different hosts
	ring1.Entries = []entry{{Token: start, Peer: peer1name}}
	ring2.Entries = []entry{{Token: start, Peer: peer2name}}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrInvalidEntry, "Expected ErrInvalidEntry")

	// Cannot merge an entry into a range I own
	ring1.Entries = []entry{{Token: start, Peer: peer1name}}
	ring2.Entries = []entry{{Token: middle, Peer: peer2name}}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrEntryInMyRange, "Expected ErrEntryInMyRange")
}

func TestMergeMore(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	assertRing := func(ring *Ring, entries []entry) {
		tempRing := New(ipStart, ipEnd, peer2name)
		tempRing.Entries = entries
		wt.AssertEquals(t, ring.Entries, tempRing.Entries)
	}

	assertRing(ring1, []entry{})
	assertRing(ring2, []entry{})

	// Claim everything for peer1
	ring1.ClaimItAll()
	assertRing(ring1, []entry{{Token: start, Peer: peer1name, Free: 255}})
	assertRing(ring2, []entry{})

	// Check the merge sends it to the other ring
	AssertSuccess(t, ring2.merge(*ring1))
	assertRing(ring1, []entry{{Token: start, Peer: peer1name, Free: 255}})
	assertRing(ring2, []entry{{Token: start, Peer: peer1name, Free: 255}})

	// Give everything to peer2
	ring1.GrantRangeToHost(ipStart, ipEnd, peer2name)
	assertRing(ring1, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}})
	assertRing(ring2, []entry{{Token: start, Peer: peer1name, Free: 255}})

	AssertSuccess(t, ring2.merge(*ring1))
	assertRing(ring1, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}})
	assertRing(ring2, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}})

	// And carve off some space
	ring2.GrantRangeToHost(ipMiddle, ipEnd, peer1name)
	assertRing(ring1, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}})
	assertRing(ring2, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}, {Token: middle, Peer: peer1name}})

	// And merge back
	AssertSuccess(t, ring1.merge(*ring2))
	assertRing(ring1, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}, {Token: middle, Peer: peer1name}})
	assertRing(ring2, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}, {Token: middle, Peer: peer1name}})

	// This should be a no-op
	AssertSuccess(t, ring2.merge(*ring1))
	assertRing(ring1, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}, {Token: middle, Peer: peer1name}})
	assertRing(ring2, []entry{{Token: start, Peer: peer2name, Version: 1, Free: 255}, {Token: middle, Peer: peer1name}})
}

// A simple test, very similar to above, but using the marshalling to byte[]s
func TestGossip(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	assertRing := func(ring *Ring, entries []entry) {
		tempRing := New(ipStart, ipEnd, peer2name)
		tempRing.Entries = entries
		wt.AssertEquals(t, ring.Entries, tempRing.Entries)
	}

	assertRing(ring1, []entry{})
	assertRing(ring2, []entry{})

	// Claim everything for peer1
	ring1.ClaimItAll()
	assertRing(ring1, []entry{{Token: start, Peer: peer1name, Free: 255}})
	assertRing(ring2, []entry{})

	// Check the merge sends it to the other ring
	AssertSuccess(t, ring2.OnGossipBroadcast(ring1.GossipState()))
	assertRing(ring1, []entry{{Token: start, Peer: peer1name, Free: 255}})
	assertRing(ring2, []entry{{Token: start, Peer: peer1name, Free: 255}})
}

func TestFindFree(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)

	_, err := ring1.ChoosePeerToAskForSpace()
	wt.AssertTrue(t, err == ErrNoFreeSpace, "Expected ErrNoFreeSpace")

	ring1.Entries = []entry{{Token: start, Peer: peer1name}}
	_, err = ring1.ChoosePeerToAskForSpace()
	wt.AssertTrue(t, err == ErrNoFreeSpace, "Expected ErrNoFreeSpace")

	// We shouldn't return outselves
	ring1.ReportFree(ipStart, 10)
	_, err = ring1.ChoosePeerToAskForSpace()
	wt.AssertTrue(t, err == ErrNoFreeSpace, "Expected ErrNoFreeSpace")

	ring1.Entries = []entry{{Token: start, Peer: peer1name, Free: 1},
		{Token: start, Peer: peer1name, Free: 1}}
	_, err = ring1.ChoosePeerToAskForSpace()
	wt.AssertTrue(t, err == ErrNoFreeSpace, "Expected ErrNoFreeSpace")

	// We should return others
	ring1.Entries = []entry{{Token: start, Peer: peer2name, Free: 1}}
	peer, err := ring1.ChoosePeerToAskForSpace()
	AssertSuccess(t, err)
	wt.AssertEquals(t, peer, peer2name)

	ring1.Entries = []entry{{Token: start, Peer: peer2name, Free: 1},
		{Token: start, Peer: peer2name, Free: 1}}
	peer, err = ring1.ChoosePeerToAskForSpace()
	AssertSuccess(t, err)
	wt.AssertEquals(t, peer, peer2name)
}

func TestMisc(t *testing.T) {
	ring := New(ipStart, ipEnd, peer1name)

	wt.AssertTrue(t, ring.Empty(), "empty")

	ring.ClaimItAll()
	println(ring.String())
}

func TestEmptyGossip(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	ring1.ClaimItAll()
	// This used to panic, and it shouldn't
	AssertSuccess(t, ring1.merge(*ring2))
}

func TestMergeOldMessage(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	ring1.ClaimItAll()
	AssertSuccess(t, ring2.merge(*ring1))

	ring1.GrantRangeToHost(ipMiddle, ipEnd, peer1name)
	AssertSuccess(t, ring1.merge(*ring2))
}

func TestSplitRangeAtBeginning(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	ring1.ClaimItAll()
	AssertSuccess(t, ring2.merge(*ring1))

	ring1.GrantRangeToHost(ipStart, ipMiddle, peer2name)
	AssertSuccess(t, ring2.merge(*ring1))
}

func TestOwnedRange(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring1.ClaimItAll()

	wt.AssertTrue(t, ring1.OwnedRanges().Equal(
		[]Range{{Start: ipStart, End: ipEnd}}), "invalid")

	ring1.GrantRangeToHost(ipMiddle, ipEnd, peer2name)
	wt.AssertTrue(t, ring1.OwnedRanges().Equal(
		[]Range{{Start: ipStart, End: ipMiddle}}), "invalid")

	ring2 := New(ipStart, ipEnd, peer2name)
	ring2.merge(*ring1)
	wt.AssertTrue(t, ring2.OwnedRanges().Equal(
		[]Range{{Start: ipMiddle, End: ipEnd}}), "invalid")

	ring2.Entries = []entry{{Token: middle, Peer: peer2name}}
	wt.AssertTrue(t, ring2.OwnedRanges().Equal(
		[]Range{{Start: ipStart, End: ipMiddle}, {Start: ipMiddle, End: ipEnd}}), "invalid")
}
