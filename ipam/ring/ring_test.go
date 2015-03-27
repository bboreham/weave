package ring

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"testing"

	"github.com/zettio/weave/ipam/utils"
	"github.com/zettio/weave/router"
	wt "github.com/zettio/weave/testing"
)

var (
	peer1name, _ = router.PeerNameFromString("01:00:00:00:02:00")
	peer2name, _ = router.PeerNameFromString("02:00:00:00:02:00")

	ipStart, ipEnd          = net.ParseIP("10.0.0.0"), net.ParseIP("10.0.0.255")
	ipStartPlus, ipEndMinus = net.ParseIP("10.0.0.1"), net.ParseIP("10.0.0.254")
	ipDot10, ipDot245       = net.ParseIP("10.0.0.10"), net.ParseIP("10.0.0.245")
	ipDot250                = net.ParseIP("10.0.0.250")
	ipMiddle                = net.ParseIP("10.0.0.128")

	start, end          = utils.IP4int(ipStart), utils.IP4int(ipEnd)
	startPlus, endMinus = utils.IP4int(ipStartPlus), utils.IP4int(ipEndMinus)
	dot10, dot245       = utils.IP4int(ipDot10), utils.IP4int(ipDot245)
	dot250              = utils.IP4int(ipDot250)
	middle              = utils.IP4int(ipMiddle)
)

func TestInvariants(t *testing.T) {
	ring := New(ipStart, ipEnd, peer1name)

	// Check ring is sorted
	ring.Entries = []*entry{{Token: dot245, Peer: peer1name}, {Token: dot10, Peer: peer2name}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrNotSorted, "Expected error")

	// Check tokens don't appear twice
	ring.Entries = []*entry{{Token: dot245, Peer: peer1name}, {Token: dot245, Peer: peer2name}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenRepeated, "Expected error")

	// Check tokens are in bounds
	ring = New(ipDot10, ipDot245, peer1name)
	ring.Entries = []*entry{{Token: start, Peer: peer1name}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenOutOfRange, "Expected error")

	ring.Entries = []*entry{{Token: end, Peer: peer1name}}
	wt.AssertTrue(t, ring.checkInvariants() == ErrTokenOutOfRange, "Expected error")
}

func TestInsert(t *testing.T) {
	ring := New(ipStart, ipEnd, peer1name)
	ring.Entries = []*entry{{Token: start, Peer: peer1name, Free: 255}}

	wt.AssertPanic(t, func() {
		ring.Entries.insert(entry{Token: start, Peer: peer1name})
	})

	ring.Entries.entry(0).Free = 0
	ring.Entries.insert(entry{Token: dot245, Peer: peer1name})
	ring2 := New(ipStart, ipEnd, peer1name)
	ring2.Entries = []*entry{{Token: start, Peer: peer1name, Free: 0}, {Token: dot245, Peer: peer1name}}
	wt.AssertEquals(t, ring, ring2)

	ring.Entries.insert(entry{Token: dot10, Peer: peer1name})
	ring2.Entries = []*entry{{Token: start, Peer: peer1name, Free: 0}, {Token: dot10, Peer: peer1name}, {Token: dot245, Peer: peer1name}}
	wt.AssertEquals(t, ring, ring2)
}

func TestBetween(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring1.Entries = []*entry{{Token: start, Peer: peer1name, Free: 255}}

	// First off, in a ring where everything is owned by the peer
	// between should return true for everything
	for i := 1; i <= 255; i++ {
		ip := utils.IP4int(net.ParseIP(fmt.Sprintf("10.0.0.%d", i)))
		wt.AssertTrue(t, ring1.Entries.between(ip, 0, 1), "between should be true!")
	}

	// Now, construct a ring with entries at +10 and -10
	// And check the correct behaviour

	ring1.Entries = []*entry{{Token: dot10, Peer: peer1name}, {Token: dot245, Peer: peer2name}}
	ring1.assertInvariants()
	for i := 10; i <= 244; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := utils.IP4int(net.ParseIP(ipStr))
		wt.AssertTrue(t, ring1.Entries.between(ip, 0, 1),
			fmt.Sprintf("Between should be true for %s!", ipStr))
		wt.AssertFalse(t, ring1.Entries.between(ip, 1, 2),
			fmt.Sprintf("Between should be false for %s!", ipStr))
	}
	for i := 0; i <= 9; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := utils.IP4int(net.ParseIP(ipStr))
		wt.AssertFalse(t, ring1.Entries.between(ip, 0, 1),
			fmt.Sprintf("Between should be false for %s!", ipStr))
		wt.AssertTrue(t, ring1.Entries.between(ip, 1, 2),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
	for i := 245; i <= 255; i++ {
		ipStr := fmt.Sprintf("10.0.0.%d", i)
		ip := utils.IP4int(net.ParseIP(ipStr))
		wt.AssertFalse(t, ring1.Entries.between(ip, 0, 1),
			fmt.Sprintf("Between should be false for %s!", ipStr))
		wt.AssertTrue(t, ring1.Entries.between(ip, 1, 2),
			fmt.Sprintf("Between should be true for %s!", ipStr))
	}
}

func TestGrantSimple(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	// Claim everything for peer1 - NB the special reservation
	ring1.ClaimItAll()
	wt.AssertEquals(t, ring1.Entries, entries{{Token: startPlus, Peer: peer1name, Free: 253},
		{Token: endMinus, Peer: router.UnknownPeerName}})

	// Now grant everything to peer2
	ring1.GrantRangeToHost(ipStartPlus, ipEndMinus, peer2name)
	ring2.Entries = []*entry{{Token: startPlus, Peer: peer2name, Free: 253, Version: 1},
		{Token: endMinus, Peer: router.UnknownPeerName}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// Now spint back to peer 1
	ring2.GrantRangeToHost(ipDot10, ipEndMinus, peer1name)
	ring1.Entries = []*entry{{Token: startPlus, Peer: peer2name, Free: 9, Version: 2},
		{Token: dot10, Peer: peer1name, Free: 244},
		{Token: endMinus, Peer: router.UnknownPeerName}}
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// And spint back to peer 2 again
	ring1.GrantRangeToHost(ipDot245, ipEndMinus, peer2name)
	wt.AssertEquals(t, ring1.Entries, entries{{Token: startPlus, Peer: peer2name, Free: 9, Version: 2},
		{Token: dot10, Peer: peer1name, Free: 235, Version: 1},
		{Token: dot245, Peer: peer2name, Free: 9},
		{Token: endMinus, Peer: router.UnknownPeerName}})
}

func TestGrantSplit(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	// Claim everything for peer1
	ring1.Entries = []*entry{{Token: start, Peer: peer1name, Free: 255}}
	ring2.merge(*ring1)
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// Now grant a split range to peer2
	ring1.GrantRangeToHost(ipDot10, ipDot245, peer2name)
	wt.AssertEquals(t, ring1.Entries, entries{{Token: start, Peer: peer1name, Version: 1, Free: 10},
		{Token: dot10, Peer: peer2name, Free: 235},
		{Token: dot245, Peer: peer1name, Free: 10}})
}

func TestMergeSimple(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	// Claim everything for peer1
	ring1.ClaimItAll()
	ring1.GrantRangeToHost(ipMiddle, ipEndMinus, peer2name)
	wt.AssertSuccess(t, ring2.merge(*ring1))

	wt.AssertEquals(t, ring1.Entries, entries{{Token: startPlus, Peer: peer1name, Version: 1, Free: 127},
		{Token: middle, Peer: peer2name, Free: 126},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// Now to two different operations on either side,
	// check we can merge again
	ring1.GrantRangeToHost(ipStartPlus, ipMiddle, peer2name)
	ring2.GrantRangeToHost(ipMiddle, ipEndMinus, peer1name)

	wt.AssertSuccess(t, ring2.merge(*ring1))
	wt.AssertSuccess(t, ring1.merge(*ring2))

	wt.AssertEquals(t, ring1.Entries, entries{{Token: startPlus, Peer: peer2name, Free: 127, Version: 2},
		{Token: middle, Peer: peer1name, Version: 1, Free: 126},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)
}

func TestMergeErrors(t *testing.T) {
	// Cannot merge in an invalid ring
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)
	ring2.Entries = []*entry{{Token: middle, Peer: peer2name}, {Token: start, Peer: peer2name}}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrNotSorted, "Expected ErrNotSorted")

	// Should merge two rings for different ranges
	ring2 = New(ipStart, ipMiddle, peer2name)
	ring2.Entries = []*entry{}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrDifferentSubnets, "Expected ErrDifferentSubnets")

	// Cannot merge newer version of entry I own
	ring2 = New(ipStart, ipEnd, peer2name)
	ring1.Entries = []*entry{{Token: start, Peer: peer1name}}
	ring2.Entries = []*entry{{Token: start, Peer: peer1name, Version: 1}}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrNewerVersion, "Expected ErrNewerVersion")

	// Cannot merge two entries with same version but different hosts
	ring1.Entries = []*entry{{Token: start, Peer: peer1name}}
	ring2.Entries = []*entry{{Token: start, Peer: peer2name}}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrInvalidEntry, "Expected ErrInvalidEntry")

	// Cannot merge an entry into a range I own
	ring1.Entries = []*entry{{Token: start, Peer: peer1name}}
	ring2.Entries = []*entry{{Token: middle, Peer: peer2name}}
	wt.AssertTrue(t, ring1.merge(*ring2) == ErrEntryInMyRange, "Expected ErrEntryInMyRange")
}

func TestMergeMore(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	assertRing := func(ring *Ring, entries entries) {
		wt.AssertEquals(t, ring.Entries, entries)
	}

	assertRing(ring1, []*entry{})
	assertRing(ring2, []*entry{})

	// Claim everything for peer1
	ring1.ClaimItAll()
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer1name, Free: 253},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring2, []*entry{})

	// Check the merge sends it to the other ring
	wt.AssertSuccess(t, ring2.merge(*ring1))
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer1name, Free: 253},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring2, []*entry{{Token: startPlus, Peer: peer1name, Free: 253},
		{Token: endMinus, Peer: router.UnknownPeerName}})

	// Give everything to peer2
	ring1.GrantRangeToHost(ipStartPlus, ipEndMinus, peer2name)
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer2name, Free: 253, Version: 1},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring2, []*entry{{Token: startPlus, Peer: peer1name, Free: 253},
		{Token: endMinus, Peer: router.UnknownPeerName}})

	wt.AssertSuccess(t, ring2.merge(*ring1))
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer2name, Free: 253, Version: 1},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring2, []*entry{{Token: startPlus, Peer: peer2name, Free: 253, Version: 1},
		{Token: endMinus, Peer: router.UnknownPeerName}})

	// And carve off some space
	ring2.GrantRangeToHost(ipMiddle, ipEndMinus, peer1name)
	assertRing(ring2, []*entry{{Token: startPlus, Peer: peer2name, Free: 127, Version: 2},
		{Token: middle, Peer: peer1name, Free: 126},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer2name, Free: 253, Version: 1},
		{Token: endMinus, Peer: router.UnknownPeerName}})

	// And merge back
	wt.AssertSuccess(t, ring1.merge(*ring2))
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer2name, Free: 127, Version: 2},
		{Token: middle, Peer: peer1name, Free: 126},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring2, []*entry{{Token: startPlus, Peer: peer2name, Free: 127, Version: 2},
		{Token: middle, Peer: peer1name, Free: 126},
		{Token: endMinus, Peer: router.UnknownPeerName}})

	// This should be a no-op
	wt.AssertSuccess(t, ring2.merge(*ring1))
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer2name, Free: 127, Version: 2},
		{Token: middle, Peer: peer1name, Free: 126},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring2, []*entry{{Token: startPlus, Peer: peer2name, Free: 127, Version: 2},
		{Token: middle, Peer: peer1name, Free: 126},
		{Token: endMinus, Peer: router.UnknownPeerName}})
}

func TestMergeSplit(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	// Claim everything for peer2
	ring1.Entries = []*entry{{Token: start, Peer: peer2name, Free: 255}}
	wt.AssertSuccess(t, ring2.merge(*ring1))
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// Now grant a split range to peer1
	ring2.GrantRangeToHost(ipDot10, ipDot245, peer1name)
	wt.AssertEquals(t, ring2.Entries, entries{{Token: start, Peer: peer2name, Version: 1, Free: 10},
		{Token: dot10, Peer: peer1name, Free: 235},
		{Token: dot245, Peer: peer2name, Free: 10}})
	wt.AssertSuccess(t, ring1.merge(*ring2))
	wt.AssertEquals(t, ring1.Entries, entries{{Token: start, Peer: peer2name, Version: 1, Free: 10},
		{Token: dot10, Peer: peer1name, Free: 235},
		{Token: dot245, Peer: peer2name, Free: 10}})
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)
}

func TestMergeSplit2(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	// Claim everything for peer2
	ring1.Entries = []*entry{{Token: start, Peer: peer2name, Free: 250}, {Token: dot250, Peer: peer2name, Free: 5}}
	wt.AssertSuccess(t, ring2.merge(*ring1))
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)

	// Now grant a split range to peer1
	ring2.GrantRangeToHost(ipDot10, ipDot245, peer1name)
	wt.AssertEquals(t, ring2.Entries, entries{{Token: start, Peer: peer2name, Version: 1, Free: 10},
		{Token: dot10, Peer: peer1name, Free: 235},
		{Token: dot245, Peer: peer2name, Free: 5}, {Token: dot250, Peer: peer2name, Free: 5}})
	wt.AssertSuccess(t, ring1.merge(*ring2))
	wt.AssertEquals(t, ring1.Entries, entries{{Token: start, Peer: peer2name, Version: 1, Free: 10},
		{Token: dot10, Peer: peer1name, Free: 235},
		{Token: dot245, Peer: peer2name, Free: 5}, {Token: dot250, Peer: peer2name, Free: 5}})
	wt.AssertEquals(t, ring1.Entries, ring2.Entries)
}

// A simple test, very similar to above, but using the marshalling to byte[]s
func TestGossip(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	assertRing := func(ring *Ring, entries entries) {
		wt.AssertEquals(t, ring.Entries, entries)
	}

	assertRing(ring1, []*entry{})
	assertRing(ring2, []*entry{})

	// Claim everything for peer1
	ring1.ClaimItAll()
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer1name, Free: 253},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring2, []*entry{})

	// Check the merge sends it to the other ring
	wt.AssertSuccess(t, ring2.UpdateRing(ring1.GossipState()))
	assertRing(ring1, []*entry{{Token: startPlus, Peer: peer1name, Free: 253},
		{Token: endMinus, Peer: router.UnknownPeerName}})
	assertRing(ring2, []*entry{{Token: startPlus, Peer: peer1name, Free: 253},
		{Token: endMinus, Peer: router.UnknownPeerName}})
}

func TestFindFree(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)

	_, err := ring1.ChoosePeerToAskForSpace()
	wt.AssertTrue(t, err == ErrNoFreeSpace, "Expected ErrNoFreeSpace")

	ring1.Entries = []*entry{{Token: start, Peer: peer1name}}
	_, err = ring1.ChoosePeerToAskForSpace()
	wt.AssertTrue(t, err == ErrNoFreeSpace, "Expected ErrNoFreeSpace")

	// We shouldn't return outselves
	ring1.ReportFree(ipStart, 10)
	_, err = ring1.ChoosePeerToAskForSpace()
	wt.AssertTrue(t, err == ErrNoFreeSpace, "Expected ErrNoFreeSpace")

	ring1.Entries = []*entry{{Token: start, Peer: peer1name, Free: 1},
		{Token: start, Peer: peer1name, Free: 1}}
	_, err = ring1.ChoosePeerToAskForSpace()
	wt.AssertTrue(t, err == ErrNoFreeSpace, "Expected ErrNoFreeSpace")

	// We should return others
	ring1.Entries = []*entry{{Token: start, Peer: peer2name, Free: 1}}
	peer, err := ring1.ChoosePeerToAskForSpace()
	wt.AssertSuccess(t, err)
	wt.AssertEquals(t, peer, peer2name)

	ring1.Entries = []*entry{{Token: start, Peer: peer2name, Free: 1},
		{Token: start, Peer: peer2name, Free: 1}}
	peer, err = ring1.ChoosePeerToAskForSpace()
	wt.AssertSuccess(t, err)
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
	wt.AssertSuccess(t, ring1.merge(*ring2))
}

func TestMergeOldMessage(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	ring1.ClaimItAll()
	wt.AssertSuccess(t, ring2.merge(*ring1))

	ring1.GrantRangeToHost(ipMiddle, ipEndMinus, peer1name)
	wt.AssertSuccess(t, ring1.merge(*ring2))
}

func TestSplitRangeAtBeginning(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring2 := New(ipStart, ipEnd, peer2name)

	ring1.ClaimItAll()
	wt.AssertSuccess(t, ring2.merge(*ring1))

	ring1.GrantRangeToHost(ipStartPlus, ipMiddle, peer2name)
	wt.AssertSuccess(t, ring2.merge(*ring1))
}

func (r1 Range) Equal(r2 Range) bool {
	return r1.Start.Equal(r2.Start) && r1.End.Equal(r2.End)
}

func RangesEqual(rs1 []Range, rs2 []Range) bool {
	if len(rs1) != len(rs2) {
		return false
	}

	for i := 0; i < len(rs1); i++ {
		if !rs1[i].Equal(rs2[i]) {
			return false
		}
	}

	return true
}

func TestOwnedRange(t *testing.T) {
	ring1 := New(ipStart, ipEnd, peer1name)
	ring1.ClaimItAll()

	wt.AssertTrue(t, RangesEqual(ring1.OwnedRanges(),
		[]Range{{Start: ipStartPlus, End: ipEndMinus}}), "invalid")

	ring1.GrantRangeToHost(ipMiddle, ipEndMinus, peer2name)
	wt.AssertTrue(t, RangesEqual(ring1.OwnedRanges(),
		[]Range{{Start: ipStartPlus, End: ipMiddle}}), "invalid")

	ring2 := New(ipStart, ipEnd, peer2name)
	ring2.merge(*ring1)
	wt.AssertTrue(t, RangesEqual(ring2.OwnedRanges(),
		[]Range{{Start: ipMiddle, End: ipEndMinus}}), "invalid")

	ring2.Entries = []*entry{{Token: middle, Peer: peer2name}}
	wt.AssertTrue(t, RangesEqual(ring2.OwnedRanges(),
		[]Range{{Start: ipStart, End: ipMiddle}, {Start: ipMiddle, End: ipEnd}}), "invalid")

	ring2.Entries = []*entry{{Token: dot10, Peer: peer2name}, {Token: middle, Peer: peer2name}}
	wt.AssertTrue(t, RangesEqual(ring2.OwnedRanges(),
		[]Range{{Start: ipStart, End: ipDot10}, {Start: ipDot10, End: ipMiddle},
			{Start: ipMiddle, End: ipEnd}}), "invalid")
}

type uint32slice []uint32

func (s uint32slice) Len() int           { return len(s) }
func (s uint32slice) Less(i, j int) bool { return s[i] < s[j] }
func (s uint32slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func TestFuzzRing(t *testing.T) {
	var (
		numPeers   = 25
		iterations = 1000
	)

	peers := make([]router.PeerName, numPeers)
	for i := 0; i < numPeers; i++ {
		peer, _ := router.PeerNameFromString(fmt.Sprintf("%02d:00:00:00:02:00", i))
		peers[i] = peer
	}

	// Make a valid, random ring
	makeGoodRandomRing := func() *Ring {
		addressSpace := end - start
		numTokens := rand.Intn(int(addressSpace))

		tokenMap := make(map[uint32]bool)
		for i := 0; i < numTokens; i++ {
			tokenMap[uint32(rand.Intn(int(addressSpace)))] = true
		}
		var tokens []uint32
		for token := range tokenMap {
			tokens = append(tokens, token)
		}
		sort.Sort(uint32slice(tokens))

		peer := peers[rand.Intn(len(peers))]
		ring := New(ipStart, ipEnd, peer)
		for _, token := range tokens {
			peer = peers[rand.Intn(len(peers))]
			ring.Entries = append(ring.Entries, &entry{Token: start + token, Peer: peer})
		}

		ring.assertInvariants()
		return ring
	}

	for i := 0; i < iterations; i++ {
		// make 2 random rings
		ring1 := makeGoodRandomRing()
		ring2 := makeGoodRandomRing()

		// Merge them - this might fail, we don't care
		// We just want to make sure it doest panic
		ring1.merge(*ring2)

		// Check whats left still passes assertions
		ring1.assertInvariants()
		ring2.assertInvariants()
	}

	// Make an invalid, random ring
	makeBadRandomRing := func() *Ring {
		addressSpace := end - start
		numTokens := rand.Intn(int(addressSpace))
		tokens := make([]uint32, numTokens)
		for i := 0; i < numTokens; i++ {
			tokens[i] = uint32(rand.Intn(int(addressSpace)))
		}

		peer := peers[rand.Intn(len(peers))]
		ring := New(ipStart, ipEnd, peer)
		for _, token := range tokens {
			peer = peers[rand.Intn(len(peers))]
			ring.Entries = append(ring.Entries, &entry{Token: start + token, Peer: peer})
		}

		return ring
	}

	for i := 0; i < iterations; i++ {
		// make 2 random rings
		ring1 := makeGoodRandomRing()
		ring2 := makeBadRandomRing()

		// Merge them - this might fail, we don't care
		// We just want to make sure it doest panic
		ring1.merge(*ring2)

		// Check whats left still passes assertions
		ring1.assertInvariants()
	}
}

func TestFuzzRingHard(t *testing.T) {
	var (
		numPeers   = 100
		iterations = 2000
	)

	peers := make([]router.PeerName, numPeers)
	rings := make([]*Ring, numPeers)
	for i := 0; i < numPeers; i++ {
		peer, _ := router.PeerNameFromString(fmt.Sprintf("%02d:00:00:00:00:00", i))
		peers[i] = peer
		rings[i] = New(ipStart, ipEnd, peer)
	}

	rings[0].ClaimItAll()

	for i := 0; i < iterations; i++ {
		var ringsWithRanges []*Ring
		for _, ring := range rings {
			if len(ring.OwnedRanges()) > 0 {
				ringsWithRanges = append(ringsWithRanges, ring)
			}
		}

		if len(ringsWithRanges) > 0 {
			// Produce a random split in a random owned range, given to a random peer
			ring := ringsWithRanges[rand.Intn(len(ringsWithRanges))]
			ownedRanges := ring.OwnedRanges()
			rangeToSplit := ownedRanges[rand.Intn(len(ownedRanges))]
			size := utils.Subtract(rangeToSplit.End, rangeToSplit.Start)
			ipInRange := utils.Add(rangeToSplit.Start, uint32(rand.Intn(int(size))))
			peerToGiveToIndex := rand.Intn(numPeers)
			peerToGiveTo := peers[peerToGiveToIndex]
			//fmt.Printf("%s: Granting [%v, %v) to %s\n", ring.Peername, ipInRange, rangeToSplit.End, peerToGiveTo)
			ring.GrantRangeToHost(ipInRange, rangeToSplit.End, peerToGiveTo)

			// Now 'gossip' this to a random host (note, not the same host as above)
			otherRing := rings[rand.Intn(numPeers)]
			wt.AssertSuccess(t, otherRing.merge(*ring))
			continue
		}

		// No rings think they own anything (as gossip might be behind)
		// We're going to pick a random host (which has entries) and gossip
		// it to a random host (which may or may not have entries).
		var ringsWithEntries []*Ring
		for _, ring := range rings {
			if len(ring.Entries) > 0 {
				ringsWithEntries = append(ringsWithEntries, ring)
			}
		}
		ring1 := ringsWithEntries[rand.Intn(len(ringsWithEntries))]
		ring2 := rings[rand.Intn(numPeers)]
		//fmt.Printf("%s: 'Gossiping' to %s\n", ring1.Peername, ring2.Peername)
		wt.AssertSuccess(t, ring2.merge(*ring1))
	}
}

func (es entries) String() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "[")
	for i, entry := range es {
		fmt.Fprintf(&buffer, "%+v", *entry)
		if i+1 < len(es) {
			fmt.Fprintf(&buffer, " ")
		}
	}
	fmt.Fprintf(&buffer, "]")
	return buffer.String()
}
