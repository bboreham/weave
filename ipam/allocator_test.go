package ipam

import (
	"github.com/zettio/weave/common"
	"github.com/zettio/weave/router"
	wt "github.com/zettio/weave/testing"
	"net"
	"testing"
	"time"
)

const (
	testStart1 = "10.0.1.0"
	testStart2 = "10.0.2.0"
	testStart3 = "10.0.3.0"
)

func TestAllocFree(t *testing.T) {
	const (
		container1 = "abcdef"
		container2 = "baddf00d"
		container3 = "b01df00d"
		testAddr1  = "10.0.3.4"
		spaceSize  = 4
	)

	alloc := testAllocator(t, "01:00:00:01:00:00", testAddr1+"/30")
	defer alloc.Stop()

	addr1 := alloc.GetFor(container1, nil)
	wt.AssertEqualString(t, addr1.String(), testAddr1, "address")

	// Ask for another address for a different container and check it's different
	addr2 := alloc.GetFor(container2, nil)
	if addr2.String() == testAddr1 {
		t.Fatalf("Expected different address but got %s", addr2)
	}

	// Ask for the first container again and we should get the same address again
	addr1a := alloc.GetFor(container1, nil)
	wt.AssertEqualString(t, addr1a.String(), testAddr1, "address")

	// Now free the first one, and we should get it back when we ask
	alloc.Free(container1, net.ParseIP(testAddr1))
	addr3 := alloc.GetFor(container3, nil)
	wt.AssertEqualString(t, addr3.String(), testAddr1, "address")

	alloc.DeleteRecordsFor(container2)
	alloc.DeleteRecordsFor(container3)
	alloc.String() // force sync-up after async call
}

func TestMultiSpaces(t *testing.T) {
	alloc := testAllocator(t, "01:00:00:01:00:00", testStart1+"/30")
	defer alloc.Stop()
	alloc.addSpace(testStart1, 1)
	alloc.addSpace(testStart2, 3)

	wt.AssertEqualUint32(t, alloc.numFreeAddresses(), 4, "Total free addresses")

	addr1 := alloc.GetFor("abcdef", nil)
	wt.AssertEqualString(t, addr1.String(), testStart1, "address")

	// First space should now be full and this address should come from second space
	addr2 := alloc.GetFor("fedcba", nil)
	wt.AssertEqualString(t, addr2.String(), testStart2, "address")
	wt.AssertEqualUint32(t, alloc.numFreeAddresses(), 2, "Total free addresses")
}

// Test the election mechanism
func TestGossip2(t *testing.T) {
	const (
		donateSize     = 5
		donateStart    = "10.0.1.7"
		peerNameString = "02:00:00:02:00:00"
	)

	baseTime := time.Date(2014, 9, 7, 12, 0, 0, 0, time.UTC)
	alloc1 := testAllocator(t, "01:00:00:01:00:00", testStart1+"/22")
	defer alloc1.Stop()
	mockTime := new(mockTimeProvider)
	mockTime.SetTime(baseTime)
	alloc1.setTimeProvider(mockTime)

	mockTime.SetTime(baseTime.Add(1 * time.Second))

	// Simulate another peer on the gossip network
	alloc2 := testAllocator(t, peerNameString, testStart1+"/22")
	defer alloc2.Stop()
	alloc2.setTimeProvider(mockTime)

	mockTime.SetTime(baseTime.Add(2 * time.Second))

	alloc1.OnGossipBroadcast(alloc2.EncodeState())
	// At first, this peer has no space, so alloc1 should do nothing

	mockTime.SetTime(baseTime.Add(3 * time.Second))
	alloc1.considerOurPosition()

	mockTime.SetTime(baseTime.Add(4 * time.Second))
	// On receipt of the GetFor, alloc1 should elect alloc2 as leader, because it has a higher name
	ExpectMessage(alloc1, peerNameString, msgLeaderElected, nil)

	done := make(chan bool)
	go func() {
		alloc1.GetFor("somecontainer", nil)
		done <- true
	}()
	time.Sleep(100 * time.Millisecond)
	AssertNothingSent(t, done)

	// Time out with no reply
	mockTime.SetTime(baseTime.Add(15 * time.Second))
	ExpectMessage(alloc1, peerNameString, msgLeaderElected, nil)
	alloc1.considerOurPosition()
	AssertNothingSent(t, done)

	// alloc2 receives the leader election message and broadcasts its winning state
	ExpectBroadcastMessage(alloc2, nil)
	msg := router.Concat([]byte{msgLeaderElected}, alloc1.EncodeState())
	alloc2.OnGossipUnicast(alloc1.ourName, msg)

	// On receipt of the broadcast, alloc1 should ask alloc2 for space
	ExpectMessage(alloc1, peerNameString, msgSpaceRequest, alloc1.EncodeState())
	alloc1.OnGossipBroadcast(alloc2.EncodeState())

	// Now make it look like alloc2 has given up half its space
	alloc2.AmendSpace(donateSize)

	ExpectBroadcastMessage(alloc1, alloc2.EncodeState())
	AssertSent(t, done)

	CheckAllExpectedMessagesSent(alloc1, alloc2)
}

func TestCancel(t *testing.T) {
	//common.InitDefaultLogging(true)
	const (
		CIDR = "10.0.1.7/22"
	)
	peer1Name, _ := router.PeerNameFromString("01:00:00:02:00:00")
	peer2Name, _ := router.PeerNameFromString("02:00:00:02:00:00")

	router := TestGossipRouter{make(map[router.PeerName]chan gossipMessage), 0.0}

	alloc1, _ := NewAllocator(peer1Name, CIDR)
	alloc1.SetGossip(router.connect(peer1Name, alloc1))

	alloc2, _ := NewAllocator(peer2Name, CIDR)
	alloc2.SetGossip(router.connect(peer2Name, alloc2))

	alloc1.Start()
	alloc2.Start()

	// This is needed to tell one another about each other
	alloc1.OnGossipBroadcast(alloc2.EncodeState())
	time.Sleep(100 * time.Millisecond)

	// Get some IPs
	res1 := alloc1.GetFor("foo", nil)
	common.Debug.Printf("res1 = %s", res1)
	res2 := alloc2.GetFor("bar", nil)
	common.Debug.Printf("res2 = %s", res2)
	if res1.Equal(res2) {
		wt.Fatalf(t, "Error: got same ips!")
	}

	// Now we're going to stop alloc2 and ask alloc1
	// for an allocation in alloc2's range
	alloc2.Stop()

	cancelChan := make(chan bool, 1)
	doneChan := make(chan error)
	go func() {
		err := alloc1.Claim("baz", res2, cancelChan)
		doneChan <- err
	}()

	AssertNothingSentErr(t, doneChan)
	time.Sleep(1000 * time.Millisecond)
	AssertNothingSentErr(t, doneChan)
	cancelChan <- true
	err := <-doneChan
	if err != nil {
		wt.Fatalf(t, "Error: err should be nil!")
	}
}

// Placeholders for test methods that touch the internals of Allocator

func (alloc *Allocator) AssertNothingPending(t *testing.T) {
	// dependent on internals that are yet to be implemented
	//wt.AssertEqualInt(t, len(alloc1.inflight), 0, "inflight")
	//wt.AssertEqualInt(t, len(alloc1.claims), 0, "claims")
}

func (alloc *Allocator) EncodeState() []byte {
	return alloc.Encode()
}

func (alloc *Allocator) EncodeClaimMsg(start string, size uint32) []byte {
	// tbd
	//claim := []Space{NewMinSpace(net.ParseIP(donateStart), donateSize)}
	//return router.Concat(GobEncode(NewMinSpace(addr1, 1)), encode(alloc1.ourSpaceSet))
	return nil
}

func (alloc *Allocator) decodeUpdate([]byte) {
	// tbd
}

func (alloc *Allocator) AmendSpace(newSize int) {
	// tbd
	//alloc.ourSpaceSet.spaces[0].(*MutableSpace).MinSpace.Size = newSize
	//alloc.ourSpaceSet.version++
}
