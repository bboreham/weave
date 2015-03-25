package ipam

import (
	"fmt"
	"github.com/zettio/weave/common"
	"github.com/zettio/weave/router"
	wt "github.com/zettio/weave/testing"
	"math/rand"
	"net"
	"sync"
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
		universe   = "10.0.3.0/30"
		testAddr1  = "10.0.3.1" // first address allocated should be .1 because .0 is network addr
		spaceSize  = 4
	)

	alloc := testAllocator(t, "01:00:00:01:00:00", universe)
	defer alloc.Stop()

	ExpectBroadcastMessage(alloc, nil) // on leader election, broadcasts its state
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

func TestElection(t *testing.T) {
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
	SetLeader(alloc1, peerNameString)
	// On receipt of the GetFor, alloc1 should elect alloc2 as leader
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
	// fixme: not implemented yet
	// ExpectMessage(alloc1, peerNameString, msgLeaderElected, nil)
	alloc1.considerOurPosition()
	AssertNothingSent(t, done)

	// alloc2 receives the leader election message and broadcasts its winning state
	ExpectBroadcastMessage(alloc2, nil)
	msg := router.Concat([]byte{msgLeaderElected}, alloc1.EncodeState())
	alloc2.OnGossipUnicast(alloc1.ourName, msg)

	// On receipt of the broadcast, alloc1 should ask alloc2 for space
	ExpectMessage(alloc1, peerNameString, msgSpaceRequest, nil)
	alloc1.OnGossipBroadcast(alloc2.EncodeState())

	// alloc2 receives the space request
	ExpectBroadcastMessage(alloc2, nil)
	alloc2.OnGossipUnicast(alloc1.ourName, router.Concat([]byte{msgSpaceRequest}, alloc1.EncodeState()))

	// Now alloc1 receives the space donation
	alloc1.OnGossipBroadcast(alloc2.EncodeState())
	AssertSent(t, done)

	CheckAllExpectedMessagesSent(alloc1, alloc2)
}

func TestCancel(t *testing.T) {
	//common.InitDefaultLogging(true)
	const (
		CIDR = "10.0.1.7/26"
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

	// tell peers about each other
	alloc1.OnGossipBroadcast(alloc2.EncodeState())

	// Get some IPs, so each allocator has some space
	res1 := alloc1.GetFor("foo", nil)
	common.Debug.Printf("res1 = %s", res1)
	res2 := alloc2.GetFor("bar", nil)
	common.Debug.Printf("res2 = %s", res2)
	if res1.Equal(res2) {
		wt.Fatalf(t, "Error: got same ips!")
	}

	// Now we're going to stop alloc2 and ask alloc1
	// for an allocation
	alloc2.Stop()
	time.Sleep(100 * time.Millisecond)

	// Use up all the IPs that alloc1 owns, so the allocation after this will prompt a request to alloc2
	for i := 0; alloc1.spaceSet.NumFreeAddresses() > 0; i++ {
		alloc1.GetFor(fmt.Sprintf("tmp%d", i), nil)
	}

	cancelChan := make(chan bool, 1)
	doneChan := make(chan bool)
	go func() {
		ip := alloc1.GetFor("baz", cancelChan)
		doneChan <- (ip == nil)
	}()

	AssertNothingSent(t, doneChan)
	time.Sleep(100 * time.Millisecond)
	AssertNothingSent(t, doneChan)
	cancelChan <- true
	flag := <-doneChan
	if !flag {
		wt.Fatalf(t, "Error: got non-nil result from GetFor")
	}
}

// Placeholders for test methods that touch the internals of Allocator

func (alloc *Allocator) AssertNothingPending(t *testing.T) {
	// dependent on internals that are yet to be implemented
	//wt.AssertEqualInt(t, len(alloc1.inflight), 0, "inflight")
	//wt.AssertEqualInt(t, len(alloc1.claims), 0, "claims")
}

func (alloc *Allocator) EncodeState() []byte {
	return alloc.ring.GossipState()
}

func (alloc *Allocator) AmendSpace(newSize int) {
	// tbd
	//alloc.ourSpaceSet.spaces[0].(*MutableSpace).MinSpace.Size = newSize
	//alloc.ourSpaceSet.version++
}

func TestFakeRouterSimple(t *testing.T) {
	common.InitDefaultLogging(true)
	const (
		cidr = "10.0.1.7/22"
	)
	allocs, _ := makeNetworkOfAllocators(2, cidr)

	alloc1 := allocs[0]
	//alloc2 := allocs[1]

	addr := alloc1.GetFor("foo", nil)

	println("Got addr", addr)
}

func BenchmarkAllocator(b *testing.B) {
	//common.InitDefaultLogging(true)
	const (
		firstpass    = 1022
		secondpass   = 5000
		nodes        = 2
		maxAddresses = 1022
		concurrency  = 1
		cidr         = "10.0.1.7/22"
	)
	allocs, _ := makeNetworkOfAllocators(nodes, cidr)

	// Test state
	// For each IP issued we store the allocator
	// that issued it and the name of the container
	// it was issued to.
	type result struct {
		name  string
		alloc int32
	}
	stateLock := sync.Mutex{}
	state := make(map[string]result)
	// Keep a list of addresses issued, so we
	// Can pick random ones
	addrs := make([]string, 0)
	numPending := 0

	rand.Seed(0)

	// Remove item from list by swapping it with last
	// and reducing slice length by 1
	rm := func(xs []string, i int32) []string {
		ls := len(xs) - 1
		xs[i] = xs[ls]
		return xs[:ls]
	}

	// Do a GetFor and check the address
	// is unique.  Needs a unique container
	// name.
	getFor := func(name string) {
		stateLock.Lock()
		if len(addrs)+numPending >= maxAddresses {
			stateLock.Unlock()
			return
		}
		numPending++
		stateLock.Unlock()

		allocIndex := rand.Int31n(nodes)
		alloc := allocs[allocIndex]
		common.Info.Printf("GetFor: asking allocator %d", allocIndex)
		addr := alloc.GetFor(name, nil)

		if addr == nil {
			panic(fmt.Sprintf("Could not allocate addr"))
		}

		common.Info.Printf("GetFor: got address %s for name %s", addr, name)
		addrStr := addr.String()

		stateLock.Lock()
		defer stateLock.Unlock()

		if res, existing := state[addrStr]; existing {
			panic(fmt.Sprintf("Dup found for address %s - %s and %s", addrStr,
				name, res.name))
		}

		state[addrStr] = result{name, allocIndex}
		addrs = append(addrs, addrStr)
		numPending--
	}

	// Free a random address.
	free := func() {
		stateLock.Lock()
		if len(addrs) == 0 {
			stateLock.Unlock()
			return
		}
		// Delete an existing allocation
		// Pick random addr
		addrIndex := rand.Int31n(int32(len(addrs)))
		addr := addrs[addrIndex]
		res := state[addr]
		addrs = rm(addrs, addrIndex)
		delete(state, addr)
		stateLock.Unlock()

		alloc := allocs[res.alloc]
		common.Info.Printf("Freeing %s on allocator %d", addr, res.alloc)

		err := alloc.Free(res.name, net.ParseIP(addr))
		if err != nil {
			panic(fmt.Sprintf("Cound not free address %s", addr))
		}
	}

	// Do a GetFor on an existing container & allocator
	// and check we get the right answer.
	getForAgain := func() {
		stateLock.Lock()
		addrIndex := rand.Int31n(int32(len(addrs)))
		addr := addrs[addrIndex]
		res := state[addr]
		stateLock.Unlock()
		alloc := allocs[res.alloc]

		common.Info.Printf("Asking for %s on allocator %d again", addr, res.alloc)

		newAddr := alloc.GetFor(res.name, nil)
		if !newAddr.Equal(net.ParseIP(addr)) {
			panic(fmt.Sprintf("Got different address for repeat request"))
		}
	}

	// Run function _f_ _iterations_ times, in _concurrency_
	// number of goroutines
	doConcurrentIterations := func(iterations int, f func(int)) {
		iterationsPerThread := iterations / concurrency

		wg := sync.WaitGroup{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				for k := 0; k < iterationsPerThread; k++ {
					f((j * iterationsPerThread) + k)
				}
			}(i)
		}
		wg.Wait()
	}

	// First pass, just allocate a bunch of ips
	doConcurrentIterations(firstpass, func(iteration int) {
		name := fmt.Sprintf("first%d", iteration)
		getFor(name)
	})

	// Second pass, random ask for more allocations,
	// or remove existing ones, or ask for allocation
	// again.
	doConcurrentIterations(secondpass, func(iteration int) {
		r := rand.Float32()
		switch {
		case 0.0 <= r && r < 0.4:
			// Ask for a new allocation
			name := fmt.Sprintf("second%d", iteration)
			getFor(name)

		case (0.4 <= r && r < 0.8):
			// free a random addr
			free()

		case 0.8 <= r && r < 1.0:
			// ask for an existing name again, check we get same ip
			getForAgain()
		}
	})
}
