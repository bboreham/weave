package ipam

import (
	"fmt"
	"github.com/zettio/weave/router"
	"net"
	"time"
)

// Start runs the allocator goroutine
func (alloc *Allocator) Start() {
	actionChan := make(chan func(), router.ChannelSize)
	alloc.actionChan = actionChan
	go alloc.actorLoop(actionChan, true)
}

// Actor client API

// GetFor (Sync) - get IP address for container with given name
func (alloc *Allocator) GetFor(ident string, cancelChan <-chan bool) net.IP {
	resultChan := make(chan net.IP, 1) // len 1 so actor can send while cancel is in progress
	alloc.actionChan <- func() {
		if alloc.shuttingDown {
			resultChan <- nil
			return
		}
		alloc.electLeaderIfNecessary()
		if addrs, found := alloc.owned[ident]; found && len(addrs) > 0 {
			resultChan <- addrs[0] // currently not supporting multiple allocations in the same subnet
		} else if !alloc.tryAllocateFor(ident, resultChan) {
			alloc.pending = append(alloc.pending, pendingAllocation{resultChan, ident})
		}
	}
	select {
	case result := <-resultChan:
		return result
	case <-cancelChan:
		alloc.actionChan <- func() {
			alloc.handleCancelGetFor(ident)
		}
		return nil
	}
}

// Free (Sync) - release IP address for container with given name
func (alloc *Allocator) Free(ident string, addr net.IP) error {
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		if alloc.removeOwned(ident, addr) {
			resultChan <- alloc.spaceSet.Free(addr)
		} else {
			resultChan <- fmt.Errorf("free: %s not owned by %s", addr, ident)
		}
	}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) String() string {
	resultChan := make(chan string)
	alloc.actionChan <- func() {
		resultChan <- alloc.string()
	}
	return <-resultChan
}

// DeleteRecordsFor is provided to satisfy the updater interface; does a free underneath.  Async.
func (alloc *Allocator) DeleteRecordsFor(ident string) error {
	alloc.actionChan <- func() {
		for _, ip := range alloc.owned[ident] {
			alloc.spaceSet.Free(ip)
		}
		delete(alloc.owned, ident)
	}
	return nil // this is to satisfy the ContainerObserver interface
}

// OnGossipUnicast (Sync)
func (alloc *Allocator) OnGossipUnicast(sender router.PeerName, msg []byte) error {
	alloc.debugln("OnGossipUnicast from", sender, ": ", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		switch msg[0] {
		case msgLeaderElected:
			resultChan <- alloc.handleLeaderElected()
		case msgSpaceRequest:
			// some other peer asked us for space
			alloc.donateSpace(sender)
			resultChan <- nil
		case msgGossip:
			resultChan <- alloc.updateRing(msg[1:])
		}
	}
	return <-resultChan
}

// OnGossipBroadcast (Sync)
func (alloc *Allocator) OnGossipBroadcast(msg []byte) error {
	alloc.debugln("OnGossipBroadcast:", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		resultChan <- alloc.updateRing(msg)
	}
	return <-resultChan
}

// Encode (Sync)
func (alloc *Allocator) Encode() []byte {
	resultChan := make(chan []byte)
	alloc.actionChan <- func() {
		resultChan <- alloc.ring.GossipState()
	}
	return <-resultChan
}

// OnGossip (Sync)
func (alloc *Allocator) OnGossip(msg []byte) (router.GossipData, error) {
	alloc.debugln("Allocator.OnGossip:", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		resultChan <- alloc.updateRing(msg)
	}
	err := <-resultChan
	return nil, err // for now, we never propagate updates. TBD
}

// OnShutdown (Sync)
func (alloc *Allocator) OnShutdown() {
	alloc.debugln("OnShutdown")
	doneChan := make(chan bool)
	alloc.actionChan <- func() {
		alloc.shuttingDown = true
		alloc.ring.TombstonePeer(alloc.ourName, 100)
		alloc.gossip.GossipBroadcast(alloc.ring.GossipState())
		alloc.spaceSet.Clear()
		time.Sleep(100 * time.Millisecond)
		doneChan <- true
	}
	<-doneChan
}

// TombstonePeer (Sync) - inserts tombstones for given peer, freeing up the ranges the
// peer owns.  Eventually the peer will go away.
func (alloc *Allocator) TombstonePeer(peer router.PeerName) error {
	alloc.debugln("TombstonePeer:", peer)
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		resultChan <- alloc.tombstonePeer(peer)
	}
	return <-resultChan
}

// ListPeers (Sync) - returns list of peer names known to the ring
func (alloc *Allocator) ListPeers() []router.PeerName {
	resultChan := make(chan []router.PeerName)
	alloc.actionChan <- func() {
		resultChan <- alloc.listPeers()
	}
	return <-resultChan
}

// Claim an address that we think we should own (Sync)
func (alloc *Allocator) Claim(ident string, addr net.IP, cancelChan <-chan bool) error {
	resultChan := make(chan error, 1)
	alloc.actionChan <- func() {
		if alloc.shuttingDown {
			resultChan <- fmt.Errorf("Claim %s: allocator is shutting down", addr)
			return
		}
		alloc.electLeaderIfNecessary()
		alloc.handleClaim(ident, addr, resultChan)
	}
	select {
	case result := <-resultChan:
		return result
	case <-cancelChan:
		alloc.actionChan <- func() {
			alloc.handleCancelClaim(ident, addr)
		}
		return nil
	}
}

// ACTOR server

func (alloc *Allocator) actorLoop(actionChan <-chan func(), withTimers bool) {
	// FIXME: not doing any timers at the moment.
	for {
		select {
		case action := <-actionChan:
			if action == nil {
				return
			}
			action()
		}
		alloc.assertInvariants()
		alloc.reportFreeSpace()
		alloc.ring.ExpireTombstones(time.Now().Unix())
	}
}
