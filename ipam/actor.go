package ipam

import (
	"fmt"
	"github.com/zettio/weave/router"
	"net"
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
	resultChan := make(chan net.IP)
	alloc.actionChan <- func() {
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

// ACTOR server

func (alloc *Allocator) actorLoop(actionChan <-chan func(), withTimers bool) {
	// FIXME: not doing any timers at the moment.
	for {
		select {
		case action, ok := <-actionChan:
			if !ok || action == nil {
				return
			}
			action()
		}
		alloc.assertInvariants()
		alloc.reportFreeSpace()
	}
}
