package ipam

import (
	"fmt"
	"github.com/weaveworks/weave/ipam/address"
)

type allocateResult struct {
	addr address.Address
	err  error
}

type allocate struct {
	*subnet
	resultChan       chan<- allocateResult
	hasBeenCancelled func() bool
	ident            string
}

// Try returns true if the request is completed, false if pending
func (g *allocate) Try(alloc *Allocator) bool {
	if g.hasBeenCancelled() {
		g.Cancel()
		return true
	}

	// If we have previously stored an address for this container, return it.
	if addr, found := alloc.owned[g.ident]; found {
		g.resultChan <- allocateResult{addr, nil}
		return true
	}

	alloc.establishRing(g.subnet)

	if ok, addr := g.subnet.space.Allocate(); ok {
		alloc.debugln("Allocated", addr, "for", g.ident)
		alloc.addOwned(g.ident, addr)
		g.resultChan <- allocateResult{addr, nil}
		return true
	}

	// out of space
	if donor, err := g.subnet.ring.ChoosePeerToAskForSpace(); err == nil {
		alloc.debugln("Decided to ask peer", donor, "for space in subnet", g.subnet)
		alloc.sendRequest(donor, msgSpaceRequest) // fixme: needs to send subnet
	}

	return false
}

func (g *allocate) Cancel() {
	g.resultChan <- allocateResult{0, fmt.Errorf("Allocate request for %s cancelled", g.ident)}
}

func (g *allocate) String() string {
	return fmt.Sprintf("Allocate for %s", g.ident)
}

func (g *allocate) ForContainer(ident string) bool {
	return g.ident == ident
}
