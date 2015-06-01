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
	cidr             address.CIDR
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

	subnet := alloc.findOrCreateSubnet(g.cidr)

	// If we have previously stored an address for this container, return it.
	if addr, found := subnet.owned[g.ident]; found {
		g.resultChan <- allocateResult{addr, nil}
		return true
	}

	alloc.establishRing(subnet) // note it might be better to do this as each subnet is created

	if ok, addr := subnet.space.Allocate(); ok {
		alloc.debugln("Allocated", addr, "for", g.ident)
		subnet.addOwned(g.ident, addr)
		g.resultChan <- allocateResult{addr, nil}
		return true
	}

	// out of space
	if donor, err := subnet.ring.ChoosePeerToAskForSpace(); err == nil {
		alloc.debugln("Decided to ask peer", donor, "for space in subnet", g.cidr)
		alloc.sendSpaceRequest(donor, g.cidr)
	}

	return false
}

func (g *allocate) Cancel() {
	g.resultChan <- allocateResult{0, fmt.Errorf("Allocate request in %s for %s cancelled", g.cidr.String(), g.ident)}
}

func (g *allocate) String() string {
	return fmt.Sprintf("Allocate in %s for %s", g.cidr.String(), g.ident)
}

func (g *allocate) ForContainer(ident string) bool {
	return g.ident == ident
}
