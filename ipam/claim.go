package ipam

import (
	"fmt"

	"github.com/weaveworks/weave/ipam/address"
	"github.com/weaveworks/weave/router"
)

type claim struct {
	resultChan       chan<- error
	hasBeenCancelled func() bool
	ident            string
	addr             address.Address
}

// Try returns true for success (or failure), false if we need to try again later
func (c *claim) Try(alloc *Allocator) bool {
	if (c.hasBeenCancelled)() {
		c.Cancel()
		return true
	}

	if len(alloc.subnets) == 0 {
		// we don't know anything about which subnets are in use, so come back later.
		// (note we are assuming users only do 'claim' on restart)
		return false
	}

	for _, subnet := range alloc.subnets {
		if subnet.ring.Contains(c.addr) {
			owner := subnet.ring.Owner(c.addr)
			// what to do if ring is being bootstrapped? Implies this is not a restart
			if owner == router.UnknownPeerName {
				c.resultChan <- fmt.Errorf("Unable to establish ownership of address %s", c.addr)
				return true
			}
			if owner != alloc.ourName {
				// fixme: put these four lines - repeated in ring.go - into a helper function
				name, found := alloc.nicknames[owner]
				if found {
					name = " (" + name + ")"
				}
				c.resultChan <- fmt.Errorf("address %s is owned by other peer %s%s", c.addr.String(), owner, name)
				return true
			}
			// We are the owner, check we haven't given it to another container
			existingIdent := alloc.findOwner(c.addr)
			if existingIdent == c.ident {
				// same identifier is claiming same address; that's OK
				c.resultChan <- nil
				return true
			}
			if existingIdent == "" {
				err := subnet.space.Claim(c.addr)
				if err != nil {
					c.resultChan <- err
					return true
				}
				subnet.addOwned(c.ident, c.addr)
				c.resultChan <- nil
				return true
			}
			// Addr already owned by container on this machine
			c.resultChan <- fmt.Errorf("address %s is already owned by %s", c.addr.String(), existingIdent)
			return true
		}
	}

	// Address not within our any subnet we know; assume it's in some manually-controlled subnet
	alloc.infof("Ignored address %s claimed by %s - not in our subnets\n", c.addr, c.ident)
	c.resultChan <- nil
	return true
}

func (c *claim) Cancel() {
	c.resultChan <- fmt.Errorf("Operation cancelled.")
}

func (c *claim) String() string {
	return fmt.Sprintf("Claim %s -> %s", c.ident, c.addr.String())
}

func (c *claim) ForContainer(ident string) bool {
	return c.ident == ident
}
