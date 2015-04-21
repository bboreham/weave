package ipam

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/weaveworks/weave/common"
	"github.com/weaveworks/weave/ipam/ring"
	"github.com/weaveworks/weave/ipam/space"
	"github.com/weaveworks/weave/ipam/utils"
	"github.com/weaveworks/weave/router"
)

const (
	tombstoneTimeout = 14 * 24 * time.Hour
)

// Kinds of message we can unicast to other peers
const (
	msgSpaceRequest = iota
	msgLeaderElected
	msgRingUpdate
)

// operation represents something which Allocator wants to do, but
// which may need to wait until some other message arrives.
type operation interface {
	// Try attempts this operations and returns false if needs to be tried again.
	Try(alloc *Allocator) bool

	Cancel()

	String() string
}

// Allocator brings together Ring and space.Set, and does the
// necessary plumbing.  Runs as a single-threaded Actor, so no locks
// are used around data structures.
type Allocator struct {
	actionChan         chan<- func()
	ourName            router.PeerName
	subnetStart        net.IP                     // start address of space all peers are allocating from
	subnetSize         uint32                     // length of space all peers are allocating from
	prefixLen          int                        // network prefix length, e.g. 24 for a /24 network
	ring               *ring.Ring                 // information on ranges owned by all peers
	spaceSet           space.Set                  // more detail on ranges owned by us
	owned              map[string][]net.IP        // who owns what address, indexed by container-ID
	otherPeerNicknames map[router.PeerName]string // so we can map nicknames for tombstoning
	pendingGetFors     []operation                // held until we get some free space
	pendingClaims      []operation                // held until we know who owns the space
	gossip             router.Gossip              // our link to the outside world for sending messages
	leadership         router.Leadership
	shuttingDown       bool // to avoid doing any requests while trying to tombstone ourself
}

// NewAllocator creates and initialises a new Allocator
func NewAllocator(ourName router.PeerName, subnetCIDR string) (*Allocator, error) {
	_, subnet, err := net.ParseCIDR(subnetCIDR)
	if err != nil {
		return nil, err
	}
	if subnet.IP.To4() == nil {
		return nil, errors.New("Non-IPv4 address not supported")
	}
	// Get the size of the network from the mask
	ones, bits := subnet.Mask.Size()
	var subnetSize uint32 = 1 << uint(bits-ones)
	if subnetSize < 4 {
		return nil, errors.New("Allocation subnet too small")
	}
	alloc := &Allocator{
		ourName:     ourName,
		subnetStart: subnet.IP,
		subnetSize:  subnetSize,
		prefixLen:   ones,
		// per RFC 1122, don't allocate the first and last address in the subnet
		ring:               ring.New(utils.Add(subnet.IP, 1), utils.Add(subnet.IP, subnetSize-1), ourName),
		owned:              make(map[string][]net.IP),
		otherPeerNicknames: make(map[router.PeerName]string),
	}
	return alloc, nil
}

// OnNewPeer is part of the NewPeerWatcher interface, and is called by the
// code in router.Peers for every new peer found.
func (alloc *Allocator) OnNewPeer(uid router.PeerName, nickname string) {
	alloc.actionChan <- func() {
		alloc.otherPeerNicknames[uid] = nickname
	}
}

// Start runs the allocator goroutine
func (alloc *Allocator) Start() {
	actionChan := make(chan func(), router.ChannelSize)
	alloc.actionChan = actionChan
	go alloc.actorLoop(actionChan)
}

// Async.
func (alloc *Allocator) Stop() {
	alloc.actionChan <- nil
}

// Operation life cycle

// Given an operation, try it, and add it to the pending queue if it didn't succeed
func (alloc *Allocator) doOperation(op operation, ops *[]operation) error {
	if alloc.shuttingDown {
		return fmt.Errorf("Allocator shutting down")
	}
	alloc.electLeaderIfNecessary()
	if !op.Try(alloc) {
		*ops = append(*ops, op)
	}
	return nil
}

// Given an operation, remove it from the pending queue
//  Note the op may not be on the queue; it may have
//  already succeeded.  If it is on the queue, we call
//  cancel on it, allowing callers waiting for the resultChans
//  to unblock.
func (alloc *Allocator) cancelOp(op operation, ops *[]operation) {
	for i, op := range *ops {
		if op == op {
			*ops = append((*ops)[:i], (*ops)[i+1:]...)
			op.Cancel()
			break
		}
	}
}

func (alloc *Allocator) cancelOps(ops *[]operation) {
	for _, op := range *ops {
		op.Cancel()
	}
	*ops = []operation{}
}

// Try all pending operations
func (alloc *Allocator) tryPendingOps() {
	// The slightly different semantics requires us to operate on 'claims' and
	// 'getfors' separately:
	// Claims must be tried before GetFors
	for i := 0; i < len(alloc.pendingClaims); {
		op := alloc.pendingClaims[i]
		if !op.Try(alloc) {
			i++
			continue
		}
		alloc.pendingClaims = append(alloc.pendingClaims[:i], alloc.pendingClaims[i+1:]...)
	}

	// When the first GetFor fails, bail - no need to
	// send too many begs for space.
	for i := 0; i < len(alloc.pendingGetFors); {
		op := alloc.pendingGetFors[i]
		if !op.Try(alloc) {
			break
		}
		alloc.pendingGetFors = append(alloc.pendingGetFors[:i], alloc.pendingGetFors[i+1:]...)
	}
}

// Actor client API

// GetFor (Sync) - get IP address for container with given name
// if there isn't any space we block indefinitely
func (alloc *Allocator) GetFor(ident string, cancelChan <-chan bool) net.IP {
	resultChan := make(chan net.IP, 1)
	op := &getfor{resultChan: resultChan, ident: ident}

	alloc.actionChan <- func() {
		if err := alloc.doOperation(op, &alloc.pendingGetFors); err != nil {
			resultChan <- nil
		}
	}

	select {
	case result := <-resultChan:
		return result
	case <-cancelChan:
		alloc.actionChan <- func() {
			alloc.cancelOp(op, &alloc.pendingGetFors)
		}
		return <-resultChan
	}
}

// Claim an address that we think we should own (Sync)
func (alloc *Allocator) Claim(ident string, addr net.IP, cancelChan <-chan bool) error {
	resultChan := make(chan error, 1)
	op := &claim{resultChan: resultChan, ident: ident, addr: addr}

	alloc.actionChan <- func() {
		if err := alloc.doOperation(op, &alloc.pendingClaims); err != nil {
			resultChan <- err
		}
	}

	select {
	case result := <-resultChan:
		return result
	case <-cancelChan:
		alloc.actionChan <- func() {
			alloc.cancelOp(op, &alloc.pendingClaims)
		}
		return <-resultChan
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

// ContainerDied is provided to satisfy the updater interface; does a free underneath.  Async.
func (alloc *Allocator) ContainerDied(ident string) error {
	alloc.debugln("Container", ident, "died; releasing addresses")
	alloc.actionChan <- func() {
		for _, ip := range alloc.owned[ident] {
			alloc.spaceSet.Free(ip)
		}
		delete(alloc.owned, ident)
	}
	return nil // this is to satisfy the ContainerObserver interface
}

// Shutdown (Sync)
func (alloc *Allocator) Shutdown() {
	alloc.infof("Shutdown")
	doneChan := make(chan struct{})
	alloc.actionChan <- func() {
		alloc.shuttingDown = true
		alloc.cancelOps(&alloc.pendingClaims)
		alloc.cancelOps(&alloc.pendingGetFors)
		alloc.ring.TombstonePeer(alloc.ourName, 100)
		alloc.gossip.GossipBroadcast(alloc.Gossip())
		alloc.spaceSet.Clear()
		time.Sleep(100 * time.Millisecond)
		doneChan <- struct{}{}
	}
	<-doneChan
}

// TombstonePeer (Sync) - inserts tombstones for given peer, freeing up the ranges the
// peer owns.  Only done on adminstrator command.
func (alloc *Allocator) TombstonePeer(peerNameOrNickname string) error {
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		peername, found := router.UnknownPeerName, false
		for name, nickname := range alloc.otherPeerNicknames {
			if nickname == peerNameOrNickname {
				peername = name
				found = true
				break
			}
		}

		if !found {
			var err error
			peername, err = router.PeerNameFromString(peerNameOrNickname)
			if err != nil {
				resultChan <- fmt.Errorf("Cannot find peer '%s'", peerNameOrNickname)
				return
			}
		}

		alloc.debugln("TombstonePeer:", peername)
		if peername == alloc.ourName {
			resultChan <- fmt.Errorf("Cannot tombstone yourself!")
			return
		}

		delete(alloc.otherPeerNicknames, peername)
		err := alloc.ring.TombstonePeer(peername, tombstoneTimeout)
		alloc.considerNewSpaces()
		resultChan <- err
	}
	return <-resultChan
}

// OnGossipUnicast (Sync)
func (alloc *Allocator) OnGossipUnicast(sender router.PeerName, msg []byte) error {
	alloc.debugln("OnGossipUnicast from", sender, ": ", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		switch msg[0] {
		case msgLeaderElected:
			// some other peer decided we were the leader:
			// if we already have tokens then they didn't get the memo; repeat
			if !alloc.ring.Empty() {
				alloc.gossip.GossipBroadcast(alloc.Gossip())
			} else {
				// re-run the election on this peer to avoid races
				alloc.electLeaderIfNecessary()
			}
			resultChan <- nil
		case msgSpaceRequest:
			// some other peer asked us for space
			alloc.donateSpace(sender)
			resultChan <- nil
		case msgRingUpdate:
			resultChan <- alloc.updateRing(msg[1:])
		}
	}
	return <-resultChan
}

// OnGossipBroadcast (Sync)
func (alloc *Allocator) OnGossipBroadcast(msg []byte) (router.GossipData, error) {
	alloc.debugln("OnGossipBroadcast:", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		resultChan <- alloc.updateRing(msg)
	}
	return alloc.Gossip(), <-resultChan
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
	return nil, <-resultChan // for now, we never propagate updates. TBD
}

// GossipData implementation is trivial - we always gossip the whole ring
type ipamGossipData struct {
	alloc *Allocator
}

func (d *ipamGossipData) Merge(other router.GossipData) {
	// no-op
}

func (d *ipamGossipData) Encode() []byte {
	return d.alloc.Encode()
}

// Gossip returns a GossipData implementation, which in this case always
// returns the latest ring state (and does nothing on merge)
func (alloc *Allocator) Gossip() router.GossipData {
	return &ipamGossipData{alloc}
}

// SetInterfaces gives the allocator two interfaces for talking to the outside world
func (alloc *Allocator) SetInterfaces(gossip router.Gossip, leadership router.Leadership) {
	alloc.gossip = gossip
	alloc.leadership = leadership
}

// ACTOR server

func (alloc *Allocator) actorLoop(actionChan <-chan func()) {
	for {
		action := <-actionChan
		if action == nil {
			break
		}
		action()
		alloc.assertInvariants()
		alloc.reportFreeSpace()
		alloc.ring.ExpireTombstones(time.Now().Unix())
	}
}

// Helper functions

func (alloc *Allocator) string() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Allocator subnet %s+%d\n", alloc.subnetStart, alloc.subnetSize)
	alloc.ring.FprintWithNicknames(&buf, alloc.otherPeerNicknames)
	fmt.Fprintf(&buf, alloc.spaceSet.String())
	if len(alloc.pendingGetFors)+len(alloc.pendingClaims) > 0 {
		fmt.Fprintf(&buf, "\nPending requests for ")
	}
	for _, op := range alloc.pendingGetFors {
		fmt.Fprintf(&buf, "%s, ", op.String())
	}
	for _, op := range alloc.pendingClaims {
		fmt.Fprintf(&buf, "%s, ", op.String())
	}
	return buf.String()
}

func (alloc *Allocator) electLeaderIfNecessary() {
	if !alloc.ring.Empty() {
		return
	}
	leader := alloc.leadership.LeaderElect()
	alloc.debugln("Elected leader:", leader)
	if leader == alloc.ourName {
		// I'm the winner; take control of the whole subnet
		alloc.ring.ClaimItAll()
		alloc.considerNewSpaces()
		alloc.infof("I was elected leader \n%s", alloc.string())
		alloc.gossip.GossipBroadcast(alloc.Gossip())
		alloc.tryPendingOps()
	} else {
		alloc.sendRequest(leader, msgLeaderElected)
	}
}

func (alloc *Allocator) sendRequest(dest router.PeerName, kind byte) {
	msg := router.Concat([]byte{kind}, alloc.ring.GossipState())
	alloc.gossip.GossipUnicast(dest, msg)
}

func (alloc *Allocator) updateRing(msg []byte) error {
	err := alloc.ring.UpdateRing(msg)
	alloc.considerNewSpaces()
	alloc.tryPendingOps()
	return err
}

func (alloc *Allocator) donateSpace(to router.PeerName) {
	// No matter what we do, we'll send a unicast gossip
	// of our ring back to tha chap who asked for space.
	// This serves to both tell him of any space we might
	// have given him, or tell him where he might find some
	// more.
	defer alloc.sendRequest(to, msgRingUpdate)

	alloc.debugln("Peer", to, "asked me for space")
	start, size, ok := alloc.spaceSet.GiveUpSpace()
	if !ok {
		free := alloc.spaceSet.NumFreeAddresses()
		utils.Assert(free == 0,
			fmt.Sprintf("Couldn't give up space but I have %d free addresses", free))
		alloc.debugln("No space to give to peer", to)
		return
	}
	end := utils.IntIP4(utils.IP4int(start) + size)
	alloc.debugln("Giving range", start, end, size, "to", to)
	alloc.ring.GrantRangeToHost(start, end, to)
}

// considerNewSpaces iterates through ranges in the ring
// and ensures we have spaces for them.  It only ever adds
// new spaces, as the invariants in the ring ensure we never
// have spaces taken away from us against our will.
func (alloc *Allocator) considerNewSpaces() {
	ownedRanges := alloc.ring.OwnedRanges()
	for _, r := range ownedRanges {
		size := uint32(utils.Subtract(r.End, r.Start))
		s, exists := alloc.spaceSet.Get(r.Start)
		if !exists {
			alloc.debugf("Found new space [%s, %s)", r.Start, r.End)
			alloc.spaceSet.AddSpace(space.Space{Start: r.Start, Size: size})
			continue
		}

		if s.Size < size {
			alloc.debugf("Growing space starting at %s to %d", s.Start, size)
			s.Grow(size)
		}
	}
}

func (alloc *Allocator) assertInvariants() {
	// We need to ensure all ranges the ring thinks we own have
	// a corresponding space in the space set, and vice versa
	ranges := alloc.ring.OwnedRanges()
	spaces := alloc.spaceSet.Spaces()

	utils.Assert(len(ranges) == len(spaces), "Ring and SpaceSet are out of sync!")

	for i := 0; i < len(ranges); i++ {
		r := ranges[i]
		s := spaces[i]

		rSize := uint32(utils.Subtract(r.End, r.Start))
		utils.Assert(s.Start.Equal(r.Start) && s.Size == rSize,
			fmt.Sprintf("Range starting at %s out of sync with space set!", r.Start))
	}
}

func (alloc *Allocator) reportFreeSpace() {
	spaces := alloc.spaceSet.Spaces()

	for _, s := range spaces {
		alloc.ring.ReportFree(s.Start, s.NumFreeAddresses())
	}
}

// Owned addresses

func (alloc *Allocator) addOwned(ident string, addr net.IP) {
	alloc.owned[ident] = append(alloc.owned[ident], addr)
}

func (alloc *Allocator) findOwner(addr net.IP) string {
	for ident, addrs := range alloc.owned {
		for _, ip := range addrs {
			if ip.Equal(addr) {
				return ident
			}
		}
	}
	return ""
}

func (alloc *Allocator) removeOwned(ident string, addr net.IP) bool {
	if addrs, found := alloc.owned[ident]; found {
		for i, ip := range addrs {
			if ip.Equal(addr) {
				alloc.owned[ident] = append(addrs[:i], addrs[i+1:]...)
				return true
			}
		}
	}
	return false
}

// Logging

func (alloc *Allocator) infof(fmt string, args ...interface{}) {
	common.Info.Printf("[allocator %s] "+fmt, append([]interface{}{alloc.ourName}, args...)...)
}
func (alloc *Allocator) debugln(args ...interface{}) {
	common.Debug.Println(append([]interface{}{fmt.Sprintf("[allocator %s]:", alloc.ourName)}, args...)...)
}
func (alloc *Allocator) debugf(fmt string, args ...interface{}) {
	common.Debug.Printf("[allocator %s] "+fmt, append([]interface{}{alloc.ourName}, args...)...)
}
