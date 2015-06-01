package ipam

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/weaveworks/weave/common"
	"github.com/weaveworks/weave/ipam/address"
	"github.com/weaveworks/weave/ipam/paxos"
	"github.com/weaveworks/weave/ipam/ring"
	"github.com/weaveworks/weave/ipam/space"
	"github.com/weaveworks/weave/router"
)

// Kinds of message we can unicast to other peers
const (
	msgSpaceRequest = iota
	msgRingUpdate

	paxosInterval = time.Second * 5
)

// operation represents something which Allocator wants to do, but
// which may need to wait until some other message arrives.
type operation interface {
	// Try attempts this operations and returns false if needs to be tried again.
	Try(alloc *Allocator) bool

	Cancel()

	String() string

	// Does this operation pertain to the given container id?
	// Used for tidying up pending operations when containers die.
	ForContainer(ident string) bool
}

type subnet struct {
	cidr  address.CIDR
	ring  *ring.Ring   // information on ranges owned by all peers
	space *space.Space // more detail on ranges owned by us
	paxos *paxos.Node
}

// Allocator brings together Ring and space.Set, and does the
// necessary plumbing.  Runs as a single-threaded Actor, so no locks
// are used around data structures.
type Allocator struct {
	actionChan       chan<- func()
	ourName          router.PeerName
	ourUID           router.PeerUID
	quorum           uint
	subnets          map[address.CIDR]*subnet // mapped by start address
	defaultSubnet    address.CIDR
	owned            map[string]address.Address // who owns what address, indexed by container-ID
	nicknames        map[router.PeerName]string // so we can map nicknames for rmpeer
	pendingAllocates []operation                // held until we get some free space
	pendingClaims    []operation                // held until we know who owns the space
	gossip           router.Gossip              // our link to the outside world for sending messages
	paxosTicker      *time.Ticker
	shuttingDown     bool // to avoid doing any requests while trying to shut down
	now              func() time.Time
}

func (alloc *Allocator) AddSubnet(cidr address.CIDR) error {
	if cidr.Size() < 4 {
		return errors.New("Allocation subnet too small")
	}
	subnetData := &subnet{
		cidr: cidr,
		// per RFC 1122, don't allocate the first and last address in the subnet
		ring:  ring.New(address.Add(cidr.Start, 1), address.Add(cidr.Start, cidr.Size()-1), alloc.ourName),
		paxos: paxos.NewNode(alloc.ourName, alloc.ourUID, alloc.quorum),
	}
	return alloc.AddSubnetData(subnetData)
}

// NewAllocator creates and initialises a new Allocator
func NewAllocator(ourName router.PeerName, ourUID router.PeerUID, ourNickname string, quorum uint) *Allocator {
	return &Allocator{
		ourName:   ourName,
		ourUID:    ourUID,
		quorum:    quorum,
		subnets:   make(map[address.CIDR]*subnet),
		owned:     make(map[string]address.Address),
		nicknames: map[router.PeerName]string{ourName: ourNickname},
		now:       time.Now,
	}
}

// Start runs the allocator goroutine
func (alloc *Allocator) Start() {
	actionChan := make(chan func(), router.ChannelSize)
	alloc.actionChan = actionChan
	go alloc.actorLoop(actionChan)
}

// Stop makes the actor routine exit, for test purposes ONLY because any
// calls after this is processed will hang. Async.
func (alloc *Allocator) Stop() {
	alloc.actionChan <- nil
}

// Operation life cycle

// Given an operation, try it, and add it to the pending queue if it didn't succeed
func (alloc *Allocator) doOperation(op operation, ops *[]operation) {
	alloc.actionChan <- func() {
		if alloc.shuttingDown {
			op.Cancel()
			return
		}
		if !op.Try(alloc) {
			*ops = append(*ops, op)
		}
	}
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

// Cancel all operations in a queue
func (alloc *Allocator) cancelOps(ops *[]operation) {
	for _, op := range *ops {
		op.Cancel()
	}
	*ops = []operation{}
}

// Cancel all operations for a given container id, returns true
// if we found any.
func (alloc *Allocator) cancelOpsFor(ops *[]operation, ident string) bool {
	var found bool
	for i, op := range *ops {
		if op.ForContainer(ident) {
			found = true
			op.Cancel()
			*ops = append((*ops)[:i], (*ops)[i+1:]...)
		}
	}
	return found
}

// Try all pending operations
func (alloc *Allocator) tryPendingOps() {
	// The slightly different semantics requires us to operate on 'claims' and
	// 'allocates' separately:
	// Claims must be tried before Allocates
	for i := 0; i < len(alloc.pendingClaims); {
		op := alloc.pendingClaims[i]
		if !op.Try(alloc) {
			i++
			continue
		}
		alloc.pendingClaims = append(alloc.pendingClaims[:i], alloc.pendingClaims[i+1:]...)
	}

	// When the first Allocate fails, bail - no need to
	// send too many begs for space.
	for i := 0; i < len(alloc.pendingAllocates); {
		op := alloc.pendingAllocates[i]
		if !op.Try(alloc) {
			break
		}
		alloc.pendingAllocates = append(alloc.pendingAllocates[:i], alloc.pendingAllocates[i+1:]...)
	}
}

func hasBeenCancelled(cancelChan <-chan bool) func() bool {
	return func() bool {
		select {
		case <-cancelChan:
			return true
		default:
			return false
		}
	}
}

// Actor client API

func (alloc *Allocator) AddSubnetData(subnetData *subnet) error {
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		// fixme: check if we already have an overlapping subnet
		alloc.subnets[subnetData.cidr] = subnetData
		// First one added is the default subnet for allocations where user doesn't specify
		if alloc.defaultSubnet.Blank() {
			alloc.defaultSubnet = subnetData.cidr
		}
	}
	return <-resultChan
}

// Allocate (Sync) - get IP address for container with given name
// if there isn't any space we block indefinitely
func (alloc *Allocator) Allocate(ident string, cidr address.CIDR, cancelChan <-chan bool) (address.Address, error) {
	subnetChan := make(chan *subnet)
	alloc.actionChan <- func() {
		subnetChan <- alloc.subnets[cidr]
	}
	subnet := <-subnetChan
	resultChan := make(chan allocateResult)
	op := &allocate{subnet: subnet, resultChan: resultChan, ident: ident,
		hasBeenCancelled: hasBeenCancelled(cancelChan)}
	alloc.doOperation(op, &alloc.pendingAllocates)
	result := <-resultChan
	return result.addr, result.err
}

// Claim an address that we think we should own (Sync)
func (alloc *Allocator) Claim(ident string, addr address.Address, cancelChan <-chan bool) error {
	resultChan := make(chan error)
	op := &claim{resultChan: resultChan, ident: ident, addr: addr,
		hasBeenCancelled: hasBeenCancelled(cancelChan)}
	alloc.doOperation(op, &alloc.pendingClaims)
	return <-resultChan
}

// Free (Sync) - release IP address for container with given name
func (alloc *Allocator) Free(ident string) error {
	return alloc.free(ident)
}

// ContainerDied is provided to satisfy the updater interface; does a free underneath.  Async.
func (alloc *Allocator) ContainerDied(ident string) error {
	alloc.debugln("Container", ident, "died; releasing addresses")
	return alloc.free(ident)
}

func (alloc *Allocator) free(ident string) error {
	errChan := make(chan error)
	alloc.actionChan <- func() {
		addr, found := alloc.owned[ident]
		if found {
			// note if we stored the prefixlen / mask we wouldn't have to iterate
			for _, subnet := range alloc.subnets {
				if subnet.ring.Contains(addr) {
					subnet.space.Free(addr)
				}
			}
			// fixme: error if not found?
		}
		delete(alloc.owned, ident)

		// Also remove any pending ops
		found = alloc.cancelOpsFor(&alloc.pendingAllocates, ident) || found
		found = alloc.cancelOpsFor(&alloc.pendingClaims, ident) || found

		if !found {
			errChan <- fmt.Errorf("Free: no addresses for %s", ident)
			return
		}
		errChan <- nil
	}
	return <-errChan
}

// Sync.
func (alloc *Allocator) String() string {
	resultChan := make(chan string)
	alloc.actionChan <- func() {
		resultChan <- alloc.string()
	}
	return <-resultChan
}

// Shutdown (Sync)
func (alloc *Allocator) Shutdown() {
	alloc.infof("Shutdown")
	doneChan := make(chan struct{})
	alloc.actionChan <- func() {
		alloc.shuttingDown = true
		alloc.cancelOps(&alloc.pendingClaims)
		alloc.cancelOps(&alloc.pendingAllocates)
		needToBroadcast := false
		for _, subnet := range alloc.subnets {
			if heir := subnet.ring.PickPeerForTransfer(); heir != router.UnknownPeerName {
				subnet.ring.Transfer(alloc.ourName, heir)
				subnet.space.Clear()
				needToBroadcast = true
			}
		}
		if needToBroadcast {
			alloc.gossip.GossipBroadcast(alloc.Gossip())
			time.Sleep(100 * time.Millisecond)
		}
		doneChan <- struct{}{}
	}
	<-doneChan
}

// AdminTakeoverRanges (Sync) - take over the ranges owned by a given peer.
// Only done on adminstrator command.
func (alloc *Allocator) AdminTakeoverRanges(peerNameOrNickname string) error {
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		peername, err := alloc.lookupPeername(peerNameOrNickname)
		if err != nil {
			resultChan <- fmt.Errorf("Cannot find peer '%s'", peerNameOrNickname)
			return
		}

		alloc.debugln("AdminTakeoverRanges:", peername)
		if peername == alloc.ourName {
			resultChan <- fmt.Errorf("Cannot take over ranges from yourself!")
			return
		}

		delete(alloc.nicknames, peername)
		for _, subnet := range alloc.subnets {
			newRanges, err := subnet.ring.Transfer(peername, alloc.ourName)
			subnet.space.AddRanges(newRanges)
		}
		resultChan <- err
	}
	return <-resultChan
}

// Lookup a PeerName by nickname or stringified PeerName.  We can't
// call into the router for this because we are interested in peers
// that have gone away but are still in the ring, which is why we
// maintain our own nicknames map.
func (alloc *Allocator) lookupPeername(name string) (router.PeerName, error) {
	for peername, nickname := range alloc.nicknames {
		if nickname == name {
			return peername, nil
		}
	}

	return router.PeerNameFromString(name)
}

// Restrict the peers in "nicknames" to those in all rings and our own
func (alloc *Allocator) pruneNicknames() {
	ringPeers := make(map[router.PeerName]struct{})
	for _, subnet := range alloc.subnets {
		ringPeers = subnet.ring.PeerNames()
		//fixme!
	}
	for name := range alloc.nicknames {
		if _, ok := ringPeers[name]; !ok && name != alloc.ourName {
			delete(alloc.nicknames, name)
		}
	}
}

// OnGossipUnicast (Sync)
func (alloc *Allocator) OnGossipUnicast(sender router.PeerName, msg []byte) error {
	alloc.debugln("OnGossipUnicast from", sender, ": ", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		switch msg[0] {
		case msgSpaceRequest:
			// some other peer asked us for space
			alloc.donateSpace(sender)
			resultChan <- nil
		case msgRingUpdate:
			resultChan <- alloc.update(msg[1:])
		}
	}
	return <-resultChan
}

// OnGossipBroadcast (Sync)
func (alloc *Allocator) OnGossipBroadcast(msg []byte) (router.GossipData, error) {
	alloc.debugln("OnGossipBroadcast:", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		resultChan <- alloc.update(msg)
	}
	return alloc.Gossip(), <-resultChan
}

type gossipState struct {
	// We send a timstamp along with the information to be
	// gossipped in order to do detect skewed clocks
	Now       int64
	Nicknames map[router.PeerName]string
	Subnets   []gossipStateSubnet
}

type gossipStateSubnet struct {
	CIDR  address.CIDR
	Paxos paxos.GossipState
	Ring  *ring.Ring
}

func (alloc *Allocator) encode() []byte {
	gss := make([]gossipStateSubnet, 0, len(alloc.subnets))
	for cidr, subnet := range alloc.subnets {
		gs := gossipStateSubnet{CIDR: cidr}
		// We're only interested in Paxos until we have a Ring.
		if subnet.ring.Empty() {
			gs.Paxos = subnet.paxos.GossipState()
		} else {
			gs.Ring = subnet.ring
		}
		gss = append(gss, gs)
	}
	data := gossipState{
		Now:       alloc.now().Unix(),
		Nicknames: alloc.nicknames,
		Subnets:   gss,
	}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(data); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// Encode (Sync)
func (alloc *Allocator) Encode() []byte {
	resultChan := make(chan []byte)
	alloc.actionChan <- func() {
		resultChan <- alloc.encode()
	}
	return <-resultChan
}

// OnGossip (Sync)
func (alloc *Allocator) OnGossip(msg []byte) (router.GossipData, error) {
	alloc.debugln("Allocator.OnGossip:", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.actionChan <- func() {
		resultChan <- alloc.update(msg)
	}
	return nil, <-resultChan // for now, we never propagate updates. TBD
}

// GossipData implementation is trivial - we always gossip the latest
// data we have at time of sending
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
func (alloc *Allocator) SetInterfaces(gossip router.Gossip) {
	alloc.gossip = gossip
}

// ACTOR server

func (alloc *Allocator) actorLoop(actionChan <-chan func()) {
	for {
		var tickChan <-chan time.Time
		if alloc.paxosTicker != nil {
			tickChan = alloc.paxosTicker.C
		}

		select {
		case action := <-actionChan:
			if action == nil {
				return
			}
			action()
		case <-tickChan:
			for _, subnet := range alloc.subnets {
				if subnet.ring.Empty() {
					alloc.propose(subnet)
				}
			}
		}

		alloc.assertInvariants()
		alloc.reportFreeSpace()
	}
}

// Helper functions

func (subnet *subnet) FprintWithNicknames(w io.Writer, nicknames map[router.PeerName]string) {
	fmt.Fprintf(w, "Subnet %s/%d\n", subnet.cidr.Start.String(), subnet.cidr.PrefixLen)

	if subnet.ring.Empty() {
		fmt.Fprintf(w, "Awaiting consensus: %s", subnet.paxos.String())
	} else {
		localFreeSpace := subnet.space.NumFreeAddresses()
		remoteFreeSpace := subnet.ring.TotalRemoteFree()
		percentFree := 100 * float64(localFreeSpace+remoteFreeSpace) / float64(subnet.cidr.Size())
		fmt.Fprintf(w, "  Free IPs: ~%.1f%%, %d local, ~%d remote\n",
			percentFree, localFreeSpace, remoteFreeSpace)

		fmt.Fprint(w, "Owned Ranges:")
		subnet.ring.FprintWithNicknames(w, nicknames)
	}
}

func (alloc *Allocator) string() string {
	var buf bytes.Buffer

	for _, subnet := range alloc.subnets {
		subnet.FprintWithNicknames(&buf, alloc.nicknames)
	}
	if len(alloc.pendingAllocates)+len(alloc.pendingClaims) > 0 {
		fmt.Fprintf(&buf, "\nPending requests for ")
		for _, op := range alloc.pendingAllocates {
			fmt.Fprintf(&buf, "%s, ", op.String())
		}
		for _, op := range alloc.pendingClaims {
			fmt.Fprintf(&buf, "%s, ", op.String())
		}
	}
	return buf.String()
}

// Ensure we are making progress towards an established ring
func (alloc *Allocator) establishRing(subnet *subnet) {
	if !subnet.ring.Empty() || alloc.paxosTicker != nil {
		return
	}

	alloc.propose(subnet)
	if ok, cons := subnet.paxos.Consensus(); ok {
		// If the quorum was 1, then proposing immediately
		// leads to consensus
		alloc.createRing(subnet, cons.Value)
	} else {
		// re-try until we get consensus
		alloc.paxosTicker = time.NewTicker(paxosInterval)
	}
}

func (alloc *Allocator) createRing(subnet *subnet, peers []router.PeerName) {
	alloc.debugln("Paxos consensus:", peers)
	subnet.ring.ClaimForPeers(normalizeConsensus(peers))
	alloc.gossip.GossipBroadcast(alloc.Gossip())
	alloc.ringUpdated(subnet)
}

func (alloc *Allocator) ringUpdated(subnet *subnet) {
	// When we have a ring, we don't need paxos any more
	if subnet.paxos != nil {
		subnet.paxos = nil

		if alloc.paxosTicker != nil { // fixme
			alloc.paxosTicker.Stop()
			alloc.paxosTicker = nil
		}
	}

	subnet.space.UpdateRanges(subnet.ring.OwnedRanges())
	alloc.tryPendingOps()
}

// For compatibility with sort.Interface
type peerNames []router.PeerName

func (a peerNames) Len() int           { return len(a) }
func (a peerNames) Less(i, j int) bool { return a[i] < a[j] }
func (a peerNames) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// When we get a consensus from Paxos, the peer names are not in a
// defined order and may contain duplicates.  This function sorts them
// and de-dupes.
func normalizeConsensus(consensus []router.PeerName) []router.PeerName {
	if len(consensus) == 0 {
		return nil
	}

	peers := make(peerNames, len(consensus))
	copy(peers, consensus)
	sort.Sort(peers)

	dst := 0
	for src := 1; src < len(peers); src++ {
		if peers[dst] != peers[src] {
			dst++
			peers[dst] = peers[src]
		}
	}

	return peers[:dst+1]
}

func (alloc *Allocator) propose(subnet *subnet) {
	alloc.debugf("Paxos proposing for subnet %s", subnet)
	subnet.paxos.Propose()
	alloc.gossip.GossipBroadcast(alloc.Gossip())
}

func (alloc *Allocator) sendRequest(dest router.PeerName, kind byte) {
	msg := router.Concat([]byte{kind}, alloc.encode())
	alloc.gossip.GossipUnicast(dest, msg)
}

func (alloc *Allocator) update(msg []byte) error {
	reader := bytes.NewReader(msg)
	decoder := gob.NewDecoder(reader)
	var data gossipState
	var err error

	// fixme: move this up to higher level
	deltat := time.Unix(data.Now, 0).Sub(alloc.now())
	if deltat > time.Hour || -deltat > time.Hour {
		return fmt.Errorf("clock skew of %v detected, ignoring update", deltat)
	}

	// Merge nicknames
	for peer, nickname := range data.Nicknames {
		alloc.nicknames[peer] = nickname
	}

	shouldBroadcast := false

	for _, s := range data.Subnets {
		if subnet, found := alloc.subnets[s.CIDR]; found {
			// only one of Ring and Paxos should be present.  And we
			// shouldn't get updates for a empty Ring. But tolerate
			// them just in case.
			if s.Ring != nil {
				if err = subnet.ring.Merge(*s.Ring); err != nil {
					return err
				}
				if !subnet.ring.Empty() {
					alloc.pruneNicknames() // fixme?
					alloc.ringUpdated(subnet)
				}
			}

			if s.Paxos != nil && subnet.ring.Empty() {
				if subnet.paxos.Update(s.Paxos) {
					if subnet.paxos.Think() {
						shouldBroadcast = true
					}

					if ok, cons := subnet.paxos.Consensus(); ok {
						alloc.createRing(subnet, cons.Value)
					}
				}
			}
		}
	}
	if shouldBroadcast {
		// If something important changed, broadcast
		alloc.gossip.GossipBroadcast(alloc.Gossip())
	}

	return nil
}

func (alloc *Allocator) donateSpace(subnet *subnet, to router.PeerName) {
	// No matter what we do, we'll send a unicast gossip
	// of our ring back to tha chap who asked for space.
	// This serves to both tell him of any space we might
	// have given him, or tell him where he might find some
	// more.
	defer alloc.sendRequest(to, msgRingUpdate) // fixme: specific subnet?

	alloc.debugln("Peer", to, "asked me for space")
	start, size, ok := subnet.space.Donate()
	if !ok {
		free := subnet.space.NumFreeAddresses()
		common.Assert(free == 0)
		alloc.debugln("No space in subnet", subnet, "to give to peer", to)
		return
	}
	end := address.Add(start, size)
	alloc.debugln("Giving range", start, end, size, "to", to)
	subnet.ring.GrantRangeToHost(start, end, to)
}

func (alloc *Allocator) assertInvariants() {
	// We need to ensure all ranges the ring thinks we own have
	// a corresponding space in the space set, and vice versa
	for _, subnet := range alloc.subnets {
		checkSpace := space.New()
		checkSpace.AddRanges(subnet.ring.OwnedRanges())
		ranges := checkSpace.OwnedRanges()
		spaces := subnet.space.OwnedRanges()

		common.Assert(len(ranges) == len(spaces))

		for i := 0; i < len(ranges); i++ {
			r := ranges[i]
			s := spaces[i]
			common.Assert(s.Start == r.Start && s.End == r.End)
		}
	}
}

func (alloc *Allocator) reportFreeSpace() {
	for _, subnet := range alloc.subnets {
		ranges := subnet.ring.OwnedRanges()
		if len(ranges) == 0 {
			continue
		}

		freespace := make(map[address.Address]address.Offset)
		for _, r := range ranges {
			freespace[r.Start] = subnet.space.NumFreeAddressesInRange(r.Start, r.End)
		}
		subnet.ring.ReportFree(freespace)
	}
}

// Owned addresses

func (alloc *Allocator) addOwned(ident string, addr address.Address) {
	alloc.owned[ident] = addr
}

func (alloc *Allocator) findOwner(addr address.Address) string {
	for ident, candidate := range alloc.owned {
		if candidate == addr {
			return ident
		}
	}
	return ""
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
