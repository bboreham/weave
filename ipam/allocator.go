package ipam

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	lg "github.com/zettio/weave/common"
	"github.com/zettio/weave/router"
	"net"
	"time"
)

const (
	GossipReqTimeout     = 1 * time.Second
	GossipDeadTimeout    = 10 * time.Second
	MaxAddressesToGiveUp = 256
)
const (
	msgSpaceRequest = iota
	msgSpaceDonate
	msgSpaceClaim
	msgSpaceClaimRefused
	msgLeaderElected
)

const (
	allocStateNeutral = iota
	allocStateLeaderless
)

type Allocator struct {
	queryChan   chan<- interface{}
	ourName     router.PeerName
	ourUID      uint64
	state       int
	universeLen int
	universe    MinSpace // all the addresses that could be allocated
	gossip      router.Gossip
	peerInfo    map[uint64]SpaceSet // indexed by peer UID
	ourSpaceSet *OurSpaceSet
	pastLife    *PeerSpaceSet       // Probably allocations from a previous incarnation
	owned       map[string][]net.IP // indexed by container-ID
	leaked      map[time.Time]Space
	claims      claimList
	pending     []getFor
	inflight    requestList
	timeProvider
}

// Some in-flight request that we have made to another peer
type request struct {
	dest    router.PeerName
	kind    byte
	details *MinSpace
	expires time.Time
}

type requestList []*request

func (list requestList) find(sender router.PeerName, spaces []Space) int {
	for i, r := range list {
		if r.dest == sender {
			if r.kind == msgSpaceRequest ||
				r.kind == msgSpaceClaim && len(spaces) == 1 && r.details.Start.Equal(spaces[0].GetStart()) {
				return i
			}
		}
	}
	return -1
}

func (list requestList) findKind(kind byte) int {
	for i, r := range list {
		if r.kind == kind {
			return i
		}
	}
	return -1
}

func (list *requestList) removeAt(pos int) {
	(*list) = append((*list)[:pos], (*list)[pos+1:]...)
}

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

// To allow time itself to be stubbed out for testing
type timeProvider interface {
	Now() time.Time
}

type defaultTime struct{}

func (defaultTime) Now() time.Time { return time.Now() }

// Concrete implementation of router.GossipKeySet for Allocator CRDT

type uidSet map[uint64]bool

func (a uidSet) Merge(b router.GossipKeySet) {
	for key, _ := range b.(uidSet) {
		a[key] = true
	}
}

func (alloc *Allocator) EmptySet() router.GossipKeySet {
	return make(uidSet)
}

// Types used to send requests from Actor client to server
type stop struct{}
type makeString struct {
	resultChan chan<- string
}
type claim struct {
	resultChan chan<- error
	Ident      string
	IP         net.IP
}
type getFor struct {
	resultChan chan<- net.IP
	Ident      string
}
type free struct {
	resultChan chan<- error
	Ident      string
	IP         net.IP
}
type deleteRecordsFor struct {
	Ident string
}
type gossipUnicast struct {
	resultChan chan<- error
	sender     router.PeerName
	bytes      []byte
}
type gossipBroadcast struct {
	resultChan chan<- error
	bytes      []byte
}
type gossipEncode struct {
	resultChan chan<- []byte
	keySet     router.GossipKeySet
}
type gossipFullSet struct {
	resultChan chan<- router.GossipKeySet
}
type gossipUpdate struct {
	resultChan chan<- gossipReply
	bytes      []byte
}
type gossipReply struct {
	err       error
	updateSet router.GossipKeySet
}
type onDead struct {
	name router.PeerName
	uid  uint64
}

type claimList []claim

func (aa *claimList) removeAt(pos int) {
	(*aa) = append((*aa)[:pos], (*aa)[pos+1:]...)
}

func (aa *claimList) find(addr net.IP) int {
	for i, a := range *aa {
		if a.IP.Equal(addr) {
			return i
		}
	}
	return -1
}

func NewAllocator(ourName router.PeerName, ourUID uint64, universeCIDR string) (*Allocator, error) {
	_, universeNet, err := net.ParseCIDR(universeCIDR)
	if err != nil {
		return nil, err
	}
	if universeNet.IP.To4() == nil {
		return nil, errors.New("Non-IPv4 address not supported")
	}
	// Get the size of the network from the mask
	ones, bits := universeNet.Mask.Size()
	var universeSize uint32 = 1 << uint(bits-ones)
	if universeSize < 4 {
		return nil, errors.New("Allocation universe too small")
	}
	// Per RFC1122, don't allocate the first (network) and last (broadcast) addresses
	alloc := &Allocator{
		ourName:      ourName,
		ourUID:       ourUID,
		state:        allocStateLeaderless,
		universeLen:  ones,
		universe:     MinSpace{Start: add(universeNet.IP, 1), Size: universeSize - 2},
		peerInfo:     make(map[uint64]SpaceSet),
		ourSpaceSet:  NewSpaceSet(ourName, ourUID),
		owned:        make(map[string][]net.IP),
		leaked:       make(map[time.Time]Space),
		timeProvider: defaultTime{},
	}
	alloc.peerInfo[ourUID] = alloc.ourSpaceSet
	return alloc, nil
}

func (alloc *Allocator) SetGossip(gossip router.Gossip) {
	alloc.gossip = gossip
}

func (alloc *Allocator) Start() {
	alloc.state = allocStateLeaderless
	queryChan := make(chan interface{}, router.ChannelSize)
	alloc.queryChan = queryChan
	go alloc.queryLoop(queryChan, true)
}

func (alloc *Allocator) startForTesting() {
	alloc.state = allocStateLeaderless
	queryChan := make(chan interface{}, router.ChannelSize)
	alloc.queryChan = queryChan
	go alloc.queryLoop(queryChan, false)
}

// Actor client API

// Sync.
func (alloc *Allocator) Stop() {
	alloc.queryChan <- stop{}
}

// Sync.
// Claim an address that we think we should own
func (alloc *Allocator) Claim(ident string, addr net.IP) error {
	lg.Info.Printf("Address %s claimed by %s", addr, ident)
	resultChan := make(chan error)
	alloc.queryChan <- claim{resultChan, ident, addr}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) GetFor(ident string) net.IP {
	resultChan := make(chan net.IP)
	alloc.queryChan <- getFor{resultChan, ident}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) Free(ident string, addr net.IP) error {
	resultChan := make(chan error)
	alloc.queryChan <- free{resultChan, ident, addr}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) String() string {
	resultChan := make(chan string)
	alloc.queryChan <- makeString{resultChan}
	return <-resultChan
}

// Async.
func (alloc *Allocator) DeleteRecordsFor(ident string) error {
	alloc.queryChan <- deleteRecordsFor{ident}
	return nil // this is to satisfy the ContainerObserver interface
}

// Sync.
func (alloc *Allocator) OnGossipUnicast(sender router.PeerName, msg []byte) error {
	lg.Debug.Printf("OnGossipUnicast from %s: %d bytes\n", sender, len(msg))
	resultChan := make(chan error)
	alloc.queryChan <- gossipUnicast{resultChan, sender, msg}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) OnGossipBroadcast(msg []byte) error {
	lg.Debug.Printf("OnGossipBroadcast: %d bytes\n", len(msg))
	resultChan := make(chan error)
	alloc.queryChan <- gossipBroadcast{resultChan, msg}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) FullSet() router.GossipKeySet {
	resultChan := make(chan router.GossipKeySet)
	alloc.queryChan <- gossipFullSet{resultChan}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) Encode(keys router.GossipKeySet) []byte {
	resultChan := make(chan []byte)
	alloc.queryChan <- gossipEncode{resultChan, keys}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) OnUpdate(msg []byte) (router.GossipKeySet, error) {
	lg.Debug.Printf("Allocator.OnGossip: %d bytes\n", len(msg))
	resultChan := make(chan gossipReply)
	alloc.queryChan <- gossipUpdate{resultChan, msg}
	ret := <-resultChan
	return ret.updateSet, ret.err
}

// No-op
func (alloc *Allocator) OnAlive(name router.PeerName, uid uint64) {
	// If it's new to us, nothing to do.
	// If we previously believed it to be dead, need to figure that case out.
}

// Async.
func (alloc *Allocator) OnDead(name router.PeerName, uid uint64) {
	alloc.queryChan <- onDead{name, uid}
}

// ACTOR server

func (alloc *Allocator) queryLoop(queryChan <-chan interface{}, withTimers bool) {
	var fastTimer, slowTimer <-chan time.Time
	if withTimers {
		fastTimer = time.Tick(GossipReqTimeout)
		slowTimer = time.Tick(router.GossipInterval)
	}
	for {
		prevState := alloc.ourSpaceSet.HasFreeAddresses()
		prevVersion := alloc.ourSpaceSet.version
		select {
		case query, ok := <-queryChan:
			if !ok {
				return
			}
			switch q := query.(type) {
			case stop:
				return
			case makeString:
				q.resultChan <- alloc.string()
			case claim:
				alloc.electLeaderIfNecessary()
				alloc.handleClaim(q.Ident, q.IP, q.resultChan)
			case getFor:
				alloc.electLeaderIfNecessary()
				if addrs, found := alloc.owned[q.Ident]; found && len(addrs) > 0 {
					q.resultChan <- addrs[0] // currently not supporting multiple allocations in the same subnet
				} else if !alloc.tryAllocateFor(q.Ident, q.resultChan) {
					alloc.pending = append(alloc.pending, getFor{q.resultChan, q.Ident})
				}
			case deleteRecordsFor:
				for _, ip := range alloc.owned[q.Ident] {
					alloc.ourSpaceSet.Free(ip)
				}
				delete(alloc.owned, q.Ident)
			case free:
				if alloc.removeOwned(q.Ident, q.IP) {
					q.resultChan <- alloc.ourSpaceSet.Free(q.IP)
				} else {
					q.resultChan <- fmt.Errorf("free: %s not owned by %s", q.IP, q.Ident)
				}
			case gossipUnicast:
				switch q.bytes[0] {
				case msgSpaceRequest:
					q.resultChan <- alloc.handleSpaceRequest(q.sender, q.bytes[1:])
				case msgSpaceDonate:
					q.resultChan <- alloc.handleSpaceDonate(q.sender, q.bytes[1:])
				case msgSpaceClaim:
					q.resultChan <- alloc.handleSpaceClaim(q.sender, q.bytes[1:])
				case msgSpaceClaimRefused:
					q.resultChan <- alloc.handleSpaceClaimRefused(q.sender, q.bytes[1:])
				case msgLeaderElected:
					// some other peer decided we were the leader:
					// if we already have a leader then they didn't get the memo; repeat
					if !alloc.leaderless() {
						alloc.gossip.GossipBroadcast(encode(alloc.ourSpaceSet))
					} else {
						// re-run the election here to avoid races
						alloc.electLeaderIfNecessary()
					}
					q.resultChan <- nil
				default:
					q.resultChan <- errors.New(fmt.Sprint("Unexpected gossip unicast message: ", q.bytes[0]))
				}
			case gossipBroadcast:
				q.resultChan <- alloc.handleGossipBroadcast(q.bytes)
			case gossipUpdate:
				updateSet, err := alloc.handleGossipReceived(q.bytes)
				q.resultChan <- gossipReply{err, updateSet}
			case gossipEncode:
				q.resultChan <- alloc.handleGossipEncode(q.keySet.(uidSet))
			case gossipFullSet:
				q.resultChan <- alloc.handleGossipFullSet()
			case onDead:
				alloc.handleDead(q.name, q.uid)
			}
		case <-slowTimer:
			alloc.slowConsiderOurPosition()
		case <-fastTimer:
			alloc.considerOurPosition()
		}
		if prevState != alloc.ourSpaceSet.HasFreeAddresses() {
			alloc.ourSpaceSet.version++
		}
		if prevVersion != alloc.ourSpaceSet.version {
			alloc.gossip.GossipBroadcast(encode(alloc.ourSpaceSet))
		}
	}
}

func encode(spaceset SpaceSet) []byte {
	return GobEncode(1, spaceset)
}

// Unpack the supplied buffer which is encoded as per encode() above.
// return a slice of MinSpace containing those PeerSpaces which are newer
// than what we had previously
func (alloc *Allocator) decodeFromDecoder(decoder *gob.Decoder) ([]*PeerSpaceSet, error) {
	var numSpaceSets int
	if err := decoder.Decode(&numSpaceSets); err != nil {
		return nil, err
	}
	ret := make([]*PeerSpaceSet, 0)
	for i := 0; i < numSpaceSets; i++ {
		newSpaceset := new(PeerSpaceSet)
		if err := newSpaceset.Decode(decoder); err != nil {
			return nil, err
		}
		// compare this received spaceset's version against the one we had prev.
		oldSpaceset, found := alloc.peerInfo[newSpaceset.UID()]
		if !found || newSpaceset.Version() > oldSpaceset.Version() {
			if newSpaceset.UID() == alloc.ourUID {
				lg.Error.Println("Received update to our own info")
				continue // Shouldn't happen
			} else if newSpaceset.PeerName() == alloc.ourName {
				lg.Debug.Println("Received update with our peerName but different UID")
				if alloc.pastLife == nil || alloc.pastLife.lastSeen.Before(newSpaceset.lastSeen) {
					alloc.pastLife = newSpaceset
				}
				continue
			} else if oldSpaceset != nil && oldSpaceset.MaybeDead() {
				lg.Info.Println("Received update for peer believed dead", newSpaceset)
			}
			lg.Debug.Println("Replacing data with newer version:", newSpaceset.peerName)
			alloc.peerInfo[newSpaceset.UID()] = newSpaceset
			if alloc.leaderless() && !newSpaceset.Empty() {
				alloc.weHaveALeader()
			}
			ret = append(ret, newSpaceset)
		}
	}
	return ret, nil
}

func (alloc *Allocator) decodeUpdate(update []byte) ([]*PeerSpaceSet, error) {
	reader := bytes.NewReader(update)
	decoder := gob.NewDecoder(reader)
	return alloc.decodeFromDecoder(decoder)
}

func (alloc *Allocator) spaceOwner(space *MinSpace) uint64 {
	for uid, spaceset := range alloc.peerInfo {
		if spaceset.Overlaps(space) {
			return uid
		}
	}
	return 0
}

func (alloc *Allocator) lookForDead(now time.Time) {
	limit := now.Add(-GossipDeadTimeout)
	for _, entry := range alloc.peerInfo {
		if peerEntry, ok := entry.(*PeerSpaceSet); ok &&
			peerEntry.MaybeDead() && !peerEntry.IsTombstone() &&
			peerEntry.lastSeen.Before(limit) {
			peerEntry.MakeTombstone()
			lg.Debug.Println("Tombstoned", peerEntry)
			alloc.gossip.GossipBroadcast(encode(peerEntry))
		}
	}
}

func (alloc *Allocator) lookForNewLeaks(now time.Time) {
	allSpace := NewSpaceSet(router.UnknownPeerName, 0)
	allSpace.AddSpace(&alloc.universe)
	for _, peerSpaceSet := range alloc.peerInfo {
		peerSpaceSet.ForEachSpace(func(space Space) {
			allSpace.Exclude(space)
		})
	}
	if !allSpace.Empty() {
		// Now remove the leaks we already knew about
		for _, leak := range alloc.leaked {
			allSpace.Exclude(leak)
		}
		if !allSpace.Empty() {
			lg.Debug.Println(allSpace.describe("New leaked spaces:"))
			for _, space := range allSpace.spaces {
				alloc.leaked[now] = space
				break // can only store one space against each time
			}
		}
	}
}

func (alloc *Allocator) discardOldLeaks() {
	for age, leak := range alloc.leaked {
		for _, peerSpaceSet := range alloc.peerInfo {
			if peerSpaceSet.Overlaps(leak) {
				lg.Debug.Printf("Discarding non-leak %+v", leak)
				// Really, we should only discard the piece that is overlapped, but
				// this way is simpler and we will recover any real leaks in the end
				delete(alloc.leaked, age)
			}
		}
	}
}

// look for leaks which are aged, and which we are heir to
func (alloc *Allocator) reclaimLeaks(now time.Time) (changed bool) {
	changed = false
	limit := now.Add(-GossipDeadTimeout)
	for age, leak := range alloc.leaked {
		if age.Before(limit) {
			for _, space := range alloc.ourSpaceSet.spaces {
				if space.IsHeirTo(leak, &alloc.universe) {
					lg.Info.Printf("Reclaiming leak %+v heir %+v", leak, space)
					delete(alloc.leaked, age)
					alloc.ourSpaceSet.AddSpace(leak)
					break
				}
			}
		}
	}
	return
}

func (alloc *Allocator) reclaimPastLife() {
	lg.Debug.Println("Reclaiming allocations from past life", alloc.pastLife)
	for _, space := range alloc.pastLife.spaces {
		alloc.ourSpaceSet.AddSpace(space)
	}
	alloc.pastLife.MakeTombstone()
	alloc.gossip.GossipBroadcast(encode(alloc.pastLife))
	lg.Debug.Println("alloc now", alloc.string())
}

func (alloc *Allocator) checkClaim(ident string, addr net.IP) (owner uint64, err error) {
	testaddr := &MinSpace{addr, 1}
	if alloc.pastLife != nil && alloc.pastLife.Overlaps(testaddr) {
		// We've been sent a peerInfo that matches our PeerName but not UID
		// We've also been asked to claim an IP that is in the range it owned
		// Conclude that this is an echo of our former self, and reclaim it.
		alloc.reclaimPastLife()
	}
	if owner := alloc.spaceOwner(testaddr); owner == 0 {
		// That address is not currently owned; wait until someone claims it
		return 0, nil
	} else if spaceSet := alloc.peerInfo[owner]; spaceSet == alloc.ourSpaceSet {
		// We own it, perhaps because we claimed it above.
		if existingIdent := alloc.findOwner(addr); existingIdent == "" {
			alloc.addOwned(ident, addr)
			err := alloc.ourSpaceSet.Claim(addr)
			return alloc.ourUID, err
		} else if existingIdent == ident {
			return alloc.ourUID, nil
		} else {
			return alloc.ourUID, fmt.Errorf("Claimed address %s is already owned by %s", addr, existingIdent)
		}
	} else {
		// That address is owned by someone else
		claimspace := MinSpace{addr, 1}
		if alloc.inflight.find(spaceSet.PeerName(), []Space{&claimspace}) < 0 { // Have we already requested this one?
			lg.Debug.Println("Claiming address", addr, "from peer:", spaceSet.PeerName())
			alloc.sendRequest(spaceSet.PeerName(), msgSpaceClaim, &claimspace)
		}
		return owner, nil
	}
}

func (alloc *Allocator) checkClaims() {
	for i := 0; i < len(alloc.claims); i++ {
		owner, err := alloc.checkClaim(alloc.claims[i].Ident, alloc.claims[i].IP)
		if err != nil || owner == alloc.ourUID {
			alloc.claims[i].resultChan <- err
			alloc.claims.removeAt(i)
			i--
		}
	}
	alloc.checkPending()
}

// return true if the request is completed, false if pending
func (alloc *Allocator) tryAllocateFor(ident string, resultChan chan<- net.IP) bool {
	if addr := alloc.ourSpaceSet.Allocate(); addr != nil {
		lg.Debug.Println("Allocated", addr, "for", ident)
		alloc.addOwned(ident, addr)
		resultChan <- addr
		return true
	} else { // out of space
		if alloc.inflight.findKind(msgSpaceRequest) < 0 && alloc.inflight.findKind(msgLeaderElected) < 0 { // is there already a request inflight
			if !alloc.requestSpace() {
				resultChan <- nil // Nobody to ask for more space, so fail now
				return true
			}
		}
	}
	return false
}

func (alloc *Allocator) checkPending() {
	i := 0
	for ; i < len(alloc.pending); i++ {
		if !alloc.tryAllocateFor(alloc.pending[i].Ident, alloc.pending[i].resultChan) {
			break
		}
	}
	alloc.pending = alloc.pending[i:]
}

// If somebody didn't come back to us, drop the record and we will ask again
// because we will still have the underlying need
func (alloc *Allocator) checkInflight(now time.Time) {
	for i := 0; i < len(alloc.inflight); i++ {
		if now.After(alloc.inflight[i].expires) {
			alloc.inflight.removeAt(i)
			i--
		}
	}
}

// Fairly quick check of what's going on; whether requests should now be
// replied to, etc.
func (alloc *Allocator) considerOurPosition() (changed bool) {
	now := alloc.timeProvider.Now()
	switch alloc.state {
	case allocStateNeutral:
		alloc.checkInflight(now)
		alloc.checkClaims()
	case allocStateLeaderless:
		alloc.checkInflight(now)
		if len(alloc.pending) > 0 {
			alloc.electLeaderIfNecessary()
		}
	}
	return
}

// Slower check looking for leaks, etc.
func (alloc *Allocator) slowConsiderOurPosition() (changed bool) {
	now := alloc.timeProvider.Now()
	changed = alloc.considerOurPosition()
	switch alloc.state {
	case allocStateNeutral:
		alloc.discardOldLeaks()
		alloc.lookForDead(now)
		changed = alloc.reclaimLeaks(now)
		alloc.lookForNewLeaks(now)
	}
	return
}

func (alloc *Allocator) leaderless() bool {
	return alloc.state == allocStateLeaderless
}

func (alloc *Allocator) weHaveALeader() {
	if pos := alloc.inflight.findKind(msgLeaderElected); pos >= 0 {
		alloc.inflight.removeAt(pos)
	}
	alloc.state = allocStateNeutral
}

func (alloc *Allocator) electLeaderIfNecessary() {
	if !alloc.leaderless() || alloc.inflight.findKind(msgLeaderElected) >= 0 {
		return
	}
	lg.Debug.Println("Time to look for a leader")
	// If anyone is already managing some space, then we don't need to elect a leader
	highest := alloc.ourUID
	for uid, spaceset := range alloc.peerInfo {
		if !spaceset.Empty() {
			// If anyone is already managing some space, then we don't need to elect a leader
			lg.Error.Println("Peer", spaceset.PeerName(), "has some space; we missed this somehow")
			alloc.weHaveALeader()
			return
		}
		if uid > highest {
			highest = uid
		}
	}
	lg.Debug.Println("Elected leader:", alloc.peerInfo[highest].PeerName())
	// The peer with the highest name is the leader
	if highest == alloc.ourUID {
		lg.Info.Printf("I was elected leader of the universe %+v", alloc.universe)
		// I'm the winner; take control of the whole universe
		alloc.ourSpaceSet.AddSpace(&alloc.universe)
		alloc.weHaveALeader()
		alloc.checkClaims()
	} else {
		alloc.sendRequest(alloc.peerInfo[highest].PeerName(), msgLeaderElected, nil)
	}
}

func (alloc *Allocator) sendRequest(dest router.PeerName, kind byte, space *MinSpace) {
	var msg []byte
	if space == nil {
		msg = router.Concat([]byte{kind}, encode(alloc.ourSpaceSet))
	} else {
		msg = router.Concat([]byte{kind}, GobEncode(space, 1, alloc.ourSpaceSet))
	}
	alloc.gossip.GossipUnicast(dest, msg)
	req := &request{dest, kind, space, alloc.timeProvider.Now().Add(GossipReqTimeout)}
	alloc.inflight = append(alloc.inflight, req)
}

func (alloc *Allocator) sendReply(dest router.PeerName, kind byte, data interface{}) {
	msg := router.Concat([]byte{kind}, GobEncode(data, 1, alloc.ourSpaceSet))
	alloc.gossip.GossipUnicast(dest, msg)
}

func (alloc *Allocator) requestSpace() bool {
	var best SpaceSet = nil
	var bestNum int = 0
	for _, spaceset := range alloc.peerInfo {
		if spaceset != alloc.ourSpaceSet && spaceset.HasFreeAddresses() && !spaceset.MaybeDead() {
			if num := alloc.ourSpaceSet.NumSpacesMergeable(spaceset, &alloc.universe); num > bestNum || best == nil {
				bestNum = num
				best = spaceset
			}
		}
	}
	if best != nil {
		lg.Debug.Println("Decided to ask peer", best.PeerName(), "for space:", best)
		alloc.sendRequest(best.PeerName(), msgSpaceRequest, nil)
		return true
	} else {
		lg.Debug.Println("Nobody available to ask for space")
		return false
	}
}

func (alloc *Allocator) handleSpaceRequest(sender router.PeerName, msg []byte) error {
	lg.Debug.Println("Received space request from", sender)
	if _, err := alloc.decodeUpdate(msg); err != nil {
		return err
	}

	if space, ok := alloc.ourSpaceSet.GiveUpSpace(); ok {
		lg.Debug.Println("Decided to give  peer", sender, "space", space, alloc.ourSpaceSet)
		alloc.sendReply(sender, msgSpaceDonate, []Space{space})
	} else {
		lg.Debug.Println("No space available; sending back empty reply to", sender, alloc.ourSpaceSet)
		alloc.sendReply(sender, msgSpaceDonate, []Space{})
	}
	return nil
}

func (alloc *Allocator) handleSpaceClaim(sender router.PeerName, msg []byte) error {
	decoder := gob.NewDecoder(bytes.NewReader(msg))
	var spaceClaimed MinSpace
	if err := decoder.Decode(&spaceClaimed); err != nil {
		return err
	}
	lg.Debug.Println("Received space claim from", sender, "for ", spaceClaimed)
	if _, err := alloc.decodeFromDecoder(decoder); err != nil {
		return err
	}
	if alloc.ourSpaceSet.GiveUpSpecificSpace(&spaceClaimed) {
		lg.Debug.Println("Giving peer", sender, "space", spaceClaimed)
		alloc.sendReply(sender, msgSpaceDonate, &spaceClaimed)
	} else {
		lg.Debug.Println("Claim refused - space occupied", spaceClaimed)
		alloc.sendReply(sender, msgSpaceClaimRefused, &spaceClaimed)
	}

	return nil
}

func (alloc *Allocator) handleSpaceDonate(sender router.PeerName, msg []byte) error {
	reader := bytes.NewReader(msg)
	decoder := gob.NewDecoder(reader)
	var donations []Space
	if err := decoder.Decode(&donations); err != nil {
		return err
	}
	pos := alloc.inflight.find(sender, donations)
	if pos < 0 {
		lg.Error.Println("Not expecting to receive space donation from", sender, alloc.inflight[0].dest)
		return nil // not a severe enough error to shut down the connection
	}
	lg.Debug.Println("Received space donation: sender", sender, "space", donations)
	// Message is concluded by an update of state of the sender
	if _, err := alloc.decodeFromDecoder(decoder); err != nil {
		return err
	}
	for _, donation := range donations {
		alloc.ourSpaceSet.AddSpace(donation)
	}
	alloc.inflight.removeAt(pos)
	alloc.checkClaims()
	return nil
}

func (alloc *Allocator) handleSpaceClaimRefused(sender router.PeerName, msg []byte) error {
	decoder := gob.NewDecoder(bytes.NewReader(msg))
	var claim MinSpace
	if err := decoder.Decode(&claim); err != nil {
		return err
	}
	pos := alloc.inflight.find(sender, []Space{&claim})
	if pos < 0 {
		lg.Error.Println("Not expecting to receive space donation refused from", sender, alloc.inflight[0].dest)
		return nil // not a severe enough error to shut down the connection
	}
	lg.Debug.Println("Received space claim refused: sender", sender, "space", claim)
	// Message is concluded by an update of state of the sender
	if _, err := alloc.decodeFromDecoder(decoder); err != nil {
		return err
	}
	for i := 0; i < len(alloc.claims); i++ {
		if claim.Contains(alloc.claims[i].IP) {
			alloc.claims[i].resultChan <- errors.New("IP address owned by" + sender.String())
			alloc.claims.removeAt(i)
			i--
		}
	}
	alloc.inflight.removeAt(pos)
	return nil
}

// Claim an address that we think we should own
func (alloc *Allocator) handleClaim(ident string, addr net.IP, resultChan chan<- error) {
	testaddr := &MinSpace{addr, 1}
	if !alloc.universe.Overlaps(testaddr) {
		// Address not within our universe; assume user knows what they are doing
		resultChan <- nil
		return
	}
	// See if it's already claimed
	if pos := alloc.claims.find(addr); pos >= 0 && alloc.claims[pos].Ident != ident {
		resultChan <- errors.New("IP address already claimed by " + alloc.claims[pos].Ident)
		return
	}
	if owner, err := alloc.checkClaim(ident, addr); err != nil {
		resultChan <- err
	} else if owner != alloc.ourUID {
		alloc.claims = append(alloc.claims, claim{resultChan, ident, addr})
	} else {
		resultChan <- nil
	}
}

func (alloc *Allocator) string() string {
	var buf bytes.Buffer
	state := "neutral"
	if alloc.state == allocStateLeaderless {
		state = "leaderless"
	}
	buf.WriteString(fmt.Sprintf("Allocator state %s universe %s+%d", state, alloc.universe.Start, alloc.universe.Size))
	for _, spaceset := range alloc.peerInfo {
		buf.WriteByte('\n')
		buf.WriteString(spaceset.String())
	}
	for _, claim := range alloc.claims {
		buf.WriteString("\nClaim ")
		buf.WriteString(fmt.Sprintf("%s %s", claim.Ident, claim.IP))
	}
	return buf.String()
}

func (alloc *Allocator) handleGossipCreate() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	panicOnError(enc.Encode(len(alloc.peerInfo)))
	for _, spaceset := range alloc.peerInfo {
		panicOnError(spaceset.Encode(enc))
	}
	return buf.Bytes()
}

func (alloc *Allocator) handleGossipBroadcast(buf []byte) error {
	_, err := alloc.decodeUpdate(buf)
	if err != nil {
		return err
	}
	alloc.considerOurPosition()
	return nil
}

// merge in state and return a buffer encoding those PeerSpaces which are newer
// than what we had previously, or nil if none were newer
func (alloc *Allocator) handleGossipReceived(buf []byte) (router.GossipKeySet, error) {
	newerPeerSpaces, err := alloc.decodeUpdate(buf)
	if err != nil {
		return nil, err
	}
	alloc.considerOurPosition()
	if len(newerPeerSpaces) == 0 {
		return nil, nil
	} else {
		updateSet := make(uidSet)
		for _, spaceset := range newerPeerSpaces {
			updateSet[spaceset.uid] = true
		}
		return updateSet, nil
	}
}

func (alloc *Allocator) handleGossipFullSet() router.GossipKeySet {
	ret := make(uidSet)
	for _, spaceset := range alloc.peerInfo {
		ret[spaceset.UID()] = true
	}
	return ret
}

func (alloc *Allocator) handleGossipEncode(uids uidSet) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	panicOnError(enc.Encode(len(uids)))
	for uid, _ := range uids {
		spaceset := alloc.peerInfo[uid]
		panicOnError(spaceset.Encode(enc))
	}
	return buf.Bytes()
}

func (alloc *Allocator) handleDead(name router.PeerName, uid uint64) {
	entry, found := alloc.peerInfo[uid]
	if found {
		if peerEntry, ok := entry.(*PeerSpaceSet); ok &&
			!peerEntry.MaybeDead() {
			lg.Info.Printf("[allocator] Marking %s as maybe dead", entry.PeerName())
			peerEntry.MarkMaybeDead(true, alloc.timeProvider.Now())
		}
	}
}
