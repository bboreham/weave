package ipam

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/zettio/weave/ipam/ring"
	"github.com/zettio/weave/ipam/space"
	"github.com/zettio/weave/ipam/utils"
	"github.com/zettio/weave/router"
	"net"
)

const (
	msgSpaceRequest = iota
	msgLeaderElected
)

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

func (alloc *Allocator) Gossip() router.GossipData {
	return &ipamGossipData{alloc}
}

type Allocator struct {
	queryChan     chan<- interface{}
	ourName       router.PeerName
	universeStart net.IP
	universeSize  uint32
	universeLen   int        // length of network prefix (e.g. 24 for a /24 network)
	ring          *ring.Ring // it's for you!
	spaceSet      *space.SpaceSet
	owned         map[string][]net.IP // who owns what address, indexed by container-ID
	pending       []getFor
	gossip        router.Gossip
}

func NewAllocator(ourName router.PeerName, universeCIDR string) (*Allocator, error) {
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
	alloc := &Allocator{
		ourName:       ourName,
		universeStart: universeNet.IP,
		universeSize:  universeSize,
		universeLen:   ones,
		ring:          ring.New(universeNet.IP, utils.Add(universeNet.IP, universeSize), ourName),
		spaceSet:      space.NewSpaceSet(),
		owned:         make(map[string][]net.IP),
	}
	return alloc, nil
}

func (alloc *Allocator) SetGossip(gossip router.Gossip) {
	alloc.gossip = gossip
}

func (alloc *Allocator) Start() {
	queryChan := make(chan interface{}, router.ChannelSize)
	alloc.queryChan = queryChan
	go alloc.queryLoop(queryChan, true)
}

func (alloc *Allocator) string() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Allocator universe %s+%d\n", alloc.universeStart, alloc.universeSize))
	buf.WriteString(alloc.ring.String())
	buf.WriteString(alloc.spaceSet.String())
	return buf.String()
}

func (alloc *Allocator) considerOurPosition() {
}

func (alloc *Allocator) electLeaderIfNecessary() {
	if !alloc.ring.Empty() {
		return
	}
	leader := alloc.gossip.(router.Leadership).LeaderElect()
	alloc.Debugln("Elected leader:", leader)
	if leader == alloc.ourName {
		// I'm the winner; take control of the whole universe
		alloc.ring.ClaimItAll()
		alloc.spaceSet.Add(alloc.universeStart, alloc.universeSize)
		alloc.Infof("I was elected leader of the universe %s", alloc.string())
	} else {
		alloc.sendRequest(leader, msgLeaderElected)
	}
}

// return true if the request is completed, false if pending
func (alloc *Allocator) tryAllocateFor(ident string, resultChan chan<- net.IP) bool {
	if addr := alloc.spaceSet.Allocate(); addr != nil {
		alloc.Debugln("Allocated", addr, "for", ident)
		alloc.addOwned(ident, addr)
		resultChan <- addr
		return true
	} else { // out of space
		// fixme: request space
	}
	return false
}

func (alloc *Allocator) handleCancelGetFor(ident string) {
	for i, pending := range alloc.pending {
		if pending.Ident == ident {
			alloc.pending = append(alloc.pending[:i], alloc.pending[i+1:]...)
			break
		}
	}
}

func (alloc *Allocator) sendRequest(dest router.PeerName, kind byte) {
	msg := router.Concat([]byte{kind}, alloc.ring.GossipState())
	alloc.gossip.GossipUnicast(dest, msg)
	//req := &request{dest, kind, space, alloc.timeProvider.Now().Add(GossipReqTimeout)}
	//alloc.inflight = append(alloc.inflight, req)
}
