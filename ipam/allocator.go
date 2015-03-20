package ipam

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/zettio/weave/router"
	"net"
)

const (
	allocStateNeutral = iota
	allocStateLeaderless
)

type Allocator struct {
	queryChan   chan<- interface{}
	ourName     router.PeerName
	state       int
	universeLen int
	gossip      router.Gossip
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
	alloc := &Allocator{
		ourName:     ourName,
		state:       allocStateLeaderless,
		universeLen: ones,
	}
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

func (alloc *Allocator) string() string {
	var buf bytes.Buffer
	state := "neutral"
	if alloc.state == allocStateLeaderless {
		state = "leaderless"
	}
	buf.WriteString(fmt.Sprintf("Allocator state %s", state))
	return buf.String()
}
