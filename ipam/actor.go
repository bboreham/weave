package ipam

import (
	"github.com/zettio/weave/router"
	"net"
)

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
type cancelClaim struct {
	Ident string
	IP    net.IP
}
type getFor struct {
	resultChan chan<- net.IP
	Ident      string
}
type cancelGetFor struct {
	Ident string
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
}
type gossipReply struct {
	err    error
	update router.GossipData
}
type gossipUpdate struct {
	resultChan chan<- gossipReply
	bytes      []byte
}

// Actor client API

// Sync.
func (alloc *Allocator) Stop() {
	alloc.queryChan <- stop{}
}

// Sync.
// Claim an address that we think we should own
func (alloc *Allocator) Claim(ident string, addr net.IP, cancelChan <-chan bool) error {
	alloc.Infof("Address %s claimed by %s", addr, ident)
	resultChan := make(chan error)
	alloc.queryChan <- claim{resultChan, ident, addr}
	select {
	case result := <-resultChan:
		return result
	case <-cancelChan:
		alloc.queryChan <- cancelClaim{ident, addr}
		return nil
	}
}

// Sync.
func (alloc *Allocator) GetFor(ident string, cancelChan <-chan bool) net.IP {
	resultChan := make(chan net.IP)
	alloc.queryChan <- getFor{resultChan, ident}
	select {
	case result := <-resultChan:
		return result
	case <-cancelChan:
		alloc.queryChan <- cancelGetFor{ident}
		return nil
	}
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
	alloc.Debugln("OnGossipUnicast from", sender, ": ", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.queryChan <- gossipUnicast{resultChan, sender, msg}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) OnGossipBroadcast(msg []byte) error {
	alloc.Debugln("OnGossipBroadcast:", len(msg), "bytes")
	resultChan := make(chan error)
	alloc.queryChan <- gossipBroadcast{resultChan, msg}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) Encode() []byte {
	resultChan := make(chan []byte)
	alloc.queryChan <- gossipEncode{resultChan}
	return <-resultChan
}

// Sync.
func (alloc *Allocator) OnGossip(msg []byte) (router.GossipData, error) {
	alloc.Debugln("Allocator.OnGossip:", len(msg), "bytes")
	resultChan := make(chan gossipReply)
	alloc.queryChan <- gossipUpdate{resultChan, msg}
	ret := <-resultChan
	return ret.update, ret.err
}

// ACTOR server

func (alloc *Allocator) queryLoop(queryChan <-chan interface{}, withTimers bool) {
	for {
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
			case cancelClaim:
			case getFor:
			case cancelGetFor:
			case deleteRecordsFor:
			case free:
			case gossipUnicast:
				switch q.bytes[0] {
				}
			case gossipBroadcast:
			case gossipUpdate:
			case gossipEncode:
			}
		}
	}
}
