package ipam

import (
	"fmt"
	lg "github.com/zettio/weave/common"
	"github.com/zettio/weave/router"
	"net"
)

// Types used to send requests from Actor client to server
type stop struct{}
type makeString struct {
	resultChan chan<- string
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
			case getFor:
				alloc.electLeaderIfNecessary()
				if addrs, found := alloc.owned[q.Ident]; found && len(addrs) > 0 {
					q.resultChan <- addrs[0] // currently not supporting multiple allocations in the same subnet
				} else if !alloc.tryAllocateFor(q.Ident, q.resultChan) {
					alloc.pending = append(alloc.pending, getFor{q.resultChan, q.Ident})
				}
			case cancelGetFor:
				alloc.handleCancelGetFor(q.Ident)
			case deleteRecordsFor:
				for _, ip := range alloc.owned[q.Ident] {
					alloc.spaceSet.Free(ip)
				}
				delete(alloc.owned, q.Ident)
			case free:
				if alloc.removeOwned(q.Ident, q.IP) {
					q.resultChan <- alloc.spaceSet.Free(q.IP)
				} else {
					q.resultChan <- fmt.Errorf("free: %s not owned by %s", q.IP, q.Ident)
				}
			case gossipUnicast:
				switch q.bytes[0] {
				case msgLeaderElected:
					q.resultChan <- alloc.handleLeaderElected()
				case msgSpaceRequest:
					// some other peer asked us for space
					alloc.donateSpace(q.sender)
					q.resultChan <- nil
				}
			case gossipBroadcast:
				q.resultChan <- alloc.ring.OnGossipBroadcast(q.bytes)
				alloc.considerNewSpaces()
				alloc.assertInvariants()
				alloc.considerOurPosition()
			case gossipUpdate:
				err := alloc.ring.OnGossipBroadcast(q.bytes)
				updateSet := (*ipamGossipData)(nil) // fixme
				q.resultChan <- gossipReply{err, updateSet}
				alloc.considerOurPosition()
			case gossipEncode:
				q.resultChan <- alloc.ring.GossipState()
			}
		}
	}
}

func (alloc *Allocator) Errorln(args ...interface{}) {
	lg.Error.Println(append([]interface{}{fmt.Sprintf("[allocator %s]:", alloc.ourName)}, args...)...)
}
func (alloc *Allocator) Infof(fmt string, args ...interface{}) {
	lg.Info.Printf("[allocator %s] "+fmt, append([]interface{}{alloc.ourName}, args...)...)
}
func (alloc *Allocator) Debugln(args ...interface{}) {
	lg.Debug.Println(append([]interface{}{fmt.Sprintf("[allocator %s]:", alloc.ourName)}, args...)...)
}
