package ipam

import (
	"fmt"
	"github.com/zettio/weave/common"
	"github.com/zettio/weave/router"
	wt "github.com/zettio/weave/testing"
	"math/rand"
	"testing"
	"time"
)

// Utility function to set up initial conditions for test
func (alloc *Allocator) addSpace(startAddr string, length uint32) *Allocator {
	// fixme
	return alloc
}

func (alloc *Allocator) numFreeAddresses() uint32 {
	// fixme
	return 123456
}

// To allow time itself to be stubbed out for testing
type timeProvider interface {
	Now() time.Time
}

type defaultTime struct{}

func (defaultTime) Now() time.Time { return time.Now() }

func (alloc *Allocator) setTimeProvider(tp timeProvider) {
	// fixme
}

type mockMessage struct {
	dst     router.PeerName
	msgType byte
	buf     []byte
}

func (m *mockMessage) String() string {
	return fmt.Sprintf("-> %s [%x]", m.dst, m.buf)
}

func toStringArray(messages []mockMessage) []string {
	out := make([]string, len(messages))
	for i := range out {
		out[i] = messages[i].String()
	}
	return out
}

type mockGossipComms struct {
	t        *testing.T
	name     string
	messages []mockMessage
}

// Note: this style of verification, using equalByteBuffer, requires
// that the contents of messages are never re-ordered.  Which, for instance,
// requires they are not based off iterating through a map.

func (m *mockGossipComms) GossipBroadcast(buf []byte) error {
	if len(m.messages) == 0 {
		wt.Fatalf(m.t, "%s: Gossip broadcast message unexpected: \n%x", m.name, buf)
	} else if msg := m.messages[0]; msg.dst != router.UnknownPeerName {
		wt.Fatalf(m.t, "%s: Expected Gossip message to %s but got broadcast", m.name, msg.dst)
	} else if msg.buf != nil && !equalByteBuffer(msg.buf, buf) {
		wt.Fatalf(m.t, "%s: Gossip message not sent as expected: \nwant: %x\ngot : %x", m.name, msg.buf, buf)
	} else {
		// Swallow this message
		m.messages = m.messages[1:]
	}
	return nil
}

func equalByteBuffer(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (m *mockGossipComms) GossipUnicast(dstPeerName router.PeerName, buf []byte) error {
	if len(m.messages) == 0 {
		wt.Fatalf(m.t, "%s: Gossip message to %s unexpected: \n%s", m.name, dstPeerName, buf)
	} else if msg := m.messages[0]; msg.dst == router.UnknownPeerName {
		wt.Fatalf(m.t, "%s: Expected Gossip broadcast message but got dest %s", m.name, dstPeerName)
	} else if msg.dst != dstPeerName {
		wt.Fatalf(m.t, "%s: Expected Gossip message to %s but got dest %s", m.name, msg.dst, dstPeerName)
	} else if buf[0] != msg.msgType {
		wt.Fatalf(m.t, "%s: Expected Gossip message of type %d but got type %d", m.name, msg.msgType, buf[0])
	} else if msg.buf != nil && !equalByteBuffer(msg.buf, buf[1:]) {
		wt.Fatalf(m.t, "%s: Gossip message not sent as expected: \nwant: %x\ngot : %x", m.name, msg.buf, buf[1:])
	} else {
		// Swallow this message
		m.messages = m.messages[1:]
	}
	return nil
}

func ExpectMessage(alloc *Allocator, dst string, msgType byte, buf []byte) {
	m := alloc.gossip.(*mockGossipComms)
	dstPeerName, _ := router.PeerNameFromString(dst)
	m.messages = append(m.messages, mockMessage{dstPeerName, msgType, buf})
}

func ExpectBroadcastMessage(alloc *Allocator, buf []byte) {
	m := alloc.gossip.(*mockGossipComms)
	m.messages = append(m.messages, mockMessage{router.UnknownPeerName, 0, buf})
}

func CheckAllExpectedMessagesSent(allocs ...*Allocator) {
	for _, alloc := range allocs {
		m := alloc.gossip.(*mockGossipComms)
		if len(m.messages) > 0 {
			wt.Fatalf(m.t, "%s: Gossip message(s) not sent as expected: \n%x", m.name, m.messages)
		}
	}
}

type mockTimeProvider struct {
	myTime time.Time
}

type mockTimer struct {
	when time.Time
	f    func()
}

func (m *mockTimeProvider) SetTime(t time.Time) { m.myTime = t }
func (m *mockTimeProvider) Now() time.Time      { return m.myTime }

func testAllocator(t *testing.T, name string, ourUID uint64, universeCIDR string) *Allocator {
	ourName, _ := router.PeerNameFromString(name)
	alloc, _ := NewAllocator(ourName, ourUID, universeCIDR)
	alloc.gossip = &mockGossipComms{t: t, name: name}
	alloc.startForTesting()
	return alloc
}

func (alloc *Allocator) startForTesting() {
	alloc.state = allocStateLeaderless
	queryChan := make(chan interface{}, router.ChannelSize)
	alloc.queryChan = queryChan
	go alloc.queryLoop(queryChan, false)
}

// Check whether or not something was sent on a channel
func AssertSent(t *testing.T, ch <-chan bool) {
	timeout := time.After(time.Second)
	select {
	case <-ch:
		// This case is ok
	case <-timeout:
		wt.Fatalf(t, "Nothing sent on channel")
	}
}

func AssertNothingSent(t *testing.T, ch <-chan bool) {
	select {
	case val := <-ch:
		wt.Fatalf(t, "Unexpected value on channel: %t", val)
	default:
		// no message received
	}
}

func AssertNothingSentErr(t *testing.T, ch <-chan error) {
	select {
	case val := <-ch:
		wt.Fatalf(t, "Unexpected value on channel: %t", val)
	default:
		// no message received
	}
}

// Router to convey gossip from one gossiper to another, for testing
type gossipMessage struct {
	isUnicast bool
	sender    *router.PeerName
	buf       []byte
}

type TestGossipRouter struct {
	gossipChans map[router.PeerName]chan gossipMessage
	loss        float32 // 0.0 means no loss
}

func (router *TestGossipRouter) GossipBroadcast(buf []byte) error {
	for _, gossipChan := range router.gossipChans {
		select {
		case gossipChan <- gossipMessage{false, nil, buf}:
		default: // drop the message if we cannot send it
		}
	}
	return nil
}

type TestGossipRouterClient struct {
	router *TestGossipRouter
	sender router.PeerName
}

func (grouter *TestGossipRouter) connect(sender router.PeerName, gossiper router.Gossiper) router.Gossip {
	gossipChan := make(chan gossipMessage, 100)

	go func() {
		gossipTimer := time.Tick(router.GossipInterval)
		for {
			select {
			case message := <-gossipChan:
				if rand.Float32() > (1.0 - grouter.loss) {
					continue
				}

				if message.isUnicast {
					gossiper.OnGossipUnicast(*message.sender, message.buf)
				} else {
					gossiper.OnGossipBroadcast(message.buf)
				}
			case <-gossipTimer:
				grouter.GossipBroadcast(gossiper.(router.GossipData).Encode(gossiper.(router.GossipData).FullSet()))
			}
		}
	}()

	grouter.gossipChans[sender] = gossipChan
	return TestGossipRouterClient{grouter, sender}
}

func (client TestGossipRouterClient) GossipUnicast(dstPeerName router.PeerName, buf []byte) error {
	common.Debug.Printf("GossipUnicast from %s to %s", client.sender, dstPeerName)
	select {
	case client.router.gossipChans[dstPeerName] <- gossipMessage{true, &client.sender, buf}:
	default: // drop the message if we cannot send it
	}
	return nil
}

func (client TestGossipRouterClient) GossipBroadcast(buf []byte) error {
	common.Debug.Printf("GossipBroadcast from %s", client.sender)
	return client.router.GossipBroadcast(buf)
}

func makeNetworkOfAllocators(size int, cidr string) ([]*Allocator, TestGossipRouter) {
	gossipRouter := TestGossipRouter{make(map[router.PeerName]chan gossipMessage), 0.0}
	allocs := make([]*Allocator, size)

	for i := 0; i < size; i++ {
		peerNameStr := fmt.Sprintf("%02d:00:00:02:00:00", i)
		peerName, _ := router.PeerNameFromString(peerNameStr)
		alloc, _ := NewAllocator(peerName, uint64(i), cidr)
		alloc.SetGossip(gossipRouter.connect(peerName, alloc))
		alloc.Start()
		allocs[i] = alloc
	}

	gossipRouter.GossipBroadcast(allocs[size-1].Encode(allocs[size-1].FullSet()))
	time.Sleep(1000 * time.Millisecond)
	return allocs, gossipRouter
}
