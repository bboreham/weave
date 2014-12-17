package sortinghat

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/zettio/weave/router"
	"net"
	"sync"
	"time"
)

type SpaceSet interface {
	Encode(enc *gob.Encoder) error
	Decode(decoder *gob.Decoder) error
	Empty() bool
	Version() uint64
	LastSeen() time.Time
	SetLastSeen(time.Time)
	PeerName() router.PeerName
	UID() uint64
	NumFreeAddresses() uint32
	Overlaps(space *MinSpace) bool
	String() string
}

// This represents a peer's space allocations, which we only hear about.
type PeerSpaceSet struct {
	peerName router.PeerName
	uid      uint64
	version  uint64
	spaces   []Space
	lastSeen time.Time
	sync.RWMutex
}

// Represents our own space, which we can allocate and free within.
type MutableSpaceSet struct {
	PeerSpaceSet
}

func NewPeerSpace(pn router.PeerName, uid uint64) *PeerSpaceSet {
	return &PeerSpaceSet{peerName: pn, uid: uid, lastSeen: time.Now()}
}

func (s *PeerSpaceSet) LastSeen() time.Time       { return s.lastSeen }
func (s *PeerSpaceSet) SetLastSeen(t time.Time)   { s.lastSeen = t }
func (s *PeerSpaceSet) PeerName() router.PeerName { return s.peerName }
func (s *PeerSpaceSet) UID() uint64               { return s.uid }
func (s *PeerSpaceSet) Version() uint64           { return s.version }

func (s *PeerSpaceSet) Encode(enc *gob.Encoder) error {
	s.RLock()
	defer s.RUnlock()
	if err := enc.Encode(s.peerName); err != nil {
		return err
	}
	if err := enc.Encode(s.uid); err != nil {
		return err
	}
	if err := enc.Encode(s.version); err != nil {
		return err
	}
	if err := enc.Encode(len(s.spaces)); err != nil {
		return err
	}
	for _, space := range s.spaces {
		if err := enc.Encode(space.GetMinSpace()); err != nil {
			return err
		}
	}
	return nil
}

func (s *PeerSpaceSet) Decode(decoder *gob.Decoder) error {
	s.Lock()
	defer s.Unlock()
	if err := decoder.Decode(&s.peerName); err != nil {
		return err
	}
	if err := decoder.Decode(&s.uid); err != nil {
		return err
	}
	if err := decoder.Decode(&s.version); err != nil {
		return err
	}
	var numSpaces int
	if err := decoder.Decode(&numSpaces); err != nil {
		return err
	}
	s.spaces = make([]Space, numSpaces)
	for i := 0; i < numSpaces; i++ {
		s.spaces[i] = new(MinSpace)
		if err := decoder.Decode(s.spaces[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *PeerSpaceSet) String() string {
	var buf bytes.Buffer
	s.RLock()
	defer s.RUnlock()
	buf.WriteString(fmt.Sprint("SpaceSet ", s.peerName, s.uid, " (v", s.version, ")\n"))
	for _, space := range s.spaces {
		buf.WriteString(fmt.Sprintf("  %s\n", space.String()))
	}
	return buf.String()
}

func (s *PeerSpaceSet) Empty() bool {
	s.RLock()
	defer s.RUnlock()
	return len(s.spaces) == 0
}

func (s *PeerSpaceSet) NumFreeAddresses() uint32 {
	s.RLock()
	defer s.RUnlock()
	var freeAddresses uint32 = 0
	for _, space := range s.spaces {
		freeAddresses += space.LargestFreeBlock()
	}
	return freeAddresses
}

func (s *PeerSpaceSet) Overlaps(space *MinSpace) bool {
	s.RLock()
	defer s.RUnlock()
	for _, space2 := range s.spaces {
		if space.Overlaps(space2) {
			return true
		}
	}
	return false
}

// -------------------------------------------------

func NewSpaceSet(pn router.PeerName, uid uint64) *MutableSpaceSet {
	return &MutableSpaceSet{PeerSpaceSet{peerName: pn, uid: uid, lastSeen: time.Now()}}
}

func (s *MutableSpaceSet) AddSpace(space *MutableSpace) {
	s.Lock()
	defer s.Unlock()
	s.spaces = append(s.spaces, space)
	s.version++
}

func (s *MutableSpaceSet) NumFreeAddresses() uint32 {
	s.RLock()
	defer s.RUnlock()
	// TODO: Optimize; perhaps maintain the count in allocate and free
	var freeAddresses uint32 = 0
	for _, space := range s.spaces {
		freeAddresses += space.(*MutableSpace).NumFreeAddresses()
	}
	return freeAddresses
}

// Give up some space because one of our peers has asked for it.
// Pick some large reasonably-sized chunk.
func (s *MutableSpaceSet) GiveUpSpace() (start net.IP, size uint32, ok bool) {
	totalFreeAddresses := s.NumFreeAddresses()
	if totalFreeAddresses < MinSafeFreeAddresses {
		return nil, 0, false
	}
	var bestFree uint32 = 0
	var bestSpace *MutableSpace = nil
	for _, space := range s.spaces {
		mSpace := space.(*MutableSpace)
		numFree := mSpace.NumFreeAddresses()
		if numFree > bestFree {
			bestFree = numFree
			bestSpace = mSpace
			if numFree >= MaxAddressesToGiveUp {
				break
			}
		}
	}
	if bestSpace != nil {
		var spaceToGiveUp uint32 = MaxAddressesToGiveUp
		if spaceToGiveUp > bestFree {
			spaceToGiveUp = bestFree
		}
		// Don't give away more than half of our available addresses
		if spaceToGiveUp > totalFreeAddresses/2 {
			spaceToGiveUp = totalFreeAddresses / 2
		}
		bestSpace.Size -= spaceToGiveUp
		s.version++
		return add(bestSpace.Start, bestSpace.Size), spaceToGiveUp, true
	}
	return nil, 0, false
}

func (s *MutableSpaceSet) AllocateFor(ident string) net.IP {
	s.Lock()
	defer s.Unlock()
	// TODO: Optimize; perhaps cache last-used space
	for _, space := range s.spaces {
		if ret := space.(*MutableSpace).AllocateFor(ident); ret != nil {
			return ret
		}
	}
	return nil
}

func (s *MutableSpaceSet) Free(addr net.IP) error {
	s.Lock()
	defer s.Unlock()
	for _, space := range s.spaces {
		if space.(*MutableSpace).Free(addr) {
			return nil
		}
	}
	return errors.New("Attempt to free IP address not in range")
}
