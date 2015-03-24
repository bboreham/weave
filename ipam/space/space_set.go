package space

import (
	"bytes"
	"errors"
	"fmt"
	lg "github.com/zettio/weave/common"
	"github.com/zettio/weave/ipam/utils"
	"net"
)

const MaxAddressesToGiveUp = 256

type SpaceSet struct {
	spaces []*Space
}

func (s *SpaceSet) String() string {
	return s.describe("SpaceSet ")
}

func (s *SpaceSet) describe(heading string) string {
	var buf bytes.Buffer
	buf.WriteString(heading)
	for _, space := range s.spaces {
		buf.WriteString(fmt.Sprintf("\n  %s", space))
	}
	return buf.String()
}

// -------------------------------------------------

func NewSpaceSet() *SpaceSet {
	return &SpaceSet{}
}

func (s *SpaceSet) Add(start net.IP, size uint32) {
	s.AddSpace(NewSpace(start, size))
}

func (s *SpaceSet) AddSpace(newspace *Space) {
	// See if we can merge this space with an existing space
	for _, space := range s.spaces {
		if space.mergeBlank(newspace) {
			return
		}
	}
	s.spaces = append(s.spaces, NewSpace(newspace.Start, newspace.Size))
}

func (s *SpaceSet) NumFreeAddresses() uint32 {
	// TODO: Optimize; perhaps maintain the count in allocate and free
	var freeAddresses uint32 = 0
	for _, space := range s.spaces {
		freeAddresses += space.NumFreeAddresses()
	}
	return freeAddresses
}

// Give up some space because one of our peers has asked for it.
// Pick some large reasonably-sized chunk.
func (s *SpaceSet) GiveUpSpace() (start net.IP, size uint32, ok bool) {
	totalFreeAddresses := s.NumFreeAddresses()
	if totalFreeAddresses == 0 {
		return nil, 0, false
	}
	var bestFree uint32 = 0
	var bestIP net.IP = nil
	for _, space := range s.spaces {
		chunkStart, chunkSize := space.BiggestFreeChunk()
		if chunkStart != nil && chunkSize > bestFree {
			bestFree = chunkSize
			bestIP = chunkStart
			if chunkSize >= MaxAddressesToGiveUp {
				break
			}
		}
	}
	if bestIP != nil {
		var spaceToGiveUp uint32 = MaxAddressesToGiveUp
		if spaceToGiveUp > bestFree {
			spaceToGiveUp = bestFree
		}
		// Don't give away more than half of our available addresses
		if spaceToGiveUp > totalFreeAddresses/2+1 {
			spaceToGiveUp = totalFreeAddresses/2 + 1
		}
		return bestIP, spaceToGiveUp, s.GiveUpSpecificSpace(bestIP, spaceToGiveUp)
	}
	return nil, 0, false
}

// If we can, give up the space requested and return true.
func (s *SpaceSet) GiveUpSpecificSpace(start net.IP, size uint32) bool {
	for i, space := range s.spaces {
		if space.Contains(start) {
			split1, split2 := space.Split(start)
			var split3 *Space = nil
			if split2.Size != size {
				endAddress := utils.Add(start, size)
				split2, split3 = split2.Split(endAddress)
			}
			lg.Debug.Println("GiveUpSpecificSpace", start, size, "from", space, "splits", split1, split2, split3)
			if split2.NumFreeAddresses() == size {
				// Take out the old space, then add up to two new spaces.  Ordering of s.spaces is not important.
				s.spaces = append(s.spaces[:i], s.spaces[i+1:]...)
				if split1.Size > 0 {
					s.spaces = append(s.spaces, split1)
				}
				if split3 != nil {
					s.spaces = append(s.spaces, split3)
				}
				return true
			} else {
				lg.Debug.Println("Unable to give up space", split2)
				return false // space not free
			}
		}
	}
	return false
}

func (s *SpaceSet) Allocate() net.IP {
	// TODO: Optimize; perhaps cache last-used space
	for _, space := range s.spaces {
		if ret := space.Allocate(); ret != nil {
			return ret
		}
	}
	return nil
}

func (s *SpaceSet) Free(addr net.IP) error {
	for _, space := range s.spaces {
		if space.Contains(addr) {
			return space.Free(addr)
		}
	}
	lg.Debug.Println("Address", addr, "not in range", s)
	return errors.New(fmt.Sprintf("IP %s address not in range", addr))
}
