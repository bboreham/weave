package space

import (
	"fmt"
	"github.com/zettio/weave/ipam/utils"
	"net"
	"sort"
)

type Space struct {
	Start        net.IP
	Size         uint32
	MaxAllocated uint32 // 0 if nothing allocated, 1 if first address allocated, etc.
	freelist     addressList
}

func (s *Space) assertInvariants() {
	utils.Assert(s.MaxAllocated <= s.Size,
		"MaxAllocated must not be greater than size")
	utils.Assert(sort.IsSorted(s.freelist),
		"Free address list must always be sorted")
	utils.Assert(uint32(len(s.freelist)) <= s.MaxAllocated,
		"Can't have more entries on free list than allocated.")
}

func (s *Space) Contains(addr net.IP) bool {
	diff := utils.Subtract(addr, s.Start)
	return diff >= 0 && diff < int64(s.Size)
}

func NewSpace(start net.IP, size uint32) *Space {
	return &Space{Start: start, Size: size, MaxAllocated: 0}
}

func (space *Space) Allocate() net.IP {
	space.assertInvariants()
	defer space.assertInvariants()

	// First ask the free list; this will get
	// the lowest availible address
	if ret := space.freelist.take(); ret != nil {
		return ret
	}

	// If nothing on the free list, have we given
	// out all the addresses?
	if space.MaxAllocated >= space.Size {
		return nil
	}

	// Otherwise increase the number of address we have given
	// out
	space.MaxAllocated++
	return utils.Add(space.Start, space.MaxAllocated-1)
}

func (space *Space) addrInRange(addr net.IP) bool {
	offset := utils.Subtract(addr, space.Start)
	return offset >= 0 && offset < int64(space.Size)
}

func (space *Space) Free(addr net.IP) error {
	space.assertInvariants()
	defer space.assertInvariants()

	if !space.addrInRange(addr) {
		return fmt.Errorf("Free out of range: %s", addr)
	}

	offset := utils.Subtract(addr, space.Start)
	if offset >= int64(space.MaxAllocated) {
		return fmt.Errorf("IP address not allocated: %s", addr)
	}

	if space.freelist.find(addr) >= 0 {
		return fmt.Errorf("Duplicate free: %s", addr)
	}
	space.freelist.add(addr)
	space.drainFreeList()
	return nil
}

// drainFreeList takes any contiguous addresses at the end
// of the allocated address space and removes them from the
// free list, reducing the allocated address space
func (space *Space) drainFreeList() {
	for len(space.freelist) > 0 {
		end := len(space.freelist) - 1
		potential := space.freelist[end]
		offset := utils.Subtract(potential, space.Start)
		utils.Assert(space.addrInRange(potential), "Free list contains address not in range")

		// Is this potential address at the end of the allocated
		// address space?
		if offset != int64(space.MaxAllocated)-1 {
			return
		}

		space.freelist.removeAt(end)
		space.MaxAllocated--
	}
}

// BiggestFreeChunk scans the freelist and returns the
// start, length of the largest free range of address it
// can find.
func (s *Space) BiggestFreeChunk() (net.IP, uint32) {
	s.assertInvariants()
	defer s.assertInvariants()

	// First, drain the free list
	s.drainFreeList()

	// Keep a track of the current chunk start and size
	// First chunk we've found is the one of unallocated space
	chunkStart := utils.Add(s.Start, s.MaxAllocated)
	chunkSize := s.Size - s.MaxAllocated

	// Now scan the free list of other chunks
	for i := 0; i < len(s.freelist); {
		// We know we have a chunk of at least one
		potentialStart := s.freelist[i]
		potentialSize := uint32(1)

		// Can we grow this chunk one by one
		curr := s.freelist[i]
		i++
		for ; i < len(s.freelist); i++ {
			if utils.Subtract(curr, s.freelist[i]) > 1 {
				break
			}

			curr = s.freelist[i]
			potentialSize++
		}

		// Is the chunk we found bigger than the
		// one we already have?
		if potentialSize > chunkSize {
			chunkStart = potentialStart
			chunkSize = potentialSize
		}
	}

	// Now return what we found
	if chunkSize > 0 {
		return chunkStart, chunkSize
	} else {
		return nil, 0
	}
}

func (s *Space) NumFreeAddresses() uint32 {
	return s.Size - s.MaxAllocated + uint32(len(s.freelist))
}

func (space *Space) String() string {
	return fmt.Sprintf("%s+%d, %d/%d", space.Start, space.Size, space.MaxAllocated, len(space.freelist))
}

// Divide a space into two new spaces at a given address, copying allocations and frees.
func (space *Space) Split(addr net.IP) (*Space, *Space) {
	breakpoint := utils.Subtract(addr, space.Start)
	if breakpoint < 0 || breakpoint >= int64(space.Size) {
		return nil, nil // Not contained within this space
	}
	ret1 := NewSpace(space.Start, uint32(breakpoint))
	ret2 := NewSpace(addr, space.Size-uint32(breakpoint))

	// find the max-allocated point for each sub-space
	if space.MaxAllocated > uint32(breakpoint) {
		ret1.MaxAllocated = ret1.Size
		ret2.MaxAllocated = space.MaxAllocated - ret1.Size
	} else {
		ret1.MaxAllocated = space.MaxAllocated
		ret2.MaxAllocated = 0
	}

	// Now copy the free list, but omit anything above MaxAllocated in each case
	for _, alloc := range space.freelist {
		offset := utils.Subtract(alloc, addr)
		if offset < 0 {
			if uint32(offset+breakpoint) < ret1.MaxAllocated {
				ret1.freelist.add(alloc)
			}
		} else {
			if uint32(offset) < ret2.MaxAllocated {
				ret2.freelist.add(alloc)
			}
		}
	}

	return ret1, ret2
}
