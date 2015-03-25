package space

import (
	"fmt"
	"github.com/zettio/weave/ipam/utils"
	"net"
)

type Space struct {
	Start        net.IP
	Size         uint32
	MaxAllocated uint32 // 0 if nothing allocated, 1 if first address allocated, etc.
	free_list    addressList
}

func (a *Space) Contains(addr net.IP) bool {
	diff := utils.Subtract(addr, a.Start)
	return diff >= 0 && diff < int64(a.Size)
}

func NewSpace(start net.IP, size uint32) *Space {
	return &Space{Start: start, Size: size, MaxAllocated: 0}
}

func (space *Space) Allocate() net.IP {
	ret := space.free_list.take()
	if ret == nil && space.MaxAllocated < space.Size {
		space.MaxAllocated++
		ret = utils.Add(space.Start, space.MaxAllocated-1)
	}
	return ret
}

func (space *Space) Free(addr net.IP) error {
	offset := utils.Subtract(addr, space.Start)
	if !(offset >= 0 && offset < int64(space.Size)) {
		return fmt.Errorf("Free out of range: %s", addr)
	} else if offset >= int64(space.MaxAllocated) {
		return fmt.Errorf("IP address not allocated: %s", addr)
	} else if space.free_list.find(addr) >= 0 {
		return fmt.Errorf("Duplicate free: %s", addr)
	}
	space.free_list.add(addr)
	// TODO: consolidate free space
	return nil
}

func (s *Space) BiggestFreeChunk() (net.IP, uint32) {
	// Return some chunk, not necessarily _the_ biggest
	if s.MaxAllocated < s.Size {
		return utils.Add(s.Start, s.MaxAllocated), s.Size - s.MaxAllocated
	} else if len(s.free_list) > 0 {
		// Find how many contiguous addresses are at the head of the free list
		size := 1
		for ; size < len(s.free_list) && utils.Subtract(s.free_list[size], s.free_list[size-1]) == 1; size++ {
		}
		return s.free_list[0], uint32(size)
	}
	return nil, 0
}

func (s *Space) NumFreeAddresses() uint32 {
	return s.Size - s.MaxAllocated + uint32(len(s.free_list))
}

func (space *Space) String() string {
	return fmt.Sprintf("%s+%d, %d/%d", space.Start, space.Size, space.MaxAllocated, len(space.free_list))
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
	for _, alloc := range space.free_list {
		offset := utils.Subtract(alloc, addr)
		if offset < 0 {
			if uint32(offset+breakpoint) < ret1.MaxAllocated {
				ret1.free_list.add(alloc)
			}
		} else {
			if uint32(offset) < ret2.MaxAllocated {
				ret2.free_list.add(alloc)
			}
		}
	}

	return ret1, ret2
}
