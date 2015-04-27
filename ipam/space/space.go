package space

import (
	"fmt"
	"math"
	"net"

	"github.com/weaveworks/weave/ipam/utils"
	"golang.org/x/tools/container/intsets"
)

// Space repsents a range of addresses owned by this peer,
// and contains the state for managing free addresses.
type Space struct {
	Start net.IP
	Size  uint32
	inuse intsets.Sparse
}

const MaxSize = math.MaxInt32 // big.Int uses 'int' for indexing, so assume it might be 32-bit

func (space *Space) assertInvariants() {
	utils.Assert(space.inuse.Max() < int(space.Size),
		"In-use list must not be bigger than size")
}

func (space *Space) contains(addr net.IP) bool {
	diff := utils.Subtract(addr, space.Start)
	return diff >= 0 && diff < int64(space.Size)
}

// Claim marks an address as allocated on behalf of some specific container
func (space *Space) Claim(addr net.IP) (bool, error) {
	offset := utils.Subtract(addr, space.Start)
	if !(offset >= 0 && offset < int64(space.Size)) {
		return false, nil
	}
	space.inuse.Insert(int(offset))
	return true, nil
}

// Allocate returns the lowest availible IP within this space.
func (space *Space) Allocate() net.IP {
	space.assertInvariants()
	defer space.assertInvariants()

	// Find the lowest available address - would be better if intsets.Sparse did this internally
	var offset uint32 = 0
	for ; offset < space.Size; offset++ {
		if !space.inuse.Has(int(offset)) {
			break
		}
	}
	if offset >= space.Size { // out of space
		return nil
	}

	space.inuse.Insert(int(offset))
	return utils.Add(space.Start, offset)
}

func (space *Space) addrInRange(addr net.IP) bool {
	offset := utils.Subtract(addr, space.Start)
	return offset >= 0 && offset < int64(space.Size)
}

// Free takes an IP in this space and record it as avalible.
func (space *Space) Free(addr net.IP) error {
	space.assertInvariants()
	defer space.assertInvariants()

	if !space.addrInRange(addr) {
		return fmt.Errorf("Free out of range: %s", addr)
	}

	offset := utils.Subtract(addr, space.Start)
	if !space.inuse.Has(int(offset)) {
		return fmt.Errorf("Freeing address that is not in use: %s", addr)
	}
	space.inuse.Remove(int(offset))

	return nil
}

// assertFree asserts that the size consequtive IPs from start
// (inclusive) are not allocated
func (space *Space) assertFree(start net.IP, size uint32) {
	utils.Assert(space.contains(start), "Range outside my care")
	utils.Assert(space.contains(utils.Add(start, size-1)), "Range outside my care")

	startOffset := int(utils.Subtract(start, space.Start))
	if startOffset > space.inuse.Max() { // Anything beyond this is free
		return
	}

	for i := startOffset; i < startOffset+int(size); i++ {
		utils.Assert(!space.inuse.Has(i), "Address in use!")
	}
}

// BiggestFreeChunk scans the in-use list and returns the
// start, length of the largest free range of address it
// can find.
func (space *Space) BiggestFreeChunk() (net.IP, uint32) {
	space.assertInvariants()
	defer space.assertInvariants()

	if space.inuse.IsEmpty() { // Check to avoid Max() returning MinInt below
		return space.Start, space.Size
	}

	// Keep a track of the current chunk start and size
	// First chunk we've found is the one of unallocated space at the end
	max := space.inuse.Max()
	chunkStart := uint32(max) + 1
	chunkSize := space.Size - chunkStart

	// Now scan the free list to find other chunks
	for i := 0; i < max; i++ {
		potentialStart := uint32(i)
		// Run forward past all the free ones
		for i < max && !space.inuse.Has(i) {
			i++
		}
		// Is the chunk we found bigger than the
		// one we already have?
		potentialSize := uint32(i) - potentialStart
		if potentialSize > chunkSize {
			chunkStart = potentialStart
			chunkSize = potentialSize
		}
	}

	// Now return what we found
	if chunkSize == 0 {
		return nil, 0
	}

	addr := utils.Add(space.Start, chunkStart)
	space.assertFree(addr, chunkSize)
	return addr, chunkSize
}

// Grow increases the size of this space to size.
func (space *Space) Grow(size uint32) {
	utils.Assert(space.Size < size, "Cannot shrink a space!")
	space.Size = size
}

// NumFreeAddresses returns the total number of free addresses in
// this space.
func (space *Space) NumFreeAddresses() uint32 {
	return space.Size - uint32(space.inuse.Len())
}

func (space *Space) String() string {
	return fmt.Sprintf("%s+%d (%d)", space.Start, space.Size, space.inuse.Len())
}

// Split divide this space into two new spaces at a given address, copying allocations and frees.
func (space *Space) Split(addr net.IP) (*Space, *Space) {
	utils.Assert(space.contains(addr), "Splitting around a point not in the space!")
	breakpoint := utils.Subtract(addr, space.Start)
	ret1 := &Space{Start: space.Start, Size: uint32(breakpoint)}
	ret2 := &Space{Start: addr, Size: space.Size - uint32(breakpoint)}

	// Now copy the in-use list - this implementation is slow but simple
	max := space.inuse.Max()
	for i := 0; i <= max; i++ {
		if space.inuse.Has(i) {
			if i < int(breakpoint) {
				ret1.inuse.Insert(i)
			} else {
				ret2.inuse.Insert(i - int(breakpoint))
			}
		}
	}

	return ret1, ret2
}
