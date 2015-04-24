package space

import (
	"fmt"
	"math"
	"math/big"
	"net"

	"github.com/weaveworks/weave/ipam/utils"
)

// Space repsents a range of addresses owned by this peer,
// and contains the state for managing free addresses.
type Space struct {
	Start net.IP
	Size  uint32
	inuse big.Int // bit is 0 if free; 1 if in use
}

const MaxSize = math.MaxInt32 // big.Int uses 'int' for indexing, so assume it might be 32-bit

func (space *Space) assertInvariants() {
	utils.Assert(uint32(space.inuse.BitLen()) <= space.Size,
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
	space.inuse.SetBit(&space.inuse, int(offset), 1)
	return true, nil
}

// Allocate returns the lowest availible IP within this space.
func (space *Space) Allocate() net.IP {
	space.assertInvariants()
	defer space.assertInvariants()

	// Find the lowest available address on the free list
	offset := 0
	for ; offset < space.inuse.BitLen(); offset++ {
		if space.inuse.Bit(offset) == 0 {
			break
		}
	}
	if uint32(offset) >= space.Size { // out of space
		return nil
	}

	space.inuse.SetBit(&space.inuse, offset, 1)
	return utils.Add(space.Start, uint32(offset))
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
	if space.inuse.Bit(int(offset)) == 0 {
		return fmt.Errorf("Duplicate free: %s", addr)
	}
	space.inuse.SetBit(&space.inuse, int(offset), 0)

	return nil
}

// assertFree asserts that the size consequtive IPs from start
// (inclusive) are not allocated
func (space *Space) assertFree(start net.IP, size uint32) {
	utils.Assert(space.contains(start), "Range outside my care")
	utils.Assert(space.contains(utils.Add(start, size-1)), "Range outside my care")

	startOffset := uint32(utils.Subtract(space.Start, start))
	endOffset := startOffset + size
	if endOffset > uint32(space.inuse.BitLen()) { // Anything beyond this is free
		endOffset = uint32(space.inuse.BitLen())
	}

	for i := startOffset; i < endOffset; i++ {
		utils.Assert(space.inuse.Bit(int(i)) == 0, "Address in use!")
	}
}

// BiggestFreeChunk scans the in-use list and returns the
// start, length of the largest free range of address it
// can find.
func (space *Space) BiggestFreeChunk() (net.IP, uint32) {
	space.assertInvariants()
	defer space.assertInvariants()

	// Keep a track of the current chunk start and size
	// First chunk we've found is the one of unallocated space at the end
	chunkStart := uint32(space.inuse.BitLen())
	chunkSize := space.Size - chunkStart

	// Now scan the free list of other chunks
	for i := 0; i < space.inuse.BitLen(); i++ {
		potentialStart := uint32(i)
		// Run forward past all the free ones
		for i < space.inuse.BitLen() && space.inuse.Bit(i) == 0 {
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
	var numInUse uint = 0
	for i := 0; i < space.inuse.BitLen(); i++ {
		numInUse += space.inuse.Bit(i)
	}
	return space.Size - uint32(numInUse)
}

func (space *Space) String() string {
	return fmt.Sprintf("%s+%d (%d)", space.Start, space.Size, space.inuse.BitLen())
}

// Split divide this space into two new spaces at a given address, copying allocations and frees.
func (space *Space) Split(addr net.IP) (*Space, *Space) {
	utils.Assert(space.contains(addr), "Splitting around a point not in the space!")
	breakpoint := utils.Subtract(addr, space.Start)
	ret1 := &Space{Start: space.Start, Size: uint32(breakpoint)}
	ret2 := &Space{Start: addr, Size: space.Size - uint32(breakpoint)}

	// Copy the inuse using mask and shift
	var mask, one big.Int
	mask.SetBit(&mask, int(breakpoint), 1)
	one.SetInt64(1)
	mask.Sub(&mask, &one)
	ret1.inuse.And(&space.inuse, &mask)
	ret2.inuse.Rsh(&space.inuse, uint(breakpoint))

	return ret1, ret2
}
