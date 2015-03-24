package space

import (
	"github.com/zettio/weave/ipam/utils"
	wt "github.com/zettio/weave/testing"
	"net"
	"testing"
)

func TestSpaceAllocate(t *testing.T) {
	const (
		testAddr1   = "10.0.3.4"
		testAddrx   = "10.0.3.19"
		testAddry   = "10.0.9.19"
		containerID = "deadbeef"
	)

	space1 := NewSpace(net.ParseIP(testAddr1), 20)
	wt.AssertEqualUint32(t, space1.NumFreeAddresses(), 20, "Free addresses")
	space1.checkInvariant(t)

	addr1 := space1.Allocate()
	wt.AssertEqualString(t, addr1.String(), testAddr1, "address")
	wt.AssertEqualUint32(t, space1.NumFreeAddresses(), 19, "Free addresses")
	space1.checkInvariant(t)

	addr2 := space1.Allocate()
	wt.AssertNotEqualString(t, addr2.String(), testAddr1, "address")
	wt.AssertEqualUint32(t, space1.NumFreeAddresses(), 18, "Free addresses")
	space1.checkInvariant(t)

	space1.Free(addr2)

	wt.AssertErrorInterface(t, space1.Free(addr2), (*error)(nil), "double free")
	wt.AssertErrorInterface(t, space1.Free(net.ParseIP(testAddrx)), (*error)(nil), "address not allocated")
	wt.AssertErrorInterface(t, space1.Free(net.ParseIP(testAddry)), (*error)(nil), "wrong out of range")

	space1.checkInvariant(t)
}

func (m *Space) checkInvariant(t *testing.T) {
	if m.MaxAllocated > m.Size {
		t.Fatalf("MaxAllocated must not be greater than size: %v", m)
	}
	for i := 0; i < len(m.free_list)-1; i++ {
		if utils.Subtract(m.free_list[i], m.free_list[i+1]) > 0 {
			t.Fatalf("Free list out of order: %v", m.free_list)
		}
	}
}

func TestSpaceSplit(t *testing.T) {
	const (
		containerID = "feedbacc"
		testAddr1   = "10.0.1.1"
		testAddr2   = "10.0.1.3"
	)

	space1 := NewSpace(net.ParseIP(testAddr1), 10)
	addr1 := space1.Allocate()
	addr2 := space1.Allocate()
	addr3 := space1.Allocate()
	space1.Free(addr2)
	space1.checkInvariant(t)
	split1, split2 := space1.Split(net.ParseIP(testAddr2))
	wt.AssertEqualUint32(t, split1.Size, 2, "split size")
	wt.AssertEqualUint32(t, split2.Size, 8, "split size")
	wt.AssertEqualUint32(t, split1.NumFreeAddresses(), 1, "Free addresses")
	wt.AssertEqualUint32(t, split2.NumFreeAddresses(), 7, "Free addresses")
	space1.checkInvariant(t)
	split1.checkInvariant(t)
	split2.checkInvariant(t)
	wt.AssertNoErr(t, split1.Free(addr1))
	wt.AssertErrorInterface(t, split1.Free(addr3), (*error)(nil), "free")
	wt.AssertErrorInterface(t, split2.Free(addr1), (*error)(nil), "free")
	wt.AssertNoErr(t, split2.Free(addr3))
}
