package space

import (
	"github.com/zettio/weave/router"
	wt "github.com/zettio/weave/testing"
	"net"
	"testing"
)

func equal(ms1 *Space, ms2 *Space) bool {
	return ms1.Start.Equal(ms2.Start) &&
		ms1.Size == ms2.Size
}

// Note: does not check version
func (ps1 *SpaceSet) Equal(ps2 *SpaceSet) bool {
	if len(ps1.spaces) == len(ps2.spaces) {
		for i := 0; i < len(ps1.spaces); i++ {
			if !equal(ps1.spaces[i], ps2.spaces[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func spaceSetWith(pn router.PeerName, uid uint64, spaces ...*Space) *SpaceSet {
	ps := NewSpaceSet()
	for _, space := range spaces {
		ps.AddSpace(space)
	}
	return ps
}

func TestGiveUp(t *testing.T) {
	const (
		peer1     = "7a:9f:eb:b6:0c:6e"
		peer1UID  = 123456
		testAddr1 = "10.0.1.0"
		testAddr2 = "10.0.1.32"
	)

	var (
		ipAddr1 = net.ParseIP(testAddr1)
	)

	pn1, _ := router.PeerNameFromString(peer1)
	ps1 := spaceSetWith(pn1, peer1UID, NewSpace(ipAddr1, 48))
	_, numGivenUp, ok := ps1.GiveUpSpace()
	wt.AssertBool(t, ok, true, "GiveUpSpace result")
	wt.AssertEqualUint32(t, numGivenUp, 25, "GiveUpSpace 1 size")
	wt.AssertEqualUint32(t, ps1.NumFreeAddresses(), 23, "num free addresses")
	count := 0 // count to avoid infinite loop
	for ; count < 1000; count++ {
		_, size, ok := ps1.GiveUpSpace()
		if !ok {
			break
		}
		numGivenUp += size
	}
	wt.AssertEqualUint32(t, ps1.NumFreeAddresses(), 0, "num free addresses")
	wt.AssertEqualUint32(t, numGivenUp, 48, "total space given up")
}
