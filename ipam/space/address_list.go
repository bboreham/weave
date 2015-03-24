package space

import (
	"github.com/zettio/weave/ipam/utils"
	"net"
)

type addressList []net.IP

// Maintain addresses in increasing order.
func (aa *addressList) add(a net.IP) {
	for i, b := range *aa {
		if utils.Subtract(b, a) > 0 {
			(*aa) = append((*aa), nil)   // make space
			copy((*aa)[i+1:], (*aa)[i:]) // move up
			(*aa)[i] = a                 // put in new element
			return
		}
	}
	*aa = append(*aa, a)
}

func (aa *addressList) removeAt(pos int) {
	// Delete, preserving order
	(*aa) = append((*aa)[:pos], (*aa)[pos+1:]...)
}

func (aa *addressList) find(addr net.IP) int {
	for i, a := range *aa {
		if a.Equal(addr) {
			return i
		}
	}
	return -1
}

func (aa *addressList) take() net.IP {
	if n := len(*aa); n > 0 {
		ret := (*aa)[n-1]
		*aa = (*aa)[:n-1]
		return ret
	}
	return nil
}
