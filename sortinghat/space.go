package sortinghat

import (
	"net"
)

type Space interface {
	Allocate() net.IP
	Free(addr net.IP)
}

type simpleSpace struct {
	start         net.IP
	size          uint32
	max_allocated uint32
	free_list     []net.IP
}

func NewSpace(start net.IP, size uint32) Space {
	return &simpleSpace{start: start, size: size, max_allocated: 0}
}

func (space *simpleSpace) Allocate() net.IP {
	n := len(space.free_list)
	if n > 0 {
		ret := space.free_list[n-1]
		space.free_list = space.free_list[:n-1]
		return ret
	} else if space.max_allocated < space.size {
		space.max_allocated++
		return Add(space.start, space.max_allocated-1)
	}
	return nil // out of space
}

func (space *simpleSpace) Free(addr net.IP) {
	space.free_list = append(space.free_list, addr)
	// TODO: consolidate free space
}

// IPv4 Address Arithmetic - convert to 32-bit unsigned integer, add, and convert back
func Add(addr net.IP, i uint32) net.IP {
	ip := addr.To4()
	if ip == nil {
		return nil
	}
	sum := (uint32(ip[0]) << 24) + (uint32(ip[1]) << 16) + (uint32(ip[2]) << 8) + uint32(ip[3]) + i
	p := make(net.IP, net.IPv4len)
	p[0] = byte(sum >> 24)
	p[1] = byte((sum & 0xffffff) >> 16)
	p[2] = byte((sum & 0xffff) >> 8)
	p[3] = byte(sum & 0xff)
	return p
}
