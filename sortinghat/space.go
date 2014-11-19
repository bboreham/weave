package sortinghat

import (
	"net"
)

type Space interface {
	Allocate() (net.IP, error)
	Free(addr net.IP) error
}

type simpleSpace struct {
	start         net.IP
	size          int
	max_allocated int
	free_list     []net.IP
}

func NewSpace(start net.IP, size int) Space {
	return &simpleSpace{start: start, size: size, max_allocated: 0}
}

func (space *simpleSpace) Allocate() (net.IP, error) {
	return space.start, nil
}

func (space *simpleSpace) Free(addr net.IP) error {
	return nil
}
