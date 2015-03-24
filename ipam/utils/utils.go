package utils

import (
	"net"
)

// We shouldn't ever get any errors on *encoding*, but if we do, this will make sure we get to hear about them.
func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func Ip4int(ip4 net.IP) (r uint32) {
	for _, b := range ip4.To4() {
		r <<= 8
		r |= uint32(b)
	}
	return
}

func Intip4(key uint32) (r net.IP) {
	r = make([]byte, net.IPv4len)
	for i := 3; i >= 0; i-- {
		r[i] = byte(key)
		key >>= 8
	}
	return
}

// IPv4 Address Arithmetic - convert to 32-bit unsigned integer, add, and convert back
func Add(addr net.IP, i uint32) net.IP {
	sum := Ip4int(addr) + i
	return Intip4(sum)
}

func Subtract(a, b net.IP) int64 {
	return int64(Ip4int(a)) - int64(Ip4int(b))
}

func Assert(test bool, message string) {
	if !test {
		panic(message)
	}
}
