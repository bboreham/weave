package ipam

import (
	"fmt"
	lg "github.com/zettio/weave/common"
	"net"
)

func (alloc *Allocator) Errorln(args ...interface{}) {
	lg.Error.Println(append([]interface{}{fmt.Sprintf("[allocator %s]:", alloc.ourName)}, args...)...)
}
func (alloc *Allocator) Infof(fmt string, args ...interface{}) {
	lg.Info.Printf("[allocator %s] "+fmt, append([]interface{}{alloc.ourName}, args...)...)
}
func (alloc *Allocator) Debugln(args ...interface{}) {
	lg.Debug.Println(append([]interface{}{fmt.Sprintf("[allocator %s]:", alloc.ourName)}, args...)...)
}

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

func intip4(key uint32) (r net.IP) {
	r = make([]byte, net.IPv4len)
	for i := 3; i >= 0; i-- {
		r[i] = byte(key)
		key >>= 8
	}
	return
}

// IPv4 Address Arithmetic - convert to 32-bit unsigned integer, add, and convert back
func add(addr net.IP, i uint32) net.IP {
	sum := Ip4int(addr) + i
	return intip4(sum)
}

func subtract(a, b net.IP) int64 {
	return int64(Ip4int(a)) - int64(Ip4int(b))
}
