package address

import (
	"fmt"
	"net"

	"github.com/weaveworks/weave/common"
)

// Using 32-bit integer to represent IPv4 address
type Address uint32
type Offset uint32

type Range struct {
	Start, End Address // [Start, End); Start <= End
}

func MakeRange(start Address, size Offset) Range { return Range{Start: start, End: Add(start, size)} }
func (r Range) Size() Offset                     { return Subtract(r.End, r.Start) }
func (r Range) String() string                   { return fmt.Sprintf("[%s-%s)", r.Start, r.End) }
func (r1 Range) Overlaps(r2 Range) bool          { return !(r1.Start >= r2.End || r1.End <= r2.Start) }
func (r Range) Contains(addr Address) bool       { return addr >= r.Start && addr < r.End }

type CIDR struct {
	Start     Address
	PrefixLen int
}

func ParseIP(s string) (Address, error) {
	if ip := net.ParseIP(s); ip != nil {
		return FromIP4(ip), nil
	}
	return 0, &net.ParseError{Type: "IP Address", Text: s}
}

func ParseCIDR(s string) (Address, CIDR, error) {
	if ip, ipnet, err := net.ParseCIDR(s); err != nil {
		return 0, CIDR{}, err
	} else if ipnet.IP.To4() == nil {
		return 0, CIDR{}, &net.ParseError{Type: "Non-IPv4 address not supported", Text: s}
	} else {
		prefixLen, _ := ipnet.Mask.Size()
		return FromIP4(ip), CIDR{Start: FromIP4(ipnet.IP), PrefixLen: prefixLen}, nil
	}
}

func (cidr CIDR) Size() Offset { return 1 << uint(32-cidr.PrefixLen) }

// End is exclusive, same as in Range
func (cidr CIDR) End() Address { return Add(cidr.Start, cidr.Size()) }
func (cidr CIDR) Range() Range { return Range{Start: cidr.Start, End: Add(cidr.Start, cidr.Size())} }
func (cidr CIDR) HostRange() Range {
	// Respect RFC1122 exclusions of first and last addresses
	return Range{Start: cidr.Start + 1, End: cidr.End() - 1}
}

func (cidr CIDR) String() string {
	return fmt.Sprintf("%s/%d", cidr.Start.String(), cidr.PrefixLen)
}

// FromIP4 converts an ipv4 address to our integer address type
func FromIP4(ip4 net.IP) (r Address) {
	for _, b := range ip4.To4() {
		r <<= 8
		r |= Address(b)
	}
	return
}

// IP4 converts our integer address type to an ipv4 address
func (addr Address) IP4() (r net.IP) {
	r = make([]byte, net.IPv4len)
	for i := 3; i >= 0; i-- {
		r[i] = byte(addr)
		addr >>= 8
	}
	return
}

func (addr Address) String() string {
	return addr.IP4().String()
}

func Add(addr Address, i Offset) Address {
	return addr + Address(i)
}

func Subtract(a, b Address) Offset {
	common.Assert(a >= b)
	return Offset(a - b)
}

func Min(a, b Offset) Offset {
	if a > b {
		return b
	}
	return a
}
