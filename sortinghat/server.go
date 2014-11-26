package sortinghat

import (
	"errors"
	. "github.com/zettio/weave/logging"
	"net"
	"time"
)

type DError struct{ err string }

func (e *DError) Error() string {
	if e == nil {
		return "d2hcp: <nil>"
	}
	return "d2hcp: " + e.err
}

var (
	ErrShortRead error = &DError{err: "short read"}
)

type Handler interface {
	ServeD2HCP(r *Msg)
}

const (
	d2hcpTimeout time.Duration = 2 * 1e9
	bufferSize                 = 1024
)

type Server struct {
}

func checkFatal(e error) {
	if e != nil {
		Error.Fatal(e)
	}
}

func checkWarn(e error) {
	if e != nil {
		Warning.Println(e)
	}
}

func NewServer() (*Server, error) {
	return new(Server), nil
}

func (server *Server) Start(space Space, iface *net.Interface, d2hcpPort int, httpPort int, wait int) error {
	ListenHttp(httpPort, space)

	return nil
}

func (srv *Server) readUDP(conn *net.UDPConn, timeout time.Duration) ([]byte, net.Addr, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	m := make([]byte, bufferSize)
	n, a, e := conn.ReadFrom(m)
	if e != nil || n == 0 {
		if e != nil {
			return nil, nil, e
		}
		return nil, nil, errors.New("short read")
	}
	m = m[:n]
	return m, a, nil
}

// Serve a new connection.
func (srv *Server) serve(a net.Addr, m []byte, u *net.UDPConn) {
	req := new(Msg)
	err := req.Unpack(m)
	if err != nil {
		// do something with the input
	}
}
