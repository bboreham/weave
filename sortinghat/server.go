package sortinghat

import (
	"errors"
	"fmt"
	. "github.com/zettio/weave/logging"
	"net"
	"sync"
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

// A Server defines parameters for running a D2HCP server.
type Server struct {
	// Address to listen on.
	Addr string
	// can be "udp", "udp4", etc
	Net string
	// UDP "Listener" to use.
	PacketConn net.PacketConn
	// Handler to invoke
	Handler Handler

	// For graceful shutdown.
	stopUDP chan bool
	wgUDP   sync.WaitGroup

	// make start/shutdown not racy
	lock    sync.Mutex
	started bool
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

func StartServer(iface *net.Interface, d2hcpPort int, httpPort int, wait int) error {
	go ListenHttp(httpPort)

	address := fmt.Sprintf(":%d", d2hcpPort)
	err := ListenAndServe(address, "udp")
	checkFatal(err)

	Info.Printf("Listening for D2HCP on %s", address)
	return nil
}

// ListenAndServe Starts a server on addresss and network specified.
func ListenAndServe(addr string, network string) error {
	server := &Server{Addr: addr, Net: network, Handler: nil} //handler}
	return server.ListenAndServe()
}

func (srv *Server) ListenAndServe() error {
	srv.lock.Lock()
	if srv.started {
		return &DError{err: "server already started"}
	}
	srv.stopUDP = make(chan bool)
	srv.started = true
	srv.lock.Unlock()
	addr := srv.Addr
	a, e := net.ResolveUDPAddr(srv.Net, addr)
	if e != nil {
		return e
	}
	l, e := net.ListenUDP(srv.Net, a)
	if e != nil {
		return e
	}
	return srv.serveUDP(l)

	return &DError{err: "bad network"}
}

func (srv *Server) ActivateAndServe() error {
	srv.lock.Lock()
	if srv.started {
		return &DError{err: "server already started"}
	}
	srv.stopUDP = make(chan bool)
	srv.started = true
	srv.lock.Unlock()
	if srv.PacketConn != nil {
		if t, ok := srv.PacketConn.(*net.UDPConn); ok {
			return srv.serveUDP(t)
		}
	}
	return &DError{err: "bad listeners"}
}

// serveUDP starts a UDP listener for the server.
// Each request is handled in a seperate goroutine.
func (srv *Server) serveUDP(l *net.UDPConn) error {
	defer l.Close()

	rtimeout := d2hcpTimeout
	// deadline is not used here
	for {
		m, a, e := srv.readUDP(l, rtimeout)
		select {
		case <-srv.stopUDP:
			return nil
		default:
		}
		if e != nil {
			continue
		}
		srv.wgUDP.Add(1)
		go srv.serve(a, m, l)
	}
	panic("d2hcp: not reached")
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
