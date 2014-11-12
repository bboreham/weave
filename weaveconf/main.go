package main

import (
	"flag"
	"github.com/bboreham/weave/logging"
	weavenet "github.com/bboreham/weave/net"
	"github.com/bboreham/weave/sortinghat"
	"net"
	//"os"
)

func main() {
	var (
		ifaceName string
		d2hcpPort int
		httpPort  int
		wait      int
		debug     bool
	)

	flag.StringVar(&ifaceName, "iface", "", "name of interface to use for multicast")
	flag.IntVar(&wait, "wait", 0, "number of seconds to wait for interface to be created and come up (defaults to 0)")
	flag.IntVar(&httpPort, "httpport", 6786, "port to listen to HTTP requests (defaults to 6786)")
	flag.BoolVar(&debug, "debug", false, "output debugging info to stderr")
	flag.Parse()

	var iface *net.Interface = nil
	if ifaceName != "" {
		var err error
		logging.Info.Println("Waiting for interface", ifaceName, "to come up")
		iface, err = weavenet.EnsureInterface(ifaceName, wait)
		if err != nil {
			logging.Error.Fatal(err)
		} else {
			logging.Info.Println("Interface", ifaceName, "is up")
		}
	}

	err := sortinghat.StartServer(iface, d2hcpPort, httpPort, wait)
	if err != nil {
		logging.Error.Fatal("Failed to start server", err)
	}
}
