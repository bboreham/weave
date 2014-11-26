package main

import (
	"flag"
	"github.com/bboreham/weave/logging"
	weavenet "github.com/bboreham/weave/net"
	"github.com/bboreham/weave/sortinghat"
	"github.com/zettio/weave/dockerutils"
	"net"
	//"os"
)

func main() {
	var (
		ifaceName string
		apiPath   string
		startAddr string
		poolSize  int
		d2hcpPort int
		httpPort  int
		wait      int
		watch     bool
		debug     bool
	)

	flag.StringVar(&ifaceName, "iface", "", "name of interface to use for multicast")
	flag.StringVar(&apiPath, "api", "unix:///var/run/docker.sock", "Path to Docker API socket")
	flag.IntVar(&wait, "wait", 0, "number of seconds to wait for interface to be created and come up (defaults to 0)")
	flag.StringVar(&startAddr, "start", "10.0.1.0", "start of address range to allocate within")
	flag.IntVar(&poolSize, "size", 128, "size of address range to allocate within")
	flag.IntVar(&d2hcpPort, "d2hcport", 6786, "port to listen to d2hcp peers")
	flag.IntVar(&httpPort, "httpport", 6787, "port to listen to HTTP requests (defaults to 6786)")
	flag.BoolVar(&watch, "watch", true, "watch the docker socket for container events")
	flag.BoolVar(&debug, "debug", false, "output debugging info to stderr")
	flag.Parse()

	space := sortinghat.NewSpace(net.ParseIP(startAddr), uint32(poolSize))

	if watch {
		err := dockerutils.StartUpdater(apiPath, space)
		if err != nil {
			logging.Error.Fatal("Unable to start watcher", err)
		}
	}

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

	server, _ := sortinghat.NewServer()

	err := server.Start(space, iface, d2hcpPort, httpPort, wait)
	if err != nil {
		logging.Error.Fatal("Failed to start server", err)
	}
}
