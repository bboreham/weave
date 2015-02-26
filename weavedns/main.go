package main

import (
	"flag"
	"fmt"
	"github.com/miekg/dns"
	. "github.com/zettio/weave/common"
	weavedns "github.com/zettio/weave/nameserver"
	weavenet "github.com/zettio/weave/net"
	"io"
	"net"
	"os"
)

var version = "(unreleased version)"

func main() {
	var (
		justVersion bool
		ifaceName   string
		apiPath     string
		fallback    string
		dnsPort     int
		httpPort    int
		wait        int
		watch       bool
		debug       bool
		err         error
	)

	flag.BoolVar(&justVersion, "version", false, "print version and exit")
	flag.StringVar(&ifaceName, "iface", "", "name of interface to use for multicast")
	flag.StringVar(&apiPath, "api", "unix:///var/run/docker.sock", "Path to Docker API socket")
	flag.IntVar(&wait, "wait", 0, "number of seconds to wait for interface to be created and come up")
	flag.IntVar(&dnsPort, "dnsport", 53, "port to listen to DNS requests")
	flag.IntVar(&httpPort, "httpport", 6785, "port to listen to HTTP requests")
	flag.StringVar(&fallback, "fallback", "", "Fallback server and port (ie, '8.8.8.8:53')")
	flag.BoolVar(&watch, "watch", true, "watch the docker socket for container events")
	flag.BoolVar(&debug, "debug", false, "output debugging info to stderr")
	flag.Parse()

	if justVersion {
		io.WriteString(os.Stdout, fmt.Sprintf("weave DNS %s\n", version))
		os.Exit(0)
	}

	InitDefaultLogging(debug)

	var zone = new(weavedns.ZoneDb)

	if watch {
		err := StartUpdater(apiPath, zone)
		if err != nil {
			Error.Fatal("Unable to start watcher", err)
		}
	}

	var iface *net.Interface
	if ifaceName != "" {
		var err error
		Info.Println("Waiting for interface", ifaceName, "to come up")
		iface, err = weavenet.EnsureInterface(ifaceName, wait)
		if err != nil {
			Error.Fatal(err)
		} else {
			Info.Println("Interface", ifaceName, "is up")
		}
	}

	var config *dns.ClientConfig
	if len(fallback) > 0 {
		fallbackHost, fallbackPort, err := net.SplitHostPort(fallback)
		if err != nil {
			Error.Fatal("Fould not parse fallback host and port", err)
		}
		config = &dns.ClientConfig{Servers: []string{fallbackHost}, Port: fallbackPort}
		Debug.Printf("DNS fallback at %s:%s", fallbackHost, fallbackPort)
	} else {
		config, err = dns.ClientConfigFromFile("/etc/resolv.conf")
		if err != nil {
			Error.Fatal("Failed obtain DNS client configuration", err)
		}
	}

	go weavedns.ListenHttp(weavedns.LOCAL_DOMAIN, zone, httpPort)
	srv := weavedns.NewDNSServer(config, zone, iface, dnsPort)
	err = srv.Start()
	if err != nil {
		Error.Fatal("Failed to start server", err)
	}
}
