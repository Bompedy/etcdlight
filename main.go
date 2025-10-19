package main

import (
	"etcd-light/client"
	"etcd-light/server"
	"log"
	"os"
)

// main is the entry point of the networking_benchmark program.
//
// Usage:
//
//	networking_benchmark server [options]
//	networking_benchmark client [options]
//
// Arguments:
//   - "server": starts the server component of the benchmark.
//   - "client": starts the client component of the benchmark.
//   - [options]: additional arguments passed to the chosen component.
//
// If the first argument is not "server" or "client", the program exits
// with a usage error.
func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s {server|client} [options]", os.Args[0])
	}

	if os.Args[1] == "client" {
		client.StartClient(os.Args[2:])
	} else if os.Args[1] == "server" {
		server.StartServer(os.Args[2:])
	} else {
		log.Fatalf("usage: %s {server|client} [options]", os.Args[0])
	}
}
