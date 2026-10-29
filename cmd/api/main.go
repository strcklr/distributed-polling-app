// Command api runs the polling HTTP server.
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/strcklr/distributed-polling-app/internal/api"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, api.NewHandler()); err != nil {
		log.Fatal(err)
	}
}
