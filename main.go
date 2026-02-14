package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/cornelmarck/durable-execution/api"
)

func main() {
	mux := http.NewServeMux()

	api.RegisterDocsRoutes(mux)

	addr := ":8080"
	fmt.Printf("Listening on %s\n", addr)
	fmt.Printf("Swagger UI: http://localhost%s/docs\n", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
