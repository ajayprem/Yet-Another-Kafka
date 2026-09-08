package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	zookeeper "yet-another-kafka/internals/zookeeper"

	"github.com/gorilla/mux"
)

const (
	PORT = 9998
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	registry := zookeeper.NewRegistry()
	go registry.LeaderHealthCheck(ctx)

	handlers := zookeeper.NewHandlers(registry)

	r := mux.NewRouter()
	r.HandleFunc("/brokers", handlers.RegisterBroker).Methods("POST")
	r.HandleFunc("/brokers", handlers.GetRandomBroker).Methods("GET")
	r.HandleFunc("/leader", handlers.GetLeader).Methods("GET")

	srv := &http.Server{Addr: fmt.Sprintf(":%d", PORT), Handler: r}

	go func() {
		log.Println("zookeeper: starting on port:", PORT)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("zookeeper: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}
