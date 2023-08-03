package main

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

const (
	PORT = 9998
)

func registerHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte{})
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/register", registerHandler).Methods("POST")

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(PORT), r))
}
