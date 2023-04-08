// socket-server project main.go
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
)

const (
	SERVER_HOST = "localhost"
	SERVER_PORT = "9988"
	SERVER_TYPE = "tcp"
)

func main() {
	fmt.Println("Starting Broker...")
	server, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	defer server.Close()

	fmt.Println("Listening on " + SERVER_HOST + ":" + SERVER_PORT)
	for {
		connection, err := server.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Received Request")
		go processClient(connection)
	}
}

func createTopic(topicName string) {
	topicDir := filepath.Join("Topics", topicName)
	if err := os.Mkdir(topicDir, os.ModePerm); err != nil {
		log.Fatal(err)
		fmt.Println(err)
	}
}

func processClient(connection net.Conn) {
	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)

	if err != nil {
		fmt.Println("Error reading:", err.Error())
	}

	fmt.Println("Received: ", string(buffer[:mLen]))

	operation := string(buffer[:mLen])

	if operation == "create-topic" {
		createTopic("Henlo")
		connection.Write([]byte("T"))
	} else {
		connection.Write([]byte("Operation Not Found"))
	}
	connection.Close()
}
