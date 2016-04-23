package main

import (
	"log"
	zmq "github.com/pebbe/zmq4"
)

type Handler func(msg []byte) []byte

func NewServer(target string, handler Handler) *Server {
	log.Println("initializing server", target)
	return &Server{target: target, workers: 10, handler: handler}
}

type Server struct {
	target 		string
	workers 	int
	handler		Handler
}

func (server *Server) serve() {
	frontend, err := context.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Fatal(err)
	}
	defer frontend.Close()
	frontend.Bind(server.target)

	//  Backend socket talks to workers over inproc
	backend, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal(err)
	}
	defer backend.Close()
	backend.Bind("inproc://backend")

	//  Launch pool of worker threads, precise number is not critical
	for i := 0; i < server.workers; i++ {
		go server.worker()
	}

	//  Connect backend to frontend via a proxy
	err = zmq.Proxy(frontend, backend, nil)
	log.Fatalln("Proxy interrupted:", err)
}

func (server *Server) worker() {
	debug("starting worker")
	worker, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal(err)
	}

	defer worker.Close()
	worker.Connect("inproc://backend")

	for {

		msg, err := worker.RecvMessageBytes(0)
		if err != nil {
			log.Fatal(err)
		}
		// the message is returned as a [][]byte. The first two slots are the address of the client socket,
		// the third slot is the message.
		response := server.handler(msg[2])
		worker.SendMessage(msg[:2], response)
	}
}
