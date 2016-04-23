package main

import (
	zmq "github.com/pebbe/zmq4"
	"log"
	"time"
	"os"
)

type MessageHandler func(msg Message) Message

var (
	context		*zmq.Context
	handlers        map[string] MessageHandler = make(map[string] MessageHandler)
	clients		map[string] *client = make(map[string] *client)
	pub		chan []byte = make(chan []byte)
	haltpub		chan bool = make(chan bool)
	subConnect	chan string = make(chan string)
	sub 		chan []byte = make(chan []byte)
)

func Open()  {
	debug("opening context")
	var err error
	context, err = zmq.NewContext()
	if err != nil {
		log.Fatal(err)
	}
}

func Send(target string, msg Message) Message {
	debug("sending message", target, msg)
	if _, ok := clients[target]; !ok {
		clients[target] = newClient(target)
	}

	res := clients[target].send(msg.serialize())
	if res != nil {
		return ParseMessage(res)
	} else {
		return EmptyMessage
	}
}

func Publish(msg Message) {
	pub <- msg.serialize()
}

func StartPublisher(target string) {
	go func() {
		publisher, err := context.NewSocket(zmq.PUB)
		if err != nil {
			log.Fatal(err)
		}
		publisher.Bind(target)

		for {
			select {
			case msg := <- pub:
				publisher.SendBytes(msg, 0)
			case <- haltpub:
				publisher.Close()
				return
			}
		}
	}()
}

func ClosePublisher()  {
	haltpub <- true
}

func ConnectSubscriber(url string) {
	subConnect <- url
}

func Subscribe(topic string)  {
	go func() {
		subscriber, err := context.NewSocket(zmq.SUB)
		if err != nil {
			log.Fatal(err)
		}
		subscriber.SetSubscribe(topic)

		go func() {
			for {
				select {
				case url := <- subConnect:
					subscriber.Connect(url)
				case <- haltpub:
					subscriber.Close()
					return
				}
			}
		}()

		for {
			msg, err := subscriber.RecvBytes(0)
			if err != nil {
				log.Fatal(err)
			}
			sub <- msg
		}
	}()

	monitorSubscriptions()
}

func monitorSubscriptions() {
	go func() {
		for {
			select {
			case msg := <- sub:
				m := ParseMessage(msg)
				debug("[sub] message", msg)
				onMessage(m)
			case <- haltpub:
				return
			}
		}
	}()
}

func onMessage(msg Message) []byte {
	if handler, ok := handlers[msg.action]; ok {
		return handler(msg).serialize()
	} else {
		log.Printf("[onMessage] No handler found %s", msg.action)
		return EmptyMessage.serialize()
	}
}

func Handle(action string, handler MessageHandler)  {
	handlers[action] = handler
}

func Serve(target string)  {
	s := NewServer(target, func(msg []byte) []byte {
		debug("[server] message", target, msg)
		m := ParseMessage(msg)
		return onMessage(m)
	})
	go s.serve()
}

func main() {
	Open()

	if os.Args[1] == "server" {
		Serve("tcp://*:3000")
		Subscribe("")
		ConnectSubscriber("tcp://localhost:3001")
	} else {
		//Send("tcp://localhost:3000", NewMessage("hello"))
		StartPublisher("tcp://*:3001")

		for {
			Publish(NewMessage("hello"))
			time.Sleep(1 * time.Second)
		}
	}

	Handle("hello", func(msg Message) Message {
		return EmptyMessage
	})

	time.Sleep(1 * time.Second)


	time.Sleep(20 * time.Second)
}
