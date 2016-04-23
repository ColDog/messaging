package main

import (
	zmq "github.com/pebbe/zmq4"
	"time"
	"log"
	"sync"
)

func newClient(target string) *client {
	client := &client{
		target: target,
		lock: sync.RWMutex{},
		connections: make([]*conn, 0),
	}
	go client.manager()
	return client
}

type client struct {
	target 		string
	lock 		sync.RWMutex
	connections 	[]*conn
}

type conn struct {
	socket 		*zmq.Socket
	available	bool
	closed 		bool
}

func (conn *conn) reserve() {
	conn.available = false
}

func (conn *conn) release() {
	conn.available = true
}

func (conn *conn) close() {
	conn.closed = true
	conn.socket.Close()
}

func (client *client) trim(count int) {
	client.lock.RLock()
	for _, conn := range client.connections {
		if count <= 0 { break }

		if conn.available {
			conn.close()
			count--
		}
	}
	client.lock.RUnlock()

	for i, conn := range client.connections {
		if conn.closed {
			client.lock.Lock()
			client.connections = append(client.connections[:i], client.connections[i+1:]...)
			client.lock.Unlock()
		}
	}
}

func (client *client) stats() (int, int, int) {
	client.lock.RLock()
	available := 0
	closed := 0
	unavailable := 0
	for _, conn := range client.connections {
		if conn.available { available++ }
		if !conn.available { unavailable++ }
		if conn.closed { closed++ }
	}
	client.lock.RUnlock()
	return available, unavailable, closed
}

func (client *client) manager() {
	for {
		available, unavailable, closed := client.stats()
		log.Printf("connections:  available=%d, unavailable=%d, closed=%d", available, unavailable, closed)

		if available >= 10 {
			client.trim(5)
		}

		time.Sleep(15 * time.Second)
	}
}

func (client *client) conn() *conn {
	client.lock.RLock()
	for _, conn := range client.connections {
		if conn.available {
			conn.reserve()
			return conn
		}
	}
	client.lock.RUnlock()

	socket, err := context.NewSocket(zmq.REQ)
	if err != nil {
		log.Fatal(err)
	}
	socket.Connect(client.target)

	c := &conn{socket, false, false}

	client.lock.Lock()
	client.connections = append(client.connections, c)
	client.lock.Unlock()
	return c
}

func (client *client) sendPolling(msg []byte, retries int, interval int) []byte {
	debug("client sending message", msg, retries, interval)
	c := client.conn()
	defer c.release()

	poller := zmq.NewPoller()
	poller.Add(c.socket, zmq.POLLIN)

	for retries > 0 {
		c.socket.SendBytes(msg, 0)
		debug("client sending message", msg, retries, interval)

		sockets, err := poller.Poll(time.Duration(time.Duration(interval) * time.Millisecond))
		if err != nil {
			log.Println(err)
			return nil
		}

		if len(sockets) > 0 {
			reply, err := c.socket.RecvBytes(0)
			if err != nil {
				log.Println(err)
				return nil
			}

			if reply != nil {
				return reply
			}
		} else {
			retries--
			c.socket.SendBytes(msg, 0)
		}
	}

	log.Println("E: server seems to be offline, abandoning")
	return nil
}

func (client *client) send(msg []byte) []byte {
	return client.sendPolling(msg, 30, 5)
}

