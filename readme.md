# Messaging Service for Golang

A simple messaging service built on top of zeromq. Implements pub/sub and request/reply with a
very simple an intuitive api.


Simple RPC Example


``` go
// client.go
package main

import "github.com/coldog/messaging"

func main() {
    messaging.Send('tcp://localhost:3000', messaging.NewMessage("hello"))
}

```

``` go
// server.go
package main

import "github.com/coldog/messaging"

func main() {
    messaging.Serve('tcp://localhost:3000')
    messaging.Handle("hello", func(msg messaging.Message) messaging.Message {
        return messaging.NewMessage("received")
    })
}

```