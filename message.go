package main

import (
	"bytes"
	"encoding/binary"
)

// This message is a very lightweight binary serialization protocol that stores messages in the following format:
// [ action len ][ action name ][  key len  ][  val len  ][  key  ][  val  ] repeat key val
//   (4 bytes)         ?         (4 bytes)    (4 bytes)      ?        ?
//
// It allows for very fast serialization and deserialization of messages without too much overhead. Messages are simply
// just byte arrays. A basic map is used to hold the message body where key's are strings only. Message values are either
// integers, strings, bytes or byte arrays. They are encoded into a generic interface{}.
//
// The message also specifies an 'action'. This is a key used to route to different functions during rpc calls and in
// pub/sub calls this is the 'topic' of the message.
//
// For encoding and decoding values, a byte is added at the start of the value which marks its type:
// 1: int
// 2: string
// 3: byte
// 4: byte array
// 5: int64

var EmptyMessage Message = Message{"empty", make(map[string] interface{})}

func NewMessage(action string) Message {
	return Message{action, make(map[string] interface{})}
}

func NewKvMessage(action string, key string, value interface{}) Message {
	msg := NewMessage(action)
	msg.params[key] = value
	return msg
}

func ParseMessage(msg []byte) Message {
	l := ti(msg[0:4])
	action := string(msg[4:4 + l])
	params := make(map[string] interface{})

	for i := 4 + l; i < len(msg); {
		kl := ti(msg[i:i + 4])
		vl := ti(msg[i + 4:i + 8])

		key := string(msg[i + 8:i + 8 + kl])
		val := msg[i + 8 + kl:i + 8 + kl + vl]

		switch val[0] {
		case byte(1):
			params[key] = ti(val[1:])
		case byte(2):
			params[key] = string(val[1:])
		case byte(3):
			params[key] = val[1]
		case byte(4):
			params[key] = val[1:]
		case byte(5):
			var value uint64
			buf := bytes.NewReader(val[1:])
			binary.Read(buf, binary.LittleEndian, &value)
			params[key] = int64(value)
		}


		i = i + 8 + kl + vl

	}

	return Message{action, params}
}

type Message struct {
	action 	string
	params 	map[string] interface{}
}

func (msg Message) serialize() []byte {
	bact := []byte(msg.action)

	buff := new(bytes.Buffer)
	buff.Write(fi(len(bact)))
	buff.Write(bact)

	for key, val := range msg.params {
		bkey := []byte(key)
		var bval []byte

		switch asserted := val.(type) {
		case int:
			bval = append([]byte{1}, fi(asserted)...)
		case string:
			bval = append([]byte{2}, []byte(asserted)...)
		case byte:
			bval = []byte{byte(3), asserted}
		case []byte:
			bval = append([]byte{4}, asserted...)
		case int64:
			bs := make([]byte, 8)
			binary.LittleEndian.PutUint64(bs, uint64(asserted))
			bval = append([]byte{5}, bs...)
		}

		buff.Write(fi(len(bkey)))
		buff.Write(fi(len(bval)))
		buff.Write(bkey)
		buff.Write(bval)
	}

	return buff.Bytes()
}


func fi(arg int) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(arg))
	return bs
}

func ti(data []byte) int {
	var value uint32
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.LittleEndian, &value)
	return int(value)
}
