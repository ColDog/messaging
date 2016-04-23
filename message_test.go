package main

import (
	"testing"
)

func TestIntegers(t *testing.T) {
	i := ti(fi(1))
	if i != 1 {
		println(ti(fi(1)))
		t.Fatal("Int parse failed")
	}
}

func TestSerializer(t *testing.T) {
	m := Message{"hello", map[string] interface{} {
		"name": "COlin",
		"name3": "COlin3",
	}}

	m2 := ParseMessage(m.serialize())

	if m2.action != m.action {
		t.Fail()
	}

	if m2.params["name"] != m.params["name"] {
		t.Fail()
	}

	if m2.params["name3"] != m.params["name3"] {
		t.Fail()
	}
}
