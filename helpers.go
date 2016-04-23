package main

import "fmt"

const DEBUG_ENABLED = true

func debug(args ...interface{})  {
	if DEBUG_ENABLED {
		fmt.Printf("[debug] %v\n", args)
	}
}
