package raft

import "log"

const Debug = false

func DPrintf(
	format string,
	a ...interface{},
) (
	n int,
	err error,
) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Min(
	a int,
	b int,
) int {
	if a < b {
		return a
	}
	return b
}

func Max(
	a int,
	b int,
) int {
	if a > b {
		return a
	}
	return b
}
