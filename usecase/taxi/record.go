package main

import "strings"

type Record struct {
	Type rune
	Val  string
}

func (r Record) Clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}
