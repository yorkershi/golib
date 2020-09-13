package golib

import (
	"testing"
	"time"
)

func TestCache_SetInt(t *testing.T) {
	cache := NewCache()
	ckey := "key123"
	cache.SetInt(ckey, 123, time.Second*20)
	if cache.GetInt(ckey) != 123 {
		t.Fatal("set int error")
	}
}

func TestCache_SetMap(t *testing.T) {
	cache := NewCache()
	ckey := "map123"
	cache.SetMap(ckey, map[string]string{
		"a": "b",
	}, time.Second*20)

	result := cache.GetMap(ckey)

	if len(result) == 0 {
		t.Fatal("set map error")
	}
}
