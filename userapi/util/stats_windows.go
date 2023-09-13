

//go:build !wasm
// +build !wasm

package util

import (
	"runtime"
)

func getMemoryStats(p *phoneHomeStats) error {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	p.stats["memory_rss"] = memStats.Alloc
	return nil
}
