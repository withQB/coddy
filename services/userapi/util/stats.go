package util

import (
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

func getMemoryStats(p *phoneHomeStats) error {
	oldUsage := p.prevData
	newUsage := syscall.Rusage{}
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &newUsage); err != nil {
		logrus.WithError(err).Error("unable to get usage")
		return err
	}
	newData := timestampToRUUsage{timestamp: time.Now().Unix(), usage: newUsage}
	p.prevData = newData

	usedCPUTime := (newUsage.Utime.Sec + newUsage.Stime.Sec) - (oldUsage.usage.Utime.Sec + oldUsage.usage.Stime.Sec)

	if usedCPUTime == 0 || newData.timestamp == oldUsage.timestamp {
		p.stats["cpu_average"] = 0
	} else {
		// conversion to int64 required for GOARCH=386
		p.stats["cpu_average"] = int64(usedCPUTime) / (newData.timestamp - oldUsage.timestamp) * 100
	}
	p.stats["memory_rss"] = newUsage.Maxrss
	return nil
}
