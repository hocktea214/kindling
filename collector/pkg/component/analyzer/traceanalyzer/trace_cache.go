package traceanalyzer

import (
	"sync"
	"time"
)

var monitedTracePids sync.Map

func IsTracePid(pid uint32) bool {
	if _, exist := monitedTracePids.Load(pid); exist {
		return true
	}
	return false
}

func AddTracePid(pid uint32, time uint64) {
	monitedTracePids.Store(pid, time)
}

func ExpireTracePids() {
	now := time.Now().UnixNano()
	monitedTracePids.Range(func(k, v interface{}) bool {
		latestTime := v.(uint64)
		// 5min
		if (now-int64(latestTime)) / 1000000000 >= 300 {
			monitedTracePids.Delete(k)
		}
		return true
	})
}

type TraceKey struct {
	Pid uint32
	TraceId string
}

type TraceInfo struct {
	Tid uint32
	Protocol string
	EndPoint string
	StartTime uint64
	EndTime uint64
}

func (trace *TraceInfo) getTimeoutTs() uint64 {
	if trace.EndTime > 0 {
		return trace.EndTime
	}
	return trace.StartTime
}
