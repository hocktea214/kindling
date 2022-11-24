package traceanalyzer

import (
	"time"
)

const (
	defaultSlowThreshold = 500
)

type Config struct {
	SlowThreshold int `mapstructure:"slow_threshold"`
}

func (cfg *Config) IsSlow(duration uint64) bool {
	if cfg.SlowThreshold > 0 {
		return uint64(cfg.SlowThreshold) * uint64(time.Millisecond) <= duration
	} else {
		return uint64(defaultSlowThreshold) * uint64(time.Millisecond) <= duration
	}
}
