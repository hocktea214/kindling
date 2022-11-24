package traceanalyzer

import (
    "strconv"
    "sync"
	"time"

    "github.com/Kindling-project/kindling/collector/pkg/component"
    "github.com/Kindling-project/kindling/collector/pkg/component/analyzer"
    "github.com/Kindling-project/kindling/collector/pkg/component/consumer"
    "github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
    "github.com/Kindling-project/kindling/collector/pkg/model/constnames"
	"github.com/Kindling-project/kindling/collector/pkg/model/constvalues"
    "go.uber.org/zap/zapcore"
)

const (
    Trace analyzer.Type = "traceanalyzer"
)

type TraceAnalyzer struct {
    cfg           *Config
    telemetry     *component.TelemetryTools
    traceMonitor  sync.Map
    nextConsumers []consumer.Consumer
}

func (ta *TraceAnalyzer) Type() analyzer.Type {
    return Trace
}

func (ta *TraceAnalyzer) ConsumableEvents() []string {
    return []string{constnames.TransactionIdEvent}
}

func NewTraceAnalyzer(cfg interface{}, telemetry *component.TelemetryTools, consumers []consumer.Consumer) analyzer.Analyzer {
    config, _ := cfg.(*Config)
    ta := &TraceAnalyzer{
        cfg:           config,
        telemetry:     telemetry,
        nextConsumers: consumers,
    }
    return ta
}

func (ta *TraceAnalyzer) Start() error {
    go ta.consumerUnFinishTrace()
    return nil
}

func (ta *TraceAnalyzer) Shutdown() error {
    return nil
}

func (ta *TraceAnalyzer) ConsumeEvent(event *model.KindlingEvent) error {
    traceKey := TraceKey {
        Pid: event.GetPid(),
        TraceId: event.GetStringUserAttribute("trace_id"),
    }

    tid := event.GetTid()
    protocol := event.GetStringUserAttribute("protocol")
    endPoint := event.GetStringUserAttribute("endpoint")
    isEntry, _ := strconv.Atoi(event.GetStringUserAttribute("is_enter"))

    if endPoint != "" {
        AddTracePid(traceKey.Pid, event.Timestamp)
    }
    if traceValue, exist := ta.traceMonitor.Load(traceKey); exist {
        trace := traceValue.(*TraceInfo)
        trace.EndTime = event.Timestamp
        if protocol != "" {
            trace.Protocol = protocol
        }
        if endPoint != "" {
            trace.EndPoint = endPoint
        }
        
        if tid == trace.Tid && isEntry == 0 {
            return ta.distributeTraceMetric(traceKey, trace, false)
        }
    } else if isEntry == 1 {
        ta.traceMonitor.Store(traceKey, &TraceInfo {
            StartTime: event.Timestamp,
            Tid: tid,
            Protocol: protocol,
            EndPoint: endPoint,
        })
    }
    return nil
}

func (ta *TraceAnalyzer) getRecord(traceKey TraceKey, trace *TraceInfo, timeout bool) *model.DataGroup {
    slow := false
    if timeout {
        slow = true
    } else if ta.cfg.IsSlow(trace.EndTime - trace.StartTime) {
        slow = true
    }
    labels := model.NewAttributeMap()
    labels.AddBoolValue(constlabels.IsError, false)
    labels.AddBoolValue(constlabels.IsSlow, slow)
    labels.AddBoolValue(constlabels.IsServer, true)
    labels.AddIntValue(constlabels.Pid, int64(traceKey.Pid))
    labels.AddStringValue(constlabels.ContentKey, trace.EndPoint)
    labels.AddStringValue(constlabels.SpanTraceId, traceKey.TraceId)
    labels.AddStringValue(constlabels.SpanTraceType, "skywalking")
    labels.AddStringValue(constlabels.Protocol, trace.Protocol)

    metric := model.NewIntMetric(constvalues.RequestTotalTime, int64(trace.EndTime - trace.StartTime))
    return model.NewDataGroup(constnames.TraceMetricGroupName, labels, trace.StartTime, metric)
}

func (ta *TraceAnalyzer) distributeTraceMetric(traceKey TraceKey, trace *TraceInfo, timeout bool) error {
    // Delete TraceInfo.
    ta.traceMonitor.Delete(traceKey)

    if trace.EndPoint == "" {
        return nil
    }
    record := ta.getRecord(traceKey, trace, timeout)
    if ce := ta.telemetry.Logger.Check(zapcore.InfoLevel, ""); ce != nil {
        ta.telemetry.Logger.Info("TraceAnalyzer To NextProcess:\n" + record.String())
    }
    for _, nexConsumer := range ta.nextConsumers {
        nexConsumer.Consume(record)
    }
    return nil
}

func (ta *TraceAnalyzer) consumerUnFinishTrace() {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			ta.traceMonitor.Range(func(k, v interface{}) bool {
				traceInfo := v.(*TraceInfo)
                var timeoutTs = traceInfo.getTimeoutTs()
                if (time.Now().UnixNano()/1000000000-int64(timeoutTs)/1000000000) >= 2 {
                    traceKey := k.(TraceKey)
                    ta.distributeTraceMetric(traceKey, traceInfo, true)
                }
				return true
			})
            ExpireTracePids()
		}
	}
}