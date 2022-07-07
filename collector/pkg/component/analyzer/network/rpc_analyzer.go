package network

import (
	"time"

	"github.com/Kindling-project/kindling/collector/pkg/component"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer"
	"github.com/Kindling-project/kindling/collector/pkg/component/consumer"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constnames"
)

const (
	Rpc analyzer.Type = "rpcanalyzer"
)

type RpcConfig struct {
}

type RpcAnalyzer struct {
	cfg           *RpcConfig
	nextConsumers []consumer.Consumer
	telemetry     *component.TelemetryTools
}

func NewRpcAnalyzer(cfg interface{}, telemetry *component.TelemetryTools, consumers []consumer.Consumer) analyzer.Analyzer {
	config, _ := cfg.(*RpcConfig)
	ra := &RpcAnalyzer{
		cfg:           config,
		nextConsumers: consumers,
		telemetry:     telemetry,
	}
	return ra
}

func (ra *RpcAnalyzer) ConsumableEvents() []string {
	return []string{
		constnames.ReadEvent,
		constnames.WriteEvent,
		constnames.ReadvEvent,
		constnames.WritevEvent,
		constnames.SendToEvent,
		constnames.RecvFromEvent,
		constnames.SendMsgEvent,
		constnames.RecvMsgEvent,
	}
}

func (ra *RpcAnalyzer) Start() error {
	go ra.consumerNotMatchTrace()
	return nil
}

func (na *RpcAnalyzer) Shutdown() error {
	return nil
}

func (na *RpcAnalyzer) Type() analyzer.Type {
	return Network
}

func (ra *RpcAnalyzer) ConsumeEvent(evt *model.KindlingEvent) error {
	rpcId := evt.GetIntUserAttribute(ATTRIBUTE_KEY_RPC_ID)
	GetRpcCache().cacheRemoteResponse(rpcId, evt)
	return nil
}

func (ra *RpcAnalyzer) consumerNotMatchTrace() {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			GetRpcCache().clientDatas.Range(func(k, v interface{}) bool {
				clientDatas := v.(*RpcClientDatas)
				mps := clientDatas.match()
				if len(mps) > 0 {
					GetRpcCache().na.parseRpcAndDistributeTraceMetric(mps)
				}

				mps = clientDatas.clearExpireDatas(uint64(time.Now().UnixNano()-1000000000), uint64(time.Now().UnixNano()-15000000000))
				if len(mps) > 0 {
					GetRpcCache().na.parseRpcAndDistributeTraceMetric(mps)
				}
				return true
			})
		}
	}
}
