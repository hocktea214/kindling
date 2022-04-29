package aggregateprocessor

import (
	"github.com/Kindling-project/kindling/collector/component"
	"github.com/Kindling-project/kindling/collector/consumer"
	"github.com/Kindling-project/kindling/collector/consumer/processor"
	"github.com/Kindling-project/kindling/collector/model"
	"github.com/Kindling-project/kindling/collector/model/constlabels"
	"github.com/Kindling-project/kindling/collector/model/constnames"
	"github.com/Kindling-project/kindling/collector/pkg/aggregator"
	"github.com/Kindling-project/kindling/collector/pkg/aggregator/defaultaggregator"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

const Type = "aggregateprocessor"

type AggregateProcessor struct {
	cfg          *Config
	telemetry    *component.TelemetryTools
	nextConsumer consumer.Consumer

	aggregator               aggregator.Aggregator
	netRequestLabelSelectors *aggregator.LabelSelectors
	tcpLabelSelectors        *aggregator.LabelSelectors
	stopCh                   chan struct{}
	ticker                   *time.Ticker
}

func New(config interface{}, telemetry *component.TelemetryTools, nextConsumer consumer.Consumer) processor.Processor {
	cfg := config.(*Config)
	p := &AggregateProcessor{
		cfg:          cfg,
		telemetry:    telemetry,
		nextConsumer: nextConsumer,

		aggregator:               defaultaggregator.NewDefaultAggregator(toAggregatedConfig(cfg.AggregateKindMap)),
		netRequestLabelSelectors: newNetRequestLabelSelectors(),
		tcpLabelSelectors:        newTcpLabelSelectors(),
		stopCh:                   make(chan struct{}),
		ticker:                   time.NewTicker(time.Duration(cfg.TickerInterval) * time.Second),
	}
	go p.runTicker()
	return p
}

func toAggregatedConfig(m map[string][]AggregatedKindConfig) *defaultaggregator.AggregatedConfig {
	ret := &defaultaggregator.AggregatedConfig{KindMap: make(map[string][]defaultaggregator.KindConfig)}
	for k, v := range m {
		kindConfig := make([]defaultaggregator.KindConfig, len(v))
		for i, kind := range v {
			if kind.OutputName == "" {
				kind.OutputName = k
			}
			kindConfig[i] = defaultaggregator.KindConfig{
				OutputName: kind.OutputName,
				Kind:       defaultaggregator.GetAggregatorKind(kind.Kind),
			}
		}
		ret.KindMap[k] = kindConfig
	}
	return ret
}

// TODO: Graceful shutdown
func (p *AggregateProcessor) runTicker() {
	for {
		select {
		case <-p.stopCh:
			return
		case <-p.ticker.C:
			aggResults := p.aggregator.Dump()
			for _, agg := range aggResults {
				err := p.nextConsumer.Consume(agg)
				if err != nil {
					p.telemetry.Logger.Warn("Error happened when consuming aggregated recordersMap",
						zap.Error(err))
				}
			}
		}
	}
}

func (p *AggregateProcessor) Consume(gaugeGroup *model.GaugeGroup) error {
	switch gaugeGroup.Name {
	case constnames.NetRequestGaugeGroupName:
		var abnormalDataErr error
		// The abnormal recordersMap will be treated as trace in later processing.
		// Must trace be merged into metrics in this place? Yes, because we have to generate histogram metrics,
		// trace recordersMap should not be recorded again, otherwise the percentiles will be much higher.
		if p.isSampled(gaugeGroup) {
			gaugeGroup.Name = constnames.SingleNetRequestGaugeGroup
			abnormalDataErr = p.nextConsumer.Consume(gaugeGroup)
		}
		gaugeGroup.Name = constnames.AggregatedNetRequestGaugeGroup
		p.aggregator.Aggregate(gaugeGroup, p.netRequestLabelSelectors)
		return abnormalDataErr
	case constnames.TcpGaugeGroupName:
		p.aggregator.Aggregate(gaugeGroup, p.tcpLabelSelectors)
		return nil
	default:
		p.aggregator.Aggregate(gaugeGroup, p.netRequestLabelSelectors)
		return nil
	}
}

// TODO: make it configurable instead of hard-coded
func newNetRequestLabelSelectors() *aggregator.LabelSelectors {
	return aggregator.NewLabelSelectors(
		aggregator.LabelSelector{Name: constlabels.Pid, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.Protocol, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.IsServer, VType: aggregator.BooleanType},
		aggregator.LabelSelector{Name: constlabels.ContainerId, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcNode, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcNodeIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcNamespace, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcPod, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcWorkloadName, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcWorkloadKind, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcService, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcContainerId, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcContainer, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstNode, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstNodeIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstNamespace, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstPod, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstWorkloadName, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstWorkloadKind, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstService, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstPort, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.DnatIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DnatPort, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.DstContainerId, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstContainer, VType: aggregator.StringType},

		aggregator.LabelSelector{Name: constlabels.IsError, VType: aggregator.BooleanType},
		aggregator.LabelSelector{Name: constlabels.IsSlow, VType: aggregator.BooleanType},
		aggregator.LabelSelector{Name: constlabels.HttpStatusCode, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.DnsRcode, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.SqlErrCode, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.ContentKey, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DnsDomain, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.KafkaTopic, VType: aggregator.StringType},
	)
}

func newTcpLabelSelectors() *aggregator.LabelSelectors {
	return aggregator.NewLabelSelectors(
		aggregator.LabelSelector{Name: constlabels.SrcNode, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcNodeIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcNamespace, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcPod, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcWorkloadName, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcWorkloadKind, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcService, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcPort, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.SrcContainerId, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.SrcContainer, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstNode, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstNodeIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstNamespace, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstPod, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstWorkloadName, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstWorkloadKind, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstService, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstPort, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.DnatIp, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DnatPort, VType: aggregator.IntType},
		aggregator.LabelSelector{Name: constlabels.DstContainerId, VType: aggregator.StringType},
		aggregator.LabelSelector{Name: constlabels.DstContainer, VType: aggregator.StringType},
	)
}

func (p *AggregateProcessor) isSampled(gaugeGroup *model.GaugeGroup) bool {
	randSeed := rand.Intn(100)
	if isAbnormal(gaugeGroup) {
		if (randSeed < p.cfg.SamplingRate.SlowData) && gaugeGroup.Labels.GetBoolValue(constlabels.IsSlow) {
			return true
		}
		if (randSeed < p.cfg.SamplingRate.ErrorData) && gaugeGroup.Labels.GetBoolValue(constlabels.IsError) {
			return true
		}
	} else {
		if randSeed < p.cfg.SamplingRate.NormalData {
			return true
		}
	}
	return false
}

// shouldAggregate returns true if the gaugeGroup is slow or has errors.
func isAbnormal(g *model.GaugeGroup) bool {
	return g.Labels.GetBoolValue(constlabels.IsSlow) || g.Labels.GetBoolValue(constlabels.IsError) ||
		g.Labels.GetIntValue(constlabels.ErrorType) > constlabels.NoError
}
