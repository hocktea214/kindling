package network

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/Kindling-project/kindling/collector/pkg/component"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol/factory"
	"github.com/Kindling-project/kindling/collector/pkg/component/consumer"
	"github.com/Kindling-project/kindling/collector/pkg/metadata/conntracker"
	"github.com/Kindling-project/kindling/collector/pkg/model/constnames"

	"go.uber.org/zap/zapcore"

	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
	"github.com/Kindling-project/kindling/collector/pkg/model/constvalues"
)

const (
	CACHE_ADD_THRESHOLD   = 50
	CACHE_RESET_THRESHOLD = 5000

	Network analyzer.Type = "networkanalyzer"
)

type NetworkAnalyzer struct {
	cfg           *Config
	nextConsumers []consumer.Consumer
	conntracker   conntracker.Conntracker

	staticPortMap    map[uint32]string
	slowThresholdMap map[string]int
	protocolMap      map[string]*protocol.ProtocolParser
	parserFactory    *factory.ParserFactory
	parsers          []*protocol.ProtocolParser

	dataGroupPool      DataGroupPool
	requestMonitor     sync.Map
	tcpMessagePairSize int64
	udpMessagePairSize int64
	telemetry          *component.TelemetryTools

	// snaplen is the maximum data size the event could accommodate bytes.
	// It is set by setting the environment variable SNAPLEN. See https://github.com/KindlingProject/kindling/pull/387.
	snaplen int
}

func NewNetworkAnalyzer(cfg interface{}, telemetry *component.TelemetryTools, consumers []consumer.Consumer) analyzer.Analyzer {
	config, _ := cfg.(*Config)
	na := &NetworkAnalyzer{
		cfg:           config,
		dataGroupPool: NewDataGroupPool(),
		nextConsumers: consumers,
		telemetry:     telemetry,
	}
	if config.EnableConntrack {
		connConfig := &conntracker.Config{
			Enabled:                      config.EnableConntrack,
			ProcRoot:                     config.ProcRoot,
			ConntrackInitTimeout:         30 * time.Second,
			ConntrackRateLimit:           config.ConntrackRateLimit,
			ConntrackMaxStateSize:        config.ConntrackMaxStateSize,
			EnableConntrackAllNamespaces: true,
		}
		na.conntracker, _ = conntracker.NewConntracker(connConfig)
	}

	na.parserFactory = factory.NewParserFactory(factory.WithUrlClusteringMethod(na.cfg.UrlClusteringMethod))
	na.snaplen = getSnaplenEnv()
	return na
}

func getSnaplenEnv() int {
	snaplen := os.Getenv("SNAPLEN")
	snaplenInt, err := strconv.Atoi(snaplen)
	if err != nil {
		// Set 1000 bytes by default.
		return 1000
	}
	return snaplenInt
}

func (na *NetworkAnalyzer) ConsumableEvents() []string {
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

func (na *NetworkAnalyzer) Start() error {
	// TODO When import multi annalyzers, this part should move to factory. The metric will relate with analyzers.
	newSelfMetrics(na.telemetry.MeterProvider, na)

	go na.consumerFdNoReusingTrace()
	// go na.consumerUnFinishTrace()
	na.staticPortMap = map[uint32]string{}
	for _, config := range na.cfg.ProtocolConfigs {
		for _, port := range config.Ports {
			na.staticPortMap[port] = config.Key
		}
	}

	na.slowThresholdMap = map[string]int{}
	disableDisernProtocols := map[string]bool{}
	for _, config := range na.cfg.ProtocolConfigs {
		protocol.SetPayLoadLength(config.Key, config.PayloadLength)
		na.slowThresholdMap[config.Key] = config.Threshold
		disableDisernProtocols[config.Key] = config.DisableDiscern
	}

	na.protocolMap = map[string]*protocol.ProtocolParser{}
	parsers := make([]*protocol.ProtocolParser, 0)
	for _, protocolName := range na.cfg.ProtocolParser {
		protocolParser := na.parserFactory.GetParser(protocolName)
		if protocolParser != nil {
			na.protocolMap[protocolName] = protocolParser
			disableDiscern, ok := disableDisernProtocols[protocolName]
			if !protocolParser.IsStreamParser() && (!ok || !disableDiscern) {
				parsers = append(parsers, protocolParser)
			}
		}
	}
	// Add Generic Last
	parsers = append(parsers, na.parserFactory.GetGenericParser())
	na.parsers = parsers

	rand.Seed(time.Now().UnixNano())
	return nil
}

func (na *NetworkAnalyzer) Shutdown() error {
	// TODO: implement
	return nil
}

func (na *NetworkAnalyzer) Type() analyzer.Type {
	return Network
}

func (na *NetworkAnalyzer) ConsumeEvent(evt *model.KindlingEvent) error {
	if evt.Category != model.Category_CAT_NET {
		return nil
	}

	ctx := evt.GetCtx()
	if ctx == nil || ctx.GetThreadInfo() == nil {
		return nil
	}
	fd := ctx.GetFdInfo()
	if fd == nil {
		return nil
	}

	if fd.GetSip() == nil {
		return nil
	}

	// if not dns and udp == 1, return
	if fd.GetProtocol() == model.L4Proto_UDP {
		if _, ok := na.protocolMap[protocol.DNS]; !ok {
			return nil
		}
	}

	if evt.GetDataLen() <= 0 || evt.GetResVal() < 0 {
		// TODO: analyse udp
		return nil
	}

	isRequest, err := evt.IsRequest()
	if err != nil {
		return err
	}
	return na.analyseRequest(evt, isRequest)
}

func (na *NetworkAnalyzer) consumerFdNoReusingTrace() {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			now := time.Now().UnixNano() / 1000000000
			fdReuseEndTime := now - int64(na.cfg.GetFdReuseTimeout())
			noResponseEndTime := now - int64(na.cfg.getNoResponseThreshold())
			na.requestMonitor.Range(func(k, v interface{}) bool {
				cache := v.(*requestCache)
				na.distributeTraceMetric(cache, cache.getTimeoutPairs(fdReuseEndTime, noResponseEndTime))
				return true
			})
		}
	}
}

func (na *NetworkAnalyzer) analyseRequest(evt *model.KindlingEvent, isRequest bool) error {
	var cache *requestCache
	if cacheInterface, exist := na.requestMonitor.Load(evt.GetSocketKey()); exist {
		cache = cacheInterface.(*requestCache)
	} else {
		cache = newRequestCache(evt, isRequest, na.staticPortMap, na.protocolMap, na.parsers, na.snaplen)
		na.requestMonitor.Store(evt.GetSocketKey(), cache)
	}
	return na.distributeTraceMetric(cache, cache.cacheRequest(evt, isRequest))
}

func (na *NetworkAnalyzer) distributeTraceMetric(cache *requestCache, mps []*messagePair) error {
	if len(mps) == 0 {
		return nil
	}

	var (
		record   *model.DataGroup
		natTuple *conntracker.IPTranslation
	)
	request := mps[0].request
	if request != nil {
		if request.event.IsUdp() == 1 {
			atomic.AddInt64(&na.udpMessagePairSize, int64(cache.getCountDiff()))
		} else {
			atomic.AddInt64(&na.tcpMessagePairSize, int64(cache.getCountDiff()))
		}
	}
	if na.cfg.EnableConntrack && request != nil {
		queryEvt := request.event
		srcIP := queryEvt.GetCtx().FdInfo.Sip[0]
		dstIP := queryEvt.GetCtx().FdInfo.Dip[0]
		srcPort := uint16(queryEvt.GetSport())
		dstPort := uint16(queryEvt.GetDport())
		isUdp := queryEvt.IsUdp()
		natTuple = na.conntracker.GetDNATTuple(srcIP, dstIP, srcPort, dstPort, isUdp)
	}
	for _, mp := range mps {
		record = na.getRecord(mp, natTuple)
		if record != nil {
			if ce := na.telemetry.Logger.Check(zapcore.DebugLevel, ""); ce != nil {
				na.telemetry.Logger.Debug("NetworkAnalyzer To NextProcess:\n" + record.String())
			}
			if len(mp.protocol) > 0 {
				netanalyzerParsedRequestTotal.Add(context.Background(), 1, attribute.String("protocol", mp.protocol))
			}
			for _, nexConsumer := range na.nextConsumers {
				_ = nexConsumer.Consume(record)
			}
			na.dataGroupPool.Free(record)
		}
	}
	return nil
}

func (na *NetworkAnalyzer) getRecord(mp *messagePair, natTuple *conntracker.IPTranslation) *model.DataGroup {
	evt := mp.request.event
	// See the issue https://github.com/KindlingProject/kindling/issues/388 for details.
	if protocol.HTTP == mp.protocol && mp.attributes != nil && mp.attributes.HasAttribute(constlabels.HttpContinue) {
		if cacheInterface, ok := na.requestMonitor.Load(evt.GetSocketKey()); ok {
			cacheInterface.(*requestCache).sequencePair.putRequestBack(mp.request)
		}
		return nil
	}

	slow := false
	if mp.response != nil {
		slow = na.isSlow(mp.getDuration(), mp.protocol)
	}

	ret := na.dataGroupPool.Get()
	labels := ret.Labels
	labels.UpdateAddIntValue(constlabels.Pid, int64(evt.GetPid()))
	labels.UpdateAddIntValue(constlabels.RequestTid, mp.getRequestTid())
	labels.UpdateAddIntValue(constlabels.ResponseTid, mp.getResponseTid())
	labels.UpdateAddStringValue(constlabels.Comm, evt.GetComm())
	labels.UpdateAddStringValue(constlabels.SrcIp, mp.GetSip())
	labels.UpdateAddStringValue(constlabels.DstIp, mp.GetDip())
	labels.UpdateAddIntValue(constlabels.SrcPort, int64(mp.GetSport()))
	labels.UpdateAddIntValue(constlabels.DstPort, int64(mp.GetDport()))
	labels.UpdateAddStringValue(constlabels.DnatIp, constlabels.STR_EMPTY)
	labels.UpdateAddIntValue(constlabels.DnatPort, -1)
	labels.UpdateAddStringValue(constlabels.ContainerId, evt.GetContainerId())
	labels.UpdateAddBoolValue(constlabels.IsError, false)
	labels.UpdateAddIntValue(constlabels.ErrorType, int64(constlabels.NoError))
	labels.UpdateAddBoolValue(constlabels.IsSlow, slow)
	labels.UpdateAddBoolValue(constlabels.IsServer, mp.GetRole())
	labels.UpdateAddStringValue(constlabels.Protocol, mp.protocol)

	labels.Merge(mp.attributes)
	if mp.response != nil {
		labels.UpdateAddIntValue(constlabels.EndTimestamp, int64(mp.response.endTime))
	}
	if mp.response == nil {
		addProtocolPayload(mp.protocol, labels, mp.request.data, nil)
	} else {
		addProtocolPayload(mp.protocol, labels, mp.request.data, mp.response.data)
	}

	// If no protocol error found, we check other errors
	if !labels.GetBoolValue(constlabels.IsError) && mp.response == nil {
		labels.AddBoolValue(constlabels.IsError, true)
		labels.AddIntValue(constlabels.ErrorType, int64(constlabels.NoResponse))
	}

	if nil != natTuple {
		labels.UpdateAddStringValue(constlabels.DnatIp, natTuple.ReplSrcIP.String())
		labels.UpdateAddIntValue(constlabels.DnatPort, int64(natTuple.ReplSrcPort))
	}

	ret.UpdateAddIntMetric(constvalues.ConnectTime, 0)
	ret.UpdateAddIntMetric(constvalues.RequestSentTime, mp.getSentTime())
	ret.UpdateAddIntMetric(constvalues.WaitingTtfbTime, mp.getWaitingTime())
	ret.UpdateAddIntMetric(constvalues.ContentDownloadTime, mp.getDownloadTime())
	ret.UpdateAddIntMetric(constvalues.RequestTotalTime, int64(mp.getDuration()))
	ret.UpdateAddIntMetric(constvalues.RequestIo, int64(mp.getRquestSize()))
	ret.UpdateAddIntMetric(constvalues.ResponseIo, int64(mp.getResponseSize()))

	ret.Timestamp = evt.GetStartTime()

	return ret
}

func addProtocolPayload(protocolName string, labels *model.AttributeMap, request []byte, response []byte) {
	labels.UpdateAddStringValue(constlabels.RequestPayload, protocol.GetPayloadString(request, protocolName))
	if response != nil {
		labels.UpdateAddStringValue(constlabels.ResponsePayload, protocol.GetPayloadString(response, protocolName))
	} else {
		labels.UpdateAddStringValue(constlabels.ResponsePayload, "")
	}
}

func (na *NetworkAnalyzer) isSlow(duration uint64, protocol string) bool {
	return int64(duration) >= int64(na.getResponseSlowThreshold(protocol))*int64(time.Millisecond)
}

func (na *NetworkAnalyzer) getResponseSlowThreshold(protocol string) int {
	if value, ok := na.slowThresholdMap[protocol]; ok && value > 0 {
		// If value is not set, use response_slow_threshold by default.
		return value
	}
	return na.cfg.getResponseSlowThreshold()
}
