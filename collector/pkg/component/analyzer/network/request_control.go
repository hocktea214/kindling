package network

import (
	"sync"

	"go.uber.org/atomic"

	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/model"
)

type requestCache struct {
	connect      *mergableEvent
	parser       *protocol.ProtocolParser
	loopParsers  []*protocol.ProtocolParser
	streamPair   *streamPair
	sequencePair *sequencePair
}

func newConnectCache(connect *model.KindlingEvent) *requestCache {
	return &requestCache{
		connect: newMergableEvent(connect),
	}
}

func newRequestCache(event *model.KindlingEvent, isRequest bool, staticPortMap map[uint32]string, protocolMap map[string]*protocol.ProtocolParser, pairParsers []*protocol.ProtocolParser, maxPayloadLength int) *requestCache {
	cache := &requestCache{}
	cache.initRequestCache(event, isRequest, staticPortMap, protocolMap, pairParsers, maxPayloadLength)
	return cache
}

func (cache *requestCache) initRequestCache(event *model.KindlingEvent, isRequest bool, staticPortMap map[uint32]string, protocolMap map[string]*protocol.ProtocolParser, pairParsers []*protocol.ProtocolParser, maxPayloadLength int) {
	port := event.GetDport()
	// Static Port
	staticProtocol, found := staticPortMap[port]
	if found {
		if parser, exist := protocolMap[staticProtocol]; exist {
			cache.parser = parser
			if parser.IsStreamParser() {
				cache.streamPair = newStreamPair(maxPayloadLength)
			} else {
				cache.sequencePair = newSequencePair(maxPayloadLength)
			}
			return
		}
	}
	// Loop Stream Protocols
	for _, parser := range protocolMap {
		if parser.IsStreamParser() && parser.Check(event.GetData(), event.GetResVal(), isRequest) {
			cache.parser = parser
			cache.streamPair = newStreamPair(maxPayloadLength)
			return
		}
	}
	cache.loopParsers = pairParsers
	cache.sequencePair = newSequencePair(maxPayloadLength)
}

func (cache *requestCache) getPairedPairs() []*messagePair {
	if cache.parser == nil || !cache.parser.IsStreamParser() {
		if cache.sequencePair == nil || cache.sequencePair.request == nil {
			return nil
		}
		return cache.getSequenceMessagePairs(cache.sequencePair.getAndResetSequencePair())
	} else {
		return nil
	}
}

func (cache *requestCache) getTimeoutPairs(fdReuseEndTime int64, noResponseEndTime int64) []*messagePair {
	if cache.parser == nil || !cache.parser.IsStreamParser() {
		if cache.sequencePair != nil && cache.sequencePair.request != nil {
			if cache.sequencePair.response != nil {
				// No New Request
				if int64(cache.sequencePair.response.endTime)/1000000000 <= fdReuseEndTime {
					return cache.getSequenceMessagePairs(cache.sequencePair.getAndResetSequencePair())
				}
			}
			// No Response
			if int64(cache.sequencePair.request.endTime)/1000000000 <= noResponseEndTime {
				return cache.getSequenceMessagePairs(cache.sequencePair.getAndResetSequencePair())
			}
		} else if cache.connect != nil {
			// Connect Timeout
			if int64(cache.connect.endTime)/1000000000 <= noResponseEndTime {
				return []*messagePair{newConnectTimeoutMessagePair(cache.getAndResetConnect())}
			}
		}
	} else {
		if cache.connect != nil && cache.streamPair.hasRequest() == false {
			// Connect Timeout
			if int64(cache.connect.endTime)/1000000000 <= noResponseEndTime {
				return []*messagePair{newConnectTimeoutMessagePair(cache.getAndResetConnect())}
			}
		}
		return cache.streamPair.getStreamTimeoutMessagePairs(cache.parser, noResponseEndTime)
	}
	return nil
}

func (cache *requestCache) getAndResetConnect() *mergableEvent {
	evt := cache.connect
	if evt != nil {
		cache.connect = nil
	}
	return evt
}

func (cache *requestCache) cacheRequest(event *model.KindlingEvent, isRequest bool) []*messagePair {
	if cache.parser == nil || !cache.parser.IsStreamParser() {
		sequencePair := cache.sequencePair.cacheRequest(event, isRequest)
		return cache.getSequenceMessagePairs(sequencePair)
	} else {
		return cache.streamPair.cacheRequest(cache.parser, event, isRequest)
	}
}

func (cache *requestCache) getSequenceMessagePairs(sequencePair *sequencePair) []*messagePair {
	if sequencePair == nil {
		return nil
	}
	if cache.parser != nil {
		// Specify Protocol for Port
		attributes := cache.parseSequencePairAttributes(sequencePair, cache.parser)
		return sequencePair.getMessagePairs(cache.parser.GetProtocol(), cache.getAndResetConnect(), attributes)
	}
	// Loop All NonStream Protocols
	for _, parser := range cache.loopParsers {
		attributes := cache.parseSequencePairAttributes(sequencePair, parser)
		if attributes != nil {
			return sequencePair.getMessagePairs(parser.GetProtocol(), cache.getAndResetConnect(), attributes)
		}
	}
	return sequencePair.getMessagePairs(protocol.NOSUPPORT, cache.getAndResetConnect(), nil)
}

func (cache *requestCache) parseSequencePairAttributes(seqPair *sequencePair, parser *protocol.ProtocolParser) *model.AttributeMap {
	if seqPair.request != nil {
		requestAttributes := parser.ParseHead(seqPair.request.data, seqPair.request.size, true)
		if requestAttributes == nil {
			return nil
		}
		if parser.ParsePayload(requestAttributes, true) {
			if seqPair.response != nil {
				responseAttributes := parser.ParseHead(seqPair.response.data, seqPair.response.size, false)
				if responseAttributes == nil {
					return nil
				}
				// Merge Request Headers to Response
				if responseAttributes.MergeRequest(requestAttributes) && parser.ParsePayload(responseAttributes, false) {
					return responseAttributes.GetAttributes()
				}
			}
		}
	}
	return nil
}

func (cache *requestCache) hasRequest() bool {
	if cache.parser == nil || !cache.parser.IsStreamParser() {
		return cache.sequencePair != nil && cache.sequencePair.request != nil
	}
	return cache.streamPair.hasRequest()
}

type streamPair struct {
	maxPayloadLength int
	requestCache     sync.Map
	requestCount     *atomic.Int32
	// Packed message with unpack(Duplex Communicate Case)
	sendUnResolvedEvent *streamMessage
	recvUnResolvedEvent *streamMessage
}

func newStreamPair(maxPayloadLength int) *streamPair {
	return &streamPair{
		maxPayloadLength: maxPayloadLength,
		requestCount:     atomic.NewInt32(0),
	}
}

func (sp *streamPair) hasRequest() bool {
	return sp.sendUnResolvedEvent != nil || sp.recvUnResolvedEvent != nil || sp.requestCount.Load() > 0
}

func (sp *streamPair) getUnResolveMessage(isRequest bool) *streamMessage {
	if isRequest {
		return sp.sendUnResolvedEvent
	}
	return sp.recvUnResolvedEvent
}

func (sp *streamPair) putUnResolveMessage(message *streamMessage, isRequest bool) {
	if isRequest {
		sp.sendUnResolvedEvent = message
	} else {
		sp.recvUnResolvedEvent = message
	}
}

func (sp *streamPair) cacheRequest(parser *protocol.ProtocolParser, event *model.KindlingEvent, isRequest bool) []*messagePair {
	var (
		attributes protocol.ProtocolMessage
	)
	unResolvedMessage := sp.getUnResolveMessage(isRequest)
	if unResolvedMessage == nil {
		attributes = parser.ParseHead(event.GetData(), event.GetResVal(), isRequest)
		if attributes == nil {
			// Skip Not Match or UnPack Data
			return nil
		}
	} else {
		attributes = unResolvedMessage.attributes
	}
	return sp.splitParseStreamPacket(parser, event, isRequest, unResolvedMessage, attributes)
}

func (sp *streamPair) splitParseStreamPacket(parser *protocol.ProtocolParser, event *model.KindlingEvent, isRequest bool, unResolvedMessage *streamMessage, attributes protocol.ProtocolMessage) []*messagePair {
	mps := make([]*messagePair, 0)
	nextPktIndex := attributes.GetLength()
	if unResolvedMessage != nil {
		nextPktIndex -= unResolvedMessage.size
	}
	/*
		UnPack & Pack Packet & SubStr SysCall
		           SubStr_0              SubStr_1
		               |                      |
		          +----|---+-------+          |
		SysCall_0 | Pkt|_0 | Pkt_1 |          |
		          +----|---+-------+          |
		          +----|---+-------+-----+----|---+
		SysCall_1 | Pkt|_1 | Pkt_2 | ... | Pkt|_N |
		          +----|---+-------+-----+----|---+
		               |                      |
	*/
	for nextPktIndex <= event.GetResVal() {
		mp := sp.parseStreamPacket(parser, event, isRequest, unResolvedMessage, attributes)
		if mp != nil {
			mps = append(mps, mp)
			unResolvedMessage = nil
		}
		// Complete Packet Or Trunacted Packet
		if nextPktIndex == event.GetResVal() || nextPktIndex >= int64(len(event.GetData())) {
			return mps
		}
		/**
		 * Loop anaylze Next Split Message
		 */
		attributes = parser.ParseHead(event.GetData()[nextPktIndex:], event.GetResVal(), isRequest)
		if attributes == nil {
			// Skip Not Match Data(Truncated Message)
			return mps
		}
		nextPktIndex += attributes.GetLength()
	}

	if unResolvedMessage == nil {
		/**
		Two Cases
		1. First UnPacked packet (Pkt_0)
		2. The last stream Packet (Pkt_1) in first unpacked Packet
		          +--------+  |            +--------+-------+
		SysCall_0 | Pkt_0  |  |  SysCall_0 | Pkt_0  | Pkt_1 |
		          +--------+  |            +--------+-------+
		          +--------+  |            +--------+-------+-----+-------+
		SysCall_1 | Pkt_0' |  |  SysCall_1 | Pkt_1' | Pkt_2 | ... | Pkt_N |
		          +--------+  |            +--------+-------+-----+-------+
			   Case 0         |               Case 1
		*/
		sp.putUnResolveMessage(newMergableStreamMessage(event, attributes, nextPktIndex-attributes.GetLength()), isRequest)
	} else {
		unResolvedMessage.mergeEvent(event, sp.maxPayloadLength)
	}
	return mps
}

func (sp *streamPair) parseStreamPacket(parser *protocol.ProtocolParser, event *model.KindlingEvent, isRequest bool, unResolvedMessage *streamMessage, attributes protocol.ProtocolMessage) *messagePair {
	// Whole Packet
	if unResolvedMessage != nil {
		if unResolvedMessage.size < attributes.GetLength() {
			unResolvedMessage.mergeEventWithFixedLength(event, attributes.GetLength()-unResolvedMessage.size, sp.maxPayloadLength)
			attributes.SetData(unResolvedMessage.data)
		}
		// Clean UnResolveMessage
		sp.putUnResolveMessage(nil, isRequest)
	}

	var mp *messagePair
	match, _ := sp.requestCache.LoadAndDelete(attributes.GetStreamId())
	if match != nil {
		sp.requestCount.Dec()
		request := match.(*streamMessage)
		if attributes.IsRequest() {
			// Send One Way Request
			mp = newMessagePair(parser.GetProtocol(), attributes.IsReverse(), nil, request.mergableEvent, nil, request.attributes.GetAttributes())
		} else if attributes.MergeRequest(request.attributes) && parser.ParsePayload(attributes, false) {
			// Send Request/Response Pair
			if unResolvedMessage != nil {
				// Use Merged Message
				mp = newMessagePair(parser.GetProtocol(), attributes.IsReverse(), nil, request.mergableEvent, unResolvedMessage.mergableEvent, attributes.GetAttributes())
			} else {
				mp = newMessagePair(parser.GetProtocol(), attributes.IsReverse(), nil, request.mergableEvent, newMergableEventWithSize(event, attributes.GetLength(), attributes.GetData()), attributes.GetAttributes())
			}
		}
	}

	if attributes.IsRequest() && parser.ParsePayload(attributes, true) {
		// Save New Request and Wait New Response
		sp.requestCache.Store(attributes.GetStreamId(), newStreamMessage(event, attributes))
		sp.requestCount.Inc()
	}

	// Ignore Not Matched Response (Cold Start Data)
	return mp
}

func (sp *streamPair) getStreamTimeoutMessagePairs(parser *protocol.ProtocolParser, noResponseEndTime int64) []*messagePair {
	if sp.recvUnResolvedEvent == nil && sp.sendUnResolvedEvent == nil && sp.requestCount.Load() == 0 {
		return nil
	}

	mps := make([]*messagePair, 0)
	unResolveRequestEvt := sp.getUnResolveMessage(true)
	if unResolveRequestEvt != nil && int64(unResolveRequestEvt.endTime)/1000000000 <= noResponseEndTime {
		sp.matchUnResolveEvent(parser, true, unResolveRequestEvt, &mps)
	}

	unResolveResponseEvt := sp.getUnResolveMessage(false)
	if unResolveResponseEvt != nil && int64(unResolveResponseEvt.endTime)/1000000000 <= noResponseEndTime {
		sp.matchUnResolveEvent(parser, false, unResolveRequestEvt, &mps)
	}

	sp.requestCache.Range(func(k, v interface{}) bool {
		request := v.(*streamMessage)
		if int64(request.endTime)/1000000000 <= noResponseEndTime {
			sp.requestCache.Delete(k)
			sp.requestCount.Dec()
			mps = append(mps, newMessagePair(parser.GetProtocol(), request.attributes.IsReverse(), nil, request.mergableEvent, nil, request.attributes.GetAttributes()))
		}
		return true
	})
	return mps
}

func (sp *streamPair) matchUnResolveEvent(parser *protocol.ProtocolParser, isRequest bool, unResolvedMessage *streamMessage, mps *[]*messagePair) {
	attributes := unResolvedMessage.attributes
	// Clean UnResolveMessage
	sp.putUnResolveMessage(nil, isRequest)

	match, _ := sp.requestCache.LoadAndDelete(attributes.GetStreamId())
	if match != nil {
		sp.requestCount.Dec()
		request := match.(*streamMessage)
		if attributes.IsRequest() {
			// Send One Way Request
			*mps = append(*mps, newMessagePair(parser.GetProtocol(), attributes.IsReverse(), nil, request.mergableEvent, nil, request.attributes.GetAttributes()))
		} else if attributes.MergeRequest(request.attributes) && parser.ParsePayload(attributes, false) {
			// Send Request/Response Pair
			*mps = append(*mps, newMessagePair(parser.GetProtocol(), attributes.IsReverse(), nil, request.mergableEvent, unResolvedMessage.mergableEvent, attributes.GetAttributes()))
		}
	}

	if attributes.IsRequest() && parser.ParsePayload(attributes, true) {
		// Save New Request and Wait New Response
		*mps = append(*mps, newMessagePair(parser.GetProtocol(), attributes.IsReverse(), nil, unResolvedMessage.mergableEvent, nil, attributes.GetAttributes()))
	}
}

type streamMessage struct {
	*mergableEvent
	attributes protocol.ProtocolMessage
}

func newStreamMessage(event *model.KindlingEvent, attributes protocol.ProtocolMessage) *streamMessage {
	message := &streamMessage{
		attributes: attributes,
	}
	message.mergableEvent = newMergableEventWithSize(event, attributes.GetLength(), attributes.GetData())
	return message
}

func newMergableStreamMessage(event *model.KindlingEvent, attributes protocol.ProtocolMessage, index int64) *streamMessage {
	message := &streamMessage{
		attributes: attributes,
	}
	message.mergableEvent = newMergableEventWithSize(event, event.GetResVal()-index, attributes.GetData())
	return message
}
