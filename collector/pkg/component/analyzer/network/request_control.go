package network

import (
	"sync"

	"go.uber.org/atomic"

	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/model"
)

type requestCache struct {
	parser       *protocol.ProtocolParser
	streamPair   *streamPair
	reChecker    *noSupportCounter
	parsers      []*protocol.ProtocolParser
	sequencePair *sequencePair
	count        int
}

func newRequestCache(event *model.KindlingEvent, isRequest bool, staticPortMap map[uint32]string, protocolMap map[string]*protocol.ProtocolParser, parsers []*protocol.ProtocolParser, maxPayloadLength int) *requestCache {
	// Static Port
	if staticProtocol, found := staticPortMap[event.GetDport()]; found {
		if parser, exist := protocolMap[staticProtocol]; exist {
			if parser.IsStreamParser() {
				return &requestCache{
					parser:     parser,
					streamPair: newStreamPair(maxPayloadLength),
				}
			} else {
				return &requestCache{
					parser:       parser,
					sequencePair: newSequencePair(maxPayloadLength),
				}
			}
		}
	}
	// Loop Stream Protocols
	streamParser := getMatchStreamParser(event, isRequest, parsers)
	if streamParser != nil {
		return &requestCache{
			parser:     streamParser,
			streamPair: newStreamPair(maxPayloadLength),
		}
	}

	return &requestCache{
		parsers:      parsers,
		sequencePair: newSequencePair(maxPayloadLength),
		reChecker:    newNoSupportCounter(),
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
		}
	} else {
		return cache.streamPair.getStreamTimeoutMessagePairs(cache.parser, noResponseEndTime)
	}
	return nil
}

func (cache *requestCache) cacheRequest(event *model.KindlingEvent, isRequest bool) []*messagePair {
	if cache.parser == nil {
		/*
		   Provide Fault-tolerant mechanism for streamParser
		*/
		if cache.reChecker.reCheck() {
			streamParser := getMatchStreamParser(event, isRequest, cache.parsers)
			if streamParser != nil {
				maxPayloadLength := cache.sequencePair.maxPayloadLength
				// Clean SequencePair Data
				cache.sequencePair = nil
				cache.parsers = nil
				cache.reChecker = nil

				cache.parser = streamParser
				cache.streamPair = newStreamPair(maxPayloadLength)
				return cache.streamPair.cacheRequest(cache.parser, event, isRequest)
			}
		}
		sequencePair := cache.sequencePair.cacheRequest(event, isRequest)
		return cache.getSequenceMessagePairs(sequencePair)
	} else if !cache.parser.IsStreamParser() {
		sequencePair := cache.sequencePair.cacheRequest(event, isRequest)
		return cache.getSequenceMessagePairs(sequencePair)
	} else {
		return cache.streamPair.cacheRequest(cache.parser, event, isRequest)
	}
}

func getMatchStreamParser(event *model.KindlingEvent, isRequest bool, parsers []*protocol.ProtocolParser) *protocol.ProtocolParser {
	for _, parser := range parsers {
		if parser.IsStreamParser() && parser.Check(event.GetData(), event.GetResVal(), isRequest) {
			return parser
		}
	}
	return nil
}

func (cache *requestCache) getSequenceMessagePairs(sequencePair *sequencePair) []*messagePair {
	if sequencePair == nil {
		return nil
	}
	if cache.parser != nil {
		// Specify Protocol for Port
		attributes := cache.parseSequencePairAttributes(sequencePair, cache.parser)
		return sequencePair.getMessagePairs(cache.parser.GetProtocol(), attributes)
	}
	// Loop All SequenceParser Protocols
	for _, sequenceParser := range cache.parsers {
		if !sequenceParser.IsStreamParser() {
			attributes := cache.parseSequencePairAttributes(sequencePair, sequenceParser)
			if attributes != nil {
				cache.reChecker.addCount(sequenceParser.GetProtocol())
				return sequencePair.getMessagePairs(sequenceParser.GetProtocol(), attributes)
			}
		}
	}
	cache.reChecker.addCount(protocol.NOSUPPORT)
	return sequencePair.getMessagePairs(protocol.NOSUPPORT, nil)
}

func (cache *requestCache) parseSequencePairAttributes(seqPair *sequencePair, parser *protocol.ProtocolParser) *model.AttributeMap {
	if seqPair.request != nil {
		requestAttributes := parser.ParseSequenceHead(seqPair.request.data, seqPair.request.size, true)
		if requestAttributes == nil {
			return nil
		}
		if parser.ParsePayload(requestAttributes) {
			if seqPair.response != nil {
				responseAttributes := parser.ParseSequenceHead(seqPair.response.data, seqPair.response.size, false)
				if responseAttributes == nil {
					return nil
				}
				// Merge Request Headers to Response
				if responseAttributes.MergeRequest(requestAttributes) && parser.ParsePayload(responseAttributes) {
					return responseAttributes.GetAttributes()
				}
			}
		}
	}
	return nil
}

func (cache *requestCache) getCountDiff() int {
	oldVal := cache.count
	if cache.parser == nil || !cache.parser.IsStreamParser() {
		if cache.sequencePair != nil && cache.sequencePair.request != nil {
			cache.count = 1
		} else {
			cache.count = 0
		}
	} else {
		cache.count = cache.streamPair.getRequestCount()
	}
	return cache.count - oldVal
}

type streamPair struct {
	maxPayloadLength int
	requestCache     sync.Map
	requestCount     *atomic.Int32 // Count for requestCache
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

func (sp *streamPair) getRequestCount() int {
	count := 0
	if sp.sendUnResolvedEvent != nil {
		count++
	}
	if sp.recvUnResolvedEvent != nil {
		count++
	}
	count += int(sp.requestCount.Load())
	return count
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
	unResolvedMessage := sp.getUnResolveMessage(isRequest)
	if unResolvedMessage == nil {
		return sp.parseNewPacket(parser, event, isRequest)
	} else {
		return sp.parseAndMergeNewPacket(parser, event, isRequest, unResolvedMessage)
	}
}

func (sp *streamPair) parseNewPacket(parser *protocol.ProtocolParser, event *model.KindlingEvent, isRequest bool) []*messagePair {
	attributes, waitNextPkt := parser.ParseStreamHead(event.GetData(), event.GetResVal(), isRequest)
	if waitNextPkt {
		// Size is less than HeadSize, save And Wait Next Pkt
		sp.putUnResolveMessage(newStreamMessage(newMergableEvent(event), nil), isRequest)
		return nil
	}
	if attributes == nil {
		// Skip Parse failed Data
		return nil
	}
	unResolvedMessage := newStreamMessage(newMergableEvent(event), attributes)
	if unResolvedMessage.size < attributes.GetLength() {
		// Size is less than PktSize, save and Wait Next Pkt
		sp.putUnResolveMessage(unResolvedMessage, isRequest)
		return nil
	}
	return sp.splitParsePacket(parser, event, isRequest, unResolvedMessage)
}

func (sp *streamPair) parseAndMergeNewPacket(parser *protocol.ProtocolParser, event *model.KindlingEvent, isRequest bool, unResolvedMessage *streamMessage) []*messagePair {
	attributes := unResolvedMessage.attributes
	// Merge New Event
	unResolvedMessage.mergeEvent(event, sp.maxPayloadLength)
	// Parse Head
	if attributes == nil {
		var waitNextPkt bool
		if attributes, waitNextPkt = parser.ParseStreamHead(unResolvedMessage.data, unResolvedMessage.size, isRequest); waitNextPkt {
			// Size is less than HeadSize, wait Next Pkt
			return nil
		}
		if attributes == nil {
			// Skip Parse failed Data
			sp.putUnResolveMessage(nil, isRequest)
			return nil
		}
		unResolvedMessage.attributes = attributes
	} else {
		attributes.SetData(unResolvedMessage.data)
	}
	if unResolvedMessage.size < attributes.GetLength() {
		// Size is less than PktSize, wait Next Pkt
		return nil
	}
	return sp.splitParsePacket(parser, event, isRequest, unResolvedMessage)
}

func (sp *streamPair) splitParsePacket(parser *protocol.ProtocolParser, event *model.KindlingEvent, isRequest bool, unResolvedMessage *streamMessage) []*messagePair {
	mps := make([]*messagePair, 0)
	attributes := unResolvedMessage.attributes
	/*
	   Packed Pkts                             New Pkts
	   +====================+----------+       +============+----------+
	   | Pkt_0   |  Pkt_0'  |  Pkt_1   |       |   Pkt_0    |  Pkt_1   |
	   +=========+==========+----------+       +============+----------+
	   |<===== Length =====>|                  |<= Length =>|
	             |<====== ResVal =====>|       |<======= ResVal ======>|
	   |<=========== Size ============>|       |<======== Size =======>|
	             ^          ^                  ^            ^
	             |          |                  |            |
	             0      nextPktIndex           0      nextPktIndex
	*/
	nextPktIndex := attributes.GetLength() + event.GetResVal() - unResolvedMessage.size
	// Reset Payload Length Data
	unResolvedMessage.mergableEvent.resetSize(attributes.GetLength())

	// Parse First Whole Packet.
	mp := sp.parseAndMatchPacket(parser, event, isRequest, unResolvedMessage, attributes)
	if mp != nil {
		mps = append(mps, mp)
	}
	// Clean UnResolveMessage
	sp.putUnResolveMessage(nil, isRequest)

	var waitNextPkt bool
	/*
	   UnPack & Pack Packet
	             +=========+==========+----------+
	   Packed    | Pkt_0   |  Pkt_0'  |  Pkt_1   |
	             +=========+==========+----------+

	             +========|-----------|==========+----------+
	   Truncate  | Pkt_0  | Truncated |  Pkt_0'  |  Pkt_1   |
	             +========|-----------|==========+----------+
	*/
	for nextPktIndex <= event.GetResVal() {
		// Complete Packet Or Trunacted Packet
		if nextPktIndex == event.GetResVal() || nextPktIndex >= int64(len(event.GetData())) {
			return mps
		}

		/**
		 * Loop anaylze Next Split Message
		 */
		data := event.GetData()[nextPktIndex:]
		size := event.GetResVal() - nextPktIndex
		if attributes, waitNextPkt = parser.ParseStreamHead(data, size, isRequest); waitNextPkt {
			// Size is less than HeadSize, save And Wait Next Pkt
			sp.putUnResolveMessage(newStreamMessage(newMergableEventWithSize(event, data, size), nil), isRequest)
			return mps
		}
		if attributes == nil {
			// Skip Parse failed Data(Truncated Message)
			return mps
		}

		nextPktIndex += attributes.GetLength()
		if nextPktIndex > event.GetResVal() {
			// Size is less than PktSize, save and Wait Next Pkt
			sp.putUnResolveMessage(newStreamMessage(newMergableEventWithSize(event, data, size), attributes), isRequest)
			return mps
		}

		// Parse Next Whole Packet.
		mp := sp.parseAndMatchPacket(parser, event, isRequest, nil, attributes)
		if mp != nil {
			mps = append(mps, mp)
		}
	}
	return mps
}

func (sp *streamPair) parseAndMatchPacket(parser *protocol.ProtocolParser, event *model.KindlingEvent, isRequest bool, unResolvedMessage *streamMessage, attributes protocol.ProtocolMessage) *messagePair {
	// Whole Packet
	if len(attributes.GetData()) > int(attributes.GetLength()) {
		attributes.SetData(attributes.GetData()[0:int(attributes.GetLength())])
	}

	var mp *messagePair
	// Load and delete cached request
	match, _ := sp.requestCache.LoadAndDelete(attributes.GetStreamId())
	if match != nil {
		sp.requestCount.Dec()
		request := match.(*streamMessage)
		if attributes.IsRequest() {
			// Send One Way Request
			mp = newMessagePair(parser.GetProtocol(), attributes.IsReverse(), request.mergableEvent, nil, request.attributes.GetAttributes())
		} else if attributes.MergeRequest(request.attributes) && parser.ParsePayload(attributes) {
			// Send Request/Response Pair
			var response *mergableEvent
			if unResolvedMessage != nil {
				// First Merged Message
				response = unResolvedMessage.mergableEvent
			} else {
				response = newMergableEventWithSize(event, attributes.GetData(), attributes.GetLength())
			}
			return newMessagePair(parser.GetProtocol(), attributes.IsReverse(), request.mergableEvent, response, attributes.GetAttributes())
		}
	}

	if attributes.IsRequest() && parser.ParsePayload(attributes) {
		var requestMessage *mergableEvent
		if unResolvedMessage != nil {
			requestMessage = unResolvedMessage.mergableEvent
		} else {
			requestMessage = newMergableEventWithSize(event, attributes.GetData(), attributes.GetLength())
		}
		// Save New Request and Wait New Response
		sp.requestCache.Store(attributes.GetStreamId(), newStreamMessage(requestMessage, attributes))
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
	sp.getTimeoutUnResolvedEvt(parser, true, noResponseEndTime, mps)
	sp.getTimeoutUnResolvedEvt(parser, false, noResponseEndTime, mps)

	sp.requestCache.Range(func(k, v interface{}) bool {
		request := v.(*streamMessage)
		if int64(request.endTime)/1000000000 <= noResponseEndTime {
			sp.requestCache.Delete(k)
			sp.requestCount.Dec()
			mps = append(mps, newMessagePair(parser.GetProtocol(), request.attributes.IsReverse(), request.mergableEvent, nil, request.attributes.GetAttributes()))
		}
		return true
	})
	return mps
}

func (sp *streamPair) getTimeoutUnResolvedEvt(parser *protocol.ProtocolParser, isRequest bool, noResponseEndTime int64, mps []*messagePair) {
	unResolveEvt := sp.getUnResolveMessage(isRequest)
	if unResolveEvt != nil && int64(unResolveEvt.endTime)/1000000000 <= noResponseEndTime {
		if unResolveEvt.attributes == nil {
			sp.putUnResolveMessage(nil, isRequest)
		} else {
			sp.matchUnResolveEvent(parser, isRequest, unResolveEvt, &mps)
		}
	}
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
			*mps = append(*mps, newMessagePair(parser.GetProtocol(), attributes.IsReverse(), request.mergableEvent, nil, request.attributes.GetAttributes()))
		} else if attributes.MergeRequest(request.attributes) && parser.ParsePayload(attributes) {
			// Send Request/Response Pair
			*mps = append(*mps, newMessagePair(parser.GetProtocol(), attributes.IsReverse(), request.mergableEvent, unResolvedMessage.mergableEvent, attributes.GetAttributes()))
		}
	}

	if attributes.IsRequest() && parser.ParsePayload(attributes) {
		// Save New Request and Wait New Response
		*mps = append(*mps, newMessagePair(parser.GetProtocol(), attributes.IsReverse(), unResolvedMessage.mergableEvent, nil, attributes.GetAttributes()))
	}
}

type streamMessage struct {
	*mergableEvent
	attributes protocol.ProtocolMessage
}

func newStreamMessage(mergableEvent *mergableEvent, attributes protocol.ProtocolMessage) *streamMessage {
	message := &streamMessage{
		attributes: attributes,
	}
	message.mergableEvent = mergableEvent
	return message
}

var window_counts = []int{5, 10, 20, 100}

type noSupportCounter struct {
	count  int
	total  int
	enable bool
}

func newNoSupportCounter() *noSupportCounter {
	return &noSupportCounter{
		count:  0,
		total:  0,
		enable: true,
	}
}

func (counter *noSupportCounter) addCount(protocolName string) {
	if counter.enable {
		if protocolName == protocol.NOSUPPORT {
			counter.count = counter.count + 1
		}
		counter.total = counter.total + 1
	}
}

func (counter *noSupportCounter) reCheck() bool {
	// Check is in window time.
	if !counter.enable {
		return false
	}
	for index, val := range window_counts {
		if counter.count == val {
			if index == len(window_counts)-1 {
				// After N times, close window time
				counter.enable = false
			}
			return counter.isNoSupport()
		}
	}
	return false
}

/*
If more than 80% pkt is identified as nosupport, then we will recheck the pkt is stream protocol.
*/
func (counter *noSupportCounter) isNoSupport() bool {
	return counter.count*100/counter.total >= 80
}
