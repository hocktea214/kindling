package network

import (
	"sync"
	"time"

	"github.com/Kindling-project/kindling/collector/pkg/model"
)

type mergableEvent struct {
	event *model.KindlingEvent

	duration uint64
	size     int64
	endTime  uint64
	data     []byte
	mergable bool
}

func newMergableEventWithSize(evt *model.KindlingEvent, size int64, data []byte) *mergableEvent {
	return &mergableEvent{
		event:    evt,
		duration: evt.GetLatency(),
		size:     size,
		endTime:  evt.Timestamp,
		data:     data,
		mergable: evt.GetResVal() == int64(len(evt.GetData())),
	}
}

func newMergableEvent(evt *model.KindlingEvent) *mergableEvent {
	return &mergableEvent{
		event:    evt,
		duration: evt.GetLatency(),
		size:     evt.GetResVal(),
		endTime:  evt.Timestamp,
		data:     evt.GetData(),
		mergable: evt.GetResVal() == int64(len(evt.GetData())),
	}
}

func (mergable *mergableEvent) putEventBack(originEvts *mergableEvent, maxPayloadLength int) {
	newEvt := mergable.event
	mergable.event = originEvts.event
	mergable.duration = originEvts.event.GetLatency()
	mergable.size = originEvts.event.GetResVal()
	mergable.endTime = originEvts.event.Timestamp
	mergable.data = originEvts.event.GetData()
	mergable.mergable = originEvts.mergable
	mergable.mergeEvent(newEvt, maxPayloadLength)
}

func (mergable *mergableEvent) mergeEvent(evt *model.KindlingEvent, maxPayloadLength int) {
	mergable.duration = evt.Timestamp - mergable.endTime + mergable.duration
	mergable.size += evt.GetResVal()
	mergable.endTime = evt.Timestamp

	if !mergable.mergable {
		return
	}
	appendLength := mergable.getAppendLength(len(evt.GetData()), maxPayloadLength)
	if appendLength > 0 {
		mergable.data = append(mergable.data, evt.GetData()[0:appendLength]...)
	}
	if evt.GetResVal() > int64(len(evt.GetData())) {
		mergable.mergable = false
	}
}

func (mergable *mergableEvent) mergeEventWithFixedLength(evt *model.KindlingEvent, size int64, maxPayloadLength int) {
	mergable.duration = evt.Timestamp - mergable.endTime + mergable.duration
	mergable.size += size
	mergable.endTime = evt.Timestamp

	if !mergable.mergable {
		return
	}
	newEventLength := int(size)
	if newEventLength > len(evt.GetData()) {
		newEventLength = len(evt.GetData())
	}
	appendLength := mergable.getAppendLength(newEventLength, maxPayloadLength)
	if appendLength > 0 {
		mergable.data = append(mergable.data, evt.GetData()[0:appendLength]...)
	}
	if evt.GetResVal() > int64(len(evt.GetData())) {
		mergable.mergable = false
	}
}

// getAppendLength returns the length to accommodate the new event according to the remaining size and
// the new event's size.
func (mergable *mergableEvent) getAppendLength(newEventLength int, maxPayloadLength int) int {
	remainingSize := maxPayloadLength - len(mergable.data)
	// If the merged data is full
	if remainingSize <= 0 {
		return 0
	}
	// If the merged data is not full, return the smaller size
	if remainingSize > newEventLength {
		return newEventLength
	} else {
		return remainingSize
	}
}

func (mergable *mergableEvent) isTimeout(newEvt *model.KindlingEvent, timeout int) bool {
	startTime := mergable.endTime - mergable.duration
	if newEvt.Timestamp < startTime {
		return false
	}
	if newEvt.Timestamp-startTime > uint64(timeout)*uint64(time.Second) || newEvt.GetSport() != mergable.event.GetSport() {
		return true
	}
	return false
}

func (mergable *mergableEvent) IsSportChanged(newEvt *model.KindlingEvent) bool {
	return newEvt.GetSport() != mergable.event.GetSport()
}

func (mergable *mergableEvent) getRole(reverse bool) bool {
	if reverse {
		return mergable.event.Ctx.FdInfo.Role
	}
	return !mergable.event.Ctx.FdInfo.Role
}

type sequencePair struct {
	request          *mergableEvent
	response         *mergableEvent
	maxPayloadLength int
	mutex            sync.RWMutex // only for update latency and resval now
}

func newSequencePair(maxPayloadLength int) *sequencePair {
	return &sequencePair{
		mutex:            sync.RWMutex{},
		maxPayloadLength: maxPayloadLength,
	}
}

func (sp *sequencePair) getMessagePairs(protocol string, connect *mergableEvent, attributes *model.AttributeMap) []*messagePair {
	return []*messagePair{&messagePair{
		protocol:   protocol,
		role:       sp.request.event.Ctx.FdInfo.Role,
		connect:    connect,
		request:    sp.request,
		response:   sp.response,
		attributes: attributes,
	}}
}

func (sp *sequencePair) getAndResetSequencePair() *sequencePair {
	newPair := &sequencePair{
		request:  sp.request,
		response: sp.response,
	}
	sp.request = nil
	sp.response = nil
	return newPair
}

func (sp *sequencePair) cacheRequest(event *model.KindlingEvent, isRequest bool) *sequencePair {
	if isRequest {
		if sp.request == nil {
			sp.request = newMergableEvent(event)
		} else if sp.response == nil {
			sp.mutex.Lock()
			sp.request.mergeEvent(event, sp.maxPayloadLength)
			sp.mutex.Unlock()
		} else {
			newPair := &sequencePair{
				request:  sp.request,
				response: sp.response,
			}
			sp.request = newMergableEvent(event)
			sp.response = nil
			return newPair
		}
	} else {
		if sp.request != nil {
			if sp.response == nil {
				sp.response = newMergableEvent(event)
			} else {
				sp.mutex.Lock()
				sp.response.mergeEvent(event, sp.maxPayloadLength)
				sp.mutex.Unlock()
			}
		}
	}
	return nil
}

func (sp *sequencePair) putRequestBack(request *mergableEvent) {
	if sp.request == nil {
		sp.request = request
	} else {
		sp.request.putEventBack(request, sp.maxPayloadLength)
	}
}

type messagePair struct {
	protocol   string
	role       bool
	connect    *mergableEvent
	request    *mergableEvent
	response   *mergableEvent
	attributes *model.AttributeMap
}

func newConnectTimeoutMessagePair(connect *mergableEvent) *messagePair {
	return &messagePair{
		connect: connect,
	}
}

func newMessagePair(protocol string, reverse bool, connect *mergableEvent, request *mergableEvent, response *mergableEvent, attributes *model.AttributeMap) *messagePair {
	return &messagePair{
		protocol:   protocol,
		role:       request.event.Ctx.FdInfo.Role != reverse, // xor operate
		connect:    connect,
		request:    request,
		response:   response,
		attributes: attributes,
	}
}

func (mp *messagePair) getSentTime() int64 {
	if mp.request == nil {
		return -1
	}

	return int64(mp.request.duration)
}

func (mp *messagePair) getWaitingTime() int64 {
	if mp.response == nil {
		return -1
	}

	return int64(mp.response.endTime - mp.response.duration - mp.request.endTime)
}

func (mp *messagePair) getDownloadTime() int64 {
	if mp.response == nil {
		return -1
	}

	return int64(mp.response.duration)
}

func (mp *messagePair) getDuration() uint64 {
	if mp.response == nil {
		return 0
	}

	return mp.response.endTime + mp.request.duration - mp.request.endTime
}

func (mp *messagePair) getRquestSize() uint64 {
	if mp.request == nil {
		return 0
	}
	return uint64(mp.request.size)
}

func (mp *messagePair) getResponseSize() uint64 {
	if mp.response == nil {
		return 0
	}
	return uint64(mp.response.size)
}

func (mp *messagePair) getRequestTid() int64 {
	if mp.request == nil {
		return 0
	}
	return int64(mp.request.event.GetTid())
}

func (mp *messagePair) getResponseTid() int64 {
	if mp.response == nil {
		return 0
	}
	return int64(mp.response.event.GetTid())
}
