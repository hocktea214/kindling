package network

import (
	"sync"

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

func newMergableEventWithSize(evt *model.KindlingEvent, data []byte, size int64) *mergableEvent {
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

func (mergable *mergableEvent) resetSize(size int64) {
	mergable.size = size
	// close mergable
	mergable.mergable = false
	if len(mergable.data) > int(size) {
		mergable.data = mergable.data[0:int(size)]
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

func (mergable *mergableEvent) IsSportChanged(newEvt *model.KindlingEvent) bool {
	return newEvt.GetSport() != mergable.event.GetSport()
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

func (sp *sequencePair) getMessagePairs(protocol string, attributes *model.AttributeMap) []*messagePair {
	return []*messagePair{newMessagePair(protocol, false, sp.request, sp.response, attributes)}
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
	reverse    bool
	request    *mergableEvent
	response   *mergableEvent
	attributes *model.AttributeMap
}

func newMessagePair(protocol string, reverse bool, request *mergableEvent, response *mergableEvent, attributes *model.AttributeMap) *messagePair {
	return &messagePair{
		protocol:   protocol,
		reverse:    reverse,
		request:    request,
		response:   response,
		attributes: attributes,
	}
}

func (mp *messagePair) GetRole() bool {
	return mp.request.event.Ctx.FdInfo.Role != mp.reverse // xor operate
}

func (mp *messagePair) GetSip() string {
	if mp.reverse {
		return mp.request.event.GetDip()
	}
	return mp.request.event.GetSip()
}

func (mp *messagePair) GetSport() uint32 {
	if mp.reverse {
		return mp.request.event.GetDport()
	}
	return mp.request.event.GetSport()
}

func (mp *messagePair) GetDip() string {
	if mp.reverse {
		return mp.request.event.GetSip()
	}
	return mp.request.event.GetDip()
}

func (mp *messagePair) GetDport() uint32 {
	if mp.reverse {
		return mp.request.event.GetSport()
	}
	return mp.request.event.GetDport()
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
