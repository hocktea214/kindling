package network

import (
	"sync"

	"github.com/Kindling-project/kindling/collector/pkg/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	ATTRIBUTE_KEY_RPC_ID = "rpcId"
)

var rpcServer *RpcServerCache = newRpcServerCache()

type RpcServerCache struct {
	rpcDatasCache sync.Map // <TuppleKey, RpcCacheDatas>
	na            *NetworkAnalyzer
}

func newRpcServerCache() *RpcServerCache {
	return &RpcServerCache{}
}

func GetRpcServer() *RpcServerCache {
	return rpcServer
}

func (cache *RpcServerCache) setNetworkAnalyzer(na *NetworkAnalyzer) {
	cache.na = na
}

func (cache *RpcServerCache) getOrCreateCacheDatas(key tuppleKey) *RpcCacheDatas {
	if rpcDatasInterface, exist := cache.rpcDatasCache.Load(key); exist {
		return rpcDatasInterface.(*RpcCacheDatas)
	}

	rpcDatas := NewRpcCacheDatas()
	cache.rpcDatasCache.Store(key, rpcDatas)
	return rpcDatas
}

func (cache *RpcServerCache) cacheEvent(id int64, event *model.KindlingEvent) {
	cache.getOrCreateCacheDatas(getTuppleKey(event)).addEvent(id, event)
}

func (cache *RpcServerCache) cacheLocalEvent(event *model.KindlingEvent) {
	cache.getOrCreateCacheDatas(getTuppleKey(event)).addLocal(event)
}

func (cache *RpcServerCache) cacheRemoteRpcData(rpcData *model.RpcData) {
	cache.getOrCreateCacheDatas(getRpcTuppleKey(rpcData)).addRemote(rpcData)
}

/*
responses:   [1/2/3,   4, 5/6]
remoteResps: [1, 2, 3, 4, 5, 6]
*/
type RpcCacheDatas struct {
	events         sync.Map // <Id, KindlingEvent>
	lastEvent      *model.KindlingEvent
	localEvents    []*model.KindlingEvent // Data is sticked and truncated.
	remoteRpcDatas []*model.RpcData       // Need correct time

	localMutex  sync.RWMutex
	remoteMutex sync.RWMutex
}

func NewRpcCacheDatas() *RpcCacheDatas {
	return &RpcCacheDatas{
		localEvents:    make([]*model.KindlingEvent, 0),
		remoteRpcDatas: make([]*model.RpcData, 0),
		localMutex:     sync.RWMutex{},
		remoteMutex:    sync.RWMutex{},
	}
}

func (datas *RpcCacheDatas) addEvent(id int64, event *model.KindlingEvent) {
	datas.events.Store(id, event)
}

func (datas *RpcCacheDatas) addLocal(event *model.KindlingEvent) {
	datas.localMutex.Lock()
	datas.localEvents = append(datas.localEvents, event)
	datas.localMutex.Unlock()
}

func (datas *RpcCacheDatas) addRemote(rpcData *model.RpcData) {
	var size int
	datas.remoteMutex.Lock()
	datas.remoteRpcDatas = append(datas.remoteRpcDatas, rpcData)
	size = len(datas.remoteRpcDatas)
	datas.remoteMutex.Unlock()

	if ce := rpcServer.na.telemetry.Logger.Check(zapcore.DebugLevel, "Cache Remote Rpc: "); ce != nil {
		ce.Write(
			zap.Int64("id", rpcData.RpcId),
			zap.Int("size", size),
		)
	}
}

func (datas *RpcCacheDatas) match() []*rpcPair {
	localSize := len(datas.localEvents)
	remoteSize := len(datas.remoteRpcDatas)
	if localSize == 0 || remoteSize == 0 {
		return nil
	}

	pairs := make([]*rpcPair, 0)
	respIndexMap := make(map[int64]int, 0)
	for i := 0; i < remoteSize; i++ {
		respIndexMap[datas.remoteRpcDatas[i].GetRpcId()] = i
	}

	var preRemoteIndex int = -1
	var preIndex int = -1
	for i := 0; i < localSize; i++ {
		localEvent := datas.localEvents[i]
		id := localEvent.GetIntUserAttribute(ATTRIBUTE_KEY_RPC_ID)
		if index, ok := respIndexMap[id]; ok {
			for j := preRemoteIndex + 1; j < index; j++ {
				preRemoteRpcData := datas.remoteRpcDatas[j]
				if datas.lastEvent == nil {
					datas.clearMissDatas(preRemoteRpcData.RpcId)
				} else {
					rpcPair := datas.getRpcPair(preRemoteRpcData, datas.lastEvent)
					if rpcPair != nil {
						pairs = append(pairs, rpcPair)
					}
				}
			}
			rpcPair := datas.getRpcPair(datas.remoteRpcDatas[index], localEvent)
			if rpcPair != nil {
				pairs = append(pairs, rpcPair)
			}

			preRemoteIndex = index
			preIndex = i
			datas.lastEvent = localEvent
		}
	}
	datas.removeResponses(preIndex, localSize)
	datas.removeRemoteResponses(preRemoteIndex, remoteSize)
	return pairs
}

func (datas *RpcCacheDatas) getRpcPair(remoteRpcData *model.RpcData, localEvent *model.KindlingEvent) *rpcPair {
	if event, ok := datas.events.LoadAndDelete(remoteRpcData.RpcId); ok {
		if ce := rpcServer.na.telemetry.Logger.Check(zapcore.DebugLevel, "Match Rpc Data "); ce != nil {
			ce.Write(
				zap.Int64("id", remoteRpcData.RpcId),
			)
		}

		return &rpcPair{
			event:      event.(*model.KindlingEvent),
			timestamp:  localEvent.Timestamp,
			latency:    localEvent.GetLatency(),
			attributes: remoteRpcData.GetUserAttributes(),
		}
	}
	return nil
}

func (datas *RpcCacheDatas) clearMissDatas(id int64) {
	datas.events.Delete(id)
}

func (datas *RpcCacheDatas) removeResponses(index int, size int) {
	if index >= 0 && index < size {
		datas.localMutex.Lock()
		datas.localEvents = datas.localEvents[index+1:]
		datas.localMutex.Unlock()
	}
}

func (datas *RpcCacheDatas) removeRemoteResponses(index int, size int) {
	if index >= 0 && index < size {
		var afterSize int
		datas.remoteMutex.Lock()
		datas.remoteRpcDatas = datas.remoteRpcDatas[index+1:]
		afterSize = len(datas.remoteRpcDatas)
		datas.remoteMutex.Unlock()
		if ce := rpcServer.na.telemetry.Logger.Check(zapcore.DebugLevel, "After Pair "); ce != nil {
			ce.Write(
				zap.Int("Match Count", index),
				zap.Int("Before Size", size),
				zap.Int("After Size", afterSize),
			)
		}
	}
}

func (datas *RpcCacheDatas) clearExpireDatas(checkTime uint64, expireTime uint64) []*rpcPair {
	remoteSize := len(datas.remoteRpcDatas)
	localSize := len(datas.localEvents)

	var pairs []*rpcPair
	if remoteSize > 0 && datas.lastEvent != nil {
		var preRemoteIndex = -1
		pairs = make([]*rpcPair, 0)
		// Mergable response which has no records in several seconds.
		for i := 0; i < remoteSize; i++ {
			remoteRpcData := datas.remoteRpcDatas[i]
			if remoteRpcData.Timestamp <= checkTime {
				rpcPair := datas.getRpcPair(remoteRpcData, datas.lastEvent)
				if rpcPair != nil {
					pairs = append(pairs, rpcPair)
				}

				preRemoteIndex = i
			} else {
				break
			}
		}
		datas.removeRemoteResponses(preRemoteIndex, remoteSize)
	}

	if localSize > 0 {
		var localIndex = -1
		for i := 0; i < localSize; i++ {
			if datas.localEvents[i].Timestamp <= expireTime {
				localIndex = i
			} else {
				break
			}
		}
		datas.removeResponses(localIndex, localSize)
	}

	datas.events.Range(func(k, v interface{}) bool {
		request := v.(*model.KindlingEvent)
		if request.Timestamp <= expireTime {
			datas.events.Delete(k)
		}
		return true
	})
	return pairs
}

type tuppleKey struct {
	sip   string
	dip   string
	sport uint32
	dport uint32
}

func getTuppleKey(evt *model.KindlingEvent) tuppleKey {
	return tuppleKey{
		sip:   evt.GetSip(),
		dip:   evt.GetDip(),
		sport: evt.GetSport(),
		dport: evt.GetDport(),
	}
}

func getRpcTuppleKey(evt *model.RpcData) tuppleKey {
	return tuppleKey{
		sip:   evt.GetSip(),
		dip:   evt.GetDip(),
		sport: evt.GetSport(),
		dport: evt.GetDport(),
	}
}
