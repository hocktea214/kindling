package network

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/Kindling-project/kindling/collector/pkg/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

const (
	ATTRIBUTE_KEY_RPC_ID = "rpcId"
)

var rpcCache *RpcCache = newRpcCache()

type RpcCache struct {
	clientDatas     sync.Map // <TuppleKey, RpcClientDatas>
	na              *NetworkAnalyzer
	grpcClientCache sync.Map // <ip, GrpcClient>
	grpcPort        int
}

func newRpcCache() *RpcCache {
	return &RpcCache{}
}

func GetRpcCache() *RpcCache {
	return rpcCache
}

func (cache *RpcCache) setNetworkAnalyzer(na *NetworkAnalyzer) {
	cache.na = na
}

func (cache *RpcCache) SetGrpcPort(port int) {
	cache.grpcPort = port
}

func (cache *RpcCache) getOrCreateClientDatas(event *model.KindlingEvent) *RpcClientDatas {
	tuppleKey := getTuppleKey(event)
	if rpcClientDatasInterface, exist := cache.clientDatas.Load(tuppleKey); exist {
		return rpcClientDatasInterface.(*RpcClientDatas)
	}

	rpcDatas := NewRpcClientDatas()
	cache.clientDatas.Store(tuppleKey, rpcDatas)
	return rpcDatas
}

func (cache *RpcCache) cacheRequest(id int64, event *model.KindlingEvent) {
	cache.getOrCreateClientDatas(event).addRequest(id, event)
}

func (cache *RpcCache) cacheResponse(id int64, event *model.KindlingEvent) {
	cache.getOrCreateClientDatas(event).addLocal(event)
}

func (cache *RpcCache) cacheRemoteResponse(id int64, event *model.KindlingEvent) {
	cache.getOrCreateClientDatas(event).addRemote(event)
}

func (cache *RpcCache) sendToCollector(event *model.KindlingEvent, ip string) {
	var (
		client model.GrpcClient
	)
	if clientInterface, exist := cache.grpcClientCache.Load(ip); exist {
		client = clientInterface.(model.GrpcClient)
	} else {
		client = createGrpcClient(ip)
		if client == nil {
			return
		}
		cache.grpcClientCache.Store(ip, client)
	}

	data, _ := json.Marshal(event)
	if _, err := client.Send(context.Background(), &model.GrpcParam{Type: model.Type_RPC, Data: string(data)}); err != nil {
		if ce := cache.na.telemetry.Logger.Check(zapcore.InfoLevel, "Fail to Send Grpc Event: "); ce != nil {
			ce.Write(
				zap.String("ip", ip),
				zap.Any("event", event),
				zap.Error(err),
			)
		}
	}
}

/*
responses:   [1/2/3,   4, 5/6]
remoteResps: [1, 2, 3, 4, 5, 6]
*/
type RpcClientDatas struct {
	requests     sync.Map // <Id, KindlingEvent>
	lastResponse *model.KindlingEvent
	responses    []*model.KindlingEvent // Data is sticked and truncated.
	remoteResps  []*model.KindlingEvent // Need correct time

	localMutex  sync.RWMutex
	remoteMutex sync.RWMutex
}

func NewRpcClientDatas() *RpcClientDatas {
	return &RpcClientDatas{
		responses:   make([]*model.KindlingEvent, 0),
		remoteResps: make([]*model.KindlingEvent, 0),
		localMutex:  sync.RWMutex{},
		remoteMutex: sync.RWMutex{},
	}
}

func (datas *RpcClientDatas) addRequest(id int64, event *model.KindlingEvent) {
	datas.requests.Store(id, event)
}

func (datas *RpcClientDatas) addLocal(event *model.KindlingEvent) {
	datas.localMutex.Lock()
	datas.responses = append(datas.responses, event)
	datas.localMutex.Unlock()
}

func (datas *RpcClientDatas) addRemote(event *model.KindlingEvent) {
	datas.remoteMutex.Lock()
	datas.remoteResps = append(datas.remoteResps, event)
	datas.remoteMutex.Unlock()
}

func (datas *RpcClientDatas) match() []*messagePair {
	localSize := len(datas.responses)
	remoteSize := len(datas.remoteResps)
	if localSize == 0 || remoteSize == 0 {
		return nil
	}

	mps := make([]*messagePair, 0)

	respIndexMap := make(map[int64]int, 0)
	for i := 0; i < remoteSize; i++ {
		respIndexMap[datas.remoteResps[i].GetIntUserAttribute(ATTRIBUTE_KEY_RPC_ID)] = i
	}

	var preRemoteIndex int = -1
	var preIndex int = -1
	for i := 0; i < localSize; i++ {
		response := datas.responses[i]
		id := response.GetIntUserAttribute(ATTRIBUTE_KEY_RPC_ID)
		if index, ok := respIndexMap[id]; ok {
			for j := preRemoteIndex + 1; j < index; j++ {
				preRemoteResp := datas.remoteResps[j]
				if datas.lastResponse == nil {
					datas.clearMissDatas(preRemoteResp.GetIntUserAttribute(ATTRIBUTE_KEY_RPC_ID))
				} else {
					mp := datas.addMessagePair(mps, preRemoteResp, datas.lastResponse)
					if mp != nil {
						mps = append(mps, mp)
					}
				}
			}
			mp := datas.addMessagePair(mps, datas.remoteResps[index], response)
			if mp != nil {
				mps = append(mps, mp)
			}

			preRemoteIndex = index
			preIndex = i
			datas.lastResponse = response
		}
	}
	datas.removeResponses(preIndex, localSize)
	datas.removeRemoteResponses(preRemoteIndex, remoteSize)
	return mps
}

func (datas *RpcClientDatas) addMessagePair(mps []*messagePair, remoteEvent *model.KindlingEvent, localEvent *model.KindlingEvent) *messagePair {
	id := remoteEvent.GetIntUserAttribute(ATTRIBUTE_KEY_RPC_ID)
	if request, ok := datas.requests.LoadAndDelete(id); ok {
		remoteEvent.Timestamp = localEvent.Timestamp
		remoteEvent.SetLatency(localEvent.GetLatency())
		remoteEvent.Ctx.FdInfo.Role = localEvent.Ctx.FdInfo.Role
		return &messagePair{
			request:  request.(*model.KindlingEvent),
			response: remoteEvent,
		}
	}
	return nil
}

func (datas *RpcClientDatas) clearMissDatas(id int64) {
	datas.requests.Delete(id)
}

func (datas *RpcClientDatas) removeResponses(index int, size int) {
	if index >= 0 && index < size {
		datas.localMutex.Lock()
		datas.responses = datas.responses[index+1:]
		datas.localMutex.Unlock()
	}
}

func (datas *RpcClientDatas) removeRemoteResponses(index int, size int) {
	if index >= 0 && index < size {
		datas.remoteMutex.Lock()
		datas.remoteResps = datas.remoteResps[index+1:]
		datas.remoteMutex.Unlock()
	}
}

func (datas *RpcClientDatas) clearExpireDatas(checkTime uint64, expireTime uint64) []*messagePair {
	remoteSize := len(datas.remoteResps)
	localSize := len(datas.responses)

	var mps []*messagePair
	if remoteSize > 0 && datas.lastResponse != nil {
		var preRemoteIndex = -1
		mps = make([]*messagePair, 0)
		// Mergable response which has no records in several seconds.
		for i := 0; i < remoteSize; i++ {
			remoteResp := datas.remoteResps[i]
			if remoteResp.Timestamp <= checkTime {
				mp := datas.addMessagePair(mps, remoteResp, datas.lastResponse)
				if mp != nil {
					mps = append(mps, mp)
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
			if datas.responses[i].Timestamp <= expireTime {
				localIndex = i
			} else {
				break
			}
		}
		datas.removeResponses(localIndex, localSize)
	}

	datas.requests.Range(func(k, v interface{}) bool {
		request := v.(*model.KindlingEvent)
		if request.Timestamp <= expireTime {
			datas.requests.Delete(k)
		}
		return true
	})
	return mps
}

type TuppleKey struct {
	sip   string
	dip   string
	sport uint32
	dport uint32
}

func getTuppleKey(evt *model.KindlingEvent) TuppleKey {
	return TuppleKey{
		sip:   evt.GetSip(),
		dip:   evt.GetDip(),
		sport: evt.GetSport(),
		dport: evt.GetDport(),
	}
}

func createGrpcClient(ip string) model.GrpcClient {
	conn, err := grpc.Dial(ip+":"+strconv.Itoa(GetRpcCache().grpcPort), grpc.WithInsecure())
	if err != nil {
		if ce := GetRpcCache().na.telemetry.Logger.Check(zapcore.InfoLevel, "Fail to Create GrpcClient: "); ce != nil {
			ce.Write(
				zap.String("ip", ip),
				zap.Error(err),
			)
		}
		return nil
	}
	return model.NewGrpcClient(conn)
}
