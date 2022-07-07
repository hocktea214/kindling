package network

import (
	"context"
	"strconv"
	"sync"

	"github.com/Kindling-project/kindling/collector/pkg/component"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

var rpcClients *RpcClients = newRpcClients()

type RpcClients struct {
	rpcClientCache sync.Map // <ip, rpcClientDatas>
	rpcPort        int
	rpcCacheSize   int
	telemetry      *component.TelemetryTools
}

func newRpcClients() *RpcClients {
	return &RpcClients{
		rpcCacheSize: 100,
	}
}

func GetRpcClients() *RpcClients {
	return rpcClients
}

func (clients *RpcClients) SetRpcCfg(port int, cacheSize int, telemetry *component.TelemetryTools) {
	clients.rpcPort = port
	clients.rpcCacheSize = cacheSize
	clients.telemetry = telemetry
}

func (clients *RpcClients) CacheRpcData(rpcData *model.RpcData, ip string) {
	var client *rpcClientDatas
	if clientInterface, exist := clients.rpcClientCache.Load(ip); exist {
		client = clientInterface.(*rpcClientDatas)
	} else {
		client = clients.newRpcClientDatas(ip)
		if client == nil {
			return
		}
		clients.rpcClientCache.Store(ip, client)
	}
	client.cacheData(rpcData)
}

func (clients *RpcClients) newRpcClientDatas(ip string) *rpcClientDatas {
	conn, err := grpc.Dial(ip+":"+strconv.Itoa(rpcClients.rpcPort), grpc.WithInsecure())
	if err != nil {
		if ce := clients.telemetry.Logger.Check(zapcore.InfoLevel, "Fail to Create GrpcClient: "); ce != nil {
			ce.Write(
				zap.String("ip", ip),
				zap.Error(err),
			)
		}
		return nil
	}
	return &rpcClientDatas{
		ip:           ip,
		client:       model.NewGrpcClient(conn),
		rpcDataCache: make([]*model.RpcData, 0),
		cacheMutex:   sync.RWMutex{},
		sendMutex:    sync.RWMutex{},
	}
}

type rpcClientDatas struct {
	ip           string
	client       model.GrpcClient
	rpcDataCache []*model.RpcData
	cacheMutex   sync.RWMutex
	sendMutex    sync.RWMutex
}

func (clientDatas *rpcClientDatas) cacheData(rpcData *model.RpcData) {
	clientDatas.cacheMutex.Lock()
	clientDatas.rpcDataCache = append(clientDatas.rpcDataCache, rpcData)
	clientDatas.cacheMutex.Unlock()

	if len(clientDatas.rpcDataCache) >= rpcClients.rpcCacheSize {
		clientDatas.sendToCollector()
	}
}

func (clientDatas *rpcClientDatas) sendToCollector() {
	// lock to avoid send twice.
	clientDatas.sendMutex.Lock()
	size := len(clientDatas.rpcDataCache)
	if size > 0 {
		rpcDatas := &model.RpcDatas{
			Datas: clientDatas.rpcDataCache[0:size],
		}
		if _, err := clientDatas.client.Send(context.Background(), rpcDatas); err != nil {
			if ce := rpcClients.telemetry.Logger.Check(zapcore.InfoLevel, "Fail to Send Grpc Event: "); ce != nil {
				ce.Write(
					zap.String("ip", clientDatas.ip),
					zap.Int("size", size),
					zap.Error(err),
				)
			}
		}

		clientDatas.cacheMutex.Lock()
		clientDatas.rpcDataCache = clientDatas.rpcDataCache[size:]
		clientDatas.cacheMutex.Unlock()
	}
	clientDatas.sendMutex.Unlock()
}

func (clientDatas *rpcClientDatas) checkAndSend(checkTime uint64) {
	dataSize := len(clientDatas.rpcDataCache)
	if dataSize == 0 {
		return
	}
	clientDatas.cacheMutex.RLock()
	firstTime := clientDatas.rpcDataCache[0].Timestamp
	clientDatas.cacheMutex.RUnlock()

	if firstTime <= checkTime {
		clientDatas.sendToCollector()
	}
}
