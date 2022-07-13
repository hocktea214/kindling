package network

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/Kindling-project/kindling/collector/pkg/component"
	"github.com/Kindling-project/kindling/collector/pkg/env"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

var rpcClients *RpcClients = newRpcClients()

type RpcClients struct {
	hostIp string

	rpcConnectCache sync.Map // <ip, rpcClientConnect>
	rpcDatasCache   sync.Map // <podKey, rpcClientDatas>
	rpcPort         int
	rpcCacheSize    int
	telemetry       *component.TelemetryTools
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

	hostIp, err := env.GetHostIpFromEnv()
	if err != nil {
		telemetry.Logger.Warn("HostIp is not found", zap.Error(err))
		return
	}
	clients.hostIp = hostIp
}

func (clients *RpcClients) CacheRpcData(sip uint32, rpcData *model.RpcData) {
	podKey := buildPodKey(sip, rpcData.Sport, rpcData.Dport)
	var client *rpcClientDatas
	if clientInterface, exist := clients.rpcDatasCache.Load(podKey); exist {
		client = clientInterface.(*rpcClientDatas)
	} else {
		client = &rpcClientDatas{
			rpcDataCache:  make([]*model.RpcData, 0),
			rpcCacheMutex: sync.RWMutex{},
		}
		clients.rpcDatasCache.Store(podKey, client)
	}
	client.cacheRpcData(podKey, rpcData)
}

func (clients *RpcClients) CachePodInfo(evt *model.KindlingEvent) {
	clientConnect := clients.getOrCreateConnect(evt.GetDip())
	if clientConnect != nil {
		clientConnect.cachePodInfo(evt.Ctx.FdInfo.Sip[0], evt.GetSport(), evt.GetDport())
	}
}

func (clients *RpcClients) CacheRemotePodInfos(hostIp string, podInfo *model.PodInfo) {
	key := getPodKey(podInfo)
	if clientDatasInterface, exist := clients.rpcDatasCache.Load(key); exist {
		clientDatas := clientDatasInterface.(*rpcClientDatas)
		if clientDatas.updateTime < podInfo.UpdateTime {
			clientDatas.updateTime = podInfo.UpdateTime
			clientDatas.hostIp = hostIp
			clients.rpcDatasCache.Store(key, clientDatas)
		}
	} else {
		// New Connect for Rpc Send
		clients.getOrCreateConnect(hostIp)
		clients.rpcDatasCache.Store(key, &rpcClientDatas{
			hostIp:        hostIp,
			updateTime:    podInfo.UpdateTime,
			rpcDataCache:  make([]*model.RpcData, 0),
			rpcCacheMutex: sync.RWMutex{},
		})
	}
}

func (clients *RpcClients) getOrCreateConnect(ip string) *rpcClientConnect {
	if clientConnectInterface, exist := clients.rpcConnectCache.Load(ip); exist {
		return clientConnectInterface.(*rpcClientConnect)
	} else {
		clientConnect := clients.newRpcClientConnect(ip)
		if clientConnect != nil {
			clients.rpcConnectCache.Store(ip, clientConnect)
		}
		return clientConnect
	}
}

func (clients *RpcClients) newRpcClientConnect(ip string) *rpcClientConnect {
	if ip == clients.hostIp {
		// Use local datas
		return &rpcClientConnect{
			ip:        ip,
			isRemote:  false,
			client:    &localClient{},
			sendMutex: sync.RWMutex{},
		}
	}
	if ce := clients.telemetry.Logger.Check(zapcore.InfoLevel, "Create GrpcClient: "); ce != nil {
		ce.Write(
			zap.String("ip", ip),
			zap.Int("port", clients.rpcPort),
		)
	}
	conn, err := grpc.Dial(ip+":"+strconv.Itoa(rpcClients.rpcPort), grpc.WithInsecure())
	if err != nil {
		if ce := clients.telemetry.Logger.Check(zapcore.WarnLevel, "Fail to Create GrpcClient: "); ce != nil {
			ce.Write(
				zap.String("ip", ip),
				zap.Error(err),
			)
		}
		return nil
	}
	return &rpcClientConnect{
		ip:        ip,
		isRemote:  true,
		client:    model.NewGrpcClient(conn),
		sendMutex: sync.RWMutex{},
	}
}

type rpcClientConnect struct {
	ip              string
	isRemote        bool
	client          model.GrpcClient
	podInfoCache    sync.Map // <tuppleKey, time>
	lastPodSendTime uint64
	sendMutex       sync.RWMutex
}

func (clientConnect *rpcClientConnect) cachePodInfo(sip uint32, sport uint32, dport uint32) {
	key := buildPodKey(sip, sport, dport)
	now := uint64(time.Now().UnixNano())

	if clientConnect.isRemote {
		if _, exist := clientConnect.podInfoCache.LoadOrStore(key, now); !exist {
			now := uint64(time.Now().UnixNano())
			clientConnect.sendPodInfos([]*model.PodInfo{model.NewPodInfo(sip, sport, dport, now)}, now)
		}
	} else {
		if _, exist := rpcClients.rpcDatasCache.Load(key); !exist {
			rpcClients.rpcDatasCache.Store(key, &rpcClientDatas{
				hostIp:        clientConnect.ip,
				updateTime:    now,
				rpcDataCache:  make([]*model.RpcData, 0),
				rpcCacheMutex: sync.RWMutex{},
			})
		}
	}
}

func (clientConnect *rpcClientConnect) checkAndSendPodInfos(sendPeriodSecond uint64, expireSecond uint64) {
	now := uint64(time.Now().UnixNano())
	expireTime := now - expireSecond*1000000000
	clientConnect.podInfoCache.Range(func(key, value interface{}) bool {
		if value.(uint64) <= expireTime {
			clientConnect.podInfoCache.Delete(key)
		}
		return true
	})

	lastSendTime := now - sendPeriodSecond*1000000000
	if clientConnect.lastPodSendTime < lastSendTime {
		pods := make([]*model.PodInfo, 0)
		clientConnect.podInfoCache.Range(func(key, value interface{}) bool {
			podKey := key.(podKey)
			pods = append(pods, model.NewPodInfo(podKey.podIp, podKey.podPort, podKey.port, now))
			return true
		})
		clientConnect.sendPodInfos(pods, now)
	}
}

func (clientConnect *rpcClientConnect) sendPodInfos(pods []*model.PodInfo, now uint64) {
	podInfos := &model.PodInfos{
		HostIp:    GetRpcClients().hostIp,
		Timestamp: now,
		Pods:      pods,
	}
	clientConnect.lastPodSendTime = now

	clientConnect.sendMutex.Lock()
	clientConnect.client.SendPodInfos(context.Background(), podInfos)
	clientConnect.sendMutex.Unlock()
}

type rpcClientDatas struct {
	hostIp        string
	updateTime    uint64
	rpcDataCache  []*model.RpcData
	rpcCacheMutex sync.RWMutex
}

func (clientDatas *rpcClientDatas) cacheRpcData(key podKey, rpcData *model.RpcData) {
	clientDatas.rpcCacheMutex.Lock()
	clientDatas.rpcDataCache = append(clientDatas.rpcDataCache, rpcData)
	clientDatas.rpcCacheMutex.Unlock()

	if len(clientDatas.rpcDataCache) >= rpcClients.rpcCacheSize {
		clientDatas.sendRpcDatas(key)
	}
}

func (clientDatas *rpcClientDatas) sendRpcDatas(key podKey) {
	// Check ip is nodeIp
	var clientConnect *rpcClientConnect
	if connectInterface, exist := GetRpcClients().rpcConnectCache.Load(model.IPLong2String(key.podIp)); exist {
		clientConnect = connectInterface.(*rpcClientConnect)
	} else if clientDatas.hostIp != "" {
		if connectInterface, exist := GetRpcClients().rpcConnectCache.Load(clientDatas.hostIp); exist {
			clientConnect = connectInterface.(*rpcClientConnect)
		}
	}

	size := len(clientDatas.rpcDataCache)
	if clientConnect != nil {
		if size > 0 {
			clientConnect.sendMutex.Lock()
			rpcDatas := &model.RpcDatas{
				Datas: clientDatas.rpcDataCache[0:size],
			}
			if clientConnect.client != nil {
				// Send Rpc Data back to client
				if _, err := clientConnect.client.SendRpcDatas(context.Background(), rpcDatas); err != nil {
					if ce := rpcClients.telemetry.Logger.Check(zapcore.WarnLevel, "Fail to Send Grpc Event: "); ce != nil {
						ce.Write(
							zap.String("sip", clientDatas.hostIp),
							zap.String("dip", clientConnect.ip),
							zap.Int("size", size),
							zap.Error(err),
						)
					}
				}
			} else {
				if ce := rpcClients.telemetry.Logger.Check(zapcore.WarnLevel, "Grpc Connect is not build "); ce != nil {
					ce.Write(
						zap.String("sip", clientDatas.hostIp),
						zap.String("dip", clientConnect.ip),
					)
				}
			}
			clientConnect.sendMutex.Unlock()
		}
	}

	if size > 0 {
		// Skip datas when connect is not build.
		clientDatas.rpcCacheMutex.Lock()
		clientDatas.rpcDataCache = clientDatas.rpcDataCache[size:]
		clientDatas.rpcCacheMutex.Unlock()
	}
}

func (clientDatas *rpcClientDatas) checkAndSendRpcDatas(key podKey, checkSecond uint64) {
	dataSize := len(clientDatas.rpcDataCache)
	if dataSize == 0 {
		return
	}
	firstTime := clientDatas.rpcDataCache[0].Timestamp
	if firstTime+checkSecond*1000000000 <= uint64(time.Now().UnixNano()) {
		clientDatas.sendRpcDatas(key)
	}
}

type podKey struct {
	podIp   uint32
	podPort uint32
	port    uint32
}

func buildPodKey(podIp uint32, podPort uint32, port uint32) podKey {
	return podKey{
		podIp:   podIp,
		podPort: podPort,
		port:    port,
	}
}

func getPodKey(podInfo *model.PodInfo) podKey {
	return podKey{
		podIp:   podInfo.Sip,
		podPort: podInfo.Sport,
		port:    podInfo.Dport,
	}
}

type localClient struct {
}

func (c *localClient) SendRpcDatas(ctx context.Context, in *model.RpcDatas, opts ...grpc.CallOption) (*model.RpcReply, error) {
	recvTime := uint64(time.Now().UnixNano())
	for _, data := range in.Datas {
		// Set Client Time for expire check.
		data.Timestamp = recvTime
		GetRpcServer().cacheRemoteRpcData(data)
	}
	return &model.RpcReply{Result: ""}, nil
}

func (c *localClient) SendPodInfos(ctx context.Context, in *model.PodInfos, opts ...grpc.CallOption) (*model.PodReply, error) {
	return nil, nil
}
