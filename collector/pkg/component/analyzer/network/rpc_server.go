package network

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"time"

	"github.com/Kindling-project/kindling/collector/pkg/component"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

func StartRpcReceiver(na *NetworkAnalyzer) {
	rpcClients.SetRpcCfg(na.cfg.RpcPort, na.cfg.RpcCacheSize, na.telemetry)

	go startRpcServer(na.telemetry, na.cfg.RpcPort)
	go consumerAndCheckRpcDatas(na)
}

func startRpcServer(telemetry *component.TelemetryTools, rpcPort int) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(rpcPort))
	if err != nil {
		telemetry.Logger.Error("Fail to listen Grpc Port", zap.Error(err))
	}

	server := grpc.NewServer()
	model.RegisterGrpcServer(server, &collectorGrpcServer{})
	if err := server.Serve(lis); err != nil {
		telemetry.Logger.Error("Fail to Start Grpc Server", zap.Error(err))
	}
}

func consumerAndCheckRpcDatas(na *NetworkAnalyzer) {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			GetRpcServer().rpcDatasCache.Range(func(k, v interface{}) bool {
				rpcCacheDatas := v.(*RpcCacheDatas)
				pairs := rpcCacheDatas.match()
				if len(pairs) > 0 {
					if ce := GetRpcClients().telemetry.Logger.Check(zapcore.InfoLevel, "Matched Dubbo Events: "); ce != nil {
						ce.Write(
							zap.Int("size", len(pairs)),
						)
					}
					na.parseRpcAndDistributeTraceMetric(pairs)
				}
				// Expire 15s, check 1s
				pairs = rpcCacheDatas.clearExpireDatas(1, 15)
				if len(pairs) > 0 {
					if ce := GetRpcClients().telemetry.Logger.Check(zapcore.InfoLevel, "Checked Match Dubbo Events: "); ce != nil {
						ce.Write(
							zap.Int("size", len(pairs)),
						)
					}
					na.parseRpcAndDistributeTraceMetric(pairs)
				}
				return true
			})
			GetRpcClients().rpcDatasCache.Range(func(k, v interface{}) bool {
				clientDatas := v.(*rpcClientDatas)
				// 1s
				clientDatas.checkAndSendRpcDatas(k.(podKey), 1)
				return true
			})
			GetRpcClients().rpcConnectCache.Range(func(k, v interface{}) bool {
				clientConnect := v.(*rpcClientConnect)
				// Send Period 1min, Expire 1hour
				clientConnect.checkAndSendPodInfos(60, 3600)
				return true
			})
		}
	}
}

type collectorGrpcServer struct {
	model.UnimplementedGrpcServer
}

func (server *collectorGrpcServer) SendRpcDatas(ctx context.Context, in *model.RpcDatas) (*model.RpcReply, error) {
	recvTime := uint64(time.Now().UnixNano())
	for _, data := range in.Datas {
		// Set Client Time for expire check.
		data.Timestamp = recvTime

		eventJson, _ := json.Marshal(&data)
		if ce := GetRpcClients().telemetry.Logger.Check(zapcore.InfoLevel, "Receive Rpc Event: "); ce != nil {
			ce.Write(
				zap.String("event", string(eventJson)),
			)
		}
		GetRpcServer().cacheRemoteRpcData(data)
	}

	return &model.RpcReply{Result: ""}, nil
}

func (server *collectorGrpcServer) SendPodInfos(ctx context.Context, in *model.PodInfos) (*model.PodReply, error) {
	recvTime := uint64(time.Now().UnixNano())
	for _, pod := range in.Pods {
		// Set UpdateTime as Client Time
		pod.UpdateTime = recvTime - (in.Timestamp - pod.UpdateTime)
		if ce := GetRpcClients().telemetry.Logger.Check(zapcore.InfoLevel, "Receive Pod Info: "); ce != nil {
			ce.Write(
				zap.String("hostIp", in.HostIp),
				zap.String("sip", model.IPLong2String(pod.Sip)),
				zap.Uint32("sport", pod.Sport),
				zap.Uint32("dport", pod.Dport),
				zap.Uint64("UpdateTime", pod.UpdateTime),
			)
		}
		GetRpcClients().CacheRemotePodInfos(in.HostIp, pod)
	}

	return &model.PodReply{Result: ""}, nil
}
