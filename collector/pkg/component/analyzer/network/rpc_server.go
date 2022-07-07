package network

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/Kindling-project/kindling/collector/pkg/model"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func StartRpcReceiver(na *NetworkAnalyzer) {
	rpcServer.setNetworkAnalyzer(na)
	go startRpcServer(na)

	rpcClients.SetRpcCfg(na.cfg.RpcPort, na.cfg.RpcCacheSize, na.telemetry)
	go consumerAndCheckRpcDatas()
}

func startRpcServer(na *NetworkAnalyzer) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(na.cfg.RpcPort))
	if err != nil {
		na.telemetry.Logger.Error("Fail to listen Grpc Port", zap.Error(err))
	}

	server := grpc.NewServer()
	model.RegisterGrpcServer(server, &rpcDatasServer{})
	if err := server.Serve(lis); err != nil {
		na.telemetry.Logger.Error("Fail to Start Grpc Server", zap.Error(err))
	}
}

func consumerAndCheckRpcDatas() {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			GetRpcServer().rpcDatasCache.Range(func(k, v interface{}) bool {
				rpcCacheDatas := v.(*RpcCacheDatas)
				pairs := rpcCacheDatas.match()
				if len(pairs) > 0 {
					GetRpcServer().na.parseRpcAndDistributeTraceMetric(pairs)
				}

				pairs = rpcCacheDatas.clearExpireDatas(uint64(time.Now().UnixNano()-1000000000), uint64(time.Now().UnixNano()-15000000000))
				if len(pairs) > 0 {
					GetRpcServer().na.parseRpcAndDistributeTraceMetric(pairs)
				}
				return true
			})
			GetRpcClients().rpcClientCache.Range(func(k, v interface{}) bool {
				clientDatas := v.(*rpcClientDatas)
				clientDatas.checkAndSend(uint64(time.Now().UnixNano() - 1000000000))

				return true
			})
		}
	}
}

type rpcDatasServer struct {
	model.UnimplementedGrpcServer
}

func (server *rpcDatasServer) Send(ctx context.Context, in *model.RpcDatas) (*model.RpcReply, error) {
	recvTime := time.Now().UnixNano()
	for _, data := range in.Datas {
		// Set Client Time for expire check.
		data.Timestamp = uint64(recvTime)

		GetRpcServer().cacheRemoteRpcData(data)
	}

	return &model.RpcReply{Result: ""}, nil
}
