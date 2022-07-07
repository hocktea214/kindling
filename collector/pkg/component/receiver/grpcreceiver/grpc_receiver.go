package grpcreceiver

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/Kindling-project/kindling/collector/pkg/component"
	analyzerpackage "github.com/Kindling-project/kindling/collector/pkg/component/analyzer"
	networkpackage "github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network"
	"github.com/Kindling-project/kindling/collector/pkg/component/receiver"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	Grpc = "grpcreceiver"
)

type GrpcReceiver struct {
	cfg             *Config
	server          *grpc.Server
	analyzerManager *analyzerpackage.Manager
	telemetry       *component.TelemetryTools
}

type Config struct {
	Port int `mapstructure:"port"`
}

func NewGrpcReceiver(config interface{}, telemetry *component.TelemetryTools, analyzerManager *analyzerpackage.Manager) receiver.Receiver {
	cfg, ok := config.(*Config)
	if !ok {
		telemetry.Logger.Sugar().Panicf("Cannot convert [%s] config", Grpc)
	}
	grpcReceiver := &GrpcReceiver{
		cfg:             cfg,
		analyzerManager: analyzerManager,
		telemetry:       telemetry,
	}
	return grpcReceiver
}

func (r *GrpcReceiver) Type() string {
	return Grpc
}

func (r *GrpcReceiver) Start() error {
	var err error
	if r.cfg.Port > 0 {
		networkpackage.GetRpcCache().SetGrpcPort(r.cfg.Port)

		lis, err := net.Listen("tcp", ":"+strconv.Itoa(r.cfg.Port))
		if err != nil {
			r.telemetry.Logger.Error("Fail to listen Grpc Port", zap.Error(err))
		}
		r.server = grpc.NewServer()
		model.RegisterGrpcServer(r.server, &grpcServer{receiver: r})
		if err := r.server.Serve(lis); err != nil {
			r.telemetry.Logger.Error("Fail to Start Grpc Server", zap.Error(err))
		}
	}
	return err
}

func (r *GrpcReceiver) Shutdown() error {
	var err error
	if r.server != nil {
		r.server.GracefulStop()
	}
	return err
}

func (r *GrpcReceiver) SendToNextConsumer(evt *model.KindlingEvent) error {
	analyzers := r.analyzerManager.GetConsumableAnalyzers(evt.Name)
	if len(analyzers) == 0 {
		r.telemetry.Logger.Info("analyzer not found for event ", zap.String("eventName", evt.Name))
		return nil
	}
	for _, analyzer := range analyzers {
		err := analyzer.ConsumeEvent(evt)
		if err != nil {
			return err
		}
		if err != nil {
			r.telemetry.Logger.Warn("Error sending event to next consumer: ", zap.Error(err))
		}
	}
	return nil
}

func (r *GrpcReceiver) handleRpcRequest(message string) (*model.GrpcReply, error) {
	recvTime := time.Now().UnixNano()

	var evt model.KindlingEvent
	err := json.Unmarshal([]byte(message), &evt)
	if err != nil {
		log.Printf("Error Occured in Unmarshal: %v\n", err)
	}
	// Set Client Time for expire check.
	evt.Timestamp = uint64(recvTime)
	r.SendToNextConsumer(&evt)
	return &model.GrpcReply{Type: model.Type_RPC, Result: strconv.FormatInt(recvTime, 10)}, nil
}

type grpcServer struct {
	model.UnimplementedGrpcServer
	receiver *GrpcReceiver
}

func (server *grpcServer) Send(ctx context.Context, req *model.GrpcParam) (*model.GrpcReply, error) {
	switch req.Type {
	case model.Type_RPC:
		return server.receiver.handleRpcRequest(req.Data)
	}

	return &model.GrpcReply{Result: ""}, nil
}
