package application

import (
	"flag"
	"fmt"

	"github.com/Kindling-project/kindling/collector/pkg/component"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/loganalyzer"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/tcpconnectanalyzer"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/tcpmetricanalyzer"
	"github.com/Kindling-project/kindling/collector/pkg/component/consumer"
	"github.com/Kindling-project/kindling/collector/pkg/component/consumer/exporter/logexporter"
	"github.com/Kindling-project/kindling/collector/pkg/component/consumer/exporter/otelexporter"
	"github.com/Kindling-project/kindling/collector/pkg/component/consumer/processor/aggregateprocessor"
	"github.com/Kindling-project/kindling/collector/pkg/component/consumer/processor/k8sprocessor"
	"github.com/Kindling-project/kindling/collector/pkg/component/receiver"
	"github.com/Kindling-project/kindling/collector/pkg/component/receiver/cgoreceiver"
	"github.com/Kindling-project/kindling/collector/pkg/component/receiver/grpcreceiver"
	"github.com/hashicorp/go-multierror"
	"github.com/spf13/viper"
)

type Application struct {
	viper             *viper.Viper
	componentsFactory *ComponentsFactory
	telemetry         *component.TelemetryManager
	receiverManager   *receiver.Manager
	analyzerManagers  []*analyzer.Manager
}

func New() (*Application, error) {
	app := &Application{
		viper:             viper.New(),
		componentsFactory: NewComponentsFactory(),
		telemetry:         component.NewTelemetryManager(),
	}
	app.registerFactory()
	// Initialize flags
	configPath := flag.String("config", "kindling-collector-config.yml", "Configuration file")
	flag.Parse()
	err := app.readInConfig(*configPath)
	if err != nil {
		return nil, fmt.Errorf("fail to read configuration: %w", err)
	}
	// Build processing pipeline
	err = app.buildPipeline()
	if err != nil {
		return nil, fmt.Errorf("failed to build pipeline: %w", err)
	}
	return app, nil
}

func (a *Application) Run() error {
	for _, analyzerManager := range a.analyzerManagers {
		err := analyzerManager.StartAll(a.telemetry.Telemetry.Logger)
		if err != nil {
			return fmt.Errorf("failed to start application: %v", err)
		}
	}
	// Wait until the receiver shutdowns
	err := a.receiverManager.StartAll(a.telemetry.Telemetry.Logger)
	if err != nil {
		return fmt.Errorf("failed to start application: %v", err)
	}
	return nil
}

func (a *Application) Shutdown() error {
	var retErr error = nil
	var err error = nil
	for _, analyzerManager := range a.analyzerManagers {
		err = analyzerManager.ShutdownAll(a.telemetry.Telemetry.Logger)
		if err != nil {
			multierror.Append(retErr, err)
		}
	}
	err = a.receiverManager.ShutdownAll(a.telemetry.Telemetry.Logger)
	if err != nil {
		multierror.Append(retErr, err)
	}
	return retErr
}

func initFlags() error {
	return nil
}

func (a *Application) registerFactory() {
	a.componentsFactory.RegisterReceiver(cgoreceiver.Cgo, cgoreceiver.NewCgoReceiver, &cgoreceiver.Config{})
	a.componentsFactory.RegisterAnalyzer(network.Network.String(), network.NewNetworkAnalyzer, &network.Config{})
	a.componentsFactory.RegisterProcessor(k8sprocessor.K8sMetadata, k8sprocessor.NewKubernetesProcessor, &k8sprocessor.DefaultConfig)
	a.componentsFactory.RegisterExporter(otelexporter.Otel, otelexporter.NewExporter, &otelexporter.Config{})
	a.componentsFactory.RegisterAnalyzer(tcpmetricanalyzer.TcpMetric.String(), tcpmetricanalyzer.NewTcpMetricAnalyzer, &tcpmetricanalyzer.Config{})
	a.componentsFactory.RegisterExporter(logexporter.Type, logexporter.New, &logexporter.Config{})
	a.componentsFactory.RegisterAnalyzer(loganalyzer.Type.String(), loganalyzer.New, &loganalyzer.Config{})
	a.componentsFactory.RegisterProcessor(aggregateprocessor.Type, aggregateprocessor.New, aggregateprocessor.NewDefaultConfig())
	a.componentsFactory.RegisterAnalyzer(tcpconnectanalyzer.Type.String(), tcpconnectanalyzer.New, tcpconnectanalyzer.NewDefaultConfig())
	a.componentsFactory.RegisterReceiver(grpcreceiver.Grpc, grpcreceiver.NewGrpcReceiver, &grpcreceiver.Config{})
	a.componentsFactory.RegisterAnalyzer(network.Rpc.String(), network.NewRpcAnalyzer, &network.RpcConfig{})
}

func (a *Application) readInConfig(path string) error {
	a.viper.SetConfigFile(path)
	err := a.viper.ReadInConfig()
	if err != nil { // Handle errors reading the config file
		return fmt.Errorf("error happened while reading config file: %w", err)
	}
	a.telemetry.ConstructConfig(a.viper)
	err = a.componentsFactory.ConstructConfig(a.viper)
	if err != nil {
		return fmt.Errorf("error happened while constructing config: %w", err)
	}
	return nil
}

// buildPipeline builds a event processing pipeline based on hard-code.
func (a *Application) buildPipeline() error {
	// TODO: Build pipeline via configuration to implement dependency injection
	// Initialize exporters
	otelExporterFactory := a.componentsFactory.Exporters[otelexporter.Otel]
	otelExporter := otelExporterFactory.NewFunc(otelExporterFactory.Config, a.telemetry.Telemetry)
	// Initialize all processors
	// 1. DataGroup Aggregator
	aggregateProcessorFactory := a.componentsFactory.Processors[aggregateprocessor.Type]
	aggregateProcessor := aggregateProcessorFactory.NewFunc(aggregateProcessorFactory.Config, a.telemetry.Telemetry, otelExporter)
	// 2. Kubernetes metadata processor
	k8sProcessorFactory := a.componentsFactory.Processors[k8sprocessor.K8sMetadata]
	k8sMetadataProcessor := k8sProcessorFactory.NewFunc(k8sProcessorFactory.Config, a.telemetry.Telemetry, aggregateProcessor)
	// Initialize all analyzers
	// 1. Common network request analyzer
	networkAnalyzerFactory := a.componentsFactory.Analyzers[network.Network.String()]
	// Now NetworkAnalyzer must be initialized before any other analyzers, because it will
	// use its configuration to initialize the conntracker module which is also used by others.
	networkAnalyzer := networkAnalyzerFactory.NewFunc(networkAnalyzerFactory.Config, a.telemetry.Telemetry, []consumer.Consumer{k8sMetadataProcessor})
	// 2. Layer 4 TCP events analyzer
	aggregateProcessorForTcp := aggregateProcessorFactory.NewFunc(aggregateProcessorFactory.Config, a.telemetry.Telemetry, otelExporter)
	k8sMetadataProcessor2 := k8sProcessorFactory.NewFunc(k8sProcessorFactory.Config, a.telemetry.Telemetry, aggregateProcessorForTcp)
	tcpAnalyzerFactory := a.componentsFactory.Analyzers[tcpmetricanalyzer.TcpMetric.String()]
	tcpAnalyzer := tcpAnalyzerFactory.NewFunc(tcpAnalyzerFactory.Config, a.telemetry.Telemetry, []consumer.Consumer{k8sMetadataProcessor2})
	tcpConnectAnalyzerFactory := a.componentsFactory.Analyzers[tcpconnectanalyzer.Type.String()]
	tcpConnectAnalyzer := tcpConnectAnalyzerFactory.NewFunc(tcpConnectAnalyzerFactory.Config, a.telemetry.Telemetry, []consumer.Consumer{k8sMetadataProcessor})
	// Initialize receiver packaged with multiple analyzers
	analyzerManager, err := analyzer.NewManager(networkAnalyzer, tcpAnalyzer, tcpConnectAnalyzer)
	if err != nil {
		return fmt.Errorf("error happened while creating analyzer manager: %w", err)
	}

	rpcAnalyzerFactory := a.componentsFactory.Analyzers[network.Rpc.String()]
	rpcAnalyzer := rpcAnalyzerFactory.NewFunc(networkAnalyzerFactory.Config, a.telemetry.Telemetry, []consumer.Consumer{})
	analyzerManager2, err := analyzer.NewManager(rpcAnalyzer)
	if err != nil {
		return fmt.Errorf("error happened while creating analyzer manager2: %w", err)
	}
	a.analyzerManagers = []*analyzer.Manager{analyzerManager, analyzerManager2}

	cgoReceiverFactory := a.componentsFactory.Receivers[cgoreceiver.Cgo]
	cgoReceiver := cgoReceiverFactory.NewFunc(cgoReceiverFactory.Config, a.telemetry.Telemetry, analyzerManager)

	grpcReceiverFactory := a.componentsFactory.Receivers[grpcreceiver.Grpc]
	grpcReceiver := grpcReceiverFactory.NewFunc(grpcReceiverFactory.Config, a.telemetry.Telemetry, analyzerManager2)

	receiverManager, err := receiver.NewManager(cgoReceiver, grpcReceiver)
	if err != nil {
		return fmt.Errorf("error happened while creating receiver manager: %w", err)
	}
	a.receiverManager = receiverManager
	return nil
}