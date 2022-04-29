package otelexporter

import (
	"context"
	"github.com/Kindling-project/kindling/collector/consumer/exporter/otelexporter/defaultadapter"
	"github.com/Kindling-project/kindling/collector/model"
	"github.com/Kindling-project/kindling/collector/model/constvalues"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	apitrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (e *OtelExporter) Consume(gaugeGroup *model.GaugeGroup) error {
	if gaugeGroup == nil {
		// no need consume
		return nil
	}

	gaugeGroupReceiverCounter.Add(context.Background(), 1, attribute.String("name", gaugeGroup.Name))
	if ce := e.telemetry.Logger.Check(zap.DebugLevel, "exporter receives a gaugeGroup: "); ce != nil {
		ce.Write(
			zap.String("gaugeGroup", gaugeGroup.String()),
		)
	}

	for i := 0; i < len(e.adapters); i++ {
		results, err := e.adapters[i].Adapt(gaugeGroup)
		if err != nil {
			e.telemetry.Logger.Error("Failed to adapt gaugeGroup", zap.Error(err))
		}
		if results != nil && len(results) > 0 {
			e.Export(results)
		}
	}
	return nil
}

func (e *OtelExporter) Export(results []*defaultadapter.AdaptedResult) {
	for i := 0; i < len(results); i++ {
		result := results[i]
		switch result.ResultType {
		case defaultadapter.Metric:
			e.exportMetric(result)
		case defaultadapter.Trace:
			e.exportTrace(result)
		default:
			e.telemetry.Logger.Error("Unexpected ResultType", zap.String("type", string(result.ResultType)))
		}
		result.Free()
	}
}

func (e *OtelExporter) exportTrace(result *defaultadapter.AdaptedResult) {
	if e.defaultTracer == nil {
		e.telemetry.Logger.Error("Send span failed: this exporter doesn't support Span Data", zap.String("exporter", e.cfg.ExportKind))
	}
	_, span := e.defaultTracer.Start(
		context.Background(),
		constvalues.SpanInfo,
		apitrace.WithAttributes(result.AttrsList...),
	)
	span.End()
}

func (e *OtelExporter) exportMetric(result *defaultadapter.AdaptedResult) {
	measurements := make([]metric.Measurement, 0, len(result.Gauges))
	for s := 0; s < len(result.Gauges); s++ {
		gauge := result.Gauges[s]
		if metricKind, ok := e.findInstrumentKind(gauge.Name); ok && metricKind == MAGaugeKind {
			if result.AttrsMap == nil {
				e.telemetry.Logger.Error("Unexpected Error: no labels find for MAGaugeKind", zap.String("GaugeName", gauge.Name))
			}
			err := e.instrumentFactory.recordLastValue(gauge.Name, &model.GaugeGroup{
				Name:      gauge.Name,
				Values:    []*model.Gauge{{gauge.Name, gauge.Value}},
				Labels:    result.AttrsMap,
				Timestamp: result.Timestamp,
			})
			if err != nil {
				e.telemetry.Logger.Error("Failed to record Gauge", zap.Error(err))
			}
		} else if ok {
			measurements = append(measurements, e.instrumentFactory.getInstrument(gauge.Name, metricKind).Measurement(gauge.Value))
		} else {
			e.telemetry.Logger.Warn("Undefined metricKind for this Gauge", zap.String("GaugeName", gauge.Name))
		}
	}
	if len(measurements) > 0 {
		e.instrumentFactory.meter.RecordBatch(context.Background(), result.AttrsList, measurements...)
	}
}
