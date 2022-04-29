package defaultaggregator

import (
	"github.com/Kindling-project/kindling/collector/model"
	"github.com/Kindling-project/kindling/collector/pkg/aggregator"
	"sync"
	"testing"
	"time"
)

func TestConcurrentAggregator(t *testing.T) {
	aggKindMap := &AggregatedConfig{KindMap: map[string][]KindConfig{
		"duration": {
			{Kind: SumKind, OutputName: "duration_sum"},
			{Kind: CountKind, OutputName: "request_count"},
		},
	}}

	aggregatorInstance := NewDefaultAggregator(aggKindMap)
	labels := model.NewAttributeMap()
	labels.AddStringValue("key", "value")

	labelSelectors := aggregator.NewLabelSelectors(
		aggregator.LabelSelector{Name: "key", VType: aggregator.StringType},
	)

	wg := sync.WaitGroup{}
	wg.Add(1)
	stopCh := make(chan bool)

	var runLoop = 100000
	var duration int64 = 100
	go func() {
		for i := 0; i < runLoop; i++ {
			gaugeValues := []*model.Gauge{
				{"duration", duration},
			}
			gaugeGroup := model.NewGaugeGroup("testGauge", labels, 0, gaugeValues...)
			aggregatorInstance.Aggregate(gaugeGroup, labelSelectors)
			time.Sleep(time.Microsecond)
		}
		stopCh <- true
		wg.Done()
	}()

	wg.Add(1)
	var durationSum int64
	var requestCount int64
	go func() {
		ticker := time.NewTicker(500 * time.Microsecond)
		for {
			select {
			case <-ticker.C:
				ret := aggregatorInstance.Dump()
				for _, g := range ret {
					for _, v := range g.Values {
						if v.Name == "duration_sum" {
							durationSum += v.Value
						} else if v.Name == "request_count" {
							requestCount += v.Value
						}
					}
				}
			case <-stopCh:
				ret := aggregatorInstance.Dump()
				for _, g := range ret {
					for _, v := range g.Values {
						if v.Name == "duration_sum" {
							durationSum += v.Value
						} else if v.Name == "request_count" {
							requestCount += v.Value
						}
					}
				}
				wg.Done()
				return
			}
		}
	}()
	wg.Wait()
	expectedDurationSum := int64(runLoop) * duration
	expectedRequestCount := int64(runLoop)
	if durationSum != expectedDurationSum || requestCount != expectedRequestCount {
		t.Errorf("request_count: exptected %v, got %v", expectedRequestCount, requestCount)
		t.Errorf("duration_sum: expected %v, got %v", expectedDurationSum, durationSum)
	}
}
