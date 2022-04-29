package model

import (
	"reflect"
	"testing"
)

func TestGaugeGroup_RemoveGauge(t *testing.T) {
	type fields struct {
		Values []*Gauge
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   fields
	}{
		{
			name: "normal",
			fields: fields{
				Values: []*Gauge{
					{Name: "a", Value: 1},
					{Name: "b", Value: 2},
					{Name: "c", Value: 3},
				},
			},
			args: args{name: "b"},
			want: fields{
				Values: []*Gauge{
					{Name: "a", Value: 1},
					{Name: "c", Value: 3},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GaugeGroup{
				Values: tt.fields.Values,
			}
			g.RemoveGauge(tt.args.name)
			// For output string
			wantG := &GaugeGroup{Values: tt.want.Values}
			if !reflect.DeepEqual(g.Values, tt.want.Values) {
				t.Errorf("expected %s, got %s", wantG, g)
			}
		})
	}
}
