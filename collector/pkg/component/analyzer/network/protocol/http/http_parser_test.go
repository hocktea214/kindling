package http

import (
	"reflect"
	"testing"
)

func Test_parseHeaders(t *testing.T) {
	tests := []struct {
		name    string
		message []byte
		want    map[string]string
	}{
		{
			name:    "normal case",
			message: []byte("HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nAPM-AgentID: TTXvC3EQS6KLwxx3eIqINFjAW2olRm+cr8M+yuvwhkY=\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\nAPM-TransactionID: 5e480579c718a4a6498a9"),
			want: map[string]string{
				"connection":        "keep-alive",
				"apm-agentid":       "TTXvC3EQS6KLwxx3eIqINFjAW2olRm+cr8M+yuvwhkY=",
				"transfer-encoding": "chunked",
				"content-type":      "application/json",
				"apm-transactionid": "5e480579c718a4a6498a9",
			},
		},
		{
			name:    "no values",
			message: []byte("HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nTransfer-Encoding: "),
			want: map[string]string{
				"connection": "keep-alive",
			},
		},
		{

			name:    "no spaces",
			message: []byte("HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nTransfer-Encoding:"),
			want: map[string]string{
				"connection": "keep-alive",
			},
		},
		{
			name:    "no colon",
			message: []byte("HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nTransfer-Encoding"),
			want: map[string]string{
				"connection": "keep-alive",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseHeaders(tt.message); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseHeaders() = %v, want %v", got, tt.want)
			}
		})
	}
}
