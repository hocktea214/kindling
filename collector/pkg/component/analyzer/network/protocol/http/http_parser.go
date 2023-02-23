package http

import (
	"strconv"
	"strings"

	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/tools"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
	"github.com/Kindling-project/kindling/collector/pkg/urlclustering"
)

var (
	clusteringMethod urlclustering.ClusteringMethod = urlclustering.NewBlankClusteringMethod()

	httpMethodsList = map[string]bool{
		"GET":     true,
		"POST":    true,
		"PUT":     true,
		"DELETE":  true,
		"HEAD":    true,
		"TRACE":   true,
		"OPTIONS": true,
		"CONNECT": true,
	}

	splitMethodsList = map[string][]byte{
		"ET": {'G', 'E', 'T'},
	}

	httpVersoinList = map[string]bool{
		"HTTP/1.0": true,
		"HTTP/1.1": true,
	}
)

func NewHttpParser(urlClusteringMethod string) *protocol.ProtocolParser {
	clusteringMethod = urlclustering.NewMethod(urlClusteringMethod)
	return protocol.NewSequenceParser(protocol.HTTP, parseHead, parsePayload)
}

func parseHead(payload []byte, size int64, isRequest bool) (attributes protocol.ProtocolMessage) {
	if len(payload) < 14 {
		return nil
	}
	if isRequest {
		return parseRequestHead(payload, size)
	} else {
		return parseResponseHead(payload, size)
	}
}

func parseRequestHead(payload []byte, size int64) (attributes protocol.ProtocolMessage) {
	var (
		method []byte
		url    []byte
	)
	/*
		Request line
			Method [GET/POST/PUT/DELETE/HEAD/TRACE/OPTIONS/CONNECT]
			Blank
			Request-URI [eg. /xxx/yyy?parm0=aaa&param1=bbb]
			Blank
			HTTP-Version [HTTP/1.0 | HTTP/1.2]
			\r\n

		Request header
	*/
	offset := 0
	offset, method = protocol.ReadUntilBlankWithLength(payload, offset, 8)
	if !httpMethodsList[string(method)] {
		if payload[offset-1] != ' ' || payload[offset] != '/' {
			return nil
		}
		// FIX ET /xxx Data with split payload.
		if replaceMethod, ok := splitMethodsList[string(method)]; ok {
			method = replaceMethod
		} else {
			return nil
		}
	}
	_, url = protocol.ReadUntilBlank(payload, offset)
	contentKey := clusteringMethod.Clustering(string(url))
	if len(contentKey) == 0 {
		contentKey = "*"
	}
	return NewHttpRequestAttributes(payload, size, string(method), tools.FormatByteArrayToUtf8(url), tools.FormatStringToUtf8(contentKey))
}

func parseResponseHead(payload []byte, size int64) (attributes protocol.ProtocolMessage) {
	var (
		version     []byte
		statusCodeI int64
		err         error
	)
	/*
		Status line
			HTTP-Version[HTTP/1.0 | HTTP/1.1]
			Blank
			Status-Code
			Blank
			Reason-Phrase
			\r\n

		Response header
	*/
	offset := 0
	offset, version = protocol.ReadUntilBlankWithLength(payload, offset, 9)
	if !httpVersoinList[string(version)] || payload[offset-1] != ' ' {
		return nil
	}

	_, statusCode := protocol.ReadUntilBlankWithLength(payload, offset, 6)
	if statusCodeI, err = strconv.ParseInt(string(statusCode), 10, 0); err != nil {
		return nil
	}

	if statusCodeI > 999 || statusCodeI < 99 {
		statusCodeI = 0
	}
	return NewHttpResponseAttributes(payload, size, statusCodeI)
}

func parsePayload(attributes protocol.ProtocolMessage) (ok bool) {
	message := attributes.(*HttpAttributes)
	if len(message.traceId) == 0 || len(message.traceType) == 0 {
		message.parseTraceHeader()
	}
	return true
}

type HttpAttributes struct {
	*protocol.PayloadMessage
	method     string
	url        string
	contentKey string
	traceType  string
	traceId    string
	statusCode int64
}

func NewHttpRequestAttributes(data []byte, size int64, method string, url string, contentKey string) *HttpAttributes {
	return &HttpAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, 0, size, true),
		method:         method,
		url:            url,
		contentKey:     contentKey,
	}
}

func NewHttpResponseAttributes(data []byte, size int64, statusCode int64) *HttpAttributes {
	return &HttpAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, 0, size, false),
		statusCode:     statusCode,
	}
}

func (http *HttpAttributes) MergeRequest(request protocol.ProtocolMessage) bool {
	if request != nil {
		requestAttributes := request.(*HttpAttributes)
		http.method = requestAttributes.method
		http.url = requestAttributes.url
		http.contentKey = requestAttributes.contentKey
		http.traceType = requestAttributes.traceType
		http.traceId = requestAttributes.traceId
	}
	return true
}

func (http *HttpAttributes) GetAttributes() *model.AttributeMap {
	attributeMap := model.NewAttributeMap()
	attributeMap.AddStringValue(constlabels.HttpMethod, http.method)
	attributeMap.AddStringValue(constlabels.HttpUrl, http.url)
	attributeMap.AddStringValue(constlabels.ContentKey, http.contentKey)
	if len(http.traceType) > 0 && len(http.traceId) > 0 {
		attributeMap.AddStringValue(constlabels.HttpApmTraceType, http.traceType)
		attributeMap.AddStringValue(constlabels.HttpApmTraceId, http.traceId)
	}
	if http.statusCode == 100 {
		// Add http_continue for merging the subsequent request.
		// See the issue https://github.com/KindlingProject/kindling/issues/388 for details.
		attributeMap.AddBoolValue(constlabels.HttpContinue, true)
	}
	attributeMap.AddIntValue(constlabels.HttpStatusCode, http.statusCode)
	if http.statusCode >= 400 {
		attributeMap.AddBoolValue(constlabels.IsError, true)
		attributeMap.AddIntValue(constlabels.ErrorType, int64(constlabels.ProtocolError))
	}
	return attributeMap
}

func (http *HttpAttributes) parseTraceHeader() {
	headers := parseHeaders(http.Data)
	traceType, traceId := tools.ParseTraceHeader(headers)

	http.traceType = traceType
	http.traceId = traceId
}

func parseHeaders(payload []byte) map[string]string {
	header := make(map[string]string)

	/*
		Requet-Line\r\n
		Key:Value\r\n
		...
		Key:Value\r\n
		\r\n
		Data
	*/
	from, data := protocol.ReadUntilCRLF(payload, 0)
	if data == nil {
		return header
	}
	for {
		from, data = protocol.ReadUntilCRLF(payload, from)
		if data == nil {
			return header
		}
		if position := strings.Index(string(data), ":"); position > 0 && position < len(data)-2 {
			header[strings.ToLower(string(data[0:position]))] = string(data[position+2:])
			continue
		}
		return header
	}
}
