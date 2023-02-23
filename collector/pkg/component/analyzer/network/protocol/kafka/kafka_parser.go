package kafka

import (
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
)

func NewKafkaParser() *protocol.ProtocolParser {
	return protocol.NewStreamParser(protocol.KAFKA, parseHead, parsePayload)
}

func parseHead(data []byte, size int64, isRequest bool) (attributes protocol.ProtocolMessage, waitNextPkt bool) {
	if isRequest {
		return parseRequestHead(data, size)
	} else {
		return parseResponseHead(data, size)
	}
}

func parseRequestHead(data []byte, size int64) (attributes protocol.ProtocolMessage, waitNextPkt bool) {
	if size < 12 {
		return nil, true
	}
	if len(data) < 12 {
		return
	}
	var (
		payloadLength  int32
		apiKey         int16
		apiVersion     int16
		correlationId  int32
		clientIdLength int16
	)
	protocol.ReadInt32(data, 0, &payloadLength)
	if payloadLength <= 8 {
		return
	}

	protocol.ReadInt16(data, 4, &apiKey)
	protocol.ReadInt16(data, 6, &apiVersion)
	if !IsValidVersion(int(apiKey), int(apiVersion)) {
		return
	}
	protocol.ReadInt32(data, 8, &correlationId)
	protocol.ReadInt16(data, 12, &clientIdLength)
	if correlationId < 0 || clientIdLength < 0 {
		return
	}
	var offset = int(clientIdLength) + 14
	if len(data) < offset {
		return
	}
	attributes = NewKafkaRequestAttributes(data, apiKey, apiVersion, correlationId, offset, payloadLength+4)
	return
}

func parseResponseHead(data []byte, size int64) (attributes protocol.ProtocolMessage, waitNextPkt bool) {
	if size < 8 {
		return nil, true
	}
	if len(data) < 8 {
		return
	}
	var (
		payloadLength int32
		correlationId int32
	)
	protocol.ReadInt32(data, 0, &payloadLength)
	if payloadLength <= 4 {
		return
	}
	protocol.ReadInt32(data, 4, &correlationId)
	attributes = NewKafkaResponseAttributes(data, correlationId, 8, payloadLength+4)
	return
}

func parsePayload(attributes protocol.ProtocolMessage) (ok bool) {
	message := attributes.(*KafkaAttributes)
	if message.apiKey == _apiFetch {
		return parseFetchPayload(message)
	}
	if message.apiKey == _apiProduce {
		return parseProducePayload(message)
	}
	return true
}

type KafkaAttributes struct {
	*protocol.PayloadMessage

	apiKey        int16
	apiVersion    int16
	correlationId int32
	topicName     string
	errorCode     int16
}

func NewKafkaRequestAttributes(data []byte, apiKey int16, apiVersion int16, correlationId int32, headLength int, payloadLength int32) *KafkaAttributes {
	return &KafkaAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, headLength, int64(payloadLength), true),
		apiKey:         apiKey,
		apiVersion:     apiVersion,
		correlationId:  correlationId,
	}
}

func NewKafkaResponseAttributes(data []byte, correlationId int32, headLength int, payloadLength int32) *KafkaAttributes {
	return &KafkaAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, headLength, int64(payloadLength), false),
		correlationId:  correlationId,
	}
}

func (kafka *KafkaAttributes) GetStreamId() int64 {
	return int64(kafka.correlationId)
}

func (kafka *KafkaAttributes) MergeRequest(request protocol.ProtocolMessage) bool {
	if request != nil {
		requestAttributes := request.(*KafkaAttributes)
		if requestAttributes.correlationId != kafka.correlationId {
			return false
		}
		kafka.apiKey = requestAttributes.apiKey
		kafka.apiVersion = requestAttributes.apiVersion
		kafka.topicName = requestAttributes.topicName
	}
	return true
}

func (kafka *KafkaAttributes) GetAttributes() *model.AttributeMap {
	attributeMap := model.NewAttributeMap()
	attributeMap.AddIntValue(constlabels.KafkaApi, int64(kafka.apiKey))
	attributeMap.AddIntValue(constlabels.KafkaVersion, int64(kafka.apiVersion))
	attributeMap.AddIntValue(constlabels.KafkaCorrelationId, int64(kafka.correlationId))
	if len(kafka.topicName) > 0 {
		attributeMap.AddStringValue(constlabels.KafkaTopic, kafka.topicName)
	}
	// if kafka.errorCode > 0 {
	attributeMap.AddIntValue(constlabels.KafkaErrorCode, int64(kafka.errorCode))
	// }
	return attributeMap
}
