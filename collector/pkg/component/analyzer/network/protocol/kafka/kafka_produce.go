package kafka

import (
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/tools"
)

func parseProducePayload(message *KafkaAttributes) (ok bool) {
	if message.IsRequest() {
		return parseRequestProduce(message)
	}
	return parseResponseProduce(message)
}

func parseRequestProduce(message *KafkaAttributes) (ok bool) {
	var (
		offset    int
		err       error
		topicNum  int32
		topicName string
	)
	version := message.apiVersion
	compact := version >= 9
	offset = message.Offset

	if version >= 3 {
		var transactionId string
		if offset, err = message.ReadNullableString(offset, compact, &transactionId); err != nil {
			return false
		}
	}
	// acks, timeout_ms
	offset += 6
	if offset, err = message.ReadArraySize(offset, compact, &topicNum); err != nil {
		return false
	}
	if topicNum > 0 {
		if _, err = message.ReadString(offset, compact, &topicName); err != nil {
			return false
		}
		// Get TopicName
		message.topicName = tools.FormatStringToUtf8(topicName)
	}
	return true
}

func parseResponseProduce(message *KafkaAttributes) (ok bool) {
	var (
		offset       int
		err          error
		topicNum     int32
		topicName    string
		partitionNum int32
		errorCode    int16
	)
	version := message.apiVersion
	compact := version >= 9
	offset = message.Offset
	if offset, err = message.ReadArraySize(offset, compact, &topicNum); err != nil {
		return false
	}
	if topicNum > 0 {
		if offset, err = message.ReadString(offset, compact, &topicName); err != nil {
			return false
		}
		if offset, err = message.ReadArraySize(offset, compact, &partitionNum); err != nil {
			return false
		}
		if partitionNum > 0 {
			offset += 4
			// Read ErrorCode in First Partition
			if _, err = protocol.ReadInt16(message.Data, offset, &errorCode); err != nil {
				return false
			}
		}
		// Get topicName
		message.topicName = tools.FormatStringToUtf8(topicName)
	}
	message.errorCode = errorCode
	return true
}
