package kafka

import (
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/tools"
)

func parseFetchPayload(message *KafkaAttributes, isRequest bool) (ok bool) {
	if isRequest {
		return parseRequestFetch(message)
	}
	return parseResponseFetch(message)
}

func parseRequestFetch(message *KafkaAttributes) (ok bool) {
	var (
		offset    int
		err       error
		topicNum  int32
		topicName string
	)
	version := message.apiVersion
	compact := version >= 12

	// replica_id, max_wait_ms, min_bytes,
	offset = message.Offset + 12
	if version >= 3 {
		offset += 4 // max_bytes
	}
	if version >= 4 {
		offset += 1 // isolation_level
	}
	if version >= 7 {
		offset += 8 // session_id, session_epoch
	}

	if offset, err = message.ReadArraySize(offset, compact, &topicNum); err != nil {
		return false
	}
	if topicNum > 0 {
		if _, err = message.ReadString(offset, compact, &topicName); err != nil {
			return false
		}
		/*
			Based on following case, we	just read first topicName.

			1. The payload is substr with fixed length(1K), it's not able to get all topic names
			2. Even if we get enough length for payload, the parser will take more performance cost
			3. There is not enough cases to cover multi-topics

			Since version 13, topicName will be repalced with topicId as uuid, therefore topicName is not able to be got.
		*/
		message.topicName = tools.FormatStringToUtf8(topicName)
	}

	return true
}

func parseResponseFetch(message *KafkaAttributes) (ok bool) {
	var (
		offset       int
		err          error
		topicNum     int32
		topicName    string
		partitionNum int32
		errorCode    int16
	)

	version := message.apiVersion
	compact := version >= 12
	offset = message.Offset

	if version >= 1 {
		offset += 4 // throttle_time_ms
	}
	if version >= 7 {
		if offset, err = protocol.ReadInt16(message.Data, offset, &errorCode); err != nil {
			return false
		}
		offset += 4 //session_id
	}

	if offset, err = message.ReadArraySize(offset, compact, &topicNum); err != nil {
		return false
	}

	if topicNum > 0 {
		if offset, err = message.ReadString(offset, compact, &topicName); err != nil {
			return false
		}

		if version < 7 {
			if offset, err = message.ReadArraySize(offset, compact, &partitionNum); err != nil {
				return false
			}
			if partitionNum > 0 {
				offset += 4
				// Read ErrorCode in First Partition when version less than 7.
				if _, err = protocol.ReadInt16(message.Data, offset, &errorCode); err != nil {
					return false
				}
			}
		}
		// Get TopicName
		// Since version 13, topicName will be repalced with topicId as uuid, therefore topicName is not able to be got.
		message.topicName = tools.FormatStringToUtf8(topicName)
	}
	message.errorCode = errorCode
	return true
}
