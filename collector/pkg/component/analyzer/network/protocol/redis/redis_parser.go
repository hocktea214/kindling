package redis

import (
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/tools"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
)

const (
	ARRAY   = '*'
	BULK    = '$'
	INTEGER = ':'
	STRING  = '+'
	ERROR   = '-'
)

type ParseRedisFn func(message *RedisAttributes) (ok bool, complete bool)

var REDIS_FNS = map[byte]ParseRedisFn{
	ARRAY:   parseRedisArray(),
	BULK:    parseRedisBulkString(),
	INTEGER: parseRedisInteger(),
	STRING:  parseRedisSimpleString(),
	ERROR:   parseRedisError(),
}

func NewRedisParser() *protocol.ProtocolParser {
	return protocol.NewSequenceParser(protocol.REDIS, parseHead, parsePayload)
}

func parseHead(data []byte, size int64, isRequest bool) (attributes protocol.ProtocolMessage, waitNextPkt bool) {
	if len(data) < 4 {
		return
	}
	firstChar := data[0]
	if firstChar == ARRAY || firstChar == BULK || firstChar == INTEGER {
		attributes = NewRedisAttributes(data, size, isRequest)
	}
	if !isRequest && (firstChar == STRING || firstChar == ERROR) {
		attributes = NewRedisAttributes(data, size, isRequest)
	}
	return
}

func parsePayload(attributes protocol.ProtocolMessage, isRequest bool) bool {
	message := attributes.(*RedisAttributes)
	for {
		if redisFn, exist := REDIS_FNS[message.Data[message.Offset]]; exist {
			ok, complete := redisFn(message)
			if !ok {
				return false
			}
			if complete {
				return true
			}
		}
	}
}

type RedisAttributes struct {
	*protocol.PayloadMessage
	contentKey string
	command    string
	errorMsg   string
}

func NewRedisAttributes(data []byte, size int64, isRequest bool) *RedisAttributes {
	return &RedisAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, 0, size, isRequest),
	}
}

func (redis *RedisAttributes) MergeRequest(request protocol.ProtocolMessage) bool {
	if request != nil {
		requestAttributes := request.(*RedisAttributes)
		redis.contentKey = requestAttributes.contentKey
		redis.command = requestAttributes.command
	}
	return true
}

func (redis *RedisAttributes) GetAttributes() *model.AttributeMap {
	attributeMap := model.NewAttributeMap()
	if len(redis.command) > 0 {
		attributeMap.AddStringValue(constlabels.ContentKey, tools.FormatStringToUtf8(redis.command))
		attributeMap.AddStringValue(constlabels.RedisCommand, tools.FormatStringToUtf8(redis.command))
	}
	if len(redis.errorMsg) > 0 {
		attributeMap.AddStringValue(constlabels.RedisErrMsg, tools.FormatStringToUtf8(redis.errorMsg))
		attributeMap.AddBoolValue(constlabels.IsError, true)
		attributeMap.AddIntValue(constlabels.ErrorType, int64(constlabels.ProtocolError))
	}
	return attributeMap
}
