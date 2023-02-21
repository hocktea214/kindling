package rocketmq

import (
	"encoding/json"
	"fmt"

	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
)

const (
	FlagRequest  = 0
	FlagResponse = 1
	FlagOneway   = 2
)

func NewRocketMQParser() *protocol.ProtocolParser {
	return protocol.NewProtocolParser(protocol.ROCKETMQ, false, parseHead, parsePayload)
}

func parseHead(data []byte, size int64, isRequest bool) (attributes protocol.ProtocolMessage) {
	if len(data) < 29 {
		return
	}
	var (
		payloadLength int32
		header        *rocketmqHeader
	)
	serializeType := data[4]

	protocol.ReadInt32(data, 0, &payloadLength)
	//When serializeType==0, the serialization type is JSON, and the json sequence is directly intercepted for deserialization
	if serializeType == 0 {
		header = parseJsonHeader(data)
	} else if serializeType == 1 {
		header = parseRocketMqHeader(data)
	}
	if header == nil {
		return nil
	}

	if isRequest && (header.Flag != FlagRequest && header.Flag != FlagOneway) {
		return nil
	} else if !isRequest && header.Flag != FlagResponse {
		return nil
	}

	if isRequest {
		var contentKey string
		//topicName maybe be stored in key `topic` or `b`
		if header.ExtFields["topic"] != "" {
			contentKey = fmt.Sprintf("Topic:%v", header.ExtFields["topic"])
		} else if header.ExtFields["b"] != "" {
			contentKey = fmt.Sprintf("Topic:%v", header.ExtFields["b"])
		} else {
			contentKey = requestMsgMap[header.Code]
		}
		return NewRocketmqRequestAttributes(data, 0, size, header.Code, contentKey, header.Opaque)
	} else {
		return NewRocketmqResponseAttributes(data, 0, size, header.Code, header.Remark, header.Opaque)
	}
}

func parsePayload(attributes protocol.ProtocolMessage, isRequest bool) (ok bool) {
	return true
}

func parseJsonHeader(data []byte) *rocketmqHeader {
	var (
		headerLength int32
		headerBytes  []byte
		err          error
		header       *rocketmqHeader = &rocketmqHeader{ExtFields: map[string]string{}}
	)
	protocol.ReadInt32(data, 4, &headerLength)
	if _, headerBytes, err = protocol.ReadBytes(data, 8, int(headerLength)); err != nil {
		return nil
	}
	if err = json.Unmarshal(headerBytes, header); err != nil {
		return nil
	}
	return header
}

/*
RocketMq Header

		   0    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F
		+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
		|   payload length  |     head length   |   Code  |Lang| Version |    Opaque    |
		+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
		|Opaq|        Flag       |    Remark Length  |             Remark               |
		+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
		|                                            |  ExtFields Length | Key Len | Key|
		+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
	    |Value Len| Value   |   ...   | Key Len | Key|Value Len| Value   |
		+----+----+----+----+----+----+----+----+----+----+----+----+----+
*/
func parseRocketMqHeader(data []byte) *rocketmqHeader {
	var (
		offset      int
		remarkLen   int32
		remark      []byte
		extFieldLen int32
		header      *rocketmqHeader = &rocketmqHeader{ExtFields: map[string]string{}}
	)

	protocol.ReadInt16(data, 8, &header.Code)
	header.LanguageCode = data[10]
	protocol.ReadInt16(data, 11, &header.Version)
	protocol.ReadInt32(data, 13, &header.Opaque)
	protocol.ReadInt32(data, 17, &header.Flag)
	offset, _ = protocol.ReadInt32(data, 21, &remarkLen)
	if remarkLen > 0 {
		offset, remark, _ = protocol.ReadBytes(data, offset, int(remarkLen))
		header.Remark = string(remark)
	}
	offset, _ = protocol.ReadInt32(data, offset, &extFieldLen)
	if extFieldLen > 0 && remarkLen == 0 {
		var (
			keyLen   int16
			valueLen int32
			key      []byte
			value    []byte
		)
		extFieldBytesLen := 0
		extFieldMap := make(map[string]string)
		// offset starts from 29
		for extFieldBytesLen < int(extFieldLen) && extFieldBytesLen+29 < len(data) {
			offset, _ = protocol.ReadInt16(data, offset, &keyLen)
			offset, key, _ = protocol.ReadBytes(data, offset, int(keyLen))
			offset, _ = protocol.ReadInt32(data, offset, &valueLen)
			offset, value, _ = protocol.ReadBytes(data, offset, int(valueLen))
			extFieldMap[string(key)] = string(value)
			extFieldBytesLen = extFieldBytesLen + 2 + int(keyLen) + 4 + int(valueLen)
			if string(key) == "topic" || string(key) == "b" {
				break
			}
		}
		//Update the field `ExtFields` of the header
		header.ExtFields = extFieldMap
	}
	return header
}

type RocketmqAttributes struct {
	*protocol.PayloadMessage
	requestCode  int16
	responseCode int16
	contentKey   string
	opaque       int32
	remark       string
}

func NewRocketmqRequestAttributes(data []byte, offset int, length int64, code int16, contentKey string, opaque int32) *RocketmqAttributes {
	return &RocketmqAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, offset, length, true),
		requestCode:    code,
		contentKey:     contentKey,
		opaque:         opaque,
	}
}

func NewRocketmqResponseAttributes(data []byte, offset int, length int64, code int16, remark string, opaque int32) *RocketmqAttributes {
	return &RocketmqAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, offset, length, false),
		responseCode:   code,
		remark:         remark,
		opaque:         opaque,
	}
}

func (rocketmq *RocketmqAttributes) MergeRequest(request protocol.ProtocolMessage) bool {
	if request != nil {
		requestAttributes := request.(*RocketmqAttributes)
		if requestAttributes.opaque != rocketmq.opaque {
			return false
		}
		rocketmq.contentKey = requestAttributes.contentKey
		rocketmq.requestCode = requestAttributes.requestCode
	}
	return true
}

func (rocketmq *RocketmqAttributes) GetAttributes() *model.AttributeMap {
	attributeMap := model.NewAttributeMap()

	attributeMap.AddStringValue(constlabels.RocketMQRequestMsg, requestMsgMap[rocketmq.requestCode])
	attributeMap.AddIntValue(constlabels.RocketMQOpaque, int64(rocketmq.opaque))
	attributeMap.AddStringValue(constlabels.ContentKey, rocketmq.contentKey)

	attributeMap.AddIntValue(constlabels.RocketMQErrCode, int64(rocketmq.responseCode))
	if rocketmq.responseCode > 0 {
		attributeMap.AddIntValue(constlabels.RocketMQErrCode, int64(rocketmq.responseCode))
		attributeMap.AddStringValue(constlabels.RocketMQErrMsg, rocketmq.getResponseErrorMsg())
		attributeMap.AddBoolValue(constlabels.IsError, true)
		attributeMap.AddIntValue(constlabels.ErrorType, int64(constlabels.ProtocolError))
	}
	return attributeMap
}

func (rocketmq *RocketmqAttributes) getResponseErrorMsg() string {
	if _, ok := responseErrMsgMap[rocketmq.responseCode]; ok {
		return responseErrMsgMap[rocketmq.responseCode]
	} else if rocketmq.remark != "" {
		return rocketmq.remark
	} else {
		return fmt.Sprintf("error:response code is %v", rocketmq.responseCode)
	}
}
