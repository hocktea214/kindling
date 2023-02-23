package dubbo

import (
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
)

const (
	// Zero : byte zero
	Zero = byte(0x00)

	// magic header
	MagicHigh = byte(0xda)
	MagicLow  = byte(0xbb)

	// message flag.
	FlagRequest = byte(0x80)
	FlagTwoWay  = byte(0x40)
	FlagEvent   = byte(0x20) // for heartbeat
	SerialMask  = 0x1f

	// head size
	DubboHeadSize = 16
)

func NewDubboParser() *protocol.ProtocolParser {
	return protocol.NewStreamParser(protocol.DUBBO, parseHead, parsePayload)
}

/*
*
https://github.com/apache/dubbo-awesome/blob/master/images/protocol/dubbo_protocol_header.png

	   0    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F
	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
	|               Magic High              |               Magic Low               |
	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
	|R/R |2Way| Evt|        Serial ID       |             Response Status           |
	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
	|                                     ID(64)                                    |
	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
	|                               Data Length(32)                                 |
	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
*/
func parseHead(data []byte, size int64, isRequest bool) (attributes protocol.ProtocolMessage, waitNextPkt bool) {
	if size < DubboHeadSize {
		return nil, true
	}
	if data[0] != MagicHigh || data[1] != MagicLow || len(data) < DubboHeadSize {
		return
	}
	serialID := data[2] & SerialMask
	requestFlag := data[2] & FlagRequest

	if serialID == Zero || (isRequest && requestFlag == Zero) || (!isRequest && requestFlag != Zero) {
		return
	}
	status := int(data[3])
	id, _ := protocol.ReadInt64(data, 4)
	length, _ := protocol.ReadUInt32(data, 12)
	attributes = NewDubboAttributes(data, isRequest, id, length, data[2], serialID, status)
	return
}

func parsePayload(attributes protocol.ProtocolMessage) (ok bool) {
	if attributes.IsRequest() {
		message := attributes.(*DubboAttributes)
		message.contentKey = message.getContentKey()
	}
	return true
}

type DubboAttributes struct {
	*protocol.PayloadMessage
	id         int64
	eventFlag  byte
	serialID   byte
	contentKey string
	status     int
}

func NewDubboAttributes(data []byte, isRequest bool, id int64, length uint32, eventFlag byte, serialID byte, status int) *DubboAttributes {
	return &DubboAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, DubboHeadSize, int64(length+DubboHeadSize), isRequest),
		id:             id,
		eventFlag:      eventFlag,
		serialID:       serialID,
		status:         status,
	}
}

func (dubbo *DubboAttributes) GetStreamId() int64 {
	return dubbo.id
}

func (dubbo *DubboAttributes) MergeRequest(request protocol.ProtocolMessage) bool {
	if request != nil {
		requestAttributes := request.(*DubboAttributes)
		if requestAttributes.id != dubbo.id {
			return false
		}
		dubbo.contentKey = request.(*DubboAttributes).contentKey
	}
	return true
}

func (dubbo *DubboAttributes) GetAttributes() *model.AttributeMap {
	attributeMap := model.NewAttributeMap()
	attributeMap.AddStringValue(constlabels.ContentKey, dubbo.contentKey)
	attributeMap.AddIntValue(constlabels.DubboErrorCode, int64(dubbo.status))
	if dubbo.status > 20 {
		attributeMap.AddBoolValue(constlabels.IsError, true)
		attributeMap.AddIntValue(constlabels.ErrorType, int64(constlabels.ProtocolError))
	}
	return attributeMap
}

func (dubbo *DubboAttributes) getContentKey() string {
	if (dubbo.eventFlag & FlagEvent) != Zero {
		return "Heartbeat"
	}
	if (dubbo.eventFlag & FlagTwoWay) == Zero {
		// Ignore Oneway Data
		return "Oneway"
	}

	serializer := GetSerializer(dubbo.serialID)
	if serializer == serialUnsupport {
		// Unsupport Serial. only support hessian and fastjson.
		return "UnSupportSerialFormat"
	}

	var (
		service string
		method  string
	)
	requestData := dubbo.Data
	offset := serializer.eatString(requestData, 16)

	// service name
	offset, service = serializer.getStringValue(requestData, offset)
	// service version
	offset = serializer.eatString(requestData, offset)
	// method name
	_, method = serializer.getStringValue(requestData, offset)

	return service + "#" + method
}
