package model

import (
	"strconv"
)

func NewRpcData(evt *KindlingEvent, rpcId int64, hostIp string, attributes *AttributeMap) *RpcData {
	// Replace DstIp with hostIp
	dstIp := hostIp
	if dstIp == "" {
		dstIp = evt.GetDip()
	}
	return &RpcData{
		Sport:     evt.GetSport(),
		Dip:       dstIp,
		Dport:     evt.GetDport(),
		Timestamp: evt.Timestamp,
		RpcId:     rpcId,
		Attrs:     convertRpcAttributes(attributes),
	}
}

func (x *RpcData) GetUserAttributes() *AttributeMap {
	attributes := NewAttributeMap()
	for _, attr := range x.Attrs {
		switch attr.Type {
		case RpcAttType_INT:
			value, _ := strconv.ParseInt(attr.Value, 10, 64)
			attributes.AddIntValue(attr.Key, value)
		case RpcAttType_BOOL:
			value, _ := strconv.ParseBool(attr.Value)
			attributes.AddBoolValue(attr.Key, value)
		default:
			attributes.AddStringValue(attr.Key, attr.Value)
		}
	}

	return attributes
}

func getRpcAttType(attrType AttributeValueType) RpcAttType {
	switch attrType {
	case IntAttributeValueType:
		return RpcAttType_INT
	case BooleanAttributeValueType:
		return RpcAttType_BOOL
	default:
		return RpcAttType_STRING
	}
}

func convertRpcAttributes(attributes *AttributeMap) []*RpcAttr {
	attrs := make([]*RpcAttr, 0)
	for k, v := range attributes.values {
		attrs = append(attrs, &RpcAttr{
			Key:   k,
			Value: v.ToString(),
			Type:  getRpcAttType(v.Type()),
		})
	}
	return attrs
}

func NewPodInfo(sip uint32, sport uint32, dport uint32, time uint64) *PodInfo {
	return &PodInfo{
		Sip:        sip,
		Sport:      sport,
		Dport:      dport,
		UpdateTime: time,
	}
}
