package protocol

import (
	"errors"

	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/tools"
	"github.com/Kindling-project/kindling/collector/pkg/model"
)

const (
	PARSE_FAIL     = 0
	PARSE_OK       = 1
	PARSE_COMPLETE = 2

	EOF = -1
)

var (
	ErrMessageComplete = errors.New("message completed")
	ErrMessageShort    = errors.New("message is too short")
	ErrMessageInvalid  = errors.New("message is invalid")
	ErrArgumentInvalid = errors.New("argument is invalid")
	ErrEof             = errors.New("EOF")
	ErrUnexpectedEOF   = errors.New("unexpected EOF")
)

type ProtocolMessage interface {
	SetData(newData []byte)
	GetData() []byte
	GetLength() int64
	IsRequest() bool
	IsReverse() bool
	GetStreamId() int64
	MergeRequest(request ProtocolMessage) bool
	GetAttributes() *model.AttributeMap
}

type PayloadMessage struct {
	Data    []byte
	Offset  int
	Size    int64
	Request bool
}

func NewPayloadMessage(data []byte, offset int, size int64, request bool) *PayloadMessage {
	var payload []byte
	if len(data) > int(size) {
		payload = data[0:int(size)]
	} else {
		payload = data
	}
	return &PayloadMessage{
		Data:    payload,
		Offset:  offset,
		Size:    size,
		Request: request,
	}
}

func (message *PayloadMessage) SetData(newData []byte) {
	message.Data = newData
}

func (message *PayloadMessage) GetData() []byte {
	return message.Data
}

func (message *PayloadMessage) IsRequest() bool {
	return message.Request
}

func (message *PayloadMessage) IsReverse() bool {
	return false
}

func (message *PayloadMessage) GetLength() int64 {
	return message.Size
}

func (message *PayloadMessage) GetStreamId() int64 {
	return 0
}

func (message *PayloadMessage) IsComplete() bool {
	return len(message.Data) <= message.Offset
}

func (message *PayloadMessage) HasMoreLength(length int) bool {
	return message.Offset+length <= len(message.Data)
}

// =============== PayLoad ===============
func (message *PayloadMessage) ReadInt32(offset int, v *int32) (toOffset int, err error) {
	if offset < 0 {
		return -1, ErrArgumentInvalid
	}
	if offset+4 > len(message.Data) {
		return -1, ErrMessageShort
	}
	*v = int32(message.Data[offset])<<24 | int32(message.Data[offset+1])<<16 | int32(message.Data[offset+2])<<8 | int32(message.Data[offset+3])
	return offset + 4, nil
}

func (message *PayloadMessage) ReadBytes(offset int, length int) (toOffset int, value []byte, err error) {
	if offset < 0 || length < 0 {
		return EOF, nil, ErrArgumentInvalid
	}
	maxLength := offset + length
	if maxLength > len(message.Data) {
		return EOF, nil, ErrMessageShort
	}
	return maxLength, message.Data[offset:maxLength], nil
}

func (message *PayloadMessage) readUnsignedVarIntCore(offset int, times int, f func(uint64)) (toOffset int, err error) {
	if offset < 0 {
		return -1, ErrArgumentInvalid
	}
	var b byte
	x := uint64(0)
	s := uint(0)
	for i := 0; i < times; i++ {
		if offset+i >= len(message.Data) {
			return -1, ErrMessageShort
		}
		b = message.Data[offset+i]
		if b < 0x80 {
			x |= uint64(b) << s
			f(x)
			return offset + i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return -1, ErrMessageInvalid
}

func (message *PayloadMessage) ReadUnsignedVarInt(offset int, v *uint64) (toOffset int, err error) {
	return message.readUnsignedVarIntCore(offset, 5, func(value uint64) { *v = value })
}

func (message *PayloadMessage) ReadVarInt(offset int, v *int64) (toOffset int, err error) {
	return message.readUnsignedVarIntCore(offset, 5, func(value uint64) { *v = int64(value>>1) ^ -(int64(value) & 1) })
}

func (message *PayloadMessage) ReadNullableString(offset int, compact bool, v *string) (toOffset int, err error) {
	if compact {
		return message.readCompactNullableString(offset, v)
	}
	return message.readNullableString(offset, v)
}

func (message *PayloadMessage) readNullableString(offset int, v *string) (toOffset int, err error) {
	var length int16
	if toOffset, err = ReadInt16(message.Data, offset, &length); err != nil {
		return toOffset, err
	}
	if length < -1 {
		return -1, ErrMessageInvalid
	}
	if length == -1 {
		return toOffset, nil
	}
	if toOffset+int(length) >= len(message.Data) {
		*v = string(message.Data[toOffset:])
		return len(message.Data), nil
	}
	*v = string(message.Data[toOffset : toOffset+int(length)])
	return toOffset + int(length), nil
}

func (message *PayloadMessage) readCompactNullableString(offset int, v *string) (toOffset int, err error) {
	var length uint64
	if toOffset, err = message.ReadUnsignedVarInt(offset, &length); err != nil {
		return toOffset, err
	}
	intLength := int(length)
	intLength -= 1
	if intLength < -1 {
		return -1, ErrMessageInvalid
	}
	if intLength == -1 {
		return toOffset, nil
	}
	if toOffset+int(length) >= len(message.Data) {
		*v = string(message.Data[toOffset:])
		return len(message.Data), nil
	}
	*v = string(message.Data[toOffset : toOffset+intLength])
	return toOffset + intLength, nil
}

func (message *PayloadMessage) ReadArraySize(offset int, compact bool, size *int32) (toOffset int, err error) {
	if compact {
		return message.readCompactArraySize(offset, size)
	}
	return message.readArraySize(offset, size)
}

func (message *PayloadMessage) readCompactArraySize(offset int, size *int32) (toOffset int, err error) {
	var length uint64
	if toOffset, err = message.ReadUnsignedVarInt(offset, &length); err != nil {
		return toOffset, err
	}
	len := int32(length)
	if len < 0 {
		return -1, ErrMessageInvalid
	}
	if len == 0 {
		*size = 0
		return toOffset, nil
	}
	len -= 1
	*size = len
	return toOffset, nil
}

func (message *PayloadMessage) readArraySize(offset int, size *int32) (toOffset int, err error) {
	var length int32
	if toOffset, err = ReadInt32(message.Data, offset, &length); err != nil {
		return toOffset, err
	}
	if length < -1 {
		return -1, ErrMessageInvalid
	}
	if length == -1 {
		*size = 0
		return toOffset, nil
	}
	*size = length
	return toOffset, nil
}

func (message *PayloadMessage) ReadString(offset int, compact bool, v *string) (toOffset int, err error) {
	if compact {
		return message.readCompactString(offset, v)
	}
	return message.readString(offset, v)
}

func (message *PayloadMessage) readCompactString(offset int, v *string) (toOffset int, err error) {
	var length uint64
	if toOffset, err = message.ReadUnsignedVarInt(offset, &length); err != nil {
		return toOffset, err
	}

	intLen := int(length)
	intLen -= 1
	if intLen < 0 {
		return -1, ErrMessageInvalid
	}
	if toOffset+int(length) >= len(message.Data) {
		*v = string(message.Data[toOffset:])
		return len(message.Data), nil
	}
	*v = string(message.Data[toOffset : toOffset+intLen])
	return toOffset + intLen, nil
}

func (message *PayloadMessage) readString(offset int, v *string) (toOffset int, err error) {
	var length int16
	if toOffset, err = ReadInt16(message.Data, offset, &length); err != nil {
		return toOffset, err
	}
	if length < 0 {
		return -1, ErrMessageInvalid
	}
	if toOffset+int(length) >= len(message.Data) {
		*v = string(message.Data[toOffset:])
		return len(message.Data), nil
	}

	*v = string(message.Data[toOffset : toOffset+int(length)])
	return toOffset + int(length), nil
}

// Read Util \r\n
func (message *PayloadMessage) ReadUntilCRLF(from int) (offset int, data []byte) {
	var length = len(message.Data)
	if from >= length {
		return EOF, nil
	}

	for i := from; i < length; i++ {
		if message.Data[i] != '\r' {
			continue
		}

		if i == length-1 {
			// End with \r
			offset = length
			data = message.Data[from : length-1]
			return
		} else if message.Data[i+1] == '\n' {
			// \r\n
			offset = i + 2
			data = message.Data[from:i]
			return
		} else {
			return EOF, nil
		}
	}

	offset = length
	data = message.Data[from:]
	return
}

type ParseStreamHeadFn func(data []byte, size int64, isRequest bool) (attributes ProtocolMessage, waitNextPkt bool)
type ParseSequenceHeadFn func(data []byte, size int64, isRequest bool) (attributes ProtocolMessage)
type ParsePayloadFn func(attributes ProtocolMessage) (ok bool)

type ProtocolParser struct {
	protocol          string
	ParseStreamHead   ParseStreamHeadFn
	ParseSequenceHead ParseSequenceHeadFn
	ParsePayload      ParsePayloadFn
}

func NewSequenceParser(protocol string, parseHead ParseSequenceHeadFn, parsePayload ParsePayloadFn) *ProtocolParser {
	return &ProtocolParser{
		protocol:          protocol,
		ParseSequenceHead: parseHead,
		ParsePayload:      parsePayload,
	}
}

func NewStreamParser(protocol string, parseHead ParseStreamHeadFn, parsePayload ParsePayloadFn) *ProtocolParser {
	return &ProtocolParser{
		protocol:        protocol,
		ParseStreamHead: parseHead,
		ParsePayload:    parsePayload,
	}
}

func (parser *ProtocolParser) IsStreamParser() bool {
	return parser.ParseStreamHead != nil
}

func (parser *ProtocolParser) GetProtocol() string {
	return parser.protocol
}

func (parser *ProtocolParser) Check(data []byte, size int64, isRequest bool) bool {
	if attributes, _ := parser.ParseStreamHead(data, size, isRequest); attributes != nil {
		if attributes.GetLength() < int64(len(data)) {
			attributes.SetData(data[0:attributes.GetLength()])
		}
		return parser.ParsePayload(attributes)
	}
	return false
}

func GetPayloadString(payload []byte, protocolName string) string {
	switch protocolName {
	case HTTP, REDIS:
		return tools.FormatByteArrayToUtf8(getSubstrBytes(payload, protocolName, 0))
	case DUBBO:
		return tools.GetAsciiString(getSubstrBytes(payload, protocolName, 16))
	default:
		return tools.GetAsciiString(getSubstrBytes(payload, protocolName, 0))
	}
}

func getSubstrBytes(payload []byte, protocolName string, offset int) []byte {
	length := GetPayLoadLength(protocolName)
	if offset >= length {
		return payload[0:0]
	}
	if offset+length > len(payload) {
		return payload[offset:]
	}
	return payload[offset : offset+length]
}

func ReadBytes(payload []byte, offset int, length int) (toOffset int, value []byte, err error) {
	if offset < 0 || length < 0 {
		return EOF, nil, ErrArgumentInvalid
	}
	maxLength := offset + length
	if maxLength > len(payload) {
		return EOF, nil, ErrMessageShort
	}
	return maxLength, payload[offset:maxLength], nil
}

func ReadUInt16(payload []byte, offset int) (value uint16, err error) {
	if offset < 0 {
		return 0, ErrArgumentInvalid
	}
	if offset+2 > len(payload) {
		return 0, ErrMessageShort
	}
	return uint16(payload[offset])<<8 | uint16(payload[offset+1]), nil
}

func ReadInt16(payload []byte, offset int, v *int16) (toOffset int, err error) {
	if offset < 0 {
		return -1, ErrArgumentInvalid
	}
	if offset+2 > len(payload) {
		return -1, ErrMessageShort
	}
	*v = int16(payload[offset])<<8 | int16(payload[offset+1])
	return offset + 2, nil
}

func ReadUInt32(payload []byte, offset int) (value uint32, err error) {
	if offset < 0 {
		return 0, ErrArgumentInvalid
	}
	if offset+4 > len(payload) {
		return 0, ErrMessageShort
	}
	return uint32(payload[offset])<<24 | uint32(payload[offset+1])<<16 | uint32(payload[offset+2])<<8 | uint32(payload[offset+3]), nil
}

func ReadInt32(payload []byte, offset int, v *int32) (toOffset int, err error) {
	if offset < 0 {
		return -1, ErrArgumentInvalid
	}
	if offset+4 > len(payload) {
		return -1, ErrMessageShort
	}
	*v = int32(payload[offset])<<24 | int32(payload[offset+1])<<16 | int32(payload[offset+2])<<8 | int32(payload[offset+3])
	return offset + 4, nil
}

func ReadInt64(payload []byte, offset int) (value int64, err error) {
	if offset < 0 {
		return 0, ErrArgumentInvalid
	}
	if offset+8 > len(payload) {
		return 0, ErrMessageShort
	}
	return int64(payload[offset])<<56 | int64(payload[offset+1])<<48 | int64(payload[offset+2])<<40 | int64(payload[offset+3])<<32 | int64(payload[offset+4])<<24 | int64(payload[offset+5])<<16 | int64(payload[offset+6])<<8 | int64(payload[offset+7]), nil
}

func ReadUntilBlankWithLength(payload []byte, from int, fixedLength int) (offset int, data []byte) {
	var length = len(payload)
	if fixedLength+from < length {
		length = from + fixedLength
	}

	for i := from; i < length; i++ {
		if payload[i] == ' ' {
			return i + 1, payload[from:i]
		}
	}
	return length, payload[from:length]
}

func ReadUntilBlank(payload []byte, from int) (offset int, data []byte) {
	var length = len(payload)

	for i := from; i < length; i++ {
		if payload[i] == ' ' {
			return i + 1, payload[from:i]
		}
	}
	return length, payload[from:length]
}

// Read Util \r\n
func ReadUntilCRLF(payload []byte, from int) (offset int, data []byte) {
	var length = len(payload)
	if from >= length {
		return EOF, nil
	}

	for i := from; i < length; i++ {
		if payload[i] != '\r' {
			continue
		}

		if i == length-1 {
			// End with \r
			offset = length
			data = payload[from : length-1]
			return
		} else if payload[i+1] == '\n' {
			// \r\n
			offset = i + 2
			data = payload[from:i]
			return
		} else {
			return EOF, nil
		}
	}

	offset = length
	data = payload[from:]
	return
}
