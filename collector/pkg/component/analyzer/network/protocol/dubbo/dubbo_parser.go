package dubbo

import (
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
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

	AsciiLow     = byte(0x20)
	AsciiHigh    = byte(0x7e)
	AsciiReplace = byte(0x2e) // .
)

func NewDubboParser() *protocol.ProtocolParser {
	requestParser := protocol.CreatePkgParser(fastfailDubboRequest(), parseDubboRequest())
	responseParser := protocol.CreatePkgParser(fastfailDubboResponse(), parseDubboResponse())
	return protocol.NewRpcProtocolParser(protocol.DUBBO, requestParser, responseParser, dubbo2RequestId())
}

func dubbo2RequestId() protocol.GetUniqueIdFn {
	return func(data []byte) (int64, bool) {
		if len(data) < 16 || data[0] != MagicHigh || data[1] != MagicLow {
			return 0, false
		}

		return int64(uint64(data[4])<<56 | uint64(data[5])<<48 | uint64(data[6])<<40 | uint64(data[7])<<32 |
			uint64(data[8])<<24 | uint64(data[9])<<16 | uint64(data[10])<<8 | uint64(data[11])), true
	}
}

/**
  Get the ascii readable string, replace other value to '.', like wireshark.
*/
func getAsciiString(data []byte) string {
	length := len(data)
	if length == 0 {
		return ""
	}

	newData := make([]byte, length)
	for i := 0; i < length; i++ {
		if data[i] > AsciiHigh || data[i] < AsciiLow {
			newData[i] = AsciiReplace
		} else {
			newData[i] = data[i]
		}
	}
	return string(newData)
}
