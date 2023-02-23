package dns

import (
	"net"
	"strings"

	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
)

const (
	DNSHeaderSize  = 12
	MaxNumRR       = 25
	MaxMessageSize = 512

	TypeA    uint16 = 1
	TypeAAAA uint16 = 28

	maxDomainNameWireOctets         = 255 // See RFC 1035 section 2.3.4
	maxCompressionPointers          = (maxDomainNameWireOctets+1)/2 - 2
	maxDomainNamePresentationLength = 61*4 + 1 + 63*4 + 1 + 63*4 + 1 + 63*4 + 1
)

/*
*
https://www.rfc-editor.org/rfc/rfc1035

	+---------------------+
	|        Header       |
	+---------------------+
	|       Question      | the question for the name server
	+---------------------+
	|        Answer       | RRs answering the question
	+---------------------+
	|      Authority      | RRs pointing toward an authority
	+---------------------+
	|      Additional     | RRs holding additional information
	+---------------------+
*/
func NewDnsParser() *protocol.ProtocolParser {
	return protocol.NewStreamParser(protocol.DNS, parseHead, parsePayload)
}

func parseHead(data []byte, size int64, isRequest bool) (attributes protocol.ProtocolMessage, waitNextPkt bool) {
	if size <= DNSHeaderSize {
		return nil, true
	}
	if len(data) <= DNSHeaderSize || len(data) > MaxMessageSize {
		return
	}
	offset := 0
	id, _ := protocol.ReadUInt16(data, offset)
	flags, _ := protocol.ReadUInt16(data, offset+2)

	qr := (flags >> 15) & 0x1
	opcode := (flags >> 11) & 0xf
	rcode := flags & 0xf

	numOfQuestions, _ := protocol.ReadUInt16(data, offset+4)
	numOfAnswers, _ := protocol.ReadUInt16(data, offset+6)
	numOfAuth, _ := protocol.ReadUInt16(data, offset+8)
	numOfAddl, _ := protocol.ReadUInt16(data, offset+10)
	numOfRR := numOfQuestions + numOfAnswers + numOfAuth + numOfAddl

	/*
		QR: Request(0) Response(1)
		Kind of query in this message
			0	a standard query (QUERY)
			1	an inverse query (IQUERY)
			2	a server status request (STATUS)
			3-15 	reserved for future use

		Response code
			0	No error condition
			1 	Format error
			2 	Server failure
			3	Name Error
			4 	Not Implemented
			5 	Refused
			6-15 	Reserved for future use.
	*/
	if opcode > 2 || rcode > 5 || numOfQuestions == 0 || numOfRR > MaxNumRR {
		return
	}
	if isRequest {
		if qr != 0 || numOfRR > MaxNumRR {
			return
		}
	} else if qr == 0 {
		return
	}
	attributes = NewDnsAttributes(data, size, isRequest, int64(id), int(numOfQuestions), int(numOfAnswers), int64(rcode))
	return
}

/*
*

	  0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F
	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	|                      ID                       |
	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	|QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |
	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	|                    QDCOUNT                    |
	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	|                    ANCOUNT                    |
	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	|                    NSCOUNT                    |
	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	|                    ARCOUNT                    |
	+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
*/
func parsePayload(attributes protocol.ProtocolMessage) (ok bool) {
	message := attributes.(*DnsAttributes)

	domain, err := readQuery(message)
	if err != nil {
		return false
	}

	message.domain = domain
	if !attributes.IsRequest() {
		ip := readIpV4Answer(message)
		if len(ip) > 0 {
			message.ip = ip
		}
	}
	return true
}

func readQuery(attributes *DnsAttributes) (domain string, err error) {
	var name string

	offset := attributes.Offset
	for i := 0; i < attributes.numOfQuestions; i++ {
		if attributes.IsComplete() {
			return "", protocol.ErrEof
		}

		/*
			uint16 qname
			uint16 qtype
			uint16 qclass
		*/
		name, offset, err = unpackDomainName(attributes.Data, offset)
		if err != nil || offset >= len(attributes.Data) {
			return "", protocol.ErrMessageInvalid
		}
		if len(domain) == 0 {
			domain = name
		}
		offset += 4
	}
	attributes.Offset = offset
	return domain, nil
}

func readIpV4Answer(message *DnsAttributes) string {
	var (
		aType  uint16
		length uint16
		ip     net.IP
		ips    []string
		err    error
	)

	ips = make([]string, 0)
	offset := message.Offset
	for i := 0; i < message.numOfAnswers; i++ {
		/*
			uint16 name
			uint16 type
			uint16 class
			uint32 ttl
			uint16 rdlength
			string rdata
		*/
		offset += 2
		aType, err = protocol.ReadUInt16(message.Data, offset)
		if err != nil {
			break
		}

		offset += 8
		length, err = protocol.ReadUInt16(message.Data, offset)
		if err != nil {
			break
		}

		offset += 2
		if aType == TypeA {
			offset, ip, err = message.ReadBytes(offset, int(length))
			if err != nil {
				break
			}
			ips = append(ips, ip.String())
		}
		offset += int(length)
	}
	message.Offset = offset
	if len(ips) == 0 {
		return ""
	}

	return strings.Join(ips, ",")
}

type DnsAttributes struct {
	*protocol.PayloadMessage
	id             int64
	numOfQuestions int
	numOfAnswers   int
	domain         string
	ip             string
	rcode          int64
}

func NewDnsAttributes(data []byte, size int64, isRequest bool, id int64, numOfQuestions int, numOfAnswers int, rcode int64) *DnsAttributes {
	return &DnsAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, DNSHeaderSize, size, isRequest),
		id:             id,
		numOfQuestions: numOfQuestions,
		numOfAnswers:   numOfAnswers,
		rcode:          rcode,
	}
}

func (dns *DnsAttributes) GetStreamId() int64 {
	return dns.id
}

func (dns *DnsAttributes) MergeRequest(request protocol.ProtocolMessage) bool {
	return request == nil || (request.(*DnsAttributes).id == dns.id)
}

func (dns *DnsAttributes) GetAttributes() *model.AttributeMap {
	attributeMap := model.NewAttributeMap()
	attributeMap.AddIntValue(constlabels.DnsId, dns.id)
	attributeMap.AddStringValue(constlabels.DnsDomain, dns.domain)
	if len(dns.ip) > 0 {
		attributeMap.AddStringValue(constlabels.DnsIp, dns.ip)
	}
	attributeMap.AddIntValue(constlabels.DnsRcode, dns.rcode)
	if dns.rcode > 0 {
		attributeMap.AddBoolValue(constlabels.IsError, true)
		attributeMap.AddIntValue(constlabels.ErrorType, int64(constlabels.ProtocolError))
	}
	return attributeMap
}
