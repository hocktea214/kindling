package mysql

import (
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol"
	mysqlTool "github.com/Kindling-project/kindling/collector/pkg/component/analyzer/network/protocol/mysql/tools"
	"github.com/Kindling-project/kindling/collector/pkg/component/analyzer/tools"

	"github.com/Kindling-project/kindling/collector/pkg/model"
	"github.com/Kindling-project/kindling/collector/pkg/model/constlabels"
)

const (
	HeadLength = 4
)

func NewMysqlParser() *protocol.ProtocolParser {
	return protocol.NewSequenceParser(protocol.MYSQL, parseHead, parsePayload)
}

func parseHead(data []byte, size int64, isRequest bool) (attributes protocol.ProtocolMessage, waitNextPkt bool) {
	if len(data) <= HeadLength {
		return
	}
	/*
		===== Head =====
		int<3>	payload_length
		int<1>	sequence_id
		===== Payload =====
		payload
	*/
	attributes = NewMysqlAttributes(data, size, isRequest)
	return
}

func parsePayload(attributes protocol.ProtocolMessage, isRequest bool) bool {
	message := attributes.(*MysqlAttributes)
	if isRequest {
		return parseRequestPayload(message)
	}
	return parseResponsePayload(message)
}

type MysqlAttributes struct {
	*protocol.PayloadMessage
	sql          string
	errorCode    uint16
	errorMessage string
}

func NewMysqlAttributes(data []byte, size int64, isRequest bool) *MysqlAttributes {
	return &MysqlAttributes{
		PayloadMessage: protocol.NewPayloadMessage(data, HeadLength, size, isRequest),
	}
}

func (mysql *MysqlAttributes) MergeRequest(request protocol.ProtocolMessage) bool {
	if request != nil {
		requestAttributes := request.(*MysqlAttributes)
		mysql.sql = requestAttributes.sql
	}
	return true
}

func (mysql *MysqlAttributes) GetAttributes() *model.AttributeMap {
	attributeMap := model.NewAttributeMap()
	if len(mysql.sql) > 0 {
		attributeMap.AddStringValue(constlabels.Sql, mysql.sql)
		attributeMap.AddStringValue(constlabels.ContentKey, mysqlTool.SQL_MERGER.ParseStatement(mysql.sql))
	}
	if mysql.errorCode > 0 {
		attributeMap.AddIntValue(constlabels.SqlErrCode, int64(mysql.errorCode))
	}
	if len(mysql.errorMessage) > 0 {
		attributeMap.AddStringValue(constlabels.SqlErrMsg, tools.FormatStringToUtf8(mysql.errorMessage))
	}
	if mysql.errorCode != 0 {
		attributeMap.AddBoolValue(constlabels.IsError, true)
		attributeMap.AddIntValue(constlabels.ErrorType, int64(constlabels.ProtocolError))
	}
	return attributeMap
}
