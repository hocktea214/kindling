package mysql

import (
	"strings"
)

const (
	COM_QUERY        = 0x3
	COM_STMT_PREPARE = 0x16
)

var (
	sqlPrefixs = []string{
		"select",
		"insert",
		"update",
		"delete",
		"drop",
		"create",
		"alter",
		"set",
		"commit",
	}
)

func parseRequestPayload(message *MysqlAttributes) (ok bool) {
	if message.Data[4] == COM_QUERY || message.Data[4] == COM_STMT_PREPARE {
		/*
			===== PayLoad =====
			1              COM_QUERY<03>
			string[EOF]    the query the server shall execute

			===== PayLoad =====
			1              COM_STMT_PREPARE<0x16>
			string[EOF]    the query to prepare
		*/
		sql := string(message.Data[5:])
		if !isSql(sql) {
			return false
		}
		message.sql = sql
		return true
	}
	return false
}

func isSql(sql string) bool {
	lowerSql := strings.ToLower(sql)

	for _, prefix := range sqlPrefixs {
		if strings.HasPrefix(lowerSql, prefix) {
			return true
		}
	}
	return false
}
