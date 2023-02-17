package mysql

import (
	"encoding/binary"
)

const (
	OK_PACKET  = 0x00
	EOF_PACKET = 0xFE
	ERR_PACKET = 0xFF
)

func parseResponsePayload(message *MysqlAttributes) (ok bool) {
	if message.Data[4] == OK_PACKET || message.Data[4] == EOF_PACKET {
		return parseMysqlOk(message)
	}
	if message.Data[4] == ERR_PACKET {
		return parseMysqlErr(message)
	}
	if len(message.sql) > 0 {
		return parseMysqlResultSet(message)
	}
	return false
}

func parseMysqlOk(message *MysqlAttributes) (ok bool) {
	/*
		===== PayLoad =====
		int<1>	header(0x00 or 0xFE)
		int<lenenc>	affected_rows
		int<lenenc>	last_insert_id

			if capabilities & CLIENT_PROTOCOL_41 {
				int<2>	status_flags
				int<2>	warnings
			} else if capabilities & CLIENT_TRANSACTIONS {

				int<2>	status_flags
			}

			if capabilities & CLIENT_SESSION_TRACK {
				string<lenenc>	info
				if status_flags & SERVER_SESSION_STATE_CHANGED {
					string<lenenc>	session state info
				}
			} else {

				string<EOF>	info
			}
	*/
	return true
}

func parseMysqlErr(message *MysqlAttributes) (ok bool) {
	if len(message.Data) < 8 {
		return false
	}
	/*
	   ===== PayLoad =====
	   int<1>	header(0xff)
	   int<2>	error_code

	   	if capabilities & CLIENT_PROTOCOL_41 {
	   		string[1]	sql state marker (#)
	   		string[5]	sql_state
	   	}

	   string<EOF>	error_message
	*/
	errorCode := binary.LittleEndian.Uint16(message.Data[5:7])
	var errorMessage string
	if len(message.Data) > 14 && message.Data[8] == '#' {
		errorMessage = string(message.Data[8:13]) + ":" + string(message.Data[13:])
	} else {
		errorMessage = string(message.Data[8:])
	}
	message.errorCode = errorCode
	message.errorMessage = errorMessage
	return true
}

func parseMysqlResultSet(message *MysqlAttributes) (ok bool) {
	/*
		if capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA {
			int<1> metadata_follows
		}

		int<lenenc>	 column_count

		if (not (capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA)) or metadata_follows == RESULTSET_METADATA_FULL {
			column_count x Column Definition	field metadata
		}

		if (not capabilities & CLIENT_DEPRECATE_EOF) {
			EOF_Packet
		}

		One or more Text Resultset Row

		if (error processing) {
			ERR_Packet
		} else if capabilities & CLIENT_DEPRECATE_EOF {
			OK_Packet
		} else {
			EOF_Packet
		}
	*/
	return true
}
