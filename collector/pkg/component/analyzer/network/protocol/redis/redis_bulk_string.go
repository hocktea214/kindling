package redis

import (
	"strconv"
)

/*
$3\r\nbar\r\n
*/
func parseRedisBulkString() ParseRedisFn {
	return func(message *RedisAttributes) (ok bool) {
		var (
			offset int
			data   []byte
			size   int
			err    error
		)
		if offset, data = message.ReadUntilCRLF(message.Offset + 1); data == nil {
			return false
		}
		if size, err = strconv.Atoi(string(data)); err != nil {
			return false
		}

		// $-1\r\n
		if size == -1 {
			message.Offset = offset
			return true
		}

		if offset, data = message.ReadUntilCRLF(offset); data == nil {
			return false
		}
		/**
		$0\r\n\r\n
		$6\r\nfoobar\r\n
		*/
		if len(data) != size {
			// Truncate Case
			message.Offset = offset
			return message.IsComplete()
		}

		command := string(data)
		if message.IsRequest() && IsRedisCommand(data) {
			if len(message.command) == 0 {
				message.command = command
			} else {
				message.command = "PIPELINE"
			}
		}

		message.Offset = offset
		return true
	}
}
