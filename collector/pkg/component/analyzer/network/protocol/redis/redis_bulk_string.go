package redis

import (
	"strconv"
)

/*
$3\r\nbar\r\n
*/
func parseRedisBulkString() ParseRedisFn {
	return func(message *RedisAttributes) (bool, bool) {
		offset, data := message.ReadUntilCRLF(message.Offset + 1)
		if data == nil {
			return false, true
		}

		size, err := strconv.Atoi(string(data))
		if err != nil {
			return false, true
		}

		// $-1\r\n
		if size == -1 {
			message.Offset = offset
			return true, message.IsComplete()
		}

		offset, data = message.ReadUntilCRLF(offset)
		if data == nil {
			return false, true
		}

		/**
		$0\r\n\r\n
		$6\r\nfoobar\r\n
		*/
		if len(data) != size {
			// Truncate Case

			return offset == len(message.Data), true
		}

		command := string(data)
		if IsRedisCommand(data) {
			if len(message.command) == 0 {
				message.command = command
			} else {
				message.command = "PIPELINE"
			}
		}

		message.Offset = offset
		return true, message.IsComplete()
	}
}
