package redis

import (
	"strconv"
)

/**
*0\r\n
*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
*3\r\n:1\r\n:2\r\n:3\r\n
 */
func parseRedisArray() ParseRedisFn {
	return func(message *RedisAttributes) (bool, bool) {
		offset, data := message.ReadUntilCRLF(message.Offset + 1)
		if data == nil {
			return false, true
		}

		_, err := strconv.Atoi(string(data))
		if err != nil {
			return false, true
		}
		message.Offset = offset
		return true, message.IsComplete()
	}
}
