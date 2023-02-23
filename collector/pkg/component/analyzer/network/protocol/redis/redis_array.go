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
	return func(message *RedisAttributes) (ok bool) {
		offset, data := message.ReadUntilCRLF(message.Offset + 1)
		if data == nil {
			return false
		}

		_, err := strconv.Atoi(string(data))
		if err != nil {
			return false
		}
		message.Offset = offset
		return true
	}
}
