package redis

import (
	"strconv"
)

/*
:1\r\n
*/
func parseRedisInteger() ParseRedisFn {
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
