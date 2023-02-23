package redis

import (
	"strconv"
)

/*
:1\r\n
*/
func parseRedisInteger() ParseRedisFn {
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
