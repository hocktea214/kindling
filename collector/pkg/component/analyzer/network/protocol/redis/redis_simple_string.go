package redis

/*
*
+OK\r\n
*/
func parseRedisSimpleString() ParseRedisFn {
	return func(message *RedisAttributes) (bool, bool) {
		offset, data := message.ReadUntilCRLF(message.Offset + 1)
		if data == nil {
			return false, true
		}

		message.Offset = offset
		return true, message.IsComplete()
	}
}
