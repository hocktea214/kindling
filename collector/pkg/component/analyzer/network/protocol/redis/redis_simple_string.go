package redis

/*
*
+OK\r\n
*/
func parseRedisSimpleString() ParseRedisFn {
	return func(message *RedisAttributes) (ok bool) {
		offset, data := message.ReadUntilCRLF(message.Offset + 1)
		if data == nil {
			return false
		}

		message.Offset = offset
		return true
	}
}
