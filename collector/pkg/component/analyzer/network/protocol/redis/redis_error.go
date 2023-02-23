package redis

/*
*
-Error message\r\n
*/
func parseRedisError() ParseRedisFn {
	return func(message *RedisAttributes) (ok bool) {
		offset, data := message.ReadUntilCRLF(message.Offset + 1)
		if data == nil {
			return false
		}

		if len(data) > 0 {
			message.errorMsg = string(data)
		}
		message.Offset = offset
		return true
	}
}
