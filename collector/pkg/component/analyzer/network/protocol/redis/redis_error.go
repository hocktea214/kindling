package redis

/*
*
-Error message\r\n
*/
func parseRedisError() ParseRedisFn {
	return func(message *RedisAttributes) (bool, bool) {
		offset, data := message.ReadUntilCRLF(message.Offset + 1)
		if data == nil {
			return false, true
		}

		message.Offset = offset
		if len(data) > 0 {
			message.errorMsg = string(data)
		}
		return true, message.IsComplete()
	}
}
