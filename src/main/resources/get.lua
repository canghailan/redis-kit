-- GET key
local value = redis.call('get', KEYS[1])
local ttl = redis.call('pttl', KEYS[1])
local time = redis.call('time')
return { value, tostring(ttl), time[1], time[2] }