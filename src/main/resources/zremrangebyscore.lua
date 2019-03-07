-- ZREMRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
-- CASE1: ZREMRANGEBYSCORE key min max
-- CASE2: ZREMRANGEBYSCORE key min max WITHSCORES
-- CASE3: ZREMRANGEBYSCORE key min max LIMIT offset count
-- CASE4: ZREMRANGEBYSCORE key min max WITHSCORES LIMIT offset count
local r
local s
local n = #ARGV
if n == 2 then
    r = redis.call('zrangebyscore', KEYS[1], ARGV[1], ARGV[2])
    s = 1
elseif n == 3 then
    r = redis.call('zrangebyscore', KEYS[1], ARGV[1], ARGV[2], ARGV[3])
    s = 2
elseif n == 5 then
    r = redis.call('zrangebyscore', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
    s = 1
elseif n == 6 then
    r = redis.call('zrangebyscore', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
    s = 2
else
    return {}
end

local i = 1
while i <= #r do
    redis.call('ZREM', KEYS[1], r[i])
    i = i + s
end

return r

