-- GETSET key value [EX seconds] [PX milliseconds] [NX|XX]
-- CASE1: GETSET key value
-- CASE2: GETSET key value [NX|XX]
-- CASE3: GETSET key value [EX seconds] [PX milliseconds]
-- CASE4: GETSET key value [EX seconds] [PX milliseconds] [NX|XX]
local value = redis.call('get', KEYS[1])
local n = #ARGV
if n == 1 then
    return redis.call('set', KEYS[1], ARGV[1])
elseif n == 2 then
    return redis.call('set', KEYS[1], ARGV[1], ARGV[2])
elseif n == 3 then
    return redis.call('set', KEYS[1], ARGV[1], ARGV[2], ARGV[3])
elseif n == 4 then
    return redis.call('set', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4])
end
return value