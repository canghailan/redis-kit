-- CAS key expect value [EX seconds] [PX milliseconds] [NX|XX]
-- CASE1: CAS key expect value
-- CASE2: CAS key expect value [NX|XX]
-- CASE3: CAS key expect value [EX seconds] [PX milliseconds]
-- CASE4: CAS key expect value [EX seconds] [PX milliseconds] [NX|XX]
local value = redis.call('get', KEYS[1])
if not value or value == ARGV[1] then
    local n = #ARGV
    if n == 2 then
        return redis.call('set', KEYS[1], ARGV[2])
    elseif n == 3 then
        return redis.call('set', KEYS[1], ARGV[2], ARGV[3])
    elseif n == 4 then
        return redis.call('set', KEYS[1], ARGV[2], ARGV[3], ARGV[4])
    elseif n == 5 then
        return redis.call('set', KEYS[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
    else
        return false
    end
end
return false