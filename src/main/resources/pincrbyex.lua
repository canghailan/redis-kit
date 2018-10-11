-- PINCRBYEX key milliseconds increment
local current = redis.call("INCRBY", KEYS[1], ARGV[2])
if current == tonumber(ARGV[2]) then
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return current