-- CAS key expect update
if tostring(redis.call("GET", KEYS[1])) == ARGV[1] then
    redis.call("SET", ARGV[2])
end