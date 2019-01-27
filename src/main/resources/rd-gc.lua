-- RD-GC node... cluster id...
-- KEYS[1..m] node key
-- ARGV[1..m] node id
-- KEYS[n] cluster
local n = #KEYS
local z = KEYS[n]
local result = {}
for i = 1, n - 1 do
    if redis.call('exists', KEYS[i]) == 0 then
        redis.call('zrem', z, ARGV[i])
        result[i] = 0
    else
        result[i] = 1
    end
end
return table.concat(result)