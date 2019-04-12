-- ACC key op value [EX seconds] [PX milliseconds] [NX|XX]
-- CASE1: ACC key op value
-- CASE2: ACC key op value [NX|XX]
-- CASE3: ACC key op value [EX seconds] [PX milliseconds]
-- CASE4: ACC key op value [EX seconds] [PX milliseconds] [NX|XX]
local value = redis.call('get', KEYS[1])

local new_value = tonumber(value)
if new_value == nil then
    new_value = 0
end

if ARGV[1] == '+' then
    new_value = new_value + tonumber(ARGV[2])
elseif ARGV[1] == '-' then
    new_value = new_value - tonumber(ARGV[2])
elseif ARGV[1] == '*' then
    new_value = new_value * tonumber(ARGV[2])
elseif ARGV[1] == '/' then
    new_value = new_value / tonumber(ARGV[2])
elseif ARGV[1] == '//' then
    local _
    new_value, _ = math.modf(new_value / tonumber(ARGV[2]))
elseif ARGV[1] == '%' then
    new_value = new_value % tonumber(ARGV[2])
elseif ARGV[1] == '^' then
    new_value = new_value ^ tonumber(ARGV[2])
end

local n = #ARGV
if n == 2 then
    redis.call('set', KEYS[1], new_value)
elseif n == 3 then
    redis.call('set', KEYS[1], new_value, ARGV[3])
elseif n == 4 then
    redis.call('set', KEYS[1], new_value, ARGV[3], ARGV[4])
elseif n == 5 then
    redis.call('set', KEYS[1], new_value, ARGV[3], ARGV[4], ARGV[5])
end

return value