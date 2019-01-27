-- EXISTKEYS key...
local result = {}
for i, key in ipairs(KEYS) do
    result[i] = redis.call('exists', key)
end
return table.concat(result)