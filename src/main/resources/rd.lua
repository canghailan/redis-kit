-- RD cluster node id random time milliseconds
-- KEYS[1] cluster
-- KEYS[2] node key
-- ARGV[1] node id
-- ARGV[2] random
-- ARGV[3] time
-- ARGV[4] milliseconds
if redis.call('hget', KEYS[2], '_rd_node.random') == ARGV[2] then
    redis.call('pexpire', KEYS[2], ARGV[4])
    return true
elseif redis.call('zadd', KEYS[1], 'nx', ARGV[3], ARGV[1]) then
    redis.call('del', KEYS[2])
    redis.call('hmset', KEYS[2], '_rd_node.id', ARGV[1], '_rd_node.random', ARGV[2])
    redis.call('pexpire', KEYS[2], ARGV[4])
    return true
else
    return false
end