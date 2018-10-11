package cc.whohow.redis.script;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Redis脚本命令
 */
public class RedisScriptCommands {
    private static final ConcurrentMap<String, RedisScript> CACHE = new ConcurrentHashMap<>();
    private final RedisCommands<ByteBuffer, ByteBuffer> redis;

    public RedisScriptCommands(RedisCommands<ByteBuffer, ByteBuffer> redis) {
        this.redis = redis;
    }

    public <T> T eval(String name, ScriptOutputType type, ByteBuffer... keys) {
        return redis.evalsha(getRedisScript(name).getSha1(), type, keys);
    }

    public <T> T eval(String name, ScriptOutputType type, ByteBuffer[] keys, ByteBuffer... args) {
        return redis.evalsha(getRedisScript(name).getSha1(), type, keys, args);
    }

    public RedisScript getRedisScript(String script) {
        return CACHE.computeIfAbsent(script, this::loadRedisScript);
    }

    public RedisScript loadRedisScript(String name) {
        RedisScript redisScript = new RedisScript(name);
        String sha = redis.scriptLoad(StandardCharsets.UTF_8.encode(redisScript.getScript()));
        if (sha.equals(redisScript.getSha1())) {
            return redisScript;
        }
        throw new IllegalStateException(name);
    }
}
