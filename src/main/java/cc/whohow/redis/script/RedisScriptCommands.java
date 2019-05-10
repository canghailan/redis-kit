package cc.whohow.redis.script;

import io.lettuce.core.RedisCommandExecutionException;
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
        return eval(getRedisScript(name), type, keys);
    }

    public <T> T eval(String name, ScriptOutputType type, ByteBuffer[] keys, ByteBuffer... args) {
        return eval(getRedisScript(name), type, keys, args);
    }

    public <T> T eval(RedisScript script, ScriptOutputType type, ByteBuffer... keys) {
        try {
            return redis.evalsha(script.getSha1(), type, keys);
        } catch (RedisCommandExecutionException e) {
            if (isNoScript(e)) {
                return redis.eval(script.getScript(), type, keys);
            }
            throw e;
        }
    }

    public <T> T eval(RedisScript script, ScriptOutputType type, ByteBuffer[] keys, ByteBuffer... args) {
        try {
            return redis.evalsha(script.getSha1(), type, keys, args);
        } catch (RedisCommandExecutionException e) {
            if (isNoScript(e)) {
                return redis.eval(script.getScript(), type, keys, args);
            }
            throw e;
        }
    }

    protected boolean isNoScript(RedisCommandExecutionException e) {
        return e != null && e.getMessage() != null && e.getMessage().contains("NOSCRIPT");
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
