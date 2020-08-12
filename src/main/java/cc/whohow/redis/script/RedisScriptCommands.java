package cc.whohow.redis.script;

import cc.whohow.redis.io.ByteBuffers;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Redis脚本命令
 */
public class RedisScriptCommands {
    private static final Logger log = LogManager.getLogger();
    private static final Cache<String, RedisScript> CACHE = Caffeine.newBuilder()
            .maximumSize(1024)
            .build();
    private final RedisCommands<ByteBuffer, ByteBuffer> redis;

    public RedisScriptCommands(RedisCommands<ByteBuffer, ByteBuffer> redis) {
        this.redis = redis;
    }

    public <T> T eval(String name, ScriptOutputType type) {
        return eval(getRedisScript(name), type);
    }

    public <T> T eval(String name, ScriptOutputType type, ByteBuffer[] keys) {
        return eval(getRedisScript(name), type, keys);
    }

    public <T> T eval(String name, ScriptOutputType type, ByteBuffer[] keys, ByteBuffer[] args) {
        return eval(getRedisScript(name), type, keys, args);
    }

    public <T> T eval(RedisScript script, ScriptOutputType type) {
        return eval(script, type, ByteBuffers.emptyArray(), ByteBuffers.emptyArray());
    }

    public <T> T eval(RedisScript script, ScriptOutputType type, ByteBuffer[] keys) {
        return eval(script, type, keys, ByteBuffers.emptyArray());
//        try {
//            log.trace("evalsha {} -> {}", script.getSha1(), script.getName());
//            return redis.evalsha(script.getSha1(), type, keys);
//        } catch (RedisCommandExecutionException e) {
//            if (isNoScript(e)) {
//                log.trace("eval {} -> {}", script.getSha1(), script.getName());
//                return redis.eval(script.getScript(), type, keys);
//            }
//            throw e;
//        }
    }

    public <T> T eval(RedisScript script, ScriptOutputType type, ByteBuffer[] keys, ByteBuffer[] args) {
        try {
            log.trace("EVALSHA {}({})", script.getName(), script.getSha1());
            return redis.evalsha(script.getSha1(), type, keys, args);
        } catch (RedisCommandExecutionException e) {
            if (isNoScript(e)) {
                log.trace("EVAL {}({})", script.getName(), script.getSha1());
                return redis.eval(script.getScript(), type, keys, args);
            }
            throw e;
        }
    }

    protected boolean isNoScript(RedisCommandExecutionException e) {
        log.warn("no script", e);
        return e != null && e.getMessage() != null && e.getMessage().contains("NOSCRIPT");
    }

    public RedisScript getRedisScript(String script) {
        return CACHE.get(script, this::loadRedisScript);
    }

    public RedisScript loadRedisScript(String name) {
        RedisScript redisScript = new RedisScript(name);
        log.trace("SCRIPT LOAD {} -> {}", redisScript.getSha1(), redisScript.getName());
        String sha = redis.scriptLoad(StandardCharsets.UTF_8.encode(redisScript.getScript()));
        if (sha.equals(redisScript.getSha1())) {
            return redisScript;
        }
        throw new IllegalStateException(name);
    }
}
