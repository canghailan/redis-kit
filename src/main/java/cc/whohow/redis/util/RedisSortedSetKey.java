package cc.whohow.redis.util;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class RedisSortedSetKey<E> {
    private static final Logger log = LogManager.getLogger();
    protected final RedisCommands<ByteBuffer, ByteBuffer> redis;
    protected final Codec<E> codec;
    protected final ByteBuffer zsetKey;

    public RedisSortedSetKey(RedisCommands<ByteBuffer, ByteBuffer> redis, Codec<E> codec, ByteBuffer zsetKey) {
        this.redis = redis;
        this.codec = codec;
        this.zsetKey = zsetKey;
    }

    protected ByteBuffer encode(E value) {
        return codec.encode(value);
    }

    protected E decode(ByteBuffer byteBuffer) {
        return codec.decode(byteBuffer);
    }

    public long zadd(Number score, E member) {
        if (log.isTraceEnabled()) {
            log.trace("ZADD {} {} {}", toString(), score, member);
        }
        return redis.zadd(zsetKey.duplicate(), score.doubleValue(), encode(member));
    }

    public long zaddnx(Number score, E member) {
        if (log.isTraceEnabled()) {
            log.trace("ZADD {} NX {} {}", toString(), score, member);
        }
        return redis.zadd(zsetKey.duplicate(), Lettuce.Z_ADD_NX, score.doubleValue(), encode(member));
    }

    public long zaddxx(Number score, E member) {
        if (log.isTraceEnabled()) {
            log.trace("ZADD {} XX {} {}", toString(), score, member);
        }
        return redis.zadd(zsetKey.duplicate(), Lettuce.Z_ADD_XX, score.doubleValue(), encode(member));
    }

    @SuppressWarnings({"unchecked"})
    public long zadd(Collection<? extends Map.Entry<E, Number>> c) {
        if (c.isEmpty()) {
            return 0;
        }
        if (log.isTraceEnabled()) {
            log.trace("ZADD {} {}", toString(), c);
        }
        return redis.zadd(zsetKey.duplicate(), c.stream()
                .map(e -> ScoredValue.fromNullable(e.getValue().doubleValue(), encode(e.getKey())))
                .toArray(ScoredValue[]::new));
    }

    @SuppressWarnings({"unchecked"})
    public long zadd(Map<? extends E, ? extends Number> m) {
        if (m.isEmpty()) {
            return 0;
        }
        if (log.isTraceEnabled()) {
            log.trace("ZADD {} {}", toString(), m);
        }
        return redis.zadd(zsetKey.duplicate(), m.entrySet().stream()
                .map(e -> ScoredValue.fromNullable(e.getValue().doubleValue(), encode(e.getKey())))
                .toArray(ScoredValue[]::new));
    }

    public long zcard() {
        if (log.isTraceEnabled()) {
            log.trace("ZCARD {}", toString());
        }
        return redis.zcard(zsetKey.duplicate());
    }

    public long zcount(Number min, Number max) {
        if (log.isTraceEnabled()) {
            log.trace("ZCOUNT {} {} {}", toString(), min, max);
        }
        return redis.zcount(zsetKey.duplicate(), Range.create(min, max));
    }

    public Number zscore(E e) {
        if (log.isTraceEnabled()) {
            log.trace("ZSCORE {} {}", toString(), e);
        }
        return redis.zscore(zsetKey.duplicate(), encode(e));
    }

    public Stream<ScoredValue<E>> zrangeWithScores(long start, long stop) {
        if (log.isTraceEnabled()) {
            log.trace("ZRANGE {} {} {} WITHSCORES", toString(), start, stop);
        }
        return redis.zrangeWithScores(zsetKey.duplicate(), start, stop).stream()
                .map(v -> v.map(this::decode));
    }

    public long zrem(E e) {
        if (log.isTraceEnabled()) {
            log.trace("ZREM {} {}", toString(), e);
        }
        return redis.zrem(zsetKey.duplicate(), encode(e));
    }

    public long zrem(Collection<? extends E> e) {
        if (e.isEmpty()) {
            return 0;
        }
        if (log.isTraceEnabled()) {
            log.trace("ZREM {} {}", toString(), e);
        }
        return redis.zrem(zsetKey.duplicate(), e.stream()
                .map(this::encode)
                .toArray(ByteBuffer[]::new));
    }

    public Stream<ScoredValue<E>> zremrangebyscoreWithScores(long min, long max, long offset, long count) {
        if (log.isTraceEnabled()) {
            log.trace("EVAL zremrangebyscore {} {} {} withscores limit {} {}", toString(), min, max, offset, count);
        }
        List<ByteBuffer> result = new RedisScriptCommands(redis).eval("zremrangebyscore", ScriptOutputType.MULTI,
                new ByteBuffer[]{
                        zsetKey.duplicate()
                },
                new ByteBuffer[]{
                        PrimitiveCodec.LONG.encode(min),
                        PrimitiveCodec.LONG.encode(max),
                        Lettuce.withscores(),
                        Lettuce.limit(),
                        PrimitiveCodec.LONG.encode(offset),
                        PrimitiveCodec.LONG.encode(count)
                });
        if (result.size() < 2) {
            return Stream.empty();
        }
        List<ScoredValue<E>> list = new ArrayList<>(result.size() / 2);
        for (int i = 0; i < result.size(); i += 2) {
            list.add(ScoredValue.just((double) PrimitiveCodec.LONG.decode(result.get(i + 1)), decode(result.get(i))));
        }
        return list.stream();
    }

    public ScoredValue<E> zpopminWithScores() {
        if (log.isTraceEnabled()) {
            log.trace("EVAL zremrangebyscore {} -inf +inf withscores limit 0 1", toString());
        }
        List<ByteBuffer> result = new RedisScriptCommands(redis).eval("zremrangebyscore", ScriptOutputType.MULTI,
                new ByteBuffer[]{
                        zsetKey.duplicate()
                },
                new ByteBuffer[]{
                        Lettuce.negInf(),
                        Lettuce.inf(),
                        Lettuce.withscores(),
                        Lettuce.limit(),
                        Lettuce.zero(),
                        Lettuce.one()
                });
        if (result.size() < 2) {
            return ScoredValue.empty();
        }
        return ScoredValue.just(PrimitiveCodec.LONG.decode(result.get(1)), decode(result.get(0)));
    }

    public long del() {
        if (log.isTraceEnabled()) {
            log.trace("DEL {}", toString());
        }
        return redis.del(zsetKey.duplicate());
    }

    @Override
    public String toString() {
        return ByteBuffers.toString(zsetKey);
    }
}
