package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisScript;
import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.io.Codec;
import cc.whohow.redis.lettuce.*;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;

import java.util.*;

public class AbstractRedisSortedSet<E> {
    protected final Redis redis;
    protected final Codec<E> codec;
    protected final ByteSequence sortedSetKey;

    public AbstractRedisSortedSet(Redis redis, Codec<E> codec, ByteSequence key) {
        this.redis = redis;
        this.codec = codec;
        this.sortedSetKey = key;
    }

    public Long zcard() {
        return redis.send(new IntegerOutput(), CommandType.ZCARD, sortedSetKey);
    }

    public Long zcount(Number min, Number max) {
        return redis.send(new IntegerOutput(), CommandType.ZCOUNT, sortedSetKey, RESP.b(min), RESP.b(max));
    }

    public Number zscore(E e) {
        return redis.send(new NumberOutput(), CommandType.ZSCORE, sortedSetKey, codec.encode(e));
    }

    public List<E> zrange(long start, long stop) {
        return redis.send(new ListOutput<>(codec::decode), CommandType.ZRANGE, sortedSetKey, RESP.b(start), RESP.b(stop));
    }

    public List<ScoredValue<E>> zrangeWithScores(long start, long stop) {
        return redis.send(new ScoredValueListOutput<>(codec::decode), CommandType.ZRANGE, sortedSetKey, RESP.b(start), RESP.b(stop), RESP.b(CommandKeyword.WITHSCORES));
    }

    public Map<E, Number> zrangeWithScoresAsMap(long start, long stop) {
        return redis.send(new MapOutput<>(codec::decode, RESP::f64), CommandType.ZRANGE, sortedSetKey, RESP.b(start), RESP.b(stop), RESP.b(CommandKeyword.WITHSCORES));
    }

    public Long zadd(Number score, E member) {
        return redis.send(new IntegerOutput(), CommandType.ZADD, sortedSetKey, RESP.b(score), codec.encode(member));
    }

    public Long zaddnx(Number score, E member) {
        return redis.send(new IntegerOutput(), CommandType.ZADD, sortedSetKey, RESP.nx(), RESP.b(score), codec.encode(member));
    }

    public Long zaddxx(Number score, E member) {
        return redis.send(new IntegerOutput(), CommandType.ZADD, sortedSetKey, RESP.xx(), RESP.b(score), codec.encode(member));
    }

    public Long zadd(Collection<? extends Map.Entry<E, Number>> c) {
        if (c.isEmpty()) {
            return 0L;
        }

        List<ByteSequence> args = new ArrayList<>(1 + c.size() * 2);
        args.add(sortedSetKey);
        for (Map.Entry<E, Number> e : c) {
            args.add(RESP.b(e.getValue()));
            args.add(codec.encode(e.getKey()));
        }
        return redis.send(new IntegerOutput(), CommandType.ZADD, args);
    }

    public Long zadd(Map<? extends E, ? extends Number> m) {
        if (m.isEmpty()) {
            return 0L;
        }

        List<ByteSequence> args = new ArrayList<>(1 + m.size() * 2);
        args.add(sortedSetKey);
        for (Map.Entry<? extends E, ? extends Number> e : m.entrySet()) {
            args.add(RESP.b(e.getValue()));
            args.add(codec.encode(e.getKey()));
        }
        return redis.send(new IntegerOutput(), CommandType.ZADD, args);
    }

    public Long zrem(E e) {
        return redis.send(new IntegerOutput(), CommandType.ZREM, sortedSetKey, codec.encode(e));
    }

    public Long zrem(Collection<? extends E> c) {
        if (c.isEmpty()) {
            return 0L;
        }

        List<ByteSequence> args = new ArrayList<>(1 + c.size());
        args.add(sortedSetKey);
        for (E e : c) {
            args.add(codec.encode(e));
        }
        return redis.send(new IntegerOutput(), CommandType.ZREM, args);
    }

    public ScoredValue<E> zpopminWithScores() {
        return redis.eval(new ScoredValueOutput<>(codec::decode),
                RedisScript.get("zremrangebyscore"),
                Collections.singletonList(sortedSetKey),
                Arrays.asList(RESP.nInf(), RESP.pInf(), RESP.b(CommandKeyword.WITHSCORES), RESP.b(CommandKeyword.LIMIT), RESP.b(0), RESP.b(1)));
    }

    public List<ScoredValue<E>> zremrangebyscoreWithScores(long min, long max, long offset, long count) {
        return redis.eval(new ScoredValueListOutput<>(codec::decode),
                RedisScript.get("zremrangebyscore"),
                Collections.singletonList(sortedSetKey),
                Arrays.asList(RESP.b(min), RESP.b(max), RESP.b(CommandKeyword.WITHSCORES), RESP.b(CommandKeyword.LIMIT), RESP.b(offset), RESP.b(count)));
    }

    public Long del() {
        return redis.send(new IntegerOutput(), CommandType.DEL, sortedSetKey);
    }

    @Override
    public String toString() {
        return sortedSetKey.toString();
    }
}
