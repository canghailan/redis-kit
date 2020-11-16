package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.lettuce.DecodeOutput;
import cc.whohow.redis.lettuce.IntegerOutput;
import cc.whohow.redis.lettuce.SetOutput;
import io.lettuce.core.protocol.CommandType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class AbstractRedisSet<E> {
    protected final Redis redis;
    protected final Codec<E> codec;
    protected final ByteSequence setKey;

    public AbstractRedisSet(Redis redis, Codec<E> codec, ByteSequence key) {
        this.redis = redis;
        this.codec = codec;
        this.setKey = key;
    }

    public Long scard() {
        return redis.send(new IntegerOutput(), CommandType.SCARD, setKey);
    }

    public Long sismember(E e) {
        return redis.send(new IntegerOutput(), CommandType.SISMEMBER, setKey, codec.encode(e));
    }

    public Set<E> smembers() {
        return redis.send(new SetOutput<>(codec::decode), CommandType.SMEMBERS, setKey);
    }

    public E srandmember() {
        return redis.send(new DecodeOutput<>(codec::decode), CommandType.SRANDMEMBER, setKey);
    }

    public Long sadd(E e) {
        return redis.send(new IntegerOutput(), CommandType.SADD, setKey, codec.encode(e));
    }

    public Long sadd(Collection<? extends E> c) {
        List<ByteSequence> args = new ArrayList<>(1 + c.size());
        args.add(setKey);
        for (E e : c) {
            args.add(codec.encode(e));
        }
        return redis.send(new IntegerOutput(), CommandType.SADD, args);
    }

    public Long srem(E e) {
        return redis.send(new IntegerOutput(), CommandType.SREM, setKey, codec.encode(e));
    }

    public Long srem(Collection<? extends E> c) {
        List<ByteSequence> args = new ArrayList<>(1 + c.size());
        args.add(setKey);
        for (E e : c) {
            args.add(codec.encode(e));
        }
        return redis.send(new IntegerOutput(), CommandType.SREM, args);
    }

    public E spop() {
        return redis.send(new DecodeOutput<>(codec::decode), CommandType.SPOP, setKey);
    }

    public Long del() {
        return redis.send(new IntegerOutput(), CommandType.DEL, setKey);
    }

    @Override
    public String toString() {
        return setKey.toString();
    }
}
