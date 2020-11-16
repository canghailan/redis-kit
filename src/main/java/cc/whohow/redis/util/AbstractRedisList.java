package cc.whohow.redis.util;

import cc.whohow.redis.RESP;
import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.lettuce.DecodeOutput;
import cc.whohow.redis.lettuce.IntegerOutput;
import cc.whohow.redis.lettuce.ListOutput;
import cc.whohow.redis.lettuce.VoidOutput;
import io.lettuce.core.protocol.CommandType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AbstractRedisList<E> {
    protected final Redis redis;
    protected final Codec<E> codec;
    protected final ByteSequence listKey;

    public AbstractRedisList(Redis redis, Codec<E> codec, ByteSequence key) {
        this.redis = redis;
        this.codec = codec;
        this.listKey = key;
    }

    public Long llen() {
        return redis.send(new IntegerOutput(), CommandType.LLEN, listKey);
    }

    public E lindex(long index) {
        return redis.send(new DecodeOutput<>(codec::decode), CommandType.LINDEX, listKey, RESP.b(index));
    }

    public void lset(long index, E element) {
        redis.send(new VoidOutput(), CommandType.LSET, listKey, RESP.b(index), codec.encode(element));
    }

    public List<E> lrange(long start, long stop) {
        return redis.send(new ListOutput<>(codec::decode), CommandType.LRANGE, listKey, RESP.b(start), RESP.b(stop));
    }

    public Long lpush(E e) {
        return redis.send(new IntegerOutput(), CommandType.LPUSH, listKey, codec.encode(e));
    }

    public Long rpush(E e) {
        return redis.send(new IntegerOutput(), CommandType.RPUSH, listKey, codec.encode(e));
    }

    public Long rpush(Collection<? extends E> c) {
        if (c.isEmpty()) {
            return 0L;
        }

        List<ByteSequence> args = new ArrayList<>(1 + c.size());
        args.add(listKey);
        for (E e : c) {
            args.add(codec.encode(e));
        }
        return redis.send(new IntegerOutput(), CommandType.RPUSH, args);
    }

    public E lpop() {
        return redis.send(new DecodeOutput<>(codec::decode), CommandType.LPOP, listKey);
    }

    public E rpop() {
        return redis.send(new DecodeOutput<>(codec::decode), CommandType.RPOP, listKey);
    }

    public E blpop(long timeout) {
        return redis.send(new DecodeOutput<>(codec::decode), CommandType.BLPOP, listKey, RESP.b(timeout));
    }

    public E brpop(long timeout) {
        return redis.send(new DecodeOutput<>(codec::decode), CommandType.BRPOP, listKey, RESP.b(timeout));
    }

    public Long lrem(int count, E e) {
        return redis.send(new IntegerOutput(), CommandType.LREM, listKey, RESP.b(count), codec.encode(e));
    }

    public Long del() {
        return redis.send(new IntegerOutput(), CommandType.DEL, listKey);
    }

    @Override
    public String toString() {
        return listKey.toString();
    }
}
