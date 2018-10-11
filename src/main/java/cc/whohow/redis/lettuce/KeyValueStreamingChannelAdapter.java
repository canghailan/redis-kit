package cc.whohow.redis.lettuce;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.KeyValueStreamingChannel;

import java.nio.ByteBuffer;

public class KeyValueStreamingChannelAdapter<K, V> implements KeyValueStreamingChannel<ByteBuffer, ByteBuffer> {
    private final KeyValueStreamingChannel<K, V> keyValueStreamingChannel;
    private final RedisCodec<K, V> codec;

    public KeyValueStreamingChannelAdapter(KeyValueStreamingChannel<K, V> keyValueStreamingChannel, RedisCodec<K, V> codec) {
        this.keyValueStreamingChannel = keyValueStreamingChannel;
        this.codec = codec;
    }

    @Override
    public void onKeyValue(ByteBuffer key, ByteBuffer value) {
        keyValueStreamingChannel.onKeyValue(codec.decodeKey(key), codec.decodeValue(value));
    }
}
