package cc.whohow.redis.lettuce;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.KeyStreamingChannel;

import java.nio.ByteBuffer;

public class KeyStreamingChannelAdapter<K> implements KeyStreamingChannel<ByteBuffer> {
    private final KeyStreamingChannel<K> keyStreamingChannel;
    private final RedisCodec<K, ?> codec;

    public KeyStreamingChannelAdapter(KeyStreamingChannel<K> keyStreamingChannel, RedisCodec<K, ?> codec) {
        this.keyStreamingChannel = keyStreamingChannel;
        this.codec = codec;
    }

    @Override
    public void onKey(ByteBuffer key) {
        keyStreamingChannel.onKey(codec.decodeKey(key));
    }
}
