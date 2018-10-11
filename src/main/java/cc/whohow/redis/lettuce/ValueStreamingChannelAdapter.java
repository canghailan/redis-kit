package cc.whohow.redis.lettuce;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.ValueStreamingChannel;

import java.nio.ByteBuffer;

public class ValueStreamingChannelAdapter<V> implements ValueStreamingChannel<ByteBuffer> {
    private final ValueStreamingChannel<V> valueStreamingChannel;
    private final RedisCodec<?, V> codec;

    public ValueStreamingChannelAdapter(ValueStreamingChannel<V> valueStreamingChannel, RedisCodec<?, V> codec) {
        this.valueStreamingChannel = valueStreamingChannel;
        this.codec = codec;
    }

    @Override
    public void onValue(ByteBuffer value) {
        valueStreamingChannel.onValue(codec.decodeValue(value));
    }
}
