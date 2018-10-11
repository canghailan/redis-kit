package cc.whohow.redis.lettuce;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.ScoredValueStreamingChannel;

import java.nio.ByteBuffer;

public class ScoredValueStreamingChannelAdapter<V> implements ScoredValueStreamingChannel<ByteBuffer> {
    private final ScoredValueStreamingChannel<V> scoredValueStreamingChannel;
    private final RedisCodec<?, V> codec;

    public ScoredValueStreamingChannelAdapter(ScoredValueStreamingChannel<V> scoredValueStreamingChannel, RedisCodec<?, V> codec) {
        this.scoredValueStreamingChannel = scoredValueStreamingChannel;
        this.codec = codec;
    }

    @Override
    public void onValue(ScoredValue<ByteBuffer> value) {
        scoredValueStreamingChannel.onValue(value.map(codec::decodeValue));
    }
}
