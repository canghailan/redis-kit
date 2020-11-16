package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class MapOutput<K, V> extends CommandOutput<ByteSequence, ByteSequence, Map<K, V>> {
    protected Function<ByteBuffer, K> keyDecoder;
    protected Function<ByteBuffer, V> valueDecoder;
    protected int index = 0;
    protected K key;

    public MapOutput(Function<ByteBuffer, K> keyDecoder, Function<ByteBuffer, V> valueDecoder) {
        this(keyDecoder, valueDecoder, new LinkedHashMap<>());
    }

    public MapOutput(Function<ByteBuffer, K> keyDecoder, Function<ByteBuffer, V> valueDecoder, Map<K, V> output) {
        super(ByteSequenceRedisCodec.get(), output);
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (index++ % 2 == 0) {
            key = keyDecoder.apply(bytes);
        } else {
            output.put(key, valueDecoder.apply(bytes));
        }
    }
}
