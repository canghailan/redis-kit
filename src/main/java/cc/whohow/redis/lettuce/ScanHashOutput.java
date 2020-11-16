package cc.whohow.redis.lettuce;

import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.util.RedisScanIteration;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Function;

public class ScanHashOutput<K, V> extends CommandOutput<ByteSequence, ByteSequence, RedisScanIteration<Map.Entry<K, V>>> {
    protected final Function<ByteBuffer, K> keyDecoder;
    protected final Function<ByteBuffer, V> valueDecoder;
    protected int index = 0;
    protected K key;

    public ScanHashOutput(Function<ByteBuffer, K> keyDecoder, Function<ByteBuffer, V> valueDecoder) {
        this(keyDecoder, valueDecoder, new RedisScanIteration<>());
    }

    public ScanHashOutput(Function<ByteBuffer, K> keyDecoder, Function<ByteBuffer, V> valueDecoder, RedisScanIteration<Map.Entry<K, V>> output) {
        super(ByteSequenceRedisCodec.get(), output);
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (output.getCursor() == null) {
            output.setCursor(decodeAscii(bytes));
        } else {
            if (index++ % 2 == 0) {
                key = keyDecoder.apply(bytes);
            } else {
                output.getArray().add(new AbstractMap.SimpleImmutableEntry<>(key, valueDecoder.apply(bytes)));
            }
        }
    }
}
