package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.jcache.annotation.GeneratedKey;
import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;

public class GeneratedKeyJacksonCodec implements Codec {
    private final ObjectArrayJacksonCodec codec;

    private final Encoder encoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            GeneratedKey generatedKey = (GeneratedKey) in;
            return codec.getValueEncoder().encode(generatedKey.getKeys());
        }
    };

    private final Decoder<Object> decoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return GeneratedKey.of((Object[]) codec.getValueDecoder().decode(buf, state));
        }
    };

    public GeneratedKeyJacksonCodec(ObjectArrayJacksonCodec codec) {
        this.codec = codec;
    }

    @Override
    public Decoder<Object> getMapValueDecoder() {
        return getValueDecoder();
    }

    @Override
    public Encoder getMapValueEncoder() {
        return getValueEncoder();
    }

    @Override
    public Decoder<Object> getMapKeyDecoder() {
        return getValueDecoder();
    }

    @Override
    public Encoder getMapKeyEncoder() {
        return getValueEncoder();
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return decoder;
    }

    @Override
    public Encoder getValueEncoder() {
        return encoder;
    }
}
