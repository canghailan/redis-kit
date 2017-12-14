package cc.whohow.redis.jcache.codec;

import cc.whohow.redis.jcache.JCacheKey;
import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;

public class JCacheKeyJacksonCodec implements Codec {
    private final ObjectArrayJacksonCodec objectArrayJacksonCodec;

    private final Encoder encoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            return objectArrayJacksonCodec.getValueEncoder().encode(((JCacheKey) in).getCacheKeys());
        }
    };

    private final Decoder<Object> decoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return new JCacheKey((Object[]) objectArrayJacksonCodec.getValueDecoder().decode(buf, state));
        }
    };

    public JCacheKeyJacksonCodec(ObjectArrayJacksonCodec objectArrayJacksonCodec) {
        this.objectArrayJacksonCodec = objectArrayJacksonCodec;
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
