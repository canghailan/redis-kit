package cc.whohow.redis.codec;

import cc.whohow.redis.jcache.annotation.GeneratedKey;
import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;
import java.util.function.Function;

public class WrapCodec implements Codec {
    protected final Codec codec;
    protected final Function<Object, GeneratedKey> wrap;
    protected final Function<GeneratedKey, Object> unwrap;

    private final Encoder valueEncoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            return codec.getValueEncoder().encode(unwrap.apply((GeneratedKey) in));
        }
    };

    private final Decoder<Object> valueDecoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return wrap.apply(codec.getValueDecoder().decode(buf, state));
        }
    };

    private final Encoder mapKeyEncoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            return codec.getMapKeyEncoder().encode(unwrap.apply((GeneratedKey) in));
        }
    };

    private final Decoder<Object> mapKeyDecoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return wrap.apply(codec.getMapKeyDecoder().decode(buf, state));
        }
    };

    private final Encoder mapValueEncoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            return codec.getMapValueEncoder().encode(unwrap.apply((GeneratedKey) in));
        }
    };

    private final Decoder<Object> mapValueDecoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return wrap.apply(codec.getMapValueDecoder().decode(buf, state));
        }
    };

    public WrapCodec(Codec codec, Function<Object, GeneratedKey> wrap, Function<GeneratedKey, Object> unwrap) {
        this.codec = codec;
        this.wrap = wrap;
        this.unwrap = unwrap;
    }

    @Override
    public Decoder<Object> getMapValueDecoder() {
        return mapValueDecoder;
    }

    @Override
    public Encoder getMapValueEncoder() {
        return mapValueEncoder;
    }

    @Override
    public Decoder<Object> getMapKeyDecoder() {
        return mapKeyDecoder;
    }

    @Override
    public Encoder getMapKeyEncoder() {
        return mapKeyEncoder;
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return valueDecoder;
    }

    @Override
    public Encoder getValueEncoder() {
        return valueEncoder;
    }

    @Override
    public String toString() {
        return "WrapCodec{" +
                "codec=" + codec +
                '}';
    }
}
