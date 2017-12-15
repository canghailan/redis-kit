package cc.whohow.redis.jcache.codec;

import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import java.io.IOException;
import java.util.Optional;

public class OptionalCodec implements Codec {
    private final Codec innerCodec;

    private final Encoder valueEncoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            Optional<?> optional = (Optional<?>) in;
            return innerCodec.getValueEncoder().encode(optional.orElse(null));
        }
    };

    private final Decoder<Object> valueDecoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return Optional.ofNullable(innerCodec.getValueDecoder().decode(buf, state));
        }
    };

    private final Encoder mapKeyEncoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            Optional<?> optional = (Optional<?>) in;
            return innerCodec.getMapKeyEncoder().encode(optional.orElse(null));
        }
    };

    private final Decoder<Object> mapKeyDecoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return Optional.ofNullable(innerCodec.getMapKeyDecoder().decode(buf, state));
        }
    };

    private final Encoder mapValueEncoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
            Optional<?> optional = (Optional<?>) in;
            return innerCodec.getMapValueEncoder().encode(optional.orElse(null));
        }
    };

    private final Decoder<Object> mapValueDecoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            return Optional.ofNullable(innerCodec.getMapValueDecoder().decode(buf, state));
        }
    };

    public OptionalCodec(Codec innerCodec) {
        this.innerCodec = innerCodec;
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
}
