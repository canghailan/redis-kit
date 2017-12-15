package cc.whohow.redis.codec;

import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public class Codecs {
    public static ByteBuf[] concat(ByteBuf[]... byteBufArrays) {
        int length = Arrays.stream(byteBufArrays).mapToInt(array -> array.length).sum();
        ByteBuf[] byteBufArray = new ByteBuf[length];
        int position = 0;
        for (ByteBuf[] a : byteBufArrays) {
            System.arraycopy(a, 0, byteBufArray, position, a.length);
            position += a.length;
        }
        return byteBufArray;
    }

    public static ByteBuf[] concat(ByteBuf byteBuf1, ByteBuf... byteBufArray2) {
        ByteBuf[] byteBufArray = new ByteBuf[1 + byteBufArray2.length];
        byteBufArray[0] = byteBuf1;
        System.arraycopy(byteBufArray2, 0, byteBufArray, 1, byteBufArray2.length);
        return byteBufArray;
    }

    public static ByteBuf[] concat(ByteBuf[] byteBufArray1, ByteBuf... byteBufArray2) {
        ByteBuf[] byteBufArray = new ByteBuf[byteBufArray1.length + byteBufArray2.length];
        System.arraycopy(byteBufArray1, 0, byteBufArray, 0, byteBufArray1.length);
        System.arraycopy(byteBufArray2, 0, byteBufArray, byteBufArray1.length, byteBufArray2.length);
        return byteBufArray;
    }

    public static ByteBuf encode(Codec codec, Object value) {
        try {
            return codec.getValueEncoder().encode(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T decode(Codec codec, ByteBuf byteBuf) {
        try {
            return (T) codec.getValueDecoder().decode(byteBuf, new State(false));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ByteBuf[] encode(Codec codec, Collection<?> values) {
        ByteBuf[] byteBufArray = new ByteBuf[values.size()];
        try {
            int i = 0;
            for (Object value : values) {
                byteBufArray[i++] = encode(codec, value);
            }
            return byteBufArray;
        } catch (RuntimeException e) {
            release(byteBufArray);
            throw e;
        }
    }

    public static <T> T[] decode(Codec codec, ByteBuf[] byteBufArray, Function<Integer, T[]> newArray) {
        T[] array = newArray.apply(byteBufArray.length);
        int i = 0;
        try {
            while (i < byteBufArray.length) {
                array[i] = decode(codec, byteBufArray[i]);
                i++;
            }
            return array;
        } catch (RuntimeException e) {
            while (i < byteBufArray.length) {
                byteBufArray[i++].release();
            }
            throw e;
        }
    }

    public static ByteBuf encodeMapKey(Codec codec, Object value) {
        try {
            return codec.getMapKeyEncoder().encode(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ByteBuf encodeMapValue(Codec codec, Object value) {
        try {
            return codec.getMapValueEncoder().encode(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ByteBuf[] encodeMapKeyValue(Codec codec, Map<?, ?> keyValues) {
        ByteBuf[] byteBufArray = new ByteBuf[keyValues.size() * 2];
        try {
            int i = 0;
            for (Map.Entry<?, ?> keyValue : keyValues.entrySet()) {
                byteBufArray[i * 2] = encodeMapKey(codec, keyValue.getKey());
                byteBufArray[i * 2 + 1] = encodeMapValue(codec, keyValue.getValue());
                i++;
            }
            return byteBufArray;
        } catch (RuntimeException e) {
            release(byteBufArray);
            throw e;
        }
    }

    public static void release(ByteBuf byteBuf) {
        if (byteBuf != null) {
            byteBuf.release();
        }
    }

    public static void release(ByteBuf[] byteBufArray) {
        for (ByteBuf byteBuf : byteBufArray) {
            release(byteBuf);
        }
    }
}
