package cc.whohow.redis;

import cc.whohow.redis.io.*;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import cc.whohow.redis.jcache.codec.ImmutableGeneratedCacheKeyCodecBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Random;
import java.util.stream.Collectors;

public class TestCodec {
    private static void log(ByteBuffer byteBuffer) {
        System.out.println(StandardCharsets.UTF_8.decode(byteBuffer.duplicate()).toString());
    }

    @Test
    public void testTypeCanonicalName() {
        System.out.println(String.class.getCanonicalName());
        System.out.println(Integer.class.getCanonicalName());
        System.out.println(int.class.getCanonicalName());
        System.out.println(Long.class.getCanonicalName());
        System.out.println(long.class.getCanonicalName());
        System.out.println(byte.class.getCanonicalName());
        System.out.println(byte[].class.getCanonicalName());
        System.out.println("---");
        System.out.println(TypeFactory.defaultInstance().constructType(String.class).toCanonical());
        System.out.println(TypeFactory.defaultInstance().constructType(Integer.class).toCanonical());
        System.out.println(TypeFactory.defaultInstance().constructType(int.class).toCanonical());
        System.out.println(TypeFactory.defaultInstance().constructType(Long.class).toCanonical());
        System.out.println(TypeFactory.defaultInstance().constructType(long.class).toCanonical());
        System.out.println(TypeFactory.defaultInstance().constructType(byte.class).toCanonical());
        System.out.println(TypeFactory.defaultInstance().constructArrayType(byte.class).toCanonical());
    }

    @Test
    public void testPrimitiveCodec() {
        Random random = new Random();

        ByteBuffer b1 = PrimitiveCodec.BOOLEAN.encode(random.nextBoolean());
        log(b1);
        System.out.println(PrimitiveCodec.BOOLEAN.decode(b1.duplicate()));

        ByteBuffer b2 = PrimitiveCodec.BYTE.encode((byte) random.nextInt(Byte.MAX_VALUE));
        log(b2);
        System.out.println(PrimitiveCodec.BYTE.decode(b2.duplicate()));

        ByteBuffer b3 = PrimitiveCodec.SHORT.encode((short) random.nextInt(Short.MAX_VALUE));
        log(b3);
        System.out.println(PrimitiveCodec.SHORT.decode(b3.duplicate()));

        ByteBuffer b4 = PrimitiveCodec.INTEGER.encode(random.nextInt());
        log(b4);
        System.out.println(PrimitiveCodec.INTEGER.decode(b4.duplicate()));

        ByteBuffer b5 = PrimitiveCodec.LONG.encode(random.nextLong());
        log(b5);
        System.out.println(PrimitiveCodec.LONG.decode(b5.duplicate()));

        ByteBuffer b6 = PrimitiveCodec.FLOAT.encode(random.nextFloat());
        log(b6);
        System.out.println(PrimitiveCodec.FLOAT.decode(b6.duplicate()));

        ByteBuffer b7 = PrimitiveCodec.DOUBLE.encode(random.nextDouble());
        log(b7);
        System.out.println(PrimitiveCodec.DOUBLE.decode(b7.duplicate()));

        ByteBuffer b8 = PrimitiveCodec.INTEGER.encode(null);
        log(b8);
        System.out.println(PrimitiveCodec.INTEGER.decode(b8.duplicate()));
    }

    @Test
    public void testStringCodec() {
        StringCodec stringCodec = new StringCodec();

        ByteBuffer b1 = stringCodec.encode("abc中文");
        log(b1);
        System.out.println(stringCodec.decode(b1.duplicate()));

        ByteBuffer b2 = stringCodec.encode("");
        log(b2);
        System.out.println(stringCodec.decode(b2.duplicate()));

        ByteBuffer b3 = stringCodec.encode(null);
        log(b3);
        System.out.println(stringCodec.decode(b3.duplicate()));
    }

    @Test
    public void testLz4Codec() {
        Random random = new Random();
        String string = random.ints(100000).mapToObj(Integer::toString).collect(Collectors.joining());

        Codec<String> stringCodec = new StringCodec();
        Codec<String> lz4Codec = new Lz4Codec<>(stringCodec);

        ByteBuffer b1 = stringCodec.encode(string);
        ByteBuffer b2 = lz4Codec.encode(string);
        String decoded = lz4Codec.decode(b2.duplicate());

        System.out.println(b1.remaining());
        System.out.println(b2.remaining());
        System.out.println(b2.getInt(0));
//        System.out.println(string);
//        System.out.println(decoded);

        Assert.assertEquals(string, decoded);
    }

    @Test
    public void testGzipCodec() {
        Random random = new Random();
        String string = random.ints(100000).mapToObj(Integer::toString).collect(Collectors.joining());

        Codec<String> stringCodec = new StringCodec();
        Codec<String> lz4Codec = new GzipCodec<>(stringCodec);

        ByteBuffer b1 = stringCodec.encode(string);
        ByteBuffer b2 = lz4Codec.encode(string);
        String decoded = lz4Codec.decode(b2.duplicate());

        System.out.println(b1.remaining());
        System.out.println(b2.remaining());
//        System.out.println(string);
//        System.out.println(decoded);

        Assert.assertEquals(string, decoded);
    }

    @Test
    public void testCompressCodec() throws Exception {
        Random random = new Random();
        String string = random.ints(100000).mapToObj(Integer::toString).collect(Collectors.joining());

        Codec<String> stringCodec = new StringCodec();
        Codec<String> compressCodec = new CompressCodec<>(CompressorStreamFactory.getGzip(), stringCodec);

        ByteBuffer b1 = stringCodec.encode(string);
        ByteBuffer b2 = compressCodec.encode(string);

        String decoded = compressCodec.decode(b2.duplicate());

        System.out.println(b1.remaining());
        System.out.println(b2.remaining());
//        System.out.println(string);
//        System.out.println(decoded);

        Assert.assertEquals(string, decoded);
    }

    @Test
    public void testImmutableGeneratedCacheKeyCodec() throws Exception {
        Codec<ImmutableGeneratedCacheKey> codec = new ImmutableGeneratedCacheKeyCodecBuilder().build(
                String.class.getCanonicalName(),
                Integer.class.getCanonicalName(),
                Integer.class.getCanonicalName(),
                Long.class.getCanonicalName(),
                Long.class.getCanonicalName()
        );

        Object[] objectArray = {"a", 1, 3, 5L, 7L};
        ByteBuffer encoded = codec.encode(ImmutableGeneratedCacheKey.of(objectArray));

        System.out.println(StandardCharsets.UTF_8.decode(encoded.duplicate()));
        for (Object key : codec.decode(encoded.duplicate()).getKeys()) {
            System.out.println(key.getClass());
        }
    }

    @Test
    public void testObjectMapper() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        System.out.println(objectMapper.writeValueAsString(new BigDecimal("3.14159")));
        System.out.println(objectMapper.writeValueAsString(new Date()));
        System.out.println(objectMapper.readValue("\"2017-12-14T15:08:14.599+0000\"", Date.class));
    }

    @Test
    public void testTextLength() throws Exception {
        System.out.println(Double.MAX_VALUE);
        System.out.println(Double.MIN_VALUE);
        System.out.println(Double.MIN_NORMAL);
        System.out.println(Double.MIN_EXPONENT);
        System.out.println(Boolean.FALSE);
        System.out.println(Byte.MIN_VALUE);
        System.out.println(Short.MIN_VALUE);
        System.out.println(Integer.MIN_VALUE);
        System.out.println(Long.MIN_VALUE);
        System.out.println(Float.MIN_VALUE);
        System.out.println(Float.MIN_NORMAL);

        System.out.println(Double.parseDouble("1.7976931348623157E308"));
    }

    @Test
    public void testNullString() {
        System.out.println("\127");
    }
}
