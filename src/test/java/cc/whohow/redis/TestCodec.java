package cc.whohow.redis;

import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.bytes.ByteSummaryStatistics;
import cc.whohow.redis.codec.*;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import cc.whohow.redis.jcache.codec.ImmutableGeneratedCacheKeyCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Random;
import java.util.stream.Collectors;

public class TestCodec {
    @Test
    public void testCeilingNextPowerOfTwo() {
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(0));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(1));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(2));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(3));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(65));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(127));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(128));
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

        ByteSequence b1 = PrimitiveCodec.BOOLEAN.encode(random.nextBoolean());
        System.out.println(b1.toString());
        System.out.println(PrimitiveCodec.BOOLEAN.decode(b1));

        ByteSequence b2 = PrimitiveCodec.BYTE.encode((byte) random.nextInt(Byte.MAX_VALUE));
        System.out.println(b2.toString());
        System.out.println(PrimitiveCodec.BYTE.decode(b2));

        ByteSequence b3 = PrimitiveCodec.SHORT.encode((short) random.nextInt(Short.MAX_VALUE));
        System.out.println(b3.toString());
        System.out.println(PrimitiveCodec.SHORT.decode(b3));

        ByteSequence b4 = PrimitiveCodec.INTEGER.encode(random.nextInt());
        System.out.println(b4.toString());
        System.out.println(PrimitiveCodec.INTEGER.decode(b4));

        ByteSequence b5 = PrimitiveCodec.LONG.encode(random.nextLong());
        System.out.println(b5.toString());
        System.out.println(PrimitiveCodec.LONG.decode(b5));

        ByteSequence b6 = PrimitiveCodec.FLOAT.encode(random.nextFloat());
        System.out.println(b6.toString());
        System.out.println(PrimitiveCodec.FLOAT.decode(b6));

        ByteSequence b7 = PrimitiveCodec.DOUBLE.encode(random.nextDouble());
        System.out.println(b7.toString());
        System.out.println(PrimitiveCodec.DOUBLE.decode(b7));

        ByteSequence b8 = PrimitiveCodec.INTEGER.encode(null);
        System.out.println(b8.toString());
        System.out.println(PrimitiveCodec.INTEGER.decode(b8));
    }

    @Test
    public void testStringCodec() {
        Codec<String> stringCodec = new StringCodec.UTF8();

        ByteSequence b1 = stringCodec.encode("abc中文");
        System.out.println(b1.toString());
        System.out.println(stringCodec.decode(b1));

        ByteSequence b2 = stringCodec.encode("");
        System.out.println(b2.toString());
        System.out.println(stringCodec.decode(b2));

        ByteSequence b3 = stringCodec.encode(null);
        System.out.println(b3.toString());
        System.out.println(stringCodec.decode(b3));
    }

    @Test
    public void testGzipCodec() {
        Random random = new Random();
        String string = random.ints(100000).mapToObj(Integer::toString).collect(Collectors.joining());

        Codec<String> stringCodec = new StringCodec.UTF8();
        Codec<String> gzipCodec = new GzipCodec<>(stringCodec);

        ByteSequence b1 = stringCodec.encode(string);
        ByteSequence b2 = gzipCodec.encode(string);
        String decoded = gzipCodec.decode(b2);

        System.out.println(b1.length());
        System.out.println(b2.length());
//        System.out.println(string);
//        System.out.println(decoded);

        Assert.assertEquals(string, decoded);
    }

    @Test
    public void testCompressCodec() throws Exception {
        Random random = new Random();
        String string = random.ints(100000).mapToObj(Integer::toString).collect(Collectors.joining());

        Codec<String> stringCodec = new StringCodec.UTF8();
        Codec<String> compressCodec = new CompressCodec<>(CompressorStreamFactory.getGzip(), stringCodec);

        ByteSequence b1 = stringCodec.encode(string);
        ByteSequence b2 = compressCodec.encode(string);

        String decoded = compressCodec.decode(b2);

        System.out.println(b1.length());
        System.out.println(b2.length());
//        System.out.println(string);
//        System.out.println(decoded);

        Assert.assertEquals(string, decoded);
    }

    @Test
    public void testImmutableGeneratedCacheKeyCodec() throws Exception {
        Codec<ImmutableGeneratedCacheKey> codec = ImmutableGeneratedCacheKeyCodec.create(
                String.class.getCanonicalName(),
                Integer.class.getCanonicalName(),
                Integer.class.getCanonicalName(),
                Long.class.getCanonicalName(),
                Long.class.getCanonicalName()
        );

        Object[] objectArray = {"a", 1, 3, 5L, 7L};
        ByteSequence encoded = codec.encode(ImmutableGeneratedCacheKey.of(objectArray));

        System.out.println(encoded.toString());
        for (Object key : codec.decode(encoded).getKeys()) {
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

    @Test
    public void testSnappy() throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new SnappyCompressorInputStream(new FileInputStream(
                "")), StandardCharsets.UTF_8))) {
            reader.lines().forEach(System.out::println);
        }
    }
}
