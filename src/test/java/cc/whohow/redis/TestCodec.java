package cc.whohow.redis;

import cc.whohow.redis.io.Codec;
import cc.whohow.redis.jcache.ImmutableGeneratedCacheKey;
import cc.whohow.redis.jcache.codec.ImmutableGeneratedCacheKeyCodecBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.sun.org.apache.bcel.internal.classfile.Code;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class TestCodec {
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
        System.out.println(codec.decode(encoded.duplicate()));
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
    public void testCodec() throws Exception {
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
        System.out.println("\0");
    }
}
