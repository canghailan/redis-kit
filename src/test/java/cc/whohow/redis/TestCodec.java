package cc.whohow.redis;

import cc.whohow.redis.jcache.annotation.GeneratedKey;
import cc.whohow.redis.jcache.codec.GeneratedKeyJacksonCodec;
import cc.whohow.redis.jcache.codec.ObjectArrayJacksonCodec;
import cc.whohow.redis.jcache.codec.ObjectJacksonCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TestCodec {
    private ObjectJacksonCodec objectCodec = new ObjectJacksonCodec("java.util.List<java.lang.Integer>");
    private ObjectArrayJacksonCodec objectArrayCodec = new ObjectArrayJacksonCodec(
            "java.lang.String",
            "int",
            "java.lang.Integer",
            "long",
            "java.lang.Long",
            "[B");
    private Codec generatedKeyCodec = new GeneratedKeyJacksonCodec(objectArrayCodec);

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
    public void testObjectEncoder() throws Exception {
        List<Integer> object = Arrays.asList(1,2,3,4);
        System.out.println(objectCodec.getValueEncoder().encode(object).toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testObjectDecoder() throws Exception {
        System.out.println(objectCodec.getValueDecoder().decode(Unpooled.copiedBuffer(
                "[1,2,3,4]",
                StandardCharsets.UTF_8), new State(false)).getClass());
    }

    @Test
    public void testObjectArrayEncoder() throws Exception {
        Object[] objectArray = {"a", 1, 3, 5L, 7L, "xyz".getBytes()};
        System.out.println(objectArrayCodec.getValueEncoder().encode(objectArray).toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testObjectArrayDecoder() throws Exception {
        Object[] objectArray = (Object[]) objectArrayCodec.getValueDecoder().decode(Unpooled.copiedBuffer(
                "[\"a\",1,3,5,7,\"eHl6\"]",
                StandardCharsets.UTF_8), new State(false));
        for (Object object : objectArray) {
            System.out.println(object);
            System.out.println(object.getClass());
        }
    }

    @Test
    public void testCacheKeyEncoder() throws Exception {
        GeneratedKey generatedKey = GeneratedKey.of("a", 1, 3, 5L, 7L, "xyz".getBytes());
        System.out.println(generatedKeyCodec.getValueEncoder().encode(generatedKey).toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testCacheKeyDecoder() throws Exception {
        GeneratedKey generatedKey = (GeneratedKey) generatedKeyCodec.getValueDecoder().decode(Unpooled.copiedBuffer(
                "[\"a\",1,3,5,7,\"eHl6\"]",
                StandardCharsets.UTF_8), new State(false));
        System.out.println(generatedKey);
        for (Object object : generatedKey.getKeys()) {
            System.out.println(object);
            System.out.println(object.getClass());
        }
    }

    @Test
    public void testObjectMapper() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        System.out.println(objectMapper.writeValueAsString(new BigDecimal("3.14159")));
        System.out.println(objectMapper.writeValueAsString(new Date()));
        System.out.println(objectMapper.readValue("\"2017-12-14T15:08:14.599+0000\"", Date.class));
    }
}
