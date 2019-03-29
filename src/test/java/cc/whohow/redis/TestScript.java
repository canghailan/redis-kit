package cc.whohow.redis;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;

public class TestScript {
    private static RedisClient redisClient = RedisClient.create();

    private static Properties properties;
    private static RedisURI redisURI;
    private static StatefulRedisConnection<ByteBuffer, ByteBuffer> connection;
    private static RedisCommands<ByteBuffer, ByteBuffer> redis;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            properties = new Properties();
            properties.load(stream);
            redisURI = RedisURI.create(properties.getProperty("uri"));
            connection = redisClient.connect(new ByteBufferCodec(), redisURI);
            redis = connection.sync();
        }
    }

    @AfterClass
    public static void tearDown() {
        connection.close();
        redisClient.shutdown();
    }

    @Test
    public void testScript() {
//        redis.set(ByteBuffers.fromUtf8("a"), ByteBuffers.fromUtf8("test"));


        RedisScriptCommands redisScriptCommands = new RedisScriptCommands(redis);
        Object r = redisScriptCommands.eval("lock", ScriptOutputType.BOOLEAN,
                new ByteBuffer[]{ByteBuffers.fromUtf8("b")},
                ByteBuffers.fromUtf8("test1"),
                ByteBuffers.fromUtf8("ex"),
                PrimitiveCodec.LONG.encode(120L));
        System.out.println(r);

//        ByteBuffer r = redisScriptCommands.eval("existkeys", ScriptOutputType.VALUE,
//                ByteBuffers.fromUtf8("b"),
//                ByteBuffers.fromUtf8("a"),
//                ByteBuffers.fromUtf8("c"));
//        System.out.println(ByteBuffers.toUtf8String(r));
    }
}
