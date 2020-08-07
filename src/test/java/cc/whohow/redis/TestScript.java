package cc.whohow.redis;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.lettuce.ByteBufferCodec;
import cc.whohow.redis.script.RedisScript;
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
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TestScript {
    private static final RedisClient redisClient = RedisClient.create();

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
    public void testGet() {
        RedisScriptCommands redisScriptCommands = new RedisScriptCommands(redis);
        List<ByteBuffer> r1 = redisScriptCommands.eval("get", ScriptOutputType.MULTI, new ByteBuffer[]{ByteBuffers.fromUtf8("nx")});
        System.out.println(r1);
        List<ByteBuffer> r2 = redisScriptCommands.eval("get", ScriptOutputType.MULTI, new ByteBuffer[]{ByteBuffers.fromUtf8("a")});
        System.out.println(r2.stream().map(PrimitiveCodec.LONG::decode).collect(Collectors.toList()));
    }

    @Test
    public void testExists() {
        RedisScriptCommands redisScriptCommands = new RedisScriptCommands(redis);
        List<ByteBuffer> r1 = redisScriptCommands.eval("exists", ScriptOutputType.MULTI,
                new ByteBuffer[]{ByteBuffers.fromUtf8("a"),
                        ByteBuffers.fromUtf8("b"),
                        ByteBuffers.fromUtf8("nx")});
        System.out.println(r1.stream().map(ByteBuffers::toUtf8String).collect(Collectors.toList()));
    }

    @Test
    public void testAcc() {
        RedisScriptCommands redisScriptCommands = new RedisScriptCommands(redis);
//        System.out.println(redisScriptCommands.loadRedisScript("acc"));
        ByteBuffer r = redisScriptCommands.eval("acc", ScriptOutputType.VALUE,
                new ByteBuffer[]{ByteBuffers.fromUtf8("a")},
                new ByteBuffer[]{ByteBuffers.fromUtf8("//"), ByteBuffers.fromUtf8("3"),
                        ByteBuffers.fromUtf8("PX"), ByteBuffers.fromUtf8("60000")});
        System.out.println(ByteBuffers.toUtf8String(r));
    }

    @Test
    public void test() {
        System.out.println(-10 / 3);
    }

    @Test
    public void testError() {
        RedisScriptCommands redisScriptCommands = new RedisScriptCommands(redis);
        RedisScript redisScript = new RedisScript("test", "return redis.call('time');");
        Object result1 = redisScriptCommands.eval(redisScript, ScriptOutputType.MULTI);
        System.out.println(result1);
        Object result2 = redisScriptCommands.eval(redisScript, ScriptOutputType.MULTI);
        System.out.println(result2);
    }

    @Test
    public void testScript() {
//        redis.set(ByteBuffers.fromUtf8("a"), ByteBuffers.fromUtf8("test"));


        RedisScriptCommands redisScriptCommands = new RedisScriptCommands(redis);
        Object r = redisScriptCommands.eval("lock", ScriptOutputType.BOOLEAN,
                new ByteBuffer[]{ByteBuffers.fromUtf8("b")},
                new ByteBuffer[]{ByteBuffers.fromUtf8("test1"),
                        ByteBuffers.fromUtf8("ex"),
                        PrimitiveCodec.LONG.encode(120L)});
        System.out.println(r);

//        ByteBuffer r = redisScriptCommands.eval("existkeys", ScriptOutputType.VALUE,
//                ByteBuffers.fromUtf8("b"),
//                ByteBuffers.fromUtf8("a"),
//                ByteBuffers.fromUtf8("c"));
//        System.out.println(ByteBuffers.toUtf8String(r));
    }
}
