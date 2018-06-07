package cc.whohow.redis;

import cc.whohow.redis.client.RedisConnectionManagerAdapter;
import cc.whohow.redis.codec.Codec;
import cc.whohow.redis.codec.ObjectJacksonCodec;
import cc.whohow.redis.codec.OptionalCodec;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RFuture;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.pubsub.PubSubType;
import org.redisson.config.Config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

public class TestRedis {
    private static Redisson redisson;
    private static Redis redis;
    private Codec optionalCodec = new OptionalCodec(new ObjectJacksonCodec(String.class));

    public static Redisson setupRedisson() throws Exception {
        try (InputStream stream = new FileInputStream("redisClient.properties")) {
            Properties properties = new Properties();
            properties.load(stream);

            String host = properties.getProperty("host");
            int port = Integer.parseInt(properties.getProperty("port", "6379"));
            String password = properties.getProperty("password", null);
            int database = Integer.parseInt(properties.getProperty("database", "0"));

            Config config = new Config();
            config.useSingleServer()
                    .setAddress("redisClient://" + host + ":" + port)
                    .setPassword(password)
                    .setDatabase(database);

            return (Redisson) Redisson.create(config);
        }
    }

    @BeforeClass
    public static void setup() throws Exception {
        redisson = setupRedisson();
        redis = new RedisConnectionManagerAdapter(redisson.getConnectionManager());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        redisson.shutdown();
    }

    @Test
    public void testGet() {
        System.out.println((String) redis.execute(StringCodec.INSTANCE, RedisCommands.GET, "test"));
    }

    @Test
    public void testPublish() {
//        System.out.println(redisClient.execute(RedisCommands.PUBLISH, "test", "test-1"));
//        System.out.println(redisClient.execute(RedisCommands.PUBLISH,  "test-ex", "test-ex-2"));
        System.out.println(redis.execute(RedisCommands.PUBLISH,  "RedisCacheManager", "SYNC doc"));
    }

    @Test
    @Ignore
    public void testSubscribe() throws Exception {
        redis.psubscribe("*", StringCodec.INSTANCE, new RedisPubSubListener() {
            @Override
            public boolean onStatus(PubSubType type, String channel) {
                System.out.println("onStatus");
                System.out.println(type);
                System.out.println(channel);
                return false;
            }

            @Override
            public void onPatternMessage(String pattern, String channel, Object message) {
                System.out.println("onPatternMessage");
                System.out.println(pattern);
                System.out.println(channel);
                System.out.println(message);
            }

            @Override
            public void onMessage(String channel, Object msg) {
                System.out.println("onMessage");
                System.out.println(channel);
                System.out.println(msg);
            }
        });
        Thread.sleep(60_000L);
    }

    @Test
    public void testPipeline() {
        RedisPipeline pipeline = redis.pipeline();
        RFuture<String> ping = pipeline.execute(RedisCommands.PING);
        RFuture<Long> dbsize = pipeline.execute(RedisCommands.DBSIZE);
        RFuture<String> name = pipeline.execute(RedisCommands.CLIENT_GETNAME);
        pipeline.flush();
        System.out.println(ping.getNow());
        System.out.println(dbsize.getNow());
        System.out.println(name.getNow());
    }

    @Test
    public void testOptionalEncoder() throws Exception {
        Optional<String> empty = Optional.empty();
        Optional<String> abc = Optional.of("abc");
        redis.execute(RedisCommands.SET, "test-empty", optionalCodec.getValueEncoder().encode(empty));
        redis.execute(RedisCommands.SET, "test-abc", optionalCodec.getValueEncoder().encode(abc));
    }

    @Test
    public void testOptionalDecoder() throws Exception {
        Optional<String> notExists = redis.execute(optionalCodec, RedisCommands.GET, "test-no-exists");
        Optional<String> empty = redis.execute(optionalCodec, RedisCommands.GET, "test-empty");
        Optional<String> abc = redis.execute(optionalCodec, RedisCommands.GET, "test-abc");
        System.out.println(notExists);
        System.out.println(empty);
        System.out.println(empty.orElse("[empty]"));
        System.out.println(abc);
        System.out.println(abc.orElse("[abc]"));
    }
}
