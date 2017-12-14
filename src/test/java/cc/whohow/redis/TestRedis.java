package cc.whohow.redis;

import cc.whohow.redis.client.ConnectionPoolRedis;
import cc.whohow.redis.client.RedisPipeline;
import org.junit.Before;
import org.junit.Test;
import org.redisson.client.RedisPubSubConnection;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.pubsub.PubSubType;
import org.redisson.config.Config;
import org.redisson.misc.RPromise;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class TestRedis {
    private Redis redis;

    @Before
    public void setup() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            Properties properties = new Properties();
            properties.load(stream);

            String host = properties.getProperty("host");
            int port = Integer.parseInt(properties.getProperty("port", "6379"));
            String password = properties.getProperty("password", "");
            int database = Integer.parseInt(properties.getProperty("database", "0"));

            Config config = new Config();
            config.useSingleServer()
                    .setAddress("redis://" + host + ":" + port)
                    .setPassword(password)
                    .setDatabase(database);
            redis = new ConnectionPoolRedis(config);
        }
    }

    @Test
    public void testPublish() {
        System.out.println(redis.execute(RedisCommands.PUBLISH,  "test", "test-1"));
//        System.out.println(redis.execute(RedisCommands.PUBLISH,  "test-ex", "test-ex-2"));
    }

    @Test
    public void testSubscribe() throws Exception {
        RedisPubSubConnection connection = redis.getPubSubConnection();
        try {
            connection.addListener(new RedisPubSubListener() {
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
            connection.psubscribe(StringCodec.INSTANCE, "*");
            Thread.sleep(60_000L);
        } finally {
            connection.closeAsync();
        }
    }

    @Test
    public void testPipeline() {
        RedisPipeline pipeline = redis.pipeline();
        RPromise<String> ping = pipeline.execute(RedisCommands.PING);
        RPromise<Long> dbsize = pipeline.execute(RedisCommands.DBSIZE);
        RPromise<String> name = pipeline.execute(RedisCommands.CLIENT_GETNAME);
        pipeline.sync();
        System.out.println(ping.getNow());
        System.out.println(dbsize.getNow());
        System.out.println(name.getNow());
    }
}
