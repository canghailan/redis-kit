package cc.whohow.redis;

import cc.whohow.redis.pool.RedisConnectionPool;
import org.junit.Before;
import org.junit.Test;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.config.Config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class TestRedisConnectionPool {
    private Config config;

    @Before
    public void setup() throws Exception {
        try (InputStream stream = new FileInputStream("redis.properties")) {
            Properties properties = new Properties();
            properties.load(stream);
            config = new Config();
            config.useSingleServer()
                    .setAddress(properties.getProperty("address"))
                    .setPassword(properties.getProperty("password", ""))
                    .setDatabase(Integer.parseInt(properties.getProperty("database", "0")));
        }
    }

    @Test
    public void test() throws Exception {
        try (Redis redis = new RedisConnectionPool(config)) {
            try (PooledRedisConnection connection = redis.getPooledConnection()) {
                System.out.println(connection.get().sync(RedisCommands.PING));
            }
        }
    }
}
