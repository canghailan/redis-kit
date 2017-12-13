package cc.whohow.redis;

import cc.whohow.redis.client.ConnectionPoolRedis;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@Warmup(iterations = 3)
//@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
//@Threads(5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TestRedis {
    static {
        System.setProperty("jmh.ignoreLock", "true");
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        public Properties properties;
        public Config config;
        public Redis redis;
        public JedisPool jedisPool;

        @Setup
        public void setup() throws Exception {
            try (InputStream stream = new FileInputStream("redis.properties")) {
                properties = new Properties();
                properties.load(stream);

                String host = properties.getProperty("host");
                int port = Integer.parseInt(properties.getProperty("port", "6379"));
                String password = properties.getProperty("password", "");
                int database = Integer.parseInt(properties.getProperty("database", "0"));

                config = new Config();
                config.useSingleServer()
                        .setAddress("redis://" + host + ":" + port)
                        .setPassword(password)
                        .setDatabase(database);
                redis = new ConnectionPoolRedis(config);

                jedisPool = new JedisPool("redis://:" + password + "@" + host + ":" + port + "/" + database);
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            redis.close();
            jedisPool.close();
        }
    }

    @Benchmark
    public void testRedisConnectionPool(BenchmarkState state) {
        state.redis.execute(RedisCommands.PING);
    }

    @Benchmark
    public void testJedis(BenchmarkState state) {
        try (Jedis jedis = state.jedisPool.getResource()) {
            jedis.ping();
        }
    }

    public static void main(String[] args) throws Exception {
        int[] threads = {1,2,4,8,16,32,64,128,256};
        for (int n : threads) {
            Options options = new OptionsBuilder()
                    .include(TestRedis.class.getSimpleName())
                    .forks(3)
                    .threads(n)
                    .result("threads-" + n + ".csv")
                    .build();
            new Runner(options).run();
        }
    }
}
