package cc.whohow.redis;

import cc.whohow.redis.client.RedisConnectionManagerAdapter;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@Warmup(iterations = 5)
@Measurement(iterations = 20)
//@Threads(32)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RedisBenchmark {
    static {
        System.setProperty("jmh.ignoreLock", "true");
    }

    public static void main(String[] args) throws Exception {
        int[] threads = {1, 2, 4, 8, 16, 32, 64, 128, 256};
//        int[] threads = {64};
        for (int n : threads) {
            Options options = new OptionsBuilder()
                    .include(RedisBenchmark.class.getSimpleName())
                    .forks(3)
                    .threads(n)
                    .output("benchmark-" + n + ".log")
                    .result("threads-" + n + ".csv")
                    .build();
            new Runner(options).run();
        }
    }

    @Benchmark
    public void testRedisAdapter(BenchmarkState state) {
        state.redis.execute(StringCodec.INSTANCE, RedisCommands.GET, "test");
    }

    @Benchmark
    public void testRedisson(BenchmarkState state) {
        state.rBucket.get();
    }

    @Benchmark
    public void testJedis(BenchmarkState state) {
        try (Jedis jedis = state.jedisPool.getResource()) {
            jedis.get("test");
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        public Properties properties;
        public Config config;
        public Redisson redisson;
        public RBucket<String> rBucket;
        public Redis redis;
        public JedisPool jedisPool;

        @Setup
        public void setup() throws Exception {
            try (InputStream stream = new FileInputStream("redisClient.properties")) {
                properties = new Properties();
                properties.load(stream);

                String host = properties.getProperty("host");
                int port = Integer.parseInt(properties.getProperty("port", "6379"));
                String password = properties.getProperty("password", null);
                int database = Integer.parseInt(properties.getProperty("database", "0"));

                config = new Config();
                config.useSingleServer()
                        .setAddress("redisClient://" + host + ":" + port)
                        .setPassword(password)
                        .setDatabase(database)
                        .setConnectionPoolSize(200);
                redisson = (Redisson) Redisson.create(config);
                rBucket = redisson.getBucket("test", StringCodec.INSTANCE);

                redis = new RedisConnectionManagerAdapter(redisson.getConnectionManager());

                if (password == null) {
                    jedisPool = new JedisPool("redisClient://" + host + ":" + port + "/" + database);
                } else {
                    jedisPool = new JedisPool("redisClient://:" + password + "@" + host + ":" + port + "/" + database);
                }
            }
        }

        @TearDown
        public void tearDown() throws Exception {
            redis.close();
            redisson.shutdown();
            jedisPool.close();
        }
    }
}
