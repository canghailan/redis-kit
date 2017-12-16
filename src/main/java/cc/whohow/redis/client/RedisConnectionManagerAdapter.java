package cc.whohow.redis.client;

import cc.whohow.redis.Redis;
import cc.whohow.redis.RedisPipeline;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.api.RFuture;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandBatchService;
import org.redisson.command.CommandExecutor;
import org.redisson.config.SingleServerConfig;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.MasterSlaveEntry;
import org.redisson.connection.PubSubConnectionEntry;

import java.net.URI;
import java.util.List;

/**
 *
 */
public class RedisConnectionManagerAdapter implements Redis {
    private static final Logger log = LogManager.getLogger();

    protected final SingleServerConfig config;
    protected final ConnectionManager connectionManager;
    protected final MasterSlaveEntry master;
    protected final CommandExecutor commandExecutor;

    public RedisConnectionManagerAdapter(ConnectionManager connectionManager) {
        this.config = connectionManager.getCfg().useSingleServer();
        this.connectionManager = connectionManager;
        this.master = connectionManager.getEntry(connectionManager.calcSlot(""));
        this.commandExecutor = connectionManager.getCommandExecutor();
    }

    @Override
    public URI getUri() {
        return config.getAddress().resolve("/" + config.getDatabase());
    }

    @Override
    public RedisPipeline pipeline() {
        return new Pipeline(this);
    }

    @Override
    public <T> T execute(RedisCommand<T> command, Object... params) {
        return execute(null, command, params);
    }

    @Override
    public <T, R> R execute(Codec codec, RedisCommand<T> command, Object... params) {
        log.trace("{}", command);
        return commandExecutor.get(commandExecutor.writeAsync(master, codec, command, params));
    }

    @Override
    public PubSubConnectionEntry subscribe(String name, Codec codec, RedisPubSubListener<?>... listeners) {
        log.trace("{} {}", RedisCommands.SUBSCRIBE, name);
        return connectionManager.subscribe(codec, name, listeners).syncUninterruptibly().getNow();
    }

    @Override
    public PubSubConnectionEntry psubscribe(String pattern, Codec codec, RedisPubSubListener<?>... listeners) {
        log.trace("{} {}", RedisCommands.PSUBSCRIBE, pattern);
        return connectionManager.psubscribe(pattern, codec, listeners).syncUninterruptibly().getNow();
    }

    @Override
    public void unsubscribe(String name) {
        log.trace("{} {}", RedisCommands.UNSUBSCRIBE, name);
        connectionManager.unsubscribe(name, false);
    }

    @Override
    public void punsubscribe(String pattern) {
        log.trace("{} {}", RedisCommands.PUNSUBSCRIBE, pattern);
        connectionManager.punsubscribe(pattern, false);
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return getUri().toString();
    }

    static class Pipeline implements RedisPipeline {
        private final RedisConnectionManagerAdapter redis;
        private final CommandBatchService commandExecutor;

        Pipeline(RedisConnectionManagerAdapter redis) {
            this.redis = redis;
            this.commandExecutor = new CommandBatchService(redis.connectionManager);
        }

        @Override
        public <T> RFuture<T> execute(RedisCommand<T> command, Object... params) {
            return execute(null, command, params);
        }

        @Override
        public <T, R> RFuture<R> execute(Codec codec, RedisCommand<T> command, Object... params) {
            log.trace("{} {}", this, command);
            return commandExecutor.writeAsync(redis.master, codec, command, params);
        }

        @Override
        public List<?> flush() {
            return commandExecutor.get(commandExecutor.executeAsync());
        }

        @Override
        public String toString() {
            return "pipeline#" + hashCode();
        }
    }
}
