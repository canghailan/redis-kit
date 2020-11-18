package cc.whohow.redis;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.protocol.RedisCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LoggingRedis implements Redis {
    private static final Logger log = LogManager.getLogger();
    protected final Redis redis;

    public LoggingRedis(Redis redis) {
        this.redis = redis;
    }

    @Override
    public <T> T send(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command) {
        log(command);
        return redis.send(output, command);
    }

    @Override
    public <T> T send(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, CommandArgs<ByteSequence, ByteSequence> commandArgs) {
        log(command, commandArgs);
        return redis.send(output, command, commandArgs);
    }

    @Override
    public <T> T send(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, ByteSequence... commandArgs) {
        log(command, commandArgs);
        return redis.send(output, command, commandArgs);
    }

    @Override
    public <T> T send(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, Iterable<ByteSequence> commandArgs) {
        log(command, commandArgs);
        return redis.send(output, command, commandArgs);
    }

    @Override
    public <T> T send(RedisCommand<ByteSequence, ByteSequence, T> command) {
        if (command.getArgs() == null) {
            log(command.getType());
        } else {
            log(command.getType(), command.getArgs());
        }
        return redis.send(command);
    }

    @Override
    public <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command) {
        log(command);
        return redis.sendAsync(output, command);
    }

    @Override
    public <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, CommandArgs<ByteSequence, ByteSequence> commandArgs) {
        log(command, commandArgs);
        return redis.sendAsync(output, command, commandArgs);
    }

    @Override
    public <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, ByteSequence commandArg) {
        log(command, commandArg);
        return redis.sendAsync(output, command, commandArg);
    }

    @Override
    public <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, ByteSequence... commandArgs) {
        log(command, commandArgs);
        return redis.sendAsync(output, command, commandArgs);
    }

    @Override
    public <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, Iterable<ByteSequence> commandArgs) {
        log(command, commandArgs);
        return redis.sendAsync(output, command, commandArgs);
    }

    @Override
    public <T> CompletableFuture<T> sendAsync(RedisCommand<ByteSequence, ByteSequence, T> command) {
        if (command.getArgs() == null) {
            log(command.getType());
        } else {
            log(command.getType(), command.getArgs());
        }
        return redis.sendAsync(command);
    }

    @Override
    public <T> T eval(CommandOutput<ByteSequence, ByteSequence, T> output, RedisScript script, List<ByteSequence> keys, List<ByteSequence> args) {
        log(CommandType.EVAL, script, keys, args);
        return redis.eval(output, script, keys, args);
    }

    @Override
    public <T> T eval(CommandOutput<ByteSequence, ByteSequence, T> output, ByteSequence script, List<ByteSequence> keys, List<ByteSequence> args) {
        log(CommandType.EVAL, script, keys, args);
        return redis.eval(output, script, keys, args);
    }

    @Override
    public <T> T evalsha(CommandOutput<ByteSequence, ByteSequence, T> output, ByteSequence sha, List<ByteSequence> keys, List<ByteSequence> args) {
        log(CommandType.EVALSHA, sha, keys, args);
        return redis.evalsha(output, sha, keys, args);
    }

    @Override
    public <T> CompletableFuture<T> evalAsync(CommandOutput<ByteSequence, ByteSequence, T> output, ByteSequence script, List<ByteSequence> keys, List<ByteSequence> args) {
        log(CommandType.EVAL, script, keys, args);
        return redis.evalAsync(output, script, keys, args);
    }

    @Override
    public <T> CompletableFuture<T> evalshaAsync(CommandOutput<ByteSequence, ByteSequence, T> output, ByteSequence sha, List<ByteSequence> keys, List<ByteSequence> args) {
        log(CommandType.EVALSHA, sha, keys, args);
        return redis.evalshaAsync(output, sha, keys, args);
    }

    @Override
    public void addListener(RedisConnectionStateListener listener) {
        redis.addListener(listener);
    }

    @Override
    public void removeListener(RedisConnectionStateListener listener) {
        redis.removeListener(listener);
    }

    @Override
    public URI getURI() {
        return redis.getURI();
    }

    @Override
    public void close() throws Exception {
        redis.close();
    }

    protected void log(ProtocolKeyword command) {
        log.trace(command);
    }

    protected void log(ProtocolKeyword command, Object[] commandArgs) {
        if (log.isTraceEnabled()) {
            StringBuilder buffer = new StringBuilder();
            buffer.append(command).append(' ');
            for (Object arg : commandArgs) {
                buffer.append(arg).append(' ');
            }
            buffer.setLength(buffer.length() - 1);
            log.trace(buffer);
        }
    }

    protected void log(ProtocolKeyword command, Iterable<?> commandArgs) {
        if (log.isTraceEnabled()) {
            StringBuilder buffer = new StringBuilder();
            buffer.append(command).append(' ');
            for (Object arg : commandArgs) {
                buffer.append(arg).append(' ');
            }
            buffer.setLength(buffer.length() - 1);
            log.trace(buffer);
        }
    }

    protected void log(ProtocolKeyword command, CommandArgs<?, ?> commandArgs) {
        log.trace("{} {}", command, commandArgs);
    }

    protected void log(ProtocolKeyword command, Object script, List<?> keys, List<?> args) {
        if (log.isTraceEnabled()) {
            StringBuilder buffer = new StringBuilder();
            buffer.append(command).append(' ');
            buffer.append(script).append(' ');
            buffer.append(keys.size()).append(' ');
            for (Object key : keys) {
                buffer.append(key).append(' ');
            }
            for (Object arg : args) {
                buffer.append(arg).append(' ');
            }
            buffer.setLength(buffer.length() - 1);
            log.trace(buffer);
        }
    }
}
