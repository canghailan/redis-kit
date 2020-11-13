package cc.whohow.redis;

import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.lettuce.ByteSequenceRedisCodec;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.RedisCommand;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Redis extends AutoCloseable {
    static boolean isNoScript(Throwable e) {
        return e instanceof RedisCommandExecutionException &&
                e.getMessage() != null &&
                e.getMessage().contains("NOSCRIPT");
    }

    default <T> T send(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command) {
        return sendAsync(output, command).join();
    }

    default <T> T send(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, CommandArgs<ByteSequence, ByteSequence> commandArgs) {
        return sendAsync(output, command, commandArgs).join();
    }

    default <T> T send(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, ByteSequence... commandArgs) {
        return sendAsync(output, command, commandArgs).join();
    }

    default <T> T send(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, Iterable<ByteSequence> commandArgs) {
        return sendAsync(output, command, commandArgs).join();
    }

    default <T> T send(RedisCommand<ByteSequence, ByteSequence, T> command) {
        return sendAsync(command).join();
    }

    default <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command) {
        return sendAsync(new Command<>(command, output));
    }

    default <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, CommandArgs<ByteSequence, ByteSequence> commandArgs) {
        return sendAsync(new Command<>(command, output, commandArgs));
    }

    default <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, ByteSequence commandArg) {
        return sendAsync(output, command, new CommandArgs<>(ByteSequenceRedisCodec.get()).addValue(commandArg));
    }

    default <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, ByteSequence... commandArgs) {
        return sendAsync(output, command, new CommandArgs<>(ByteSequenceRedisCodec.get()).addValues(commandArgs));
    }

    default <T> CompletableFuture<T> sendAsync(CommandOutput<ByteSequence, ByteSequence, T> output, CommandType command, Iterable<ByteSequence> commandArgs) {
        return sendAsync(output, command, new CommandArgs<>(ByteSequenceRedisCodec.get()).addValues(commandArgs));
    }

    <T> CompletableFuture<T> sendAsync(RedisCommand<ByteSequence, ByteSequence, T> command);

    default <T> T eval(CommandOutput<ByteSequence, ByteSequence, T> output,
                       RedisScript script, List<ByteSequence> keys, List<ByteSequence> args) {
        try {
            return evalsha(output, ByteSequence.ascii(script.getSha1()), keys, args);
        } catch (RedisCommandExecutionException e) {
            if (isNoScript(e)) {
                return eval(output, ByteSequence.utf8(script.getScript()), keys, args);
            }
            throw e;
        }
    }

    default <T> T eval(CommandOutput<ByteSequence, ByteSequence, T> output,
                       ByteSequence script, List<ByteSequence> keys, List<ByteSequence> args) {
        return evalAsync(output, script, keys, args).join();
    }

    default <T> T evalsha(CommandOutput<ByteSequence, ByteSequence, T> output,
                          ByteSequence sha, List<ByteSequence> keys, List<ByteSequence> args) {
        return evalshaAsync(output, sha, keys, args).join();
    }

    default <T> CompletableFuture<T> evalAsync(CommandOutput<ByteSequence, ByteSequence, T> output,
                                               ByteSequence script, List<ByteSequence> keys, List<ByteSequence> args) {
        List<ByteSequence> commandArgs = new ArrayList<>(1 + 1 + keys.size() + args.size());
        commandArgs.add(script);
        commandArgs.add(RESP.b(keys.size()));
        commandArgs.addAll(keys);
        commandArgs.addAll(args);
        return sendAsync(output, CommandType.EVAL, commandArgs);
    }

    default <T> CompletableFuture<T> evalshaAsync(CommandOutput<ByteSequence, ByteSequence, T> output,
                                                  ByteSequence sha, List<ByteSequence> keys, List<ByteSequence> args) {
        List<ByteSequence> commandArgs = new ArrayList<>(1 + 1 + keys.size() + args.size());
        commandArgs.add(sha);
        commandArgs.add(RESP.b(keys.size()));
        commandArgs.addAll(keys);
        commandArgs.addAll(args);
        return sendAsync(output, CommandType.EVAL, commandArgs);
    }

    void addListener(RedisConnectionStateListener listener);

    void removeListener(RedisConnectionStateListener listener);

    URI getURI();
}
