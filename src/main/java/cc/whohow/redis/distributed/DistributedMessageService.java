package cc.whohow.redis.distributed;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface DistributedMessageService {
    CompletableFuture<Integer> broadcast(ByteBuffer message);
    CompletableFuture<Boolean> anycast(ByteBuffer message);
    CompletableFuture<Boolean> unicast(String destination, ByteBuffer message);
    CompletableFuture<Integer> multicast(Collection<String> destination, ByteBuffer message);
}
