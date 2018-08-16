package cc.whohow.redis.distributed;

import java.util.stream.Stream;

public interface DistributedSystem {
    DistributedClient localClient();
    Stream<DistributedClient> getClients();
    DistributedMessageService getMessageService();
}
