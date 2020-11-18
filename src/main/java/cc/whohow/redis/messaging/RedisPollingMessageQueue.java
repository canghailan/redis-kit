package cc.whohow.redis.messaging;

import cc.whohow.redis.RedisTracking;
import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisConnectionStateListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Redis消息队列，基于键空间事件触发轮询
 */
public class RedisPollingMessageQueue<E> extends PollingMessageQueue<E>
        implements RedisTracking.Listener, RedisConnectionStateListener, AutoCloseable {
    private static final Logger log = LogManager.getLogger();
    private final String name;
    private final ExecutorService executor;
    private final RedisTracking redisTracking;

    public RedisPollingMessageQueue(Queue<E> queue, String name,
                                    Consumer<E> consumer,
                                    ExecutorService executor,
                                    RedisTracking redisTracking) {
        super(queue, consumer);
        this.name = name;
        this.executor = executor;
        this.redisTracking = redisTracking;
        this.redisTracking.addListener(name, this);
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean setReady() {
        if (super.setReady()) {
            // 异步执行，防止阻塞键空间事件
            executor.submit(this);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void onRedisConnected(RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
        // 断线重连，触发轮询
        log.debug("RedisConnected, start polling: {}", getName());
        start();
    }

    @Override
    public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
        // 断线，停止轮询
        log.debug("RedisDisconnected, stop polling: {}", getName());
        stop();
    }

    @Override
    public void onRedisExceptionCaught(RedisChannelHandler<?, ?> connection, Throwable cause) {
    }

    @Override
    public void onInvalidate(ByteSequence key) {
        // 接收事件通知，触发轮询
        log.trace("RedisInvalidate, start polling: {}", getName());
        start();
    }

    @Override
    public void close() throws Exception {
        // 关闭消息队列，取消监听，停止轮询
        redisTracking.removeListener(name, this);
        stop();
    }

    @Override
    public String toString() {
        return name;
    }
}
