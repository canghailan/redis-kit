package cc.whohow.redis.messaging;

import cc.whohow.redis.util.RedisKeyspaceEvents;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Redis消息队列，基于键空间事件触发轮询
 */
public class RedisPollingMessageQueue<E> extends PollingMessageQueue<E> implements RedisKeyspaceEvents.Listener, AutoCloseable {
    private final String name;
    private final ExecutorService executor;
    private final RedisKeyspaceEvents redisKeyspaceEvents;

    public RedisPollingMessageQueue(Queue<E> queue, String name,
                                    Consumer<E> consumer,
                                    ExecutorService executor,
                                    RedisKeyspaceEvents redisKeyspaceEvents) {
        super(queue, consumer);
        this.name = name;
        this.executor = executor;
        this.redisKeyspaceEvents = redisKeyspaceEvents;
        this.redisKeyspaceEvents.addListener(name, this);
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
    public void onKeyEvent(ByteBuffer key) {
        // 接收事件通知，触发轮询
        setReady();
    }

    @Override
    public void subscribed() {
        // 断线重连，触发轮询
        setReady();
    }

    @Override
    public void close() throws Exception {
        // 关闭消息队列，取消监听，停止轮询
        redisKeyspaceEvents.removeListener(name, this);
        setWaiting();
    }

    @Override
    public String toString() {
        return name;
    }
}
