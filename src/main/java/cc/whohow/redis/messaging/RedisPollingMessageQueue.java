package cc.whohow.redis.messaging;

import cc.whohow.redis.buffer.ByteSequence;
import cc.whohow.redis.util.RedisKeyspaceNotification;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Redis消息队列，基于键空间事件触发轮询
 */
public class RedisPollingMessageQueue<E>
        extends PollingMessageQueue<E>
        implements RedisKeyspaceNotification.Listener, AutoCloseable {
    private final String name;
    private final ExecutorService executor;
    private final RedisKeyspaceNotification redisKeyspaceNotification;

    public RedisPollingMessageQueue(Queue<E> queue, String name,
                                    Consumer<E> consumer,
                                    ExecutorService executor,
                                    RedisKeyspaceNotification redisKeyspaceNotification) {
        super(queue, consumer);
        this.name = name;
        this.executor = executor;
        this.redisKeyspaceNotification = redisKeyspaceNotification;
        this.redisKeyspaceNotification.addListener(name, this);
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

//    @Override
//    public void onKeyEvent(ByteBuffer key) {
//        // 接收事件通知，触发轮询
//        start();
//    }
//
//    @Override
//    public void subscribed() {
//        // 断线重连，触发轮询
//        start();
//    }

    @Override
    public void onInvalidate(ByteSequence key) {
        // 断线重连，触发轮询
        start();
    }

    @Override
    public void close() throws Exception {
        // 关闭消息队列，取消监听，停止轮询
        redisKeyspaceNotification.removeListener(name, this);
        stop();
    }

    @Override
    public String toString() {
        return name;
    }
}
