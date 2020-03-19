package cc.whohow.redis.messaging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 消息队列轮询任务
 */
public class PollingMessageQueue<E> implements Callable<Long> {
    protected static final Logger log = LogManager.getLogger();
    /**
     * 等待数据就绪
     */
    protected static final int STATE_WAITING = 0;
    /**
     * 数据就绪，等待消费
     */
    protected static final int STATE_READY = 1;
    /**
     * 数据消费中
     */
    protected static final int STATE_RUNNING = 2;

    /**
     * 消息队列
     */
    protected final Queue<E> queue;
    /**
     * 消费者
     */
    protected final Consumer<E> consumer;
    /**
     * 轮询状态
     */
    protected final AtomicInteger state = new AtomicInteger(STATE_WAITING);

    public PollingMessageQueue(Queue<E> queue, Consumer<E> consumer) {
        this.queue = queue;
        this.consumer = consumer;
    }

    public boolean isWaiting() {
        return state.get() == STATE_WAITING;
    }

    public boolean isReady() {
        return state.get() == STATE_READY;
    }

    public boolean isRunning() {
        return state.get() == STATE_RUNNING;
    }

    /**
     * 通知数据已准备就绪
     */
    public boolean setReady() {
        return state.compareAndSet(STATE_WAITING, STATE_READY);
    }

    /**
     * 停止消费
     */
    public boolean setWaiting() {
        state.set(STATE_WAITING);
        return true;
    }

    /**
     * 获取任务状态
     */
    public String getState() {
        switch (state.get()) {
            case STATE_WAITING:
                return "waiting";
            case STATE_READY:
                return "ready";
            case STATE_RUNNING:
                return "running";
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * 生产
     */
    public void offer(E e) {
        queue.offer(e);
    }

    /**
     * 消费
     */
    public E poll() {
        return queue.poll();
    }

    /**
     * 轮询，消费完所有数据后停止，返回本次轮询消费数量
     */
    @Override
    public Long call() {
        if (state.compareAndSet(STATE_READY, STATE_RUNNING)) {
            long n = 0;
            while (isRunning() && n < Long.MAX_VALUE) {
                try {
                    E value = poll();
                    if (value == null) {
                        break;
                    }
                    consumer.accept(value);
                    n++;
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }
            }
            state.compareAndSet(STATE_RUNNING, STATE_WAITING);
            log.debug("poll: {}", n);
            return n;
        } else {
            log.debug("state: {}", getState());
            return 0L;
        }
    }
}
