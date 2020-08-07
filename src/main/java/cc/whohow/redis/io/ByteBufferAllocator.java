package cc.whohow.redis.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * 自适应缓冲区分配器
 */
public class ByteBufferAllocator {
    private static final Logger log = LogManager.getLogger();
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();
    private final LongAccumulator max = new LongAccumulator(Long::max, 0);
    private final int initial;
    private final int threshold;

    public ByteBufferAllocator() {
        this(128, 256);
    }

    public ByteBufferAllocator(int initial, int threshold) {
        this.initial = initial;
        this.threshold = threshold;
    }

    public void record(int bufferSize) {
        log.trace("record: {}", bufferSize);
        if (bufferSize < 0) {
            throw new IllegalArgumentException();
        }
        count.increment();
        sum.add(bufferSize);
        max.accumulate(bufferSize);
    }

    public int guess() {
        int maxBufferSize = getMax();
        if (maxBufferSize == 0) {
            return initial;
        }
        if (maxBufferSize < threshold) {
            return maxBufferSize;
        }

        // ignore concurrency
        int avgBufferSize = getAvg();
        return avgBufferSize + (Integer.min(maxBufferSize - avgBufferSize, avgBufferSize) * 3 / 4);
    }

    public ByteBuffer allocate() {
        int bufferSize = guess();
        log.trace("allocate: {}", bufferSize);
        return ByteBuffer.allocate(bufferSize);
    }

    protected long getCount() {
        return count.longValue();
    }

    protected int getMax() {
        return max.intValue();
    }

    protected int getAvg() {
        return (int) (sum.doubleValue() / count.doubleValue());
    }

    @Override
    public String toString() {
        return "ByteBufferAllocator{" +
                "count=" + getCount() +
                ", max=" + getMax() +
                ", avg=" + getAvg() +
                '}';
    }
}
