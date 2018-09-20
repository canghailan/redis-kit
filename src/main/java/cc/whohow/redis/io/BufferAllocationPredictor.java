package cc.whohow.redis.io;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;

/**
 * 缓冲区分配预测器（基于统计）
 */
public class BufferAllocationPredictor implements IntConsumer {
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();
    private final LongAccumulator min = new LongAccumulator(Long::min, 0);
    private final LongAccumulator max = new LongAccumulator(Long::max, 0);
    private final int initial;
    private final int threshold;

    public BufferAllocationPredictor() {
        this(128, 256);
    }

    public BufferAllocationPredictor(int initial, int threshold) {
        this.initial = initial;
        this.threshold = threshold;
    }

    @Override
    public void accept(int value) {
        if (value < 0) {
            throw new IllegalArgumentException();
        }
        count.increment();
        sum.add(value);
        min.accumulate(-value);
        max.accumulate(value);
    }

    public long getCount() {
        return count.longValue();
    }

    public double getSum() {
        return sum.doubleValue();
    }

    public int getMin() {
        return -min.intValue();
    }

    public int getMax() {
        return max.intValue();
    }

    public int getPredicted() {
        int max = getMax();
        if (max == 0) {
            return initial;
        }
        if (max < threshold) {
            return max;
        }

        long count = getCount();
        double sum = getSum();
        double avg = sum / count;
        if (max > avg * 2) {
            return (int) avg;
        }
        return (int) (avg + max) * 3 / 4;
    }
}
