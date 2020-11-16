package cc.whohow.redis.bytes;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * 字节统计
 *
 * @see java.util.IntSummaryStatistics
 */
public class ByteSummaryStatistics {
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();
    private final LongAccumulator min = new LongAccumulator(Long::min, Integer.MAX_VALUE);
    private final LongAccumulator max = new LongAccumulator(Long::max, 0);

    public static int ceilingNextPowerOfTwo(int x) {
        return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
    }

    public void accept(int bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException();
        }
        count.increment();
        sum.add(bytes);
        min.accumulate(bytes);
        max.accumulate(bytes);
    }

    public long getCount() {
        return count.longValue();
    }

    public double getSum() {
        return sum.doubleValue();
    }

    public int getMin() {
        return min.intValue();
    }

    public int getMax() {
        return max.intValue();
    }

    public double getAverage() {
        return count.longValue() == 0 ? 0 : sum.doubleValue() / count.doubleValue();
    }

    @Override
    public String toString() {
        return "ByteSummaryStatistics{" +
                "count=" + getCount() +
                ", sum=" + getSum() +
                ", min=" + getMin() +
                ", max=" + getMax() +
                ", avg=" + getAverage() +
                '}';
    }
}
