package cc.whohow.redis.bytes;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * 编码器统计
 */
public class ByteStatistics {
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();
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
        max.accumulate(bytes);
    }

    public long getCount() {
        return count.longValue();
    }

    public int getMax() {
        return max.intValue();
    }

    public int getAvg() {
        return count.longValue() == 0 ? 0 : (int) (sum.doubleValue() / count.doubleValue());
    }

    @Override
    public String toString() {
        return "CodecStatistics{" +
                "count=" + getCount() +
                ", max=" + getMax() +
                ", avg=" + getAvg() +
                '}';
    }
}
