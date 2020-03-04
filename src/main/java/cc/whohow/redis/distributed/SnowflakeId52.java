package cc.whohow.redis.distributed;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public class SnowflakeId52 extends SnowflakeId {
    public SnowflakeId52() {
        this(Clock.systemDefaultZone(), Worker.ZERO);
    }

    public SnowflakeId52(Clock clock, LongSupplier worker) {
        this(clock, worker, Y2K);
    }

    public SnowflakeId52(Clock clock, LongSupplier worker, Instant epoch) {
        super(clock, worker, epoch, TimeUnit.SECONDS, 36L, 6L, 10L);
    }
}