package cc.whohow.redis.util;

import java.util.concurrent.Delayed;
import java.util.function.Supplier;

public interface DelayedValue<T> extends Delayed, Supplier<T> {
}
