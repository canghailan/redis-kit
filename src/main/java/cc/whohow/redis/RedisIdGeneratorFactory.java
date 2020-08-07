package cc.whohow.redis;

import java.util.function.LongSupplier;

public interface RedisIdGeneratorFactory {
    LongSupplier newI64Generator();

    LongSupplier newI52Generator();
}
