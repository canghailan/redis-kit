package cc.whohow.redis.util;

import io.lettuce.core.SetArgs;

import java.nio.ByteBuffer;

public class RedisUtils {
    public static final SetArgs NX = SetArgs.Builder.nx();
    public static final SetArgs XX = SetArgs.Builder.xx();

    public static boolean ok(String reply) {
        return "OK".equals(reply);
    }

    public static boolean isNil(ByteBuffer bytes) {
        return bytes == null || bytes.remaining() == 0;
    }
}
