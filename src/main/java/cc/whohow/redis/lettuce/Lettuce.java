package cc.whohow.redis.lettuce;

import cc.whohow.redis.io.ByteBuffers;
import io.lettuce.core.SetArgs;
import io.lettuce.core.ZAddArgs;

import java.nio.ByteBuffer;

public class Lettuce {
    public static final ByteBuffer NX = ByteBuffers.fromUtf8("NX");
    public static final ByteBuffer XX = ByteBuffers.fromUtf8("XX");
    public static final SetArgs SET_NX = SetArgs.Builder.nx();
    public static final SetArgs SET_XX = SetArgs.Builder.xx();
    public static final ZAddArgs Z_ADD_NX = ZAddArgs.Builder.nx();
    public static final ZAddArgs Z_ADD_XX = ZAddArgs.Builder.xx();

    public static ByteBuffer nx() {
        return NX.duplicate();
    }

    public static ByteBuffer xx() {
        return XX.duplicate();
    }

    public static boolean ok(String reply) {
        return "OK".equals(reply);
    }
}
