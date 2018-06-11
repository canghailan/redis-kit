package cc.whohow.redis.lettuce;

import io.lettuce.core.SetArgs;
import io.lettuce.core.ZAddArgs;

import java.nio.ByteBuffer;

public class Lettuce {
    public static final SetArgs SET_NX = SetArgs.Builder.nx();
    public static final SetArgs SET_XX = SetArgs.Builder.xx();
    public static final ZAddArgs Z_ADD_NX = ZAddArgs.Builder.nx();
    public static final ZAddArgs Z_ADD_XX = ZAddArgs.Builder.xx();
    private static final ByteBuffer NIL = ByteBuffer.allocate(0);

    public static ByteBuffer nil() {
        return NIL;
    }

    public static boolean ok(String reply) {
        return "OK".equals(reply);
    }

    public static boolean isNil(ByteBuffer bytes) {
        return bytes == null || bytes.remaining() == 0;
    }
}
