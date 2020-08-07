package cc.whohow.redis.lettuce;

import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import io.lettuce.core.SetArgs;
import io.lettuce.core.ZAddArgs;

import java.nio.ByteBuffer;

public class Lettuce {
    public static final SetArgs SET_NX = SetArgs.Builder.nx();
    public static final SetArgs SET_XX = SetArgs.Builder.xx();
    public static final ZAddArgs Z_ADD_NX = ZAddArgs.Builder.nx();
    public static final ZAddArgs Z_ADD_XX = ZAddArgs.Builder.xx();
    private static final ByteBuffer ZERO = PrimitiveCodec.INTEGER.encode(0);
    private static final ByteBuffer ONE = PrimitiveCodec.INTEGER.encode(1);
    private static final ByteBuffer NEG_INF = ByteBuffers.fromUtf8("-inf");
    private static final ByteBuffer INF = ByteBuffers.fromUtf8("+inf");
    private static final ByteBuffer WITHSCORES = ByteBuffers.fromUtf8("WITHSCORES");
    private static final ByteBuffer LIMIT = ByteBuffers.fromUtf8("LIMIT");
    private static final ByteBuffer PX = ByteBuffers.fromUtf8("PX");
    private static final ByteBuffer EX = ByteBuffers.fromUtf8("EX");
    private static final ByteBuffer NX = ByteBuffers.fromUtf8("NX");
    private static final ByteBuffer XX = ByteBuffers.fromUtf8("XX");

    public static ByteBuffer zero() {
        return ZERO.duplicate();
    }

    public static ByteBuffer one() {
        return ONE.duplicate();
    }

    public static ByteBuffer inf() {
        return INF.duplicate();
    }

    public static ByteBuffer negInf() {
        return NEG_INF.duplicate();
    }

    public static ByteBuffer withscores() {
        return WITHSCORES.duplicate();
    }

    public static ByteBuffer limit() {
        return LIMIT.duplicate();
    }

    public static ByteBuffer px() {
        return PX.duplicate();
    }

    public static ByteBuffer ex() {
        return EX.duplicate();
    }

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
