package cc.whohow.redis.lettuce;

import io.lettuce.core.SetArgs;
import io.lettuce.core.ZAddArgs;

public class Lettuce {
    public static final SetArgs SET_NX = SetArgs.Builder.nx();
    public static final SetArgs SET_XX = SetArgs.Builder.xx();
    public static final ZAddArgs Z_ADD_NX = ZAddArgs.Builder.nx();
    public static final ZAddArgs Z_ADD_XX = ZAddArgs.Builder.xx();

    public static boolean ok(String reply) {
        return "OK".equals(reply);
    }
}
