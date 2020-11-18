package cc.whohow.redis;

import cc.whohow.redis.bytes.ByteSequence;
import io.lettuce.core.protocol.CommandKeyword;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * REdis Serialization Protocol
 */
public class RESP {
    private static final ByteSequence[] KEYWORDS = Arrays.stream(CommandKeyword.values())
            .map(CommandKeyword::getBytes)
            .map(ByteSequence::of)
            .toArray(ByteSequence[]::new);
    private static final ByteSequence[] INTEGERS = IntStream.rangeClosed(0, 128)
            .mapToObj(Integer::toString)
            .map(ByteSequence::ascii)
            .toArray(ByteSequence[]::new);
    private static final ByteSequence PX = ByteSequence.ascii("PX");
    private static final ByteSequence XX = ByteSequence.ascii("XX");
    private static final ByteSequence NX = ByteSequence.ascii("NX");
    private static final ByteSequence N_INF = ByteSequence.ascii("-inf");
    private static final ByteSequence P_INF = ByteSequence.ascii("+inf");

    public static ByteSequence px() {
        return PX;
    }

    public static ByteSequence xx() {
        return XX;
    }

    public static ByteSequence nx() {
        return NX;
    }

    public static ByteSequence nInf() {
        return N_INF;
    }

    public static ByteSequence pInf() {
        return P_INF;
    }

    public static ByteSequence b(CommandKeyword keyword) {
        return KEYWORDS[keyword.ordinal()];
    }

    public static ByteSequence b(long i64) {
        if (0 <= i64 && i64 < INTEGERS.length) {
            return INTEGERS[(int) i64];
        }
        return b(String.valueOf(i64));
    }

    public static ByteSequence b(double f64) {
        if (f64 == 0) {
            return INTEGERS[0];
        }
        return b(String.valueOf(f64));
    }

    public static ByteSequence b(Number number) {
        return b(number.toString());
    }

    public static ByteSequence b(String ascii) {
        return ByteSequence.ascii(ascii);
    }

    public static long i64(String text) {
        return Long.parseLong(text);
    }

    public static long i64(ByteBuffer bytes) {
        return i64(ascii(bytes));
    }

    public static double f64(String text) {
        return Double.parseDouble(text);
    }

    public static double f64(ByteBuffer bytes) {
        return f64(ascii(bytes));
    }

    public static String ascii(ByteBuffer bytes) {
        return StandardCharsets.US_ASCII.decode(bytes).toString();
    }

    public static String utf8(ByteBuffer bytes) {
        return StandardCharsets.UTF_8.decode(bytes).toString();
    }

    public static boolean ok(String reply) {
        return "OK".equals(reply);
    }
}
