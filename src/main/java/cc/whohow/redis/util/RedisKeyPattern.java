package cc.whohow.redis.util;

import cc.whohow.redis.bytes.ByteSequence;

import java.nio.charset.StandardCharsets;
import java.util.PrimitiveIterator;
import java.util.regex.Pattern;

public class RedisKeyPattern implements Comparable<RedisKeyPattern>, CharSequence {
    private final ByteSequence keyPattern;
    private final int hashCode;
    private final Pattern pattern;

    public RedisKeyPattern(ByteSequence keyPattern) {
        this(keyPattern, keyPattern.bytes().anyMatch(c -> c == '*'));
    }

    public RedisKeyPattern(ByteSequence keyPattern, boolean pattern) {
        this.keyPattern = keyPattern;
        this.hashCode = ByteSequence.hashCode(keyPattern);
        this.pattern = pattern ? compile(keyPattern) : null;
    }

    private Pattern compile(ByteSequence keyPattern) {
        PrimitiveIterator.OfInt iterator = keyPattern.byteIterator();
        StringBuilder part = new StringBuilder();
        StringBuilder pattern = new StringBuilder();
        while (iterator.hasNext()) {
            int b = iterator.nextInt();
            if (b == '*') {
                if (part.length() > 0) {
                    pattern.append(Pattern.quote(part.toString()));
                    part.setLength(0);
                }
                pattern.append(".*");
            } else {
                part.append((char) b);
            }
        }
        if (part.length() > 0) {
            pattern.append(Pattern.quote(part.toString()));
        }
        return Pattern.compile(pattern.toString());
    }

    public boolean isPattern() {
        return pattern != null;
    }

    public boolean match(RedisKeyPattern key) {
        if (!match0(key)) {
            return false;
        }
        if (pattern != null) {
            return pattern.matcher(key).matches();
        } else {
            return keyPattern.contentEquals(key.keyPattern);
        }
    }

    private boolean match0(RedisKeyPattern key) {
        int this0 = keyPattern.get(0);
        int that0 = key.keyPattern.get(0);
        return this0 == that0 || this0 == '*';
    }

    @Override
    public int length() {
        return keyPattern.length();
    }

    @Override
    public char charAt(int index) {
        return (char) keyPattern.get(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new RedisKeyPattern(keyPattern.subSequence(start, end));
    }

    @Override
    public int compareTo(RedisKeyPattern o) {
        return keyPattern.compareTo(o.keyPattern);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RedisKeyPattern) {
            RedisKeyPattern that = (RedisKeyPattern) o;
            return keyPattern.contentEquals(that.keyPattern);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return keyPattern.toString(StandardCharsets.ISO_8859_1);
    }
}
