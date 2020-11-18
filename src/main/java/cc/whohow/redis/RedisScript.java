package cc.whohow.redis;

import cc.whohow.redis.io.IO;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Redis脚本
 */
public class RedisScript {
    private static final Cache<String, RedisScript> CACHE = Caffeine.newBuilder()
            .maximumSize(1024)
            .build();

    private final String name;
    private final String sha1;
    private final String script;

    public RedisScript(String name) {
        this(name, load(name));
    }

    public RedisScript(URL url) {
        this(url.toString(), load(url));
    }

    public RedisScript(String name, String script) {
        this.name = name;
        this.sha1 = sha1Hex(script);
        this.script = script;
    }

    public static RedisScript get(String script) {
        return CACHE.get(script, RedisScript::new);
    }

    private static String load(URL url) {
        try (InputStream stream = url.openStream()) {
            return StandardCharsets.UTF_8.decode(IO.read(stream, 1024)).toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String load(String name) {
        String lua = name + ".lua";
        try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(lua)) {
            if (stream == null) {
                throw new IllegalArgumentException(lua + " not found");
            }
            return StandardCharsets.UTF_8.decode(IO.read(stream, 1024)).toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sha1Hex(String script) {
        try {
            byte[] bytes = script.getBytes(StandardCharsets.UTF_8);
            byte[] sha1 = MessageDigest.getInstance("sha1").digest(bytes);
            char[] hex = new char[sha1.length * 2];
            for (int i = 0; i < sha1.length; i++) {
                int v = sha1[i] & 0xFF;
                hex[i * 2] = Character.forDigit(v >>> 4, 16);
                hex[i * 2 + 1] = Character.forDigit(v & 0x0F, 16);
            }
            return new String(hex);
        } catch (NoSuchAlgorithmException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    public String getName() {
        return name;
    }

    public String getSha1() {
        return sha1;
    }

    public String getScript() {
        return script;
    }

    @Override
    public String toString() {
        return name + "#" + sha1;
    }
}
