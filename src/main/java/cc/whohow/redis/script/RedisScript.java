package cc.whohow.redis.script;

import cc.whohow.redis.io.Java9InputStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Redis脚本
 */
public class RedisScript {
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

    private static String load(URL url) {
        try (Java9InputStream stream = new Java9InputStream(url.openStream())) {
            return StandardCharsets.UTF_8.decode(stream.readAllBytes(1024)).toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String load(String name) {
        try (Java9InputStream stream = new Java9InputStream(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(name + ".lua"))) {
            return StandardCharsets.UTF_8.decode(stream.readAllBytes(1024)).toString();
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
        return script;
    }
}
