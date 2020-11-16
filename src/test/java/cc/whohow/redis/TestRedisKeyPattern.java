package cc.whohow.redis;

import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.util.RedisKeyPattern;
import org.junit.Assert;
import org.junit.Test;

public class TestRedisKeyPattern {
    @Test
    public void test() {
        RedisKeyPattern a = new RedisKeyPattern(ByteSequence.ascii("a"));
        RedisKeyPattern a1 = new RedisKeyPattern(ByteSequence.ascii("a*"));
        RedisKeyPattern a2 = new RedisKeyPattern(ByteSequence.ascii("*a"));
        RedisKeyPattern a3 = new RedisKeyPattern(ByteSequence.ascii("*a*"));
        RedisKeyPattern a4 = new RedisKeyPattern(ByteSequence.ascii("a*a"));
        RedisKeyPattern a5 = new RedisKeyPattern(ByteSequence.ascii("*a*a*"));
        RedisKeyPattern a6 = new RedisKeyPattern(ByteSequence.ascii("a*a*a"));
        RedisKeyPattern any = new RedisKeyPattern(ByteSequence.ascii("*"));

        Assert.assertFalse(a.isPattern());
        Assert.assertTrue(a1.isPattern());
        Assert.assertTrue(a2.isPattern());
        Assert.assertTrue(a3.isPattern());
        Assert.assertTrue(a4.isPattern());
        Assert.assertTrue(a5.isPattern());
        Assert.assertTrue(a6.isPattern());
        Assert.assertTrue(any.isPattern());

        Assert.assertTrue(a.match(new RedisKeyPattern(ByteSequence.utf8("a"))));
        Assert.assertTrue(a1.match(new RedisKeyPattern(ByteSequence.utf8("a"))));
        Assert.assertTrue(a1.match(new RedisKeyPattern(ByteSequence.utf8("ab"))));
        Assert.assertFalse(a1.match(new RedisKeyPattern(ByteSequence.utf8("ba"))));
        Assert.assertFalse(a1.match(new RedisKeyPattern(ByteSequence.utf8("中文"))));
        Assert.assertTrue(a2.match(new RedisKeyPattern(ByteSequence.utf8("a"))));
    }
}
