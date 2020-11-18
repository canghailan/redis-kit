package cc.whohow.redis;

import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.bytes.ByteSummaryStatistics;
import cc.whohow.redis.bytes.ConcatByteSequence;
import org.junit.Assert;
import org.junit.Test;

public class TestBytes {
    @Test
    public void testCeilingNextPowerOfTwo() {
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(0));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(1));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(2));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(3));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(65));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(127));
        System.out.println(ByteSummaryStatistics.ceilingNextPowerOfTwo(128));
    }

    @Test
    public void test() {
        Assert.assertEquals(3, ByteSequence.ascii("abc").length());
        Assert.assertTrue(ByteSequence.ascii("").isEmpty());
        Assert.assertTrue(ByteSequence.ascii("abc").hasArray());
        Assert.assertEquals('a', ByteSequence.ascii("abc").get(0));
        Assert.assertEquals('b', ByteSequence.ascii("abc").get(1));
        Assert.assertEquals('c', ByteSequence.ascii("abc").get(2));
        Assert.assertTrue(ByteSequence.utf8("bc").contentEquals(ByteSequence.ascii("abc").subSequence(1, 3)));
        Assert.assertEquals("abc", ByteSequence.ascii("abc").toString());
        Assert.assertEquals(0, ByteSequence.utf8("abc").compareTo(ByteSequence.utf8("abc")));
        Assert.assertEquals(-1, ByteSequence.utf8("abc").compareTo(ByteSequence.utf8("b")));
        Assert.assertEquals(-1, ByteSequence.utf8("abc").compareTo(ByteSequence.utf8("abcd")));
        Assert.assertEquals(1, ByteSequence.utf8("b").compareTo(ByteSequence.utf8("a")));
        Assert.assertEquals(1, ByteSequence.utf8("bc").compareTo(ByteSequence.utf8("b")));
        Assert.assertTrue(ByteSequence.utf8("abc").startsWiths(ByteSequence.utf8("ab")));
        Assert.assertTrue(ByteSequence.utf8("abc").startsWiths(ByteSequence.utf8("abc")));
        Assert.assertFalse(ByteSequence.utf8("abc").startsWiths(ByteSequence.utf8("abcd")));
        Assert.assertFalse(ByteSequence.utf8("abc").startsWiths(ByteSequence.utf8("bc")));
        Assert.assertTrue(ByteSequence.utf8("abc").contentEquals(ByteSequence.ascii("abc")));
        Assert.assertFalse(ByteSequence.utf8("abc").contentEquals(ByteSequence.ascii("abd")));
        Assert.assertTrue(new ConcatByteSequence(ByteSequence.utf8("abc"), ByteSequence.ascii("de")).contentEquals(ByteSequence.ascii("abcde")));
    }
}
