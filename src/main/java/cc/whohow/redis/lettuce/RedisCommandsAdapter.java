package cc.whohow.redis.lettuce;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisCommandsAdapter<K, V> implements RedisCommands<K, V> {
    private final RedisCommands<ByteBuffer, ByteBuffer> redisCommands;
    private final RedisCodec<K, V> codec;

    public RedisCommandsAdapter(RedisCommands<ByteBuffer, ByteBuffer> redisCommands, RedisCodec<K, V> codec) {
        this.redisCommands = redisCommands;
        this.codec = codec;
    }

    protected ByteBuffer encodeKey(K key) {
        return codec.encodeKey(key);
    }

    protected ByteBuffer[] encodeKeyArray(K[] keyArray) {
        return Arrays.stream(keyArray).map(codec::encodeKey).toArray(ByteBuffer[]::new);
    }

    protected ByteBuffer encodeValue(V value) {
        return codec.encodeValue(value);
    }

    protected ByteBuffer[] encodeValueArray(V[] valueArray) {
        return Arrays.stream(valueArray).map(codec::encodeValue).toArray(ByteBuffer[]::new);
    }

    protected Range<ByteBuffer> encodeValueRange(Range<? extends V> range) {
        return Range.from(encodeValueBoundary(range.getLower()), encodeValueBoundary(range.getUpper()));
    }

    protected Range.Boundary<ByteBuffer> encodeValueBoundary(Range.Boundary<? extends V> boundary) {
        return boundary.isIncluding() ?
                Range.Boundary.including(codec.encodeValue(boundary.getValue())) :
                Range.Boundary.excluding(codec.encodeValue(boundary.getValue()));
    }

    protected Map<ByteBuffer, ByteBuffer> encodeKeyValueMap(Map<K, V> keyValueMap) {
        return keyValueMap.entrySet().stream()
                .collect(Collectors.toMap(
                        kv -> codec.encodeKey(kv.getKey()),
                        kv -> codec.encodeValue(kv.getValue())));
    }

    protected ScoredValue<ByteBuffer> encodeScoredValue(ScoredValue<V> scoredValue) {
        return ScoredValue.fromNullable(scoredValue.getScore(), encodeValue(scoredValue.getValue()));
    }

    @SuppressWarnings("unchecked")
    protected ScoredValue<ByteBuffer>[] encodeScoredValueArray(ScoredValue<V>[] scoredValueArray) {
        return Arrays.stream(scoredValueArray).map(this::encodeScoredValue).toArray(ScoredValue[]::new);
    }

    @SuppressWarnings("unchecked")
    protected Object[] encodeScoresAndValues(Object[] scoresAndValues) {
        Object[] r = new Object[scoresAndValues.length];
        for (int i = 0, j = 1; i < scoresAndValues.length; i += 2, j += 2) {
            r[i] = scoresAndValues[i];
            r[j] = codec.encodeValue((V) scoresAndValues[j]);
        }
        return r;
    }

    @SuppressWarnings("unchecked")
    protected Object[] encodeLngLatMember(Object[] lngLatMember) {
        Object[] r = new Object[lngLatMember.length];
        for (int i = 0, j = 1, k = 2; i < lngLatMember.length; i += 3, j += 3, k += 3) {
            r[i] = lngLatMember[i];
            r[j] = lngLatMember[j];
            r[k] = codec.encodeValue((V) lngLatMember[k]);
        }
        return r;
    }

    protected K decodeKey(ByteBuffer byteBuffer) {
        return codec.decodeKey(byteBuffer);
    }

    protected List<K> decodeKeyList(List<ByteBuffer> keyList) {
        return keyList.stream()
                .map(codec::decodeKey)
                .collect(Collectors.toList());
    }

    protected V decodeValue(ByteBuffer byteBuffer) {
        return codec.decodeValue(byteBuffer);
    }

    protected List<V> decodeValueList(List<ByteBuffer> valueList) {
        return valueList.stream()
                .map(codec::decodeValue)
                .collect(Collectors.toList());
    }

    protected Set<V> decodeValueSet(Set<ByteBuffer> set) {
        return set.stream()
                .map(codec::decodeValue)
                .collect(Collectors.toSet());
    }

    protected Map<K, V> decodeKeyValueMap(Map<ByteBuffer, ByteBuffer> keyValueMap) {
        return keyValueMap.entrySet().stream()
                .collect(Collectors.toMap(
                        kv -> codec.decodeKey(kv.getKey()),
                        kv -> codec.decodeValue(kv.getValue())));
    }

    protected Map<K, Long> decodeKeyMap(Map<ByteBuffer, Long> keyMap) {
        return keyMap.entrySet().stream()
                .collect(Collectors.toMap(
                        kv -> codec.decodeKey(kv.getKey()),
                        Map.Entry::getValue));
    }

    protected KeyValue<K, V> decodeKeyValue(KeyValue<ByteBuffer, ByteBuffer> keyValue) {
        return KeyValue.fromNullable(codec.decodeKey(keyValue.getKey()),
                keyValue.hasValue() ? codec.decodeValue(keyValue.getValue()) : null);
    }

    protected List<KeyValue<K, V>> decodeKeyValueList(List<KeyValue<ByteBuffer, ByteBuffer>> keyValueList) {
        return keyValueList.stream()
                .map(this::decodeKeyValue)
                .collect(Collectors.toList());
    }

    protected ScoredValue<V> decodeScoredValue(ScoredValue<ByteBuffer> scoredValue) {
        return scoredValue.map(codec::decodeValue);
    }

    protected List<ScoredValue<V>> decodeScoredValueList(List<ScoredValue<ByteBuffer>> scoredValueList) {
        return scoredValueList.stream()
                .map(this::decodeScoredValue)
                .collect(Collectors.toList());
    }

    protected GeoWithin<V> decodeGeoWithin(GeoWithin<ByteBuffer> geoWithin) {
        return new GeoWithin<>(codec.decodeValue(geoWithin.getMember()), geoWithin.getDistance(), geoWithin.getGeohash(), geoWithin.getCoordinates());
    }

    protected List<GeoWithin<V>> decodeGeoWithinList(List<GeoWithin<ByteBuffer>> geoWithinList) {
        return geoWithinList.stream()
                .map(this::decodeGeoWithin)
                .collect(Collectors.toList());
    }

    protected KeyScanCursor<K> decodeKeyScanCursor(KeyScanCursor<ByteBuffer> keyScanCursor) {
        KeyScanCursor<K> r = new KeyScanCursor<>();
        r.setCursor(keyScanCursor.getCursor());
        r.setFinished(keyScanCursor.isFinished());
        List<K> list = r.getKeys();
        for (ByteBuffer key : keyScanCursor.getKeys()) {
            list.add(codec.decodeKey(key));
        }
        return r;
    }

    protected ValueScanCursor<V> decodeValueScanCursor(ValueScanCursor<ByteBuffer> valueScanCursor) {
        ValueScanCursor<V> r = new ValueScanCursor<>();
        r.setCursor(valueScanCursor.getCursor());
        r.setFinished(valueScanCursor.isFinished());
        List<V> list = r.getValues();
        for (ByteBuffer value : valueScanCursor.getValues()) {
            list.add(codec.decodeValue(value));
        }
        return r;
    }

    protected MapScanCursor<K, V> decodeMapScanCursor(MapScanCursor<ByteBuffer, ByteBuffer> mapScanCursor) {
        MapScanCursor<K, V> r = new MapScanCursor<>();
        r.setCursor(mapScanCursor.getCursor());
        r.setFinished(mapScanCursor.isFinished());
        Map<K, V> map = r.getMap();
        for (Map.Entry<ByteBuffer, ByteBuffer> e : mapScanCursor.getMap().entrySet()) {
            map.put(codec.decodeKey(e.getKey()), codec.decodeValue(e.getValue()));
        }
        return r;
    }

    protected ScoredValueScanCursor<V> decodeScoredValueScanCursor(ScoredValueScanCursor<ByteBuffer> scoredValueScanCursor) {
        ScoredValueScanCursor<V> r = new ScoredValueScanCursor<>();
        r.setCursor(scoredValueScanCursor.getCursor());
        r.setFinished(scoredValueScanCursor.isFinished());
        List<ScoredValue<V>> list = r.getValues();
        for (ScoredValue<ByteBuffer> scoredValue : scoredValueScanCursor.getValues()) {
            list.add(scoredValue.map(codec::decodeValue));
        }
        return r;
    }

    @Override
    public String auth(String password) {
        return redisCommands.auth(password);
    }

    @Override
    public String select(int db) {
        return redisCommands.select(db);
    }

    @Override
    public String swapdb(int db1, int db2) {
        return redisCommands.swapdb(db1, db2);
    }

    @Override
    public Long hdel(K key, K... fields) {
        return redisCommands.hdel(encodeKey(key), encodeKeyArray(fields));
    }

    @Override
    public Boolean hexists(K key, K field) {
        return redisCommands.hexists(encodeKey(key), encodeKey(field));
    }

    @Override
    public V hget(K key, K field) {
        return decodeValue(redisCommands.hget(codec.encodeKey(key), codec.encodeKey(field)));
    }

    @Override
    public Long hincrby(K key, K field, long amount) {
        return redisCommands.hincrby(encodeKey(key), encodeKey(field), amount);
    }

    @Override
    public Double hincrbyfloat(K key, K field, double amount) {
        return redisCommands.hincrbyfloat(encodeKey(key), encodeKey(field), amount);
    }

    @Override
    public Map<K, V> hgetall(K key) {
        return decodeKeyValueMap(redisCommands.hgetall(encodeKey(key)));
    }

    @Override
    public Long hgetall(KeyValueStreamingChannel<K, V> channel, K key) {
        return redisCommands.hgetall(new KeyValueStreamingChannelAdapter<>(channel, codec), encodeKey(key));
    }

    @Override
    public List<K> hkeys(K key) {
        return decodeKeyList(redisCommands.hkeys(encodeKey(key)));
    }

    @Override
    public Long hkeys(KeyStreamingChannel<K> channel, K key) {
        return redisCommands.hkeys(new KeyStreamingChannelAdapter<>(channel, codec), encodeKey(key));
    }

    @Override
    public Long hlen(K key) {
        return redisCommands.hlen(encodeKey(key));
    }

    @Override
    public List<KeyValue<K, V>> hmget(K key, K... fields) {
        return decodeKeyValueList(redisCommands.hmget(encodeKey(key), encodeKeyArray(fields)));
    }

    @Override
    public Long hmget(KeyValueStreamingChannel<K, V> channel, K key, K... fields) {
        return redisCommands.hmget(new KeyValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), encodeKeyArray(fields));
    }

    @Override
    public String hmset(K key, Map<K, V> map) {
        return redisCommands.hmset(encodeKey(key), encodeKeyValueMap(map));
    }

    @Override
    public MapScanCursor<K, V> hscan(K key) {
        return decodeMapScanCursor(redisCommands.hscan(encodeKey(key)));
    }

    @Override
    public MapScanCursor<K, V> hscan(K key, ScanArgs scanArgs) {
        return decodeMapScanCursor(redisCommands.hscan(encodeKey(key), scanArgs));
    }

    @Override
    public MapScanCursor<K, V> hscan(K key, ScanCursor scanCursor, ScanArgs scanArgs) {
        return decodeMapScanCursor(redisCommands.hscan(encodeKey(key), scanCursor, scanArgs));
    }

    @Override
    public MapScanCursor<K, V> hscan(K key, ScanCursor scanCursor) {
        return decodeMapScanCursor(redisCommands.hscan(encodeKey(key), scanCursor));
    }

    @Override
    public StreamScanCursor hscan(KeyValueStreamingChannel<K, V> channel, K key) {
        return redisCommands.hscan(new KeyValueStreamingChannelAdapter<>(channel, codec), encodeKey(key));
    }

    @Override
    public StreamScanCursor hscan(KeyValueStreamingChannel<K, V> channel, K key, ScanArgs scanArgs) {
        return redisCommands.hscan(new KeyValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanArgs);
    }

    @Override
    public StreamScanCursor hscan(KeyValueStreamingChannel<K, V> channel, K key, ScanCursor scanCursor, ScanArgs scanArgs) {
        return redisCommands.hscan(new KeyValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanCursor, scanArgs);
    }

    @Override
    public StreamScanCursor hscan(KeyValueStreamingChannel<K, V> channel, K key, ScanCursor scanCursor) {
        return redisCommands.hscan(new KeyValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanCursor);
    }

    @Override
    public Boolean hset(K key, K field, V value) {
        return redisCommands.hset(encodeKey(key), encodeKey(field), encodeValue(value));
    }

    @Override
    public Boolean hsetnx(K key, K field, V value) {
        return redisCommands.hsetnx(encodeKey(key), encodeKey(field), encodeValue(value));
    }

    @Override
    public Long hstrlen(K key, K field) {
        return redisCommands.hstrlen(encodeKey(key), encodeKey(field));
    }

    @Override
    public List<V> hvals(K key) {
        return decodeValueList(redisCommands.hvals(encodeKey(key)));
    }

    @Override
    public Long hvals(ValueStreamingChannel<V> channel, K key) {
        return redisCommands.hvals(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key));
    }

    @Override
    public Long del(K... keys) {
        return redisCommands.del(encodeKeyArray(keys));
    }

    @Override
    public Long unlink(K... keys) {
        return redisCommands.unlink(encodeKeyArray(keys));
    }

    @Override
    public byte[] dump(K key) {
        return redisCommands.dump(encodeKey(key));
    }

    @Override
    public Long exists(K... keys) {
        return redisCommands.exists(encodeKeyArray(keys));
    }

    @Override
    public Boolean expire(K key, long seconds) {
        return redisCommands.expire(encodeKey(key), seconds);
    }

    @Override
    public Boolean expireat(K key, Date timestamp) {
        return redisCommands.expireat(encodeKey(key), timestamp);
    }

    @Override
    public Boolean expireat(K key, long timestamp) {
        return redisCommands.expireat(encodeKey(key), timestamp);
    }

    @Override
    public List<K> keys(K pattern) {
        return decodeKeyList(redisCommands.keys(encodeKey(pattern)));
    }

    @Override
    public Long keys(KeyStreamingChannel<K> channel, K pattern) {
        return redisCommands.keys(new KeyStreamingChannelAdapter<>(channel, codec), encodeKey(pattern));
    }

    @Override
    public String migrate(String host, int port, K key, int db, long timeout) {
        return redisCommands.migrate(host, port, encodeKey(key), db, timeout);
    }

    @Override
    public String migrate(String host, int port, int db, long timeout, MigrateArgs<K> migrateArgs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean move(K key, int db) {
        return redisCommands.move(encodeKey(key), db);
    }

    @Override
    public String objectEncoding(K key) {
        return redisCommands.objectEncoding(encodeKey(key));
    }

    @Override
    public Long objectIdletime(K key) {
        return redisCommands.objectIdletime(encodeKey(key));
    }

    @Override
    public Long objectRefcount(K key) {
        return redisCommands.objectRefcount(encodeKey(key));
    }

    @Override
    public Boolean persist(K key) {
        return redisCommands.persist(encodeKey(key));
    }

    @Override
    public Boolean pexpire(K key, long milliseconds) {
        return redisCommands.pexpire(encodeKey(key), milliseconds);
    }

    @Override
    public Boolean pexpireat(K key, Date timestamp) {
        return redisCommands.pexpireat(encodeKey(key), timestamp);
    }

    @Override
    public Boolean pexpireat(K key, long timestamp) {
        return redisCommands.pexpireat(encodeKey(key), timestamp);
    }

    @Override
    public Long pttl(K key) {
        return redisCommands.pttl(encodeKey(key));
    }

    @Override
    public V randomkey() {
        return decodeValue(redisCommands.randomkey());
    }

    @Override
    public String rename(K key, K newKey) {
        return redisCommands.rename(encodeKey(key), encodeKey(newKey));
    }

    @Override
    public Boolean renamenx(K key, K newKey) {
        return redisCommands.renamenx(encodeKey(key), encodeKey(newKey));
    }

    @Override
    public String restore(K key, long ttl, byte[] value) {
        return redisCommands.restore(encodeKey(key), ttl, value);
    }

    @Override
    public List<V> sort(K key) {
        return decodeValueList(redisCommands.sort(encodeKey(key)));
    }

    @Override
    public Long sort(ValueStreamingChannel<V> channel, K key) {
        return redisCommands.sort(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key));
    }

    @Override
    public List<V> sort(K key, SortArgs sortArgs) {
        return decodeValueList(redisCommands.sort(encodeKey(key), sortArgs));
    }

    @Override
    public Long sort(ValueStreamingChannel<V> channel, K key, SortArgs sortArgs) {
        return redisCommands.sort(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), sortArgs);
    }

    @Override
    public Long sortStore(K key, SortArgs sortArgs, K destination) {
        return redisCommands.sortStore(encodeKey(key), sortArgs, encodeKey(destination));
    }

    @Override
    public Long touch(K... keys) {
        return redisCommands.touch(encodeKeyArray(keys));
    }

    @Override
    public Long ttl(K key) {
        return redisCommands.ttl(encodeKey(key));
    }

    @Override
    public String type(K key) {
        return redisCommands.type(encodeKey(key));
    }

    @Override
    public KeyScanCursor<K> scan() {
        return decodeKeyScanCursor(redisCommands.scan());
    }

    @Override
    public KeyScanCursor<K> scan(ScanArgs scanArgs) {
        return decodeKeyScanCursor(redisCommands.scan(scanArgs));
    }

    @Override
    public KeyScanCursor<K> scan(ScanCursor scanCursor, ScanArgs scanArgs) {
        return decodeKeyScanCursor(redisCommands.scan(scanCursor, scanArgs));
    }

    @Override
    public KeyScanCursor<K> scan(ScanCursor scanCursor) {
        return decodeKeyScanCursor(redisCommands.scan(scanCursor));
    }

    @Override
    public StreamScanCursor scan(KeyStreamingChannel<K> channel) {
        return redisCommands.scan(new KeyStreamingChannelAdapter<>(channel, codec));
    }

    @Override
    public StreamScanCursor scan(KeyStreamingChannel<K> channel, ScanArgs scanArgs) {
        return redisCommands.scan(new KeyStreamingChannelAdapter<>(channel, codec), scanArgs);
    }

    @Override
    public StreamScanCursor scan(KeyStreamingChannel<K> channel, ScanCursor scanCursor, ScanArgs scanArgs) {
        return redisCommands.scan(new KeyStreamingChannelAdapter<>(channel, codec), scanCursor, scanArgs);
    }

    @Override
    public StreamScanCursor scan(KeyStreamingChannel<K> channel, ScanCursor scanCursor) {
        return redisCommands.scan(new KeyStreamingChannelAdapter<>(channel, codec), scanCursor);
    }

    @Override
    public KeyValue<K, V> blpop(long timeout, K... keys) {
        return decodeKeyValue(redisCommands.blpop(timeout, encodeKeyArray(keys)));
    }

    @Override
    public KeyValue<K, V> brpop(long timeout, K... keys) {
        return decodeKeyValue(redisCommands.brpop(timeout, encodeKeyArray(keys)));
    }

    @Override
    public V brpoplpush(long timeout, K source, K destination) {
        return decodeValue(redisCommands.brpoplpush(timeout, encodeKey(source), encodeKey(destination)));
    }

    @Override
    public V lindex(K key, long index) {
        return decodeValue(redisCommands.lindex(encodeKey(key), index));
    }

    @Override
    public Long linsert(K key, boolean before, V pivot, V value) {
        return redisCommands.linsert(encodeKey(key), before, encodeValue(pivot), encodeValue(value));
    }

    @Override
    public Long llen(K key) {
        return redisCommands.llen(encodeKey(key));
    }

    @Override
    public V lpop(K key) {
        return decodeValue(redisCommands.lpop(encodeKey(key)));
    }

    @Override
    public Long lpush(K key, V... values) {
        return redisCommands.lpush(encodeKey(key), encodeValueArray(values));
    }

    @Override
    public Long lpushx(K key, V... values) {
        return redisCommands.lpushx(encodeKey(key), encodeValueArray(values));
    }

    @Override
    public List<V> lrange(K key, long start, long stop) {
        return decodeValueList(redisCommands.lrange(encodeKey(key), start, stop));
    }

    @Override
    public Long lrange(ValueStreamingChannel<V> channel, K key, long start, long stop) {
        return redisCommands.lrange(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), start, stop);
    }

    @Override
    public Long lrem(K key, long count, V value) {
        return redisCommands.lrem(encodeKey(key), count, encodeValue(value));
    }

    @Override
    public String lset(K key, long index, V value) {
        return redisCommands.lset(encodeKey(key), index, encodeValue(value));
    }

    @Override
    public String ltrim(K key, long start, long stop) {
        return redisCommands.ltrim(encodeKey(key), start, stop);
    }

    @Override
    public V rpop(K key) {
        return decodeValue(redisCommands.rpop(encodeKey(key)));
    }

    @Override
    public V rpoplpush(K source, K destination) {
        return decodeValue(redisCommands.rpoplpush(encodeKey(source), encodeKey(destination)));
    }

    @Override
    public Long rpush(K key, V... values) {
        return redisCommands.rpush(encodeKey(key), encodeValueArray(values));
    }

    @Override
    public Long rpushx(K key, V... values) {
        return redisCommands.rpushx(encodeKey(key), encodeValueArray(values));
    }

    @Override
    public Long sadd(K key, V... members) {
        return redisCommands.sadd(encodeKey(key), encodeValueArray(members));
    }

    @Override
    public Long scard(K key) {
        return redisCommands.scard(encodeKey(key));
    }

    @Override
    public Set<V> sdiff(K... keys) {
        return decodeValueSet(redisCommands.sdiff(encodeKeyArray(keys)));
    }

    @Override
    public Long sdiff(ValueStreamingChannel<V> channel, K... keys) {
        return redisCommands.sdiff(new ValueStreamingChannelAdapter<>(channel, codec), encodeKeyArray(keys));
    }

    @Override
    public Long sdiffstore(K destination, K... keys) {
        return redisCommands.sdiffstore(encodeKey(destination), encodeKeyArray(keys));
    }

    @Override
    public Set<V> sinter(K... keys) {
        return decodeValueSet(redisCommands.sinter(encodeKeyArray(keys)));
    }

    @Override
    public Long sinter(ValueStreamingChannel<V> channel, K... keys) {
        return redisCommands.sinter(new ValueStreamingChannelAdapter<>(channel, codec), encodeKeyArray(keys));
    }

    @Override
    public Long sinterstore(K destination, K... keys) {
        return redisCommands.sinterstore(encodeKey(destination), encodeKeyArray(keys));
    }

    @Override
    public Boolean sismember(K key, V member) {
        return redisCommands.sismember(encodeKey(key), encodeValue(member));
    }

    @Override
    public Boolean smove(K source, K destination, V member) {
        return redisCommands.smove(encodeKey(source), encodeKey(destination), encodeValue(member));
    }

    @Override
    public Set<V> smembers(K key) {
        return decodeValueSet(redisCommands.smembers(encodeKey(key)));
    }

    @Override
    public Long smembers(ValueStreamingChannel<V> channel, K key) {
        return redisCommands.smembers(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key));
    }

    @Override
    public V spop(K key) {
        return decodeValue(redisCommands.spop(encodeKey(key)));
    }

    @Override
    public Set<V> spop(K key, long count) {
        return decodeValueSet(redisCommands.spop(encodeKey(key), count));
    }

    @Override
    public V srandmember(K key) {
        return decodeValue(redisCommands.srandmember(encodeKey(key)));
    }

    @Override
    public List<V> srandmember(K key, long count) {
        return decodeValueList(redisCommands.srandmember(encodeKey(key), count));
    }

    @Override
    public Long srandmember(ValueStreamingChannel<V> channel, K key, long count) {
        return redisCommands.srandmember(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), count);
    }

    @Override
    public Long srem(K key, V... members) {
        return redisCommands.srem(encodeKey(key), encodeValueArray(members));
    }

    @Override
    public Set<V> sunion(K... keys) {
        return decodeValueSet(redisCommands.sunion(encodeKeyArray(keys)));
    }

    @Override
    public Long sunion(ValueStreamingChannel<V> channel, K... keys) {
        return redisCommands.sunion(new ValueStreamingChannelAdapter<>(channel, codec), encodeKeyArray(keys));
    }

    @Override
    public Long sunionstore(K destination, K... keys) {
        return redisCommands.sunionstore(encodeKey(destination), encodeKeyArray(keys));
    }

    @Override
    public ValueScanCursor<V> sscan(K key) {
        return decodeValueScanCursor(redisCommands.sscan(encodeKey(key)));
    }

    @Override
    public ValueScanCursor<V> sscan(K key, ScanArgs scanArgs) {
        return decodeValueScanCursor(redisCommands.sscan(encodeKey(key), scanArgs));
    }

    @Override
    public ValueScanCursor<V> sscan(K key, ScanCursor scanCursor, ScanArgs scanArgs) {
        return decodeValueScanCursor(redisCommands.sscan(encodeKey(key), scanCursor, scanArgs));
    }

    @Override
    public ValueScanCursor<V> sscan(K key, ScanCursor scanCursor) {
        return decodeValueScanCursor(redisCommands.sscan(encodeKey(key), scanCursor));
    }

    @Override
    public StreamScanCursor sscan(ValueStreamingChannel<V> channel, K key) {
        return redisCommands.sscan(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key));
    }

    @Override
    public StreamScanCursor sscan(ValueStreamingChannel<V> channel, K key, ScanArgs scanArgs) {
        return redisCommands.sscan(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanArgs);
    }

    @Override
    public StreamScanCursor sscan(ValueStreamingChannel<V> channel, K key, ScanCursor scanCursor, ScanArgs scanArgs) {
        return redisCommands.sscan(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanCursor, scanArgs);
    }

    @Override
    public StreamScanCursor sscan(ValueStreamingChannel<V> channel, K key, ScanCursor scanCursor) {
        return redisCommands.sscan(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanCursor);
    }

    @Override
    public Long zadd(K key, double score, V member) {
        return redisCommands.zadd(encodeKey(key), score, encodeValue(member));
    }

    @Override
    public Long zadd(K key, Object... scoresAndValues) {
        return redisCommands.zadd(encodeKey(key), encodeScoresAndValues(scoresAndValues));
    }

    @Override
    public Long zadd(K key, ScoredValue<V>... scoredValues) {
        return redisCommands.zadd(encodeKey(key), encodeScoredValueArray(scoredValues));
    }

    @Override
    public Long zadd(K key, ZAddArgs zAddArgs, double score, V member) {
        return redisCommands.zadd(encodeKey(key), zAddArgs, score, encodeValue(member));
    }

    @Override
    public Long zadd(K key, ZAddArgs zAddArgs, Object... scoresAndValues) {
        return redisCommands.zadd(encodeKey(key), zAddArgs, encodeScoresAndValues(scoresAndValues));
    }

    @Override
    public Long zadd(K key, ZAddArgs zAddArgs, ScoredValue<V>... scoredValues) {
        return redisCommands.zadd(encodeKey(key), zAddArgs, encodeScoredValueArray(scoredValues));
    }

    @Override
    public Double zaddincr(K key, double score, V member) {
        return redisCommands.zaddincr(encodeKey(key), score, encodeValue(member));
    }

    @Override
    public Double zaddincr(K key, ZAddArgs zAddArgs, double score, V member) {
        return redisCommands.zaddincr(encodeKey(key), zAddArgs, score, encodeValue(member));
    }

    @Override
    public Long zcard(K key) {
        return redisCommands.zcard(encodeKey(key));
    }

    @Override
    @Deprecated
    public Long zcount(K key, double min, double max) {
        return redisCommands.zcount(encodeKey(key), min, max);
    }

    @Override
    @Deprecated
    public Long zcount(K key, String min, String max) {
        return redisCommands.zcount(encodeKey(key), min, max);
    }

    @Override
    public Long zcount(K key, Range<? extends Number> range) {
        return redisCommands.zcount(encodeKey(key), range);
    }

    @Override
    public Double zincrby(K key, double amount, K member) {
        return redisCommands.zincrby(encodeKey(key), amount, encodeKey(member));
    }

    @Override
    public Long zinterstore(K destination, K... keys) {
        return redisCommands.zinterstore(encodeKey(destination), encodeKeyArray(keys));
    }

    @Override
    public Long zinterstore(K destination, ZStoreArgs storeArgs, K... keys) {
        return redisCommands.zinterstore(encodeKey(destination), storeArgs, encodeKeyArray(keys));
    }

    @Override
    @Deprecated
    public Long zlexcount(K key, String min, String max) {
        return redisCommands.zlexcount(encodeKey(key), min, max);
    }

    @Override
    public Long zlexcount(K key, Range<? extends V> range) {
        return redisCommands.zlexcount(encodeKey(key), encodeValueRange(range));
    }

    @Override
    public List<V> zrange(K key, long start, long stop) {
        return decodeValueList(redisCommands.zrange(encodeKey(key), start, stop));
    }

    @Override
    public Long zrange(ValueStreamingChannel<V> channel, K key, long start, long stop) {
        return redisCommands.zrange(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), start, stop);
    }

    @Override
    public List<ScoredValue<V>> zrangeWithScores(K key, long start, long stop) {
        return decodeScoredValueList(redisCommands.zrangeWithScores(encodeKey(key), start, stop));
    }

    @Override
    public Long zrangeWithScores(ScoredValueStreamingChannel<V> channel, K key, long start, long stop) {
        return redisCommands.zrangeWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), start, stop);
    }

    @Override
    @Deprecated
    public List<V> zrangebylex(K key, String min, String max) {
        return decodeValueList(redisCommands.zrangebylex(encodeKey(key), min, max));
    }

    @Override
    public List<V> zrangebylex(K key, Range<? extends V> range) {
        return decodeValueList(redisCommands.zrangebylex(encodeKey(key), encodeValueRange(range)));
    }

    @Override
    @Deprecated
    public List<V> zrangebylex(K key, String min, String max, long offset, long count) {
        return decodeValueList(redisCommands.zrangebylex(encodeKey(key), min, max, offset, count));
    }

    @Override
    public List<V> zrangebylex(K key, Range<? extends V> range, Limit limit) {
        return decodeValueList(redisCommands.zrangebylex(encodeKey(key), encodeValueRange(range), limit));
    }

    @Override
    @Deprecated
    public List<V> zrangebyscore(K key, double min, double max) {
        return decodeValueList(redisCommands.zrangebyscore(encodeKey(key), min, max));
    }

    @Override
    @Deprecated
    public List<V> zrangebyscore(K key, String min, String max) {
        return decodeValueList(redisCommands.zrangebyscore(encodeKey(key), min, max));
    }

    @Override
    public List<V> zrangebyscore(K key, Range<? extends Number> range) {
        return decodeValueList(redisCommands.zrangebyscore(encodeKey(key), range));
    }

    @Override
    @Deprecated
    public List<V> zrangebyscore(K key, double min, double max, long offset, long count) {
        return decodeValueList(redisCommands.zrangebyscore(encodeKey(key), min, max, offset, count));
    }

    @Override
    @Deprecated
    public List<V> zrangebyscore(K key, String min, String max, long offset, long count) {
        return decodeValueList(redisCommands.zrangebyscore(encodeKey(key), min, max, offset, count));
    }

    @Override
    public List<V> zrangebyscore(K key, Range<? extends Number> range, Limit limit) {
        return decodeValueList(redisCommands.zrangebyscore(encodeKey(key), range, limit));
    }

    @Override
    @Deprecated
    public Long zrangebyscore(ValueStreamingChannel<V> channel, K key, double min, double max) {
        return redisCommands.zrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max);
    }

    @Override
    @Deprecated
    public Long zrangebyscore(ValueStreamingChannel<V> channel, K key, String min, String max) {
        return redisCommands.zrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max);
    }

    @Override
    public Long zrangebyscore(ValueStreamingChannel<V> channel, K key, Range<? extends Number> range) {
        return redisCommands.zrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), range);
    }

    @Override
    @Deprecated
    public Long zrangebyscore(ValueStreamingChannel<V> channel, K key, double min, double max, long offset, long count) {
        return redisCommands.zrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max, offset, count);
    }

    @Override
    @Deprecated
    public Long zrangebyscore(ValueStreamingChannel<V> channel, K key, String min, String max, long offset, long count) {
        return redisCommands.zrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max, offset, count);
    }

    @Override
    public Long zrangebyscore(ValueStreamingChannel<V> channel, K key, Range<? extends Number> range, Limit limit) {
        return redisCommands.zrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), range, limit);
    }

    @Override
    @Deprecated
    public List<ScoredValue<V>> zrangebyscoreWithScores(K key, double min, double max) {
        return decodeScoredValueList(redisCommands.zrangebyscoreWithScores(encodeKey(key), min, max));
    }

    @Override
    @Deprecated
    public List<ScoredValue<V>> zrangebyscoreWithScores(K key, String min, String max) {
        return decodeScoredValueList(redisCommands.zrangebyscoreWithScores(encodeKey(key), min, max));
    }

    @Override
    public List<ScoredValue<V>> zrangebyscoreWithScores(K key, Range<? extends Number> range) {
        return decodeScoredValueList(redisCommands.zrangebyscoreWithScores(encodeKey(key), range));
    }

    @Override
    @Deprecated
    public List<ScoredValue<V>> zrangebyscoreWithScores(K key, double min, double max, long offset, long count) {
        return decodeScoredValueList(redisCommands.zrangebyscoreWithScores(encodeKey(key), min, max, offset, count));
    }

    @Override
    @Deprecated
    public List<ScoredValue<V>> zrangebyscoreWithScores(K key, String min, String max, long offset, long count) {
        return decodeScoredValueList(redisCommands.zrangebyscoreWithScores(encodeKey(key), min, max, offset, count));
    }

    @Override
    public List<ScoredValue<V>> zrangebyscoreWithScores(K key, Range<? extends Number> range, Limit limit) {
        return decodeScoredValueList(redisCommands.zrangebyscoreWithScores(encodeKey(key), range, limit));
    }

    @Override
    @Deprecated
    public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, double min, double max) {
        return redisCommands.zrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max);
    }

    @Override
    @Deprecated
    public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, String min, String max) {
        return redisCommands.zrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max);
    }

    @Override
    public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, Range<? extends Number> range) {
        return redisCommands.zrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), range);
    }

    @Override
    @Deprecated
    public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, double min, double max, long offset, long count) {
        return redisCommands.zrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max, offset, count);
    }

    @Override
    @Deprecated
    public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, String min, String max, long offset, long count) {
        return redisCommands.zrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max, offset, count);
    }

    @Override
    public Long zrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, Range<? extends Number> range, Limit limit) {
        return redisCommands.zrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), range, limit);
    }

    @Override
    public Long zrank(K key, V member) {
        return redisCommands.zrank(encodeKey(key), encodeValue(member));
    }

    @Override
    public Long zrem(K key, V... members) {
        return redisCommands.zrem(encodeKey(key), encodeValueArray(members));
    }

    @Override
    @Deprecated
    public Long zremrangebylex(K key, String min, String max) {
        return redisCommands.zremrangebylex(encodeKey(key), min, max);
    }

    @Override
    public Long zremrangebylex(K key, Range<? extends V> range) {
        return redisCommands.zremrangebylex(encodeKey(key), encodeValueRange(range));
    }

    @Override
    public Long zremrangebyrank(K key, long start, long stop) {
        return redisCommands.zremrangebyrank(encodeKey(key), start, stop);
    }

    @Override
    @Deprecated
    public Long zremrangebyscore(K key, double min, double max) {
        return redisCommands.zremrangebyscore(encodeKey(key), min, max);
    }

    @Override
    @Deprecated
    public Long zremrangebyscore(K key, String min, String max) {
        return redisCommands.zremrangebyscore(encodeKey(key), min, max);
    }

    @Override
    public Long zremrangebyscore(K key, Range<? extends Number> range) {
        return redisCommands.zremrangebyscore(encodeKey(key), range);
    }

    @Override
    public List<V> zrevrange(K key, long start, long stop) {
        return decodeValueList(redisCommands.zrevrange(encodeKey(key), start, stop));
    }

    @Override
    public Long zrevrange(ValueStreamingChannel<V> channel, K key, long start, long stop) {
        return redisCommands.zrevrange(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), start, stop);
    }

    @Override
    public List<ScoredValue<V>> zrevrangeWithScores(K key, long start, long stop) {
        return decodeScoredValueList(redisCommands.zrevrangeWithScores(encodeKey(key), start, stop));
    }

    @Override
    public Long zrevrangeWithScores(ScoredValueStreamingChannel<V> channel, K key, long start, long stop) {
        return redisCommands.zrevrangeWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), start, stop);
    }

    @Override
    public List<V> zrevrangebylex(K key, Range<? extends V> range) {
        return decodeValueList(redisCommands.zrevrangebylex(encodeKey(key), encodeValueRange(range)));
    }

    @Override
    public List<V> zrevrangebylex(K key, Range<? extends V> range, Limit limit) {
        return decodeValueList(redisCommands.zrevrangebylex(encodeKey(key), encodeValueRange(range), limit));
    }

    @Override
    @Deprecated
    public List<V> zrevrangebyscore(K key, double max, double min) {
        return decodeValueList(redisCommands.zrevrangebyscore(encodeKey(key), min, max));
    }

    @Override
    @Deprecated
    public List<V> zrevrangebyscore(K key, String max, String min) {
        return decodeValueList(redisCommands.zrevrangebyscore(encodeKey(key), min, max));
    }

    @Override
    public List<V> zrevrangebyscore(K key, Range<? extends Number> range) {
        return decodeValueList(redisCommands.zrevrangebyscore(encodeKey(key), range));
    }

    @Override
    @Deprecated
    public List<V> zrevrangebyscore(K key, double max, double min, long offset, long count) {
        return decodeValueList(redisCommands.zrevrangebyscore(encodeKey(key), min, max, offset, count));
    }

    @Override
    @Deprecated
    public List<V> zrevrangebyscore(K key, String max, String min, long offset, long count) {
        return decodeValueList(redisCommands.zrevrangebyscore(encodeKey(key), min, max, offset, count));
    }

    @Override
    public List<V> zrevrangebyscore(K key, Range<? extends Number> range, Limit limit) {
        return decodeValueList(redisCommands.zrevrangebyscore(encodeKey(key), range, limit));
    }

    @Override
    @Deprecated
    public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key, double max, double min) {
        return redisCommands.zrevrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max);
    }

    @Override
    @Deprecated
    public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key, String max, String min) {
        return redisCommands.zrevrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max);
    }

    @Override
    public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key, Range<? extends Number> range) {
        return redisCommands.zrevrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), range);
    }

    @Override
    @Deprecated
    public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key, double max, double min, long offset, long count) {
        return redisCommands.zrevrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max, offset, count);
    }

    @Override
    @Deprecated
    public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key, String max, String min, long offset, long count) {
        return redisCommands.zrevrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max, offset, count);
    }

    @Override
    public Long zrevrangebyscore(ValueStreamingChannel<V> channel, K key, Range<? extends Number> range, Limit limit) {
        return redisCommands.zrevrangebyscore(new ValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), range, limit);
    }

    @Override
    @Deprecated
    public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key, double max, double min) {
        return decodeScoredValueList(redisCommands.zrevrangebyscoreWithScores(encodeKey(key), min, max));
    }

    @Override
    @Deprecated
    public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key, String max, String min) {
        return decodeScoredValueList(redisCommands.zrevrangebyscoreWithScores(encodeKey(key), min, max));
    }

    @Override
    public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key, Range<? extends Number> range) {
        return decodeScoredValueList(redisCommands.zrevrangebyscoreWithScores(encodeKey(key), range));
    }

    @Override
    @Deprecated
    public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key, double max, double min, long offset, long count) {
        return decodeScoredValueList(redisCommands.zrevrangebyscoreWithScores(encodeKey(key), min, max, offset, count));
    }

    @Override
    @Deprecated
    public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key, String max, String min, long offset, long count) {
        return decodeScoredValueList(redisCommands.zrevrangebyscoreWithScores(encodeKey(key), min, max, offset, count));
    }

    @Override
    public List<ScoredValue<V>> zrevrangebyscoreWithScores(K key, Range<? extends Number> range, Limit limit) {
        return decodeScoredValueList(redisCommands.zrevrangebyscoreWithScores(encodeKey(key), range, limit));
    }

    @Override
    @Deprecated
    public Long zrevrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, double max, double min) {
        return redisCommands.zrevrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max);
    }

    @Override
    @Deprecated
    public Long zrevrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, String max, String min) {
        return redisCommands.zrevrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max);
    }

    @Override
    public Long zrevrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, Range<? extends Number> range) {
        return redisCommands.zrevrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), range);
    }

    @Override
    @Deprecated
    public Long zrevrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, double max, double min, long offset, long count) {
        return redisCommands.zrevrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max, offset, count);
    }

    @Override
    @Deprecated
    public Long zrevrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, String max, String min, long offset, long count) {
        return redisCommands.zrevrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), min, max, offset, count);
    }

    @Override
    public Long zrevrangebyscoreWithScores(ScoredValueStreamingChannel<V> channel, K key, Range<? extends Number> range, Limit limit) {
        return redisCommands.zrevrangebyscoreWithScores(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), range, limit);
    }

    @Override
    public Long zrevrank(K key, V member) {
        return redisCommands.zrevrank(encodeKey(key), encodeValue(member));
    }

    @Override
    public ScoredValueScanCursor<V> zscan(K key) {
        return decodeScoredValueScanCursor(redisCommands.zscan(encodeKey(key)));
    }

    @Override
    public ScoredValueScanCursor<V> zscan(K key, ScanArgs scanArgs) {
        return decodeScoredValueScanCursor(redisCommands.zscan(encodeKey(key), scanArgs));
    }

    @Override
    public ScoredValueScanCursor<V> zscan(K key, ScanCursor scanCursor, ScanArgs scanArgs) {
        return decodeScoredValueScanCursor(redisCommands.zscan(encodeKey(key), scanCursor, scanArgs));
    }

    @Override
    public ScoredValueScanCursor<V> zscan(K key, ScanCursor scanCursor) {
        return decodeScoredValueScanCursor(redisCommands.zscan(encodeKey(key), scanCursor));
    }

    @Override
    public StreamScanCursor zscan(ScoredValueStreamingChannel<V> channel, K key) {
        return redisCommands.zscan(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key));
    }

    @Override
    public StreamScanCursor zscan(ScoredValueStreamingChannel<V> channel, K key, ScanArgs scanArgs) {
        return redisCommands.zscan(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanArgs);
    }

    @Override
    public StreamScanCursor zscan(ScoredValueStreamingChannel<V> channel, K key, ScanCursor scanCursor, ScanArgs scanArgs) {
        return redisCommands.zscan(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanCursor, scanArgs);
    }

    @Override
    public StreamScanCursor zscan(ScoredValueStreamingChannel<V> channel, K key, ScanCursor scanCursor) {
        return redisCommands.zscan(new ScoredValueStreamingChannelAdapter<>(channel, codec), encodeKey(key), scanCursor);
    }

    @Override
    public Double zscore(K key, V member) {
        return redisCommands.zscore(encodeKey(key), encodeValue(member));
    }

    @Override
    public Long zunionstore(K destination, K... keys) {
        return redisCommands.zunionstore(encodeKey(destination), encodeKeyArray(keys));
    }

    @Override
    public Long zunionstore(K destination, ZStoreArgs storeArgs, K... keys) {
        return redisCommands.zunionstore(encodeKey(destination), storeArgs, encodeKeyArray(keys));
    }

    @Override
    public Long append(K key, V value) {
        return redisCommands.append(encodeKey(key), encodeValue(value));
    }

    @Override
    public Long bitcount(K key) {
        return redisCommands.bitcount(encodeKey(key));
    }

    @Override
    public Long bitcount(K key, long start, long end) {
        return redisCommands.bitcount(encodeKey(key), start, end);
    }

    @Override
    public List<Long> bitfield(K key, BitFieldArgs bitFieldArgs) {
        return redisCommands.bitfield(encodeKey(key), bitFieldArgs);
    }

    @Override
    public Long bitpos(K key, boolean state) {
        return redisCommands.bitpos(encodeKey(key), state);
    }

    @Override
    public Long bitpos(K key, boolean state, long start) {
        return redisCommands.bitpos(encodeKey(key), state, start);
    }

    @Override
    public Long bitpos(K key, boolean state, long start, long end) {
        return redisCommands.bitpos(encodeKey(key), state, start, end);
    }

    @Override
    public Long bitopAnd(K destination, K... keys) {
        return redisCommands.bitopAnd(encodeKey(destination), encodeKeyArray(keys));
    }

    @Override
    public Long bitopNot(K destination, K source) {
        return redisCommands.bitopNot(encodeKey(destination), encodeKey(source));
    }

    @Override
    public Long bitopOr(K destination, K... keys) {
        return redisCommands.bitopOr(encodeKey(destination), encodeKeyArray(keys));
    }

    @Override
    public Long bitopXor(K destination, K... keys) {
        return redisCommands.bitopXor(encodeKey(destination), encodeKeyArray(keys));
    }

    @Override
    public Long decr(K key) {
        return redisCommands.decr(encodeKey(key));
    }

    @Override
    public Long decrby(K key, long amount) {
        return redisCommands.decrby(encodeKey(key), amount);
    }

    @Override
    public V get(K key) {
        return decodeValue(redisCommands.get(encodeKey(key)));
    }

    @Override
    public Long getbit(K key, long offset) {
        return redisCommands.getbit(encodeKey(key), offset);
    }

    @Override
    public V getrange(K key, long start, long end) {
        return decodeValue(redisCommands.getrange(encodeKey(key), start, end));
    }

    @Override
    public V getset(K key, V value) {
        return decodeValue(redisCommands.getset(encodeKey(key), encodeValue(value)));
    }

    @Override
    public Long incr(K key) {
        return redisCommands.incr(encodeKey(key));
    }

    @Override
    public Long incrby(K key, long amount) {
        return redisCommands.incrby(encodeKey(key), amount);
    }

    @Override
    public Double incrbyfloat(K key, double amount) {
        return redisCommands.incrbyfloat(encodeKey(key), amount);
    }

    @Override
    public List<KeyValue<K, V>> mget(K... keys) {
        return decodeKeyValueList(redisCommands.mget(encodeKeyArray(keys)));
    }

    @Override
    public Long mget(KeyValueStreamingChannel<K, V> channel, K... keys) {
        return redisCommands.mget(new KeyValueStreamingChannelAdapter<>(channel, codec), encodeKeyArray(keys));
    }

    @Override
    public String mset(Map<K, V> map) {
        return redisCommands.mset(encodeKeyValueMap(map));
    }

    @Override
    public Boolean msetnx(Map<K, V> map) {
        return redisCommands.msetnx(encodeKeyValueMap(map));
    }

    @Override
    public String set(K key, V value) {
        return redisCommands.set(encodeKey(key), encodeValue(value));
    }

    @Override
    public String set(K key, V value, SetArgs setArgs) {
        return redisCommands.set(encodeKey(key), encodeValue(value), setArgs);
    }

    @Override
    public Long setbit(K key, long offset, int value) {
        return redisCommands.setbit(encodeKey(key), offset, value);
    }

    @Override
    public String setex(K key, long seconds, V value) {
        return redisCommands.setex(encodeKey(key), seconds, encodeValue(value));
    }

    @Override
    public String psetex(K key, long milliseconds, V value) {
        return redisCommands.psetex(encodeKey(key), milliseconds, encodeValue(value));
    }

    @Override
    public Boolean setnx(K key, V value) {
        return redisCommands.setnx(encodeKey(key), encodeValue(value));
    }

    @Override
    public Long setrange(K key, long offset, V value) {
        return redisCommands.setrange(encodeKey(key), offset, encodeValue(value));
    }

    @Override
    public Long strlen(K key) {
        return redisCommands.strlen(encodeKey(key));
    }

    @Override
    public Long geoadd(K key, double longitude, double latitude, V member) {
        return redisCommands.geoadd(encodeKey(key), longitude, latitude, encodeValue(member));
    }

    @Override
    public Long geoadd(K key, Object... lngLatMember) {
        return redisCommands.geoadd(encodeKey(key), encodeLngLatMember(lngLatMember));
    }

    @Override
    public List<Value<String>> geohash(K key, V... members) {
        return redisCommands.geohash(encodeKey(key), encodeValueArray(members));
    }

    @Override
    public Set<V> georadius(K key, double longitude, double latitude, double distance, GeoArgs.Unit unit) {
        return decodeValueSet(redisCommands.georadius(encodeKey(key), longitude, latitude, distance, unit));
    }

    @Override
    public List<GeoWithin<V>> georadius(K key, double longitude, double latitude, double distance, GeoArgs.Unit unit, GeoArgs geoArgs) {
        return decodeGeoWithinList(redisCommands.georadius(encodeKey(key), latitude, latitude, distance, unit, geoArgs));
    }

    @Override
    public Long georadius(K key, double longitude, double latitude, double distance, GeoArgs.Unit unit, GeoRadiusStoreArgs<K> geoRadiusStoreArgs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<V> georadiusbymember(K key, V member, double distance, GeoArgs.Unit unit) {
        return decodeValueSet(redisCommands.georadiusbymember(encodeKey(key), encodeValue(member), distance, unit));
    }

    @Override
    public List<GeoWithin<V>> georadiusbymember(K key, V member, double distance, GeoArgs.Unit unit, GeoArgs geoArgs) {
        return decodeGeoWithinList(redisCommands.georadiusbymember(encodeKey(key), encodeValue(member), distance, unit, geoArgs));
    }

    @Override
    public Long georadiusbymember(K key, V member, double distance, GeoArgs.Unit unit, GeoRadiusStoreArgs<K> geoRadiusStoreArgs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<GeoCoordinates> geopos(K key, V... members) {
        return redisCommands.geopos(encodeKey(key), encodeValueArray(members));
    }

    @Override
    public Double geodist(K key, V from, V to, GeoArgs.Unit unit) {
        return redisCommands.geodist(encodeKey(key), encodeValue(from), encodeValue(to), unit);
    }

    @Override
    public StatefulRedisConnection<K, V> getStatefulConnection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long pfadd(K key, V... values) {
        return redisCommands.pfadd(encodeKey(key), encodeValueArray(values));
    }

    @Override
    public String pfmerge(K destkey, K... sourcekeys) {
        return redisCommands.pfmerge(encodeKey(destkey), encodeKeyArray(sourcekeys));
    }

    @Override
    public Long pfcount(K... keys) {
        return redisCommands.pfcount(encodeKeyArray(keys));
    }

    @Override
    public <T> T eval(String script, ScriptOutputType type, K... keys) {
        return redisCommands.eval(script, type, encodeKeyArray(keys));
    }

    @Override
    public <T> T eval(String script, ScriptOutputType type, K[] keys, V... values) {
        return redisCommands.eval(script, type, encodeKeyArray(keys), encodeValueArray(values));
    }

    @Override
    public <T> T evalsha(String digest, ScriptOutputType type, K... keys) {
        return redisCommands.evalsha(digest, type, encodeKeyArray(keys));
    }

    @Override
    public <T> T evalsha(String digest, ScriptOutputType type, K[] keys, V... values) {
        return redisCommands.evalsha(digest, type, encodeKeyArray(keys), encodeValueArray(values));
    }

    @Override
    public List<Boolean> scriptExists(String... digests) {
        return redisCommands.scriptExists(digests);
    }

    @Override
    public String scriptFlush() {
        return redisCommands.scriptFlush();
    }

    @Override
    public String scriptKill() {
        return redisCommands.scriptFlush();
    }

    @Override
    public String scriptLoad(V script) {
        return redisCommands.scriptLoad(encodeValue(script));
    }

    @Override
    public String digest(V script) {
        return redisCommands.digest(encodeValue(script));
    }

    @Override
    public String discard() {
        return redisCommands.discard();
    }

    @Override
    public TransactionResult exec() {
        return redisCommands.exec();
    }

    @Override
    public String multi() {
        return redisCommands.multi();
    }

    @Override
    public String unwatch() {
        return redisCommands.unwatch();
    }

    @Override
    public void setTimeout(Duration timeout) {
        redisCommands.setTimeout(timeout);
    }

    @Override
    @Deprecated
    public void setTimeout(long timeout, TimeUnit unit) {
        redisCommands.setTimeout(timeout, unit);
    }

    @Override
    public String clusterBumpepoch() {
        return redisCommands.clusterBumpepoch();
    }

    @Override
    public String clusterMeet(String ip, int port) {
        return redisCommands.clusterMeet(ip, port);
    }

    @Override
    public String clusterForget(String nodeId) {
        return redisCommands.clusterForget(nodeId);
    }

    @Override
    public String clusterAddSlots(int... slots) {
        return redisCommands.clusterAddSlots(slots);
    }

    @Override
    public String clusterDelSlots(int... slots) {
        return redisCommands.clusterDelSlots(slots);
    }

    @Override
    public String clusterSetSlotNode(int slot, String nodeId) {
        return redisCommands.clusterSetSlotNode(slot, nodeId);
    }

    @Override
    public String clusterSetSlotStable(int slot) {
        return redisCommands.clusterSetSlotStable(slot);
    }

    @Override
    public String clusterSetSlotMigrating(int slot, String nodeId) {
        return redisCommands.clusterSetSlotMigrating(slot, nodeId);
    }

    @Override
    public String clusterSetSlotImporting(int slot, String nodeId) {
        return redisCommands.clusterSetSlotImporting(slot, nodeId);
    }

    @Override
    public String clusterInfo() {
        return redisCommands.clusterInfo();
    }

    @Override
    public String clusterMyId() {
        return redisCommands.clusterMyId();
    }

    @Override
    public String clusterNodes() {
        return redisCommands.clusterNodes();
    }

    @Override
    public List<String> clusterSlaves(String nodeId) {
        return redisCommands.clusterSlaves(nodeId);
    }

    @Override
    public Long clusterCountKeysInSlot(int slot) {
        return redisCommands.clusterCountKeysInSlot(slot);
    }

    @Override
    public Long clusterCountFailureReports(String nodeId) {
        return redisCommands.clusterCountFailureReports(nodeId);
    }

    @Override
    public String clusterSaveconfig() {
        return redisCommands.clusterSaveconfig();
    }

    @Override
    public String clusterSetConfigEpoch(long configEpoch) {
        return redisCommands.clusterSetConfigEpoch(configEpoch);
    }

    @Override
    public List<Object> clusterSlots() {
        return redisCommands.clusterSlots();
    }

    @Override
    public String asking() {
        return redisCommands.asking();
    }

    @Override
    public String clusterReplicate(String nodeId) {
        return redisCommands.clusterReplicate(nodeId);
    }

    @Override
    public String clusterFailover(boolean force) {
        return redisCommands.clusterFailover(force);
    }

    @Override
    public String clusterReset(boolean hard) {
        return redisCommands.clusterReset(hard);
    }

    @Override
    public String clusterFlushslots() {
        return redisCommands.clusterFlushslots();
    }

    @Override
    public String bgrewriteaof() {
        return redisCommands.bgrewriteaof();
    }

    @Override
    public String bgsave() {
        return redisCommands.bgsave();
    }

    @Override
    public String clientKill(String addr) {
        return redisCommands.clientKill(addr);
    }

    @Override
    public Long clientKill(KillArgs killArgs) {
        return redisCommands.clientKill(killArgs);
    }

    @Override
    public String clientPause(long timeout) {
        return redisCommands.clientPause(timeout);
    }

    @Override
    public String clientList() {
        return redisCommands.clientList();
    }

    @Override
    public List<Object> command() {
        return redisCommands.command();
    }

    @Override
    public List<Object> commandInfo(String... commands) {
        return redisCommands.commandInfo(commands);
    }

    @Override
    public List<Object> commandInfo(CommandType... commands) {
        return redisCommands.commandInfo(commands);
    }

    @Override
    public Long commandCount() {
        return redisCommands.commandCount();
    }

    @Override
    public Map<String, String> configGet(String parameter) {
        return redisCommands.configGet(parameter);
    }

    @Override
    public String configResetstat() {
        return redisCommands.configResetstat();
    }

    @Override
    public String configRewrite() {
        return redisCommands.configRewrite();
    }

    @Override
    public String configSet(String parameter, String value) {
        return redisCommands.configSet(parameter, value);
    }

    @Override
    public Long dbsize() {
        return redisCommands.dbsize();
    }

    @Override
    public String debugCrashAndRecover(Long delay) {
        return redisCommands.debugCrashAndRecover(delay);
    }

    @Override
    public String debugHtstats(int db) {
        return redisCommands.debugHtstats(db);
    }

    @Override
    public void debugOom() {
        redisCommands.debugOom();
    }

    @Override
    public void debugSegfault() {
        redisCommands.debugSegfault();
    }

    @Override
    public String debugReload() {
        return redisCommands.debugReload();
    }

    @Override
    public String debugRestart(Long delay) {
        return redisCommands.debugRestart(delay);
    }

    @Override
    public String flushall() {
        return redisCommands.flushall();
    }

    @Override
    public String flushallAsync() {
        return redisCommands.flushallAsync();
    }

    @Override
    public String flushdb() {
        return redisCommands.flushdb();
    }

    @Override
    public String flushdbAsync() {
        return redisCommands.flushdbAsync();
    }

    @Override
    public String info() {
        return redisCommands.info();
    }

    @Override
    public String info(String section) {
        return redisCommands.info(section);
    }

    @Override
    public Date lastsave() {
        return redisCommands.lastsave();
    }

    @Override
    public String save() {
        return redisCommands.save();
    }

    @Override
    public void shutdown(boolean save) {
        redisCommands.shutdown(save);
    }

    @Override
    public String slaveof(String host, int port) {
        return redisCommands.slaveof(host, port);
    }

    @Override
    public String slaveofNoOne() {
        return redisCommands.slaveofNoOne();
    }

    @Override
    public List<Object> slowlogGet() {
        return redisCommands.slowlogGet();
    }

    @Override
    public List<Object> slowlogGet(int count) {
        return redisCommands.slowlogGet(count);
    }

    @Override
    public Long slowlogLen() {
        return redisCommands.slowlogLen();
    }

    @Override
    public String slowlogReset() {
        return redisCommands.slowlogReset();
    }

    @Override
    public Long pubsubNumpat() {
        return redisCommands.pubsubNumpat();
    }

    @Override
    public List<Object> role() {
        return redisCommands.role();
    }

    @Override
    public String ping() {
        return redisCommands.ping();
    }

    @Override
    public String readOnly() {
        return redisCommands.readOnly();
    }

    @Override
    public String readWrite() {
        return redisCommands.readWrite();
    }

    @Override
    public String quit() {
        return redisCommands.quit();
    }

    @Override
    public Long waitForReplication(int replicas, long timeout) {
        return redisCommands.waitForReplication(replicas, timeout);
    }

    @Override
    public boolean isOpen() {
        return redisCommands.isOpen();
    }

    @Override
    public void reset() {
        redisCommands.reset();
    }

    @Override
    public String watch(K... keys) {
        return redisCommands.watch(encodeKeyArray(keys));
    }

    @Override
    public List<K> clusterGetKeysInSlot(int slot, int count) {
        return decodeKeyList(redisCommands.clusterGetKeysInSlot(slot, count));
    }

    @Override
    public Long clusterKeyslot(K key) {
        return redisCommands.clusterKeyslot(encodeKey(key));
    }

    @Override
    public Long publish(K channel, V message) {
        return redisCommands.publish(encodeKey(channel), encodeValue(message));
    }

    @Override
    public List<K> pubsubChannels() {
        return decodeKeyList(redisCommands.pubsubChannels());
    }

    @Override
    public List<K> pubsubChannels(K channel) {
        return decodeKeyList(redisCommands.pubsubChannels(encodeKey(channel)));
    }

    @Override
    public Map<K, Long> pubsubNumsub(K... channels) {
        return decodeKeyMap(redisCommands.pubsubNumsub(encodeKeyArray(channels)));
    }

    @Override
    public V echo(V msg) {
        return decodeValue(redisCommands.echo(encodeValue(msg)));
    }

    @Override
    public <T> T dispatch(ProtocolKeyword type, CommandOutput<K, V, T> output) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T dispatch(ProtocolKeyword type, CommandOutput<K, V, T> output, CommandArgs<K, V> args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K clientGetname() {
        return decodeKey(redisCommands.clientGetname());
    }

    @Override
    public String clientSetname(K name) {
        return redisCommands.clientSetname(encodeKey(name));
    }

    @Override
    public String debugObject(K key) {
        return redisCommands.debugObject(encodeKey(key));
    }

    @Override
    public String debugSdslen(K key) {
        return redisCommands.debugSdslen(encodeKey(key));
    }

    @Override
    public List<V> time() {
        return decodeValueList(redisCommands.time());
    }
}
