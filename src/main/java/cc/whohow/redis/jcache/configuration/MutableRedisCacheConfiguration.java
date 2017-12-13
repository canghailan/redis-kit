package cc.whohow.redis.jcache.configuration;

import org.redisson.client.codec.Codec;

import java.util.concurrent.TimeUnit;

public class MutableRedisCacheConfiguration<K, V> implements RedisCacheConfiguration<K, V> {
    private String name;
    private Class<K> keyType;
    private Class<V> valueType;
    private long expiryForUpdate = -1;
    private TimeUnit expiryForUpdateTimeUnit = TimeUnit.SECONDS;
    private boolean statisticsEnabled = true;
    private boolean managementEnabled = false;

    // redis
    private boolean redisCacheEnabled = true;
    private String redisKey;
    private String[] keyTypeCanonicalName;
    private String valueTypeCanonicalName;
    private Codec keyCodec;
    private Codec valueCodec;
    private boolean publishCacheEntryEventEnabled = true;

    // in-process
    private boolean inProcessCacheEnabled = true;
    private int inProcessCacheMaxEntry = -1;
    private long inProcessCacheExpiryForUpdate = -1;
    private TimeUnit inProcessCacheExpiryForUpdateTimeUnit = TimeUnit.SECONDS;

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Class<K> getKeyType() {
        return keyType;
    }

    public void setKeyType(Class<K> keyType) {
        this.keyType = keyType;
    }

    @Override
    public Class<V> getValueType() {
        return valueType;
    }

    public void setValueType(Class<V> valueType) {
        this.valueType = valueType;
    }

    @Override
    public long getExpiryForUpdate() {
        return expiryForUpdate;
    }

    public void setExpiryForUpdate(long expiryForUpdate) {
        this.expiryForUpdate = expiryForUpdate;
    }

    @Override
    public TimeUnit getExpiryForUpdateTimeUnit() {
        return expiryForUpdateTimeUnit;
    }

    public void setExpiryForUpdateTimeUnit(TimeUnit expiryForUpdateTimeUnit) {
        this.expiryForUpdateTimeUnit = expiryForUpdateTimeUnit;
    }

    @Override
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    @Override
    public boolean isManagementEnabled() {
        return managementEnabled;
    }

    public void setManagementEnabled(boolean managementEnabled) {
        this.managementEnabled = managementEnabled;
    }

    @Override
    public boolean isRedisCacheEnabled() {
        return redisCacheEnabled;
    }

    public void setRedisCacheEnabled(boolean redisCacheEnabled) {
        this.redisCacheEnabled = redisCacheEnabled;
    }

    @Override
    public String getRedisKey() {
        return redisKey;
    }

    public void setRedisKey(String redisKey) {
        this.redisKey = redisKey;
    }

    @Override
    public String[] getKeyTypeCanonicalName() {
        return keyTypeCanonicalName;
    }

    public void setKeyTypeCanonicalName(String[] keyTypeCanonicalName) {
        this.keyTypeCanonicalName = keyTypeCanonicalName;
    }

    @Override
    public String getValueTypeCanonicalName() {
        return valueTypeCanonicalName;
    }

    public void setValueTypeCanonicalName(String valueTypeCanonicalName) {
        this.valueTypeCanonicalName = valueTypeCanonicalName;
    }

    @Override
    public Codec getKeyCodec() {
        return keyCodec;
    }

    public void setKeyCodec(Codec keyCodec) {
        this.keyCodec = keyCodec;
    }

    @Override
    public Codec getValueCodec() {
        return valueCodec;
    }

    public void setValueCodec(Codec valueCodec) {
        this.valueCodec = valueCodec;
    }

    @Override
    public boolean isPublishCacheEntryEventEnabled() {
        return publishCacheEntryEventEnabled;
    }

    public void setPublishCacheEntryEventEnabled(boolean publishCacheEntryEventEnabled) {
        this.publishCacheEntryEventEnabled = publishCacheEntryEventEnabled;
    }

    @Override
    public boolean isInProcessCacheEnabled() {
        return inProcessCacheEnabled;
    }

    public void setInProcessCacheEnabled(boolean inProcessCacheEnabled) {
        this.inProcessCacheEnabled = inProcessCacheEnabled;
    }

    @Override
    public int getInProcessCacheMaxEntry() {
        return inProcessCacheMaxEntry;
    }

    public void setInProcessCacheMaxEntry(int inProcessCacheMaxEntry) {
        this.inProcessCacheMaxEntry = inProcessCacheMaxEntry;
    }

    @Override
    public long getInProcessCacheExpiryForUpdate() {
        return inProcessCacheExpiryForUpdate;
    }

    public void setInProcessCacheExpiryForUpdate(long inProcessCacheExpiryForUpdate) {
        this.inProcessCacheExpiryForUpdate = inProcessCacheExpiryForUpdate;
    }

    @Override
    public TimeUnit getInProcessCacheExpiryForUpdateTimeUnit() {
        return inProcessCacheExpiryForUpdateTimeUnit;
    }

    public void setInProcessCacheExpiryForUpdateTimeUnit(TimeUnit inProcessCacheExpiryForUpdateTimeUnit) {
        this.inProcessCacheExpiryForUpdateTimeUnit = inProcessCacheExpiryForUpdateTimeUnit;
    }
}
