package cc.whohow.redis.jcache.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MutableRedisCacheConfiguration<K, V> implements RedisCacheConfiguration<K, V> {
    protected String name;
    protected Class<K> keyType;
    protected Class<V> valueType;
    protected String[] keyTypeCanonicalName = {};
    protected String valueTypeCanonicalName = "";
    protected String keyCodec = "";
    protected String valueCodec = "";

    protected boolean statisticsEnabled = true;
    protected boolean managementEnabled = false;
    protected long expiryForUpdate = -1;
    protected TimeUnit expiryForUpdateTimeUnit = TimeUnit.SECONDS;

    protected boolean redisCacheEnabled = true;

    protected boolean inProcessCacheEnabled = true;
    protected int inProcessCacheMaxEntry = -1;
    protected long inProcessCacheExpiryForUpdate = -1;
    protected TimeUnit inProcessCacheExpiryForUpdateTimeUnit = TimeUnit.SECONDS;

    private List<String> extraConfigurations = new ArrayList<>();

    public MutableRedisCacheConfiguration() {
    }

    public MutableRedisCacheConfiguration(RedisCacheConfiguration<K, V> that) {
        this.name = that.getName();
        this.keyType = that.getKeyType();
        this.valueType = that.getValueType();
        this.keyTypeCanonicalName = that.getKeyTypeCanonicalName();
        this.valueTypeCanonicalName = that.getValueTypeCanonicalName();
        this.keyCodec = that.getKeyCodec();
        this.valueCodec = that.getValueCodec();
        this.statisticsEnabled = that.isStatisticsEnabled();
        this.managementEnabled = that.isManagementEnabled();
        this.expiryForUpdate = that.getExpiryForUpdate();
        this.expiryForUpdateTimeUnit = that.getExpiryForUpdateTimeUnit();
        this.redisCacheEnabled = that.isRedisCacheEnabled();
        this.inProcessCacheEnabled = that.isInProcessCacheEnabled();
        this.inProcessCacheMaxEntry = that.getInProcessCacheMaxEntry();
        this.inProcessCacheExpiryForUpdate = that.getInProcessCacheExpiryForUpdate();
        this.inProcessCacheExpiryForUpdateTimeUnit = that.getInProcessCacheExpiryForUpdateTimeUnit();
        this.extraConfigurations = that.getExtraConfigurations();
    }

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
    public String getKeyCodec() {
        return keyCodec;
    }

    public void setKeyCodec(String keyCodec) {
        this.keyCodec = keyCodec;
    }

    @Override
    public String getValueCodec() {
        return valueCodec;
    }

    public void setValueCodec(String valueCodec) {
        this.valueCodec = valueCodec;
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
    public boolean isRedisCacheEnabled() {
        return redisCacheEnabled;
    }

    public void setRedisCacheEnabled(boolean redisCacheEnabled) {
        this.redisCacheEnabled = redisCacheEnabled;
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

    @Override
    public List<String> getExtraConfigurations() {
        return extraConfigurations;
    }

    public void setExtraConfigurations(List<String> extraConfigurations) {
        this.extraConfigurations = extraConfigurations;
    }
}
