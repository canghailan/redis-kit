package cc.whohow.redis.util;

import cc.whohow.redis.distributed.SystemInfo;
import cc.whohow.redis.io.ByteBuffers;
import cc.whohow.redis.io.PrimitiveCodec;
import cc.whohow.redis.io.StringCodec;
import cc.whohow.redis.lettuce.Lettuce;
import cc.whohow.redis.script.RedisScriptCommands;
import io.lettuce.core.KeyValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 类似ThreadLocal，每个RedisLocal有一个唯一ID(getId)、一个锁、一个独立存储空间(getRedisLocalMap)；并可通过isLeader判断选举结果。
 */
public class RedisLocal implements Runnable {
    private static final Logger log = LogManager.getLogger();
    private static final Long NULL_ID = -1L;
    private static final String REDIS_LOCAL = "_rl_";
    private static final String REDIS_LOCK = "_rl_:";
    private static final String REDIS_LOCAL_MAP = "_rlm_:";
    /**
     * ID过期时间，180s
     */
    private final long expiresIn;
    /**
     * 最大ID，4字节
     */
    private final long maxId;
    private final RedisCommands<ByteBuffer, ByteBuffer> redis;
    private final RedisScriptCommands redisScript;
    private final ScheduledExecutorService executor;
    private final String random;
    private volatile CompletableFuture<Long> id = new CompletableFuture<>();

    public RedisLocal(RedisCommands<ByteBuffer, ByteBuffer> redis, ScheduledExecutorService executor) {
        this.redis = redis;
        this.redisScript = new RedisScriptCommands(redis);
        this.executor = executor;
        // ID过期时间，180s
        this.expiresIn = 180_000L;
        // 最大ID，4字节
        this.maxId = 65535L;
        this.random = UUID.randomUUID().toString();
        this.executor.scheduleAtFixedRate(this, 0, expiresIn / 5, TimeUnit.MILLISECONDS);
    }

    /**
     * RedisLocal状态更新，需定时执行
     */
    @Override
    public void run() {
        try {
            Long currentId = id.getNow(NULL_ID);
            log.debug("Current ID: {}", currentId);
            if (NULL_ID.equals(currentId)) {
                log.debug("New ID");
                // ID未注册
                // 已注册ID
                Map<Long, Long> ids = getIds();
                // 活跃ID
                Set<Long> activeIds = getActiveIds(ids.keySet());
                // 过期ID
                Set<Long> expireIds = getExpireIds(ids.keySet(), activeIds);
                log.debug("ID: {}", ids);
                log.debug("Active ID: {}", activeIds);
                log.debug("Expire ID: {}", expireIds);

                // 回收过期ID
                gc(expireIds);

                // 遍历可用ID，并进行注册
                for (long newId = 0; newId <= maxId; newId++) {
                    if (activeIds.contains(newId)) {
                        // 如果ID已使用，跳过
                        continue;
                    }
                    long time = currentTimeMillis();
                    if (register(newId, time) &&
                            lock(newId, random, expiresIn)) {
                        // 注册并锁定ID成功
                        id.complete(newId);
                        log.debug("New ID OK: {} @{}", newId, time);
                        // 更新本地系统信息到RedisLocalMap，便于排查问题
                        executor.submit(this::putLocalSystemInfo);
                        return;
                    } else {
                        log.debug("Skip ID: {}", newId);
                    }
                }
                log.error("New ID ERROR");
            } else {
                log.debug("Renew ID");
                // ID已注册，更新锁定
                if (renewLock(currentId, random, expiresIn)) {
                    // 锁定成功
                    log.debug("Renew ID OK: {}", currentId);
                } else {
                    // 锁定失败，ID已被回收，重新注册ID
                    log.debug("Renew ID ERROR: {}", currentId);
                    id = new CompletableFuture<>();
                    run();
                }
            }
        } catch (Exception e) {
            log.error("New/Renew ID ERROR", e);
        }
    }

    /**
     * 注册ID
     */
    private boolean register(Long id, long time) {
        boolean ok = redis.zadd(ByteBuffers.fromUtf8(REDIS_LOCAL), Lettuce.Z_ADD_NX, time, PrimitiveCodec.LONG.encode(id)) > 0;
        log.debug("Register: {} {}", id, ok);
        return ok;
    }

    /**
     * 锁定ID
     */
    private boolean lock(Long id, String random, long ttl) {
        boolean ok = Lettuce.ok(redis.set(getRedisLockKey(id), ByteBuffers.fromUtf8(random), SetArgs.Builder.px(ttl).nx()));
        log.debug("Lock: {} {}", id, ok);
        return ok;
    }

    /**
     * 更新锁定ID
     */
    private boolean renewLock(Long id, String random, long ttl) {
        ByteBuffer rnd = ByteBuffers.fromUtf8(random);
        Boolean ok = redisScript.eval("cas", ScriptOutputType.BOOLEAN,
                new ByteBuffer[]{getRedisLockKey(id)},
                rnd.duplicate(), rnd.duplicate(),
                Lettuce.px(), PrimitiveCodec.LONG.encode(ttl), Lettuce.xx());
        log.debug("Renew Lock: {} {}", id, ok);
        return ok;
    }

    /**
     * 获取ID，阻塞直到ID注册成功
     */
    public long getId() {
        return id.join();
    }

    /**
     * 获取本地存储空间
     */
    public RedisMap<String, String> getRedisLocalMap() {
        return new RedisMap<>(redis,
                StringCodec.defaultInstance(), StringCodec.defaultInstance(),
                getRedisLocalMapKey(getId()));
    }

    /**
     * 获取所有活跃ID
     */
    public Set<Long> getActiveIds() {
        return getActiveIds(getIds().keySet());
    }

    /**
     * 是否是选举领导者
     */
    public boolean isLeader() {
        if (id.isDone()) {
            return id.getNow(NULL_ID).equals(getLeaderId());
        } else {
            return false;
        }
    }

    /**
     * 获取所有注册ID及注册时间
     */
    private Map<Long, Long> getIds() {
        return redis.zrangeWithScores(ByteBuffers.fromUtf8(REDIS_LOCAL), 0, -1).stream()
                .collect(Collectors.toMap(
                        e -> PrimitiveCodec.LONG.decode(e.getValue()),
                        e -> (long) e.getScore(),
                        (a, b) -> b,
                        LinkedHashMap::new));
    }

    /**
     * 获取所有活跃ID
     */
    private Set<Long> getActiveIds(Set<Long> ids) {
        if (ids.isEmpty()) {
            return Collections.emptySet();
        }
        return redis.mget(ids.stream()
                .map(this::getRedisLockKey)
                .peek(Buffer::mark)
                .toArray(ByteBuffer[]::new))
                .stream()
                .filter(KeyValue::hasValue)
                .map(KeyValue::getKey)
                .peek(ByteBuffer::reset)
                .map(this::getRedisLockId)
                .collect(Collectors.toSet());
    }

    /**
     * 获取所有过期ID
     */
    private Set<Long> getExpireIds(Set<Long> ids, Set<Long> activeIds) {
        if (ids.size() == activeIds.size()) {
            return Collections.emptySet();
        } else {
            Set<Long> expireIds = new HashSet<>(ids);
            expireIds.removeAll(activeIds);
            return expireIds;
        }
    }

    /**
     * 选举领导者ID，不建议直接暴露本方法
     */
    private Long getLeaderId() {
        Map<Long, Long> ids = getIds();
        Set<Long> activeIds = getActiveIds(ids.keySet());
        for (Long id : ids.keySet()) {
            if (activeIds.contains(id)) {
                log.debug("Leader ID: {}", id);
                return id;
            }
        }
        return null;
    }

    /**
     * 当前Redis时间
     */
    private long currentTimeMillis() {
        List<ByteBuffer> time = redis.time();
        long s = PrimitiveCodec.LONG.decode(time.get(0));
        long ss = PrimitiveCodec.LONG.decode(time.get(1));
        return TimeUnit.SECONDS.toMillis(s) + TimeUnit.MICROSECONDS.toMillis(ss);
    }

    /**
     * 从ID生成RedisKey
     */
    private ByteBuffer getRedisLockKey(Long id) {
        return ByteBuffers.fromUtf8(REDIS_LOCK + id);
    }

    /**
     * 从RedisKey获取ID
     */
    private Long getRedisLockId(ByteBuffer key) {
        key.position(key.position() + REDIS_LOCK.length());
        return PrimitiveCodec.LONG.decode(key);
    }

    /**
     * 本地存储空间RedisKey
     */
    private ByteBuffer getRedisLocalMapKey(Long id) {
        return ByteBuffers.fromUtf8(REDIS_LOCAL_MAP + id);
    }

    /**
     * 回收ID
     */
    private void gc(Set<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        log.info("GC ID: {}", ids);
        redis.zrem(ByteBuffers.fromUtf8(REDIS_LOCAL), ids.stream()
                .map(PrimitiveCodec.LONG::encode)
                .toArray(ByteBuffer[]::new));
        redis.del(ids.stream()
                .map(this::getRedisLocalMapKey)
                .toArray(ByteBuffer[]::new));
    }

    /**
     * 更新本地系统信息
     */
    private void putLocalSystemInfo() {
        getRedisLocalMap().putAll(new SystemInfo().get());
        log.debug("putLocalSystemInfo OK");
    }

    @Override
    public String toString() {
        Long currentId = id.getNow(NULL_ID);
        if (NULL_ID.equals(currentId)) {
            return "RedisLocal:NULL";
        } else {
            return "RedisLocal:" + currentId;
        }
    }
}
