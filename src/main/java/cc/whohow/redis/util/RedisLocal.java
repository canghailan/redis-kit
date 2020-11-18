package cc.whohow.redis.util;

import cc.whohow.redis.Redis;
import cc.whohow.redis.bytes.ByteSequence;
import cc.whohow.redis.codec.PrimitiveCodec;
import cc.whohow.redis.codec.StringCodec;
import cc.whohow.redis.lettuce.ListOutput;
import cc.whohow.redis.lettuce.VoidOutput;
import io.lettuce.core.protocol.CommandType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 类似ThreadLocal
 * {key} -> 存储所有注册ID的有序集合，按注册时间排序
 * {key}:{id} -> 实例锁
 * {key}:{id}:map -> 实例存储
 */
public class RedisLocal implements Runnable {
    private static final Logger log = LogManager.getLogger();
    /**
     * 事件监听
     */
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    /**
     * ID有效期
     */
    private final Duration expiresIn;
    /**
     * 最大ID
     */
    private final long maxId;
    /**
     * Redis连接
     */
    private final Redis redis;
    /**
     * 定时任务线程池
     */
    private final ScheduledExecutorService executor;
    /**
     * RedisKey空间
     */
    private final String key;
    /**
     * Redis时钟
     */
    private final RedisClock clock;
    /**
     * 实例ID集合
     */
    private final RedisSortedSet<Long> idSet;
    /**
     * 当前实例ID
     */
    private volatile CompletableFuture<Long> id;
    /**
     * 当前实例锁 {key}:{id}
     */
    private volatile RedisLock lock;
    /**
     * 当前实例存储 {key}:{id}:map
     */
    private volatile RedisMap<String, String> localMap;

    public RedisLocal(Redis redis, ScheduledExecutorService executor, String key) {
        this.redis = redis;
        this.executor = executor;
        this.key = key;
        this.expiresIn = Duration.ofMinutes(3); // ID有效期，3分钟，需及时续期
        this.maxId = 65535L; // 最大ID，4字节
        this.clock = new RedisClock(redis); // 时钟
        this.idSet = new RedisSortedSet<>(redis, PrimitiveCodec.LONG, key);
        this.id = new CompletableFuture<>();

        // 启动注册/续期任务
        this.executor.scheduleWithFixedDelay(this,
                0, expiresIn.toMillis() / 5, TimeUnit.MILLISECONDS);
    }

    /**
     * 实例RedisKey
     */
    public String getLocalRedisKey() {
        return getLocalRedisKey(getId());
    }

    /**
     * 实例RedisKey
     */
    public String getLocalRedisKey(Long id) {
        return key + ":" + id;
    }

    /**
     * 实例存储RedisKey
     */
    public String getLocalMapRedisKey() {
        return getLocalMapRedisKey(getId());
    }

    /**
     * 实例存储RedisKey
     */
    public String getLocalMapRedisKey(Long id) {
        return getLocalRedisKey(id) + ":map";
    }

    /**
     * 获取Redis时钟
     */
    public RedisClock getClock() {
        return clock;
    }

    /**
     * 获取实例存储空间
     */
    public Optional<RedisMap<String, String>> getLocalMap() {
        return Optional.ofNullable(localMap);
    }

    /**
     * 注册/续期
     */
    @Override
    public void run() {
        try {
            Optional<Long> currentId = tryGetId();
            if (currentId.isPresent()) {
                renewId(currentId.get()); // 如果ID存在，续期ID
            } else {
                newId(); // 如果ID不存在，新注册ID
            }
        } catch (Throwable e) {
            log.error("newId/renewId error", e);
        }
    }

    /**
     * 注册ID
     */
    protected synchronized void newId() {
        log.debug("newId");
        Map<Long, Number> ids = getIds(); // 已注册ID
        log.debug("ids: {}", ids);
        Set<Long> activeIds = getActiveIds(ids.keySet()); // 活跃ID
        log.debug("activeIds: {}", activeIds);

        gc(getExpireIds(ids.keySet(), activeIds)); // 回收过期ID

        // 遍历可用ID，并尝试注册
        for (long newId = 0; newId <= maxId; newId++) {
            if (activeIds.contains(newId)) {
                continue;
            }
            long time = clock.millis();
            if (idSet.zaddnx(time, newId) > 0) { // 尝试注册ID
                log.debug("registerId ok: {} @{}", newId, time);
                RedisLock lock = new RedisLock(redis, getLocalRedisKey(newId), expiresIn);
                if (lock.tryLock()) { // 尝试获取实例锁
                    log.debug("lockId ok: {}", newId);
                    this.id.complete(newId); // 注册ID成功
                    log.debug("newId ok: {} @{}", newId, time);
                    this.lock = lock; // 更新实例锁
                    this.localMap = new RedisMap<>(redis, StringCodec.UTF8.get(), StringCodec.UTF8.get(),
                            getLocalMapRedisKey(newId)); // 创建实例存储空间
                    executor.execute(() -> {
                        Map<String, String> map = localMap;
                        if (map != null) {
                            log.debug("synchronize LocalInfo to LocalMap");
                            map.putAll(getLocalInfo()); // 异步同步本地信息
                        } else {
                            log.debug("LocalMap state error");
                        }
                    });
                    notifyNewIdAsync();
                    return;
                } else {
                    log.debug("lockId failed: {}", newId);
                }
            } else {
                log.debug("registerId failed: {} @{}", newId, time);
            }
        }
        log.error("newId error");
    }

    /**
     * 续期ID
     */
    protected synchronized void renewId(Long currentId) {
        log.debug("renewId: {}", currentId);
        if (lock.renew(expiresIn)) { // 尝试续期实例锁
            log.debug("renewId ok: {}", currentId);
            notifyRenewIdAsync();
        } else {
            log.debug("renewId error: {}", currentId);
            this.id = new CompletableFuture<>(); // 重置ID
            this.lock = null; // 移除实例锁
            this.localMap = null; // 移除实例存储空间
            notifyRenewIdErrorAsync();
            newId(); // 重新注册ID
        }
    }

    /**
     * 获取ID，阻塞直到ID注册成功
     */
    public long getId() {
        return id.join();
    }

    /**
     * 获取ID，立即返回
     */
    public Optional<Long> tryGetId() {
        Long value = id.getNow(-1L);
        if (value == -1L) {
            return Optional.empty();
        } else {
            return Optional.of(value);
        }
    }

    /**
     * 获取所有注册ID及注册时间
     */
    public Map<Long, Number> getIds() {
        return idSet.copy();
    }

    /**
     * 获取所有活跃ID
     */
    public Set<Long> getActiveIds() {
        return getActiveIds(getIds().keySet());
    }

    /**
     * 获取所有活跃ID
     */
    protected Set<Long> getActiveIds(Set<Long> ids) {
        if (ids.isEmpty()) {
            return Collections.emptySet();
        }

        // 所有实例锁RedisKey与ID映射关系
        Map<String, Long> locks = ids.stream()
                .collect(Collectors.toMap(this::getLocalRedisKey, Function.identity()));
        // 读取实例锁token
        List<ByteSequence> tokens = redis.send(new ListOutput<>(ByteSequence::of),
                CommandType.MGET,
                locks.keySet().stream().map(ByteSequence::utf8).collect(Collectors.toList()));

        LinkedHashSet<Long> activeIds = new LinkedHashSet<>(ids.size());
        int i = 0;
        for (Long id : locks.values()) {
            // 实例锁存在，则为活跃ID
            if (tokens.get(i++) != null) {
                activeIds.add(id);
            }
        }
        return activeIds;
    }

    /**
     * 获取所有过期ID
     */
    protected Set<Long> getExpireIds(Set<Long> ids, Set<Long> activeIds) {
        if (ids.size() == activeIds.size()) {
            return Collections.emptySet();
        } else {
            Set<Long> expireIds = new HashSet<>(ids);
            expireIds.removeAll(activeIds);
            return expireIds;
        }
    }

    /**
     * 是否是选举领导者
     */
    public boolean isLeader() {
        Optional<Long> currentId = tryGetId();
        if (currentId.isPresent()) {
            Optional<Long> leaderId = tryGetLeaderId();
            if (leaderId.isPresent()) {
                return currentId.get().equals(leaderId.get());
            }
        }
        return false;
    }

    /**
     * 选举领导者ID，不建议直接暴露本方法，可能返回null
     */
    private Optional<Long> tryGetLeaderId() {
        Map<Long, Number> ids = getIds(); // 按注册时间排序好的id集合
        log.trace("ids: {}", ids);
        Set<Long> activeIds = getActiveIds(ids.keySet());
        log.trace("activeIds: {}", activeIds);
        for (Long id : ids.keySet()) {
            if (activeIds.contains(id)) {
                log.debug("leaderId: {}", id);
                return Optional.of(id);
            }
        }
        return Optional.empty();
    }

    /**
     * 回收ID
     */
    private void gc(Set<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        log.debug("gc: {}", ids);

        // 回收实例存储空间
        redis.send(new VoidOutput(), CommandType.DEL, ids.stream()
                .map(this::getLocalMapRedisKey)
                .map(ByteSequence::utf8)
                .collect(Collectors.toList()));
        idSet.zrem(ids);
    }

    public void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    protected void notifyNewIdAsync() {
        if (listeners.isEmpty()) {
            return;
        }
        executor.execute(this::notifyRenewId);
    }

    protected void notifyNewId() {
        log.debug("notifyNewId");
        for (Listener listener : listeners) {
            try {
                listener.onNewId(this);
            } catch (Throwable e) {
                log.warn("notifyNewId error", e);
            }
        }
    }

    protected void notifyRenewIdAsync() {
        if (listeners.isEmpty()) {
            return;
        }
        executor.execute(this::notifyRenewId);
    }

    protected void notifyRenewId() {
        log.debug("notifyRenewId");
        for (Listener listener : listeners) {
            try {
                listener.onRenewId(this);
            } catch (Throwable e) {
                log.warn("notifyRenewId error", e);
            }
        }
    }

    protected void notifyRenewIdErrorAsync() {
        if (listeners.isEmpty()) {
            return;
        }
        executor.execute(this::notifyRenewIdError);
    }

    protected void notifyRenewIdError() {
        log.debug("notifyRenewIdError");
        for (Listener listener : listeners) {
            try {
                listener.onRenewIdError(this);
            } catch (Throwable e) {
                log.warn("notifyNewId error", e);
            }
        }
    }

    @Override
    public String toString() {
        return tryGetId().map(currentId -> key + ":" + currentId).orElseGet(() -> key + ":null");
    }

    public Map<String, String> getLocalInfo() {
        log.debug("getLocalInfo");
        Map<String, String> localInfo = new TreeMap<>();
        // SystemProperties
        Properties systemProperties = System.getProperties();
        for (String name : systemProperties.stringPropertyNames()) {
            localInfo.put(name, systemProperties.getProperty(name));
        }
        // SystemEnv
        StringBuilder env = new StringBuilder();
        for (Map.Entry<String, String> e : System.getenv().entrySet()) {
            env.append(e.getKey()).append("=").append(e.getValue()).append("\n");
        }
        localInfo.put("system.env", env.toString());
        // Network
        try {
            StringBuilder network = new StringBuilder();
            for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                List<InetAddress> addressList = Collections.list(networkInterface.getInetAddresses()).stream()
                        .filter(i -> !i.isLoopbackAddress() &&
                                !i.isLinkLocalAddress() &&
                                !i.isAnyLocalAddress() &&
                                !i.isMulticastAddress()
                        )
                        .collect(Collectors.toList());
                if (addressList.isEmpty()) {
                    continue;
                }

                network.append("Name: ").append(networkInterface.getName()).append("\n");
                network.append("DisplayName: ").append(networkInterface.getDisplayName()).append("\n");
                for (InetAddress address : addressList) {
                    network.append("InetAddress: ").append(address).append("\n");
                }
                network.append("\n");
            }
            localInfo.put("network", network.toString());
        } catch (Throwable e) {
            log.warn("getNetworkInterfaces error", e);
        }
        return localInfo;
    }

    interface Listener {
        default void onNewId(RedisLocal local) {
        }

        default void onRenewId(RedisLocal local) {
        }

        default void onRenewIdError(RedisLocal local) {
        }
    }
}
