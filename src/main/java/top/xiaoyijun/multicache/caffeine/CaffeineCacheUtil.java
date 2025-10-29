package top.xiaoyijun.multicache.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.micrometer.common.lang.NonNull;
import io.micrometer.common.lang.Nullable;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Caffeine 缓存工具类，支持动态设置每个 key 的过期时间
 */
public class CaffeineCacheUtil<K, V> {

    // 底层 Caffeine 缓存
    private final Cache<K, V> cache;

    // 存储每个 key 的过期时间（纳秒），用于动态调整
    private final Map<K, Long> keyExpireMap = new ConcurrentHashMap<>();

    // 随机数生成器（复用避免性能损耗）
    private final Random random = new Random();

    /**
     * 初始化缓存
     * @param initialCapacity 初始容量
     * @param maximumSize 最大容量
     */
    public CaffeineCacheUtil(int initialCapacity, long maximumSize) {
        // 构建缓存，使用自定义 Expiry 策略
        this.cache = Caffeine.newBuilder()
                .initialCapacity(initialCapacity)
                .maximumSize(maximumSize)
                .expireAfter(new CustomExpiry<>()) // 自定义过期策略
                .recordStats()
                .build();
    }

    /**
     * 添加缓存（永不过期）
     */
    public void put(K key, V value) {
        // 不设置过期时间，不在 keyExpireMap 中存储，视为永不过期
        cache.put(key, value);
    }

    /**
     * 添加缓存（使用默认过期时间：3分钟 + 0-300秒随机）
     */
    public void putWithDefaultExpire(K key, V value) {
        // 生成默认随机过期时间（3-8分钟）
        long baseExpire = TimeUnit.MINUTES.toNanos(3); // 3分钟（纳秒）
        long randomOffset = TimeUnit.SECONDS.toNanos(random.nextInt(301)); // 0-300秒随机
        long expireNanos = baseExpire + randomOffset;
        put(key, value, expireNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * 添加缓存（自定义过期时间）
     */
    public void put(K key, V value, long duration, TimeUnit unit) {
        // 转换为纳秒并存储
        long expireNanos = unit.toNanos(duration);
        keyExpireMap.put(key, expireNanos);
        // 放入缓存
        cache.put(key, value);
    }

    /**
     * 获取缓存
     */
    @Nullable
    public V getIfPresent(K key) {
        return cache.getIfPresent(key);
    }

    /**
     * 移除缓存
     */
    public void invalidate(K key) {
        cache.invalidate(key);
        keyExpireMap.remove(key);
    }

    /**
     * 清空缓存
     */
    public void invalidateAll() {
        cache.invalidateAll();
        keyExpireMap.clear();
    }

    /**
     * 获取缓存统计信息
     */
    public String stats() {
        return cache.stats().toString();
    }

    /**
     * 自定义过期策略：从 keyExpireMap 中获取每个 key 的过期时间
     */
    private class CustomExpiry<K, V> implements Expiry<K, V> {

        @Override
        public long expireAfterCreate(@NonNull K key, @NonNull V value, long currentTime) {
            // 创建时，从 map 中获取预设的过期时间（默认 0 表示立即过期，避免未设置的 key 永不过期）
            return keyExpireMap.getOrDefault(key, 0L);
        }

        @Override
        public long expireAfterUpdate(@NonNull K key, @NonNull V value, long currentTime, @NonNegative long currentDuration) {
            // 更新时，使用新设置的过期时间（若未重新设置则沿用旧值）
            return keyExpireMap.getOrDefault(key, currentDuration);
        }

        @Override
        public long expireAfterRead(@NonNull K key, @NonNull V value, long currentTime, @NonNegative long currentDuration) {
            // 读取时不改变过期时间（可根据需求改为延长过期）
            return currentDuration;
        }
    }
}