package top.xiaoyijun.multicache.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import jakarta.annotation.Resource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import top.xiaoyijun.multicache.caffeine.CaffeineCacheUtil;
import top.xiaoyijun.multicache.rabbitmq.RabbitMQConfig;
import top.xiaoyijun.multicache.model.Product;
import top.xiaoyijun.multicache.mapper.ProductMapper;
import top.xiaoyijun.multicache.service.ProductService;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 商品服务实现类
 * 提供基于多级缓存（本地缓存+Redis）的商品查询和更新功能
 * 包含多种缓存策略：分布式锁缓存、逻辑过期缓存、MQ更新缓存等
 *
 * @author 君乐
 * @description 针对表【product(商品信息表)】的数据库操作Service实现
 * @createDate 2025-10-27 11:02:23
 */
// 留存将来用
@Service
@Slf4j
public class ProductServiceImpl2 extends ServiceImpl<ProductMapper, Product> implements ProductService {

    // ==================== 常量定义 ====================

    /** 本地缓存实例 */
    private final CaffeineCacheUtil<String, String> localCache = new CaffeineCacheUtil<>(1024, 10000L);
    
    /** 缓存空值占位符（用于防止缓存穿透） */
    private static final String NULL_PLACEHOLDER = "NULL_PLACEHOLDER";
    
    /** 随机数生成器（用于生成随机过期时间） */
    private static final Random RANDOM = new Random();
    
    /** Redis缓存Key前缀 */
    private static final String REDIS_CACHE_PREFIX = "";
    
    /** 本地缓存Key前缀 */
    private static final String LOCAL_CACHE_PREFIX = "cache";
    
    /** 分布式锁前缀 - 商品详情查询 */
    private static final String LOCK_PRODUCT_DETAIL_PREFIX = "lock:product:detail:";
    
    /** 分布式锁前缀 - 缓存更新 */
    private static final String LOCK_PRODUCT_UPDATE_PREFIX = "lock:product:update:";
    
    /** 默认基础过期时间（秒） */
    private static final int BASE_EXPIRE_SECONDS = 300;
    
    /** 最大随机过期时间（秒） */
    private static final int MAX_RANDOM_EXPIRE_SECONDS = 300;
    
    /** 逻辑过期时间（5分钟，毫秒） */
    private static final long LOGIC_EXPIRE_MILLIS = 5 * 60 * 1000;
    
    /** 延时双删间隔时间（毫秒） */
    private static final long DELAY_DELETE_MILLIS = 3000;

    // ==================== 资源注入 ====================

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Resource
    private ThreadPoolTaskExecutor asyncTaskExecutor;


    // ==================== 基于分布式锁的缓存查询 ====================

    /**
     * 通过分布式锁实现的商品详情查询（防止缓存击穿）
     * 查询流程：本地缓存 -> Redis -> 数据库（加分布式锁控制并发）
     *
     * @param id 商品id
     * @return 商品信息
     */
    @Override
    public Product getProductDetailByRedisson(Long id) {
        // 合法性校验
        if (id == null || id < 0) {
            log.error("商品id非法: {}", id);
            return null;
        }

        // 构建缓存key
        String redisKey = buildRedisKey(id);
        String localCacheKey = buildLocalCacheKey(id);

        // 1. 查询本地缓存
        String cacheValue = localCache.getIfPresent(localCacheKey);
        if (StrUtil.isNotBlank(cacheValue)) {
            return handleCacheHit(localCacheKey, cacheValue);
        }

        // 2. 查询Redis缓存
        cacheValue = stringRedisTemplate.opsForValue().get(redisKey);
        if (StrUtil.isNotBlank(cacheValue)) {
            return handleCacheHit(localCacheKey, cacheValue);
        }

        // 3. 缓存未命中，通过分布式锁控制并发查库
        return getProductWithDistributedLock(id, redisKey, localCacheKey);
    }

    /**
     * 使用分布式锁查询数据库并更新缓存
     * 包含双重检查机制，避免重复查询数据库
     *
     * @param id 商品id
     * @param redisKey Redis缓存键
     * @param localCacheKey 本地缓存键
     * @return 商品信息
     */
    private Product getProductWithDistributedLock(Long id, String redisKey, String localCacheKey) {
        String lockKey = LOCK_PRODUCT_DETAIL_PREFIX + id;
        RLock lock = redissonClient.getLock(lockKey);
        Product product = null;

        try {
            // 尝试获取锁：最多等待100ms，10秒后自动释放
            boolean isLocked = lock.tryLock(100, 10, TimeUnit.SECONDS);
            if (isLocked) {
                // 双重检查：防止其他线程已更新缓存
                String doubleCheckValue = stringRedisTemplate.opsForValue().get(redisKey);
                if (StrUtil.isNotBlank(doubleCheckValue)) {
                    return handleCacheHit(localCacheKey, doubleCheckValue);
                }

                // 查询数据库
                product = this.getById(id);
                
                // 构建缓存值（空值处理）
                String cacheValue = (product != null) ? JSONUtil.toJsonStr(product) : NULL_PLACEHOLDER;
                
                // 回写缓存（随机过期时间避免缓存雪崩）
                int expireSeconds = BASE_EXPIRE_SECONDS + generateRandomExpireSeconds(MAX_RANDOM_EXPIRE_SECONDS);
                stringRedisTemplate.opsForValue().set(redisKey, cacheValue, expireSeconds, TimeUnit.SECONDS);
                localCache.put(localCacheKey, cacheValue);
            } else {
                // 未获取到锁，短暂休眠后重试
                Thread.sleep(50 + RANDOM.nextInt(51));
                return getProductDetailByRedisson(id);
            }
        } catch (InterruptedException e) {
            log.error("获取分布式锁或重试失败", e);
            Thread.currentThread().interrupt(); // 恢复中断状态
        } finally {
            // 释放锁（仅当前持有锁的线程可释放）
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }

        return product;
    }


    // ==================== 基于逻辑过期的缓存查询 ====================

    /**
     * 基于逻辑过期的商品详情查询（适用于热点数据，避免缓存击穿）
     * 缓存中存储数据和过期时间，过期后异步更新缓存
     *
     * @param id 商品id
     * @return 商品信息
     */
    @Override
    public Product getProductDetailByLogicExpire(Long id) {
        // 合法性校验
        if (id == null || id < 0) {
            log.error("商品id非法: {}", id);
            return null;
        }

        // 构建缓存key
        String redisKey = buildRedisKey(id);
        String localCacheKey = buildLocalCacheKey(id);

        // 1. 查询本地缓存
        String cacheValue = localCache.getIfPresent(localCacheKey);
        if (StrUtil.isNotBlank(cacheValue)) {
            return handleLogicCacheHit(localCacheKey, cacheValue);
        }

        // 2. 查询Redis缓存
        cacheValue = stringRedisTemplate.opsForValue().get(redisKey);
        if (StrUtil.isNotBlank(cacheValue)) {
            return handleLogicCacheHit(localCacheKey, cacheValue);
        }

        // 3. 缓存未命中，初始化逻辑过期缓存
        return initLogicExpireCache(id, redisKey, localCacheKey);
    }

    /**
     * 初始化逻辑过期缓存（首次查询或缓存被删除时）
     *
     * @param id 商品id
     * @param redisKey Redis缓存键
     * @param localCacheKey 本地缓存键
     * @return 商品信息
     */
    private Product initLogicExpireCache(Long id, String redisKey, String localCacheKey) {
        Product product = this.getById(id);
        
        if (product != null) {
            // 封装带逻辑过期时间的缓存数据
            LogicExpireCache cacheData = new LogicExpireCache();
            cacheData.setData(product);
            cacheData.setExpireTime(System.currentTimeMillis() + LOGIC_EXPIRE_MILLIS);
            
            String jsonValue = JSONUtil.toJsonStr(cacheData);
            stringRedisTemplate.opsForValue().set(redisKey, jsonValue); // Redis不设物理过期
            localCache.put(localCacheKey, jsonValue);
            return product;
        } else {
            // 缓存空值（设置物理过期）
            stringRedisTemplate.opsForValue().set(redisKey, NULL_PLACEHOLDER, 5, TimeUnit.MINUTES);
            localCache.put(localCacheKey, NULL_PLACEHOLDER);
            return null;
        }
    }

    /**
     * 处理逻辑过期缓存命中
     * 判断是否过期，过期则异步更新缓存
     *
     * @param cacheKey 缓存键
     * @param cacheValue 缓存值
     * @return 商品信息
     */
    private Product handleLogicCacheHit(String cacheKey, String cacheValue) {
        // 空值处理
        if (NULL_PLACEHOLDER.equals(cacheValue)) {
            int expireSeconds = 60 + generateRandomExpireSeconds(180);
            localCache.put(cacheKey, cacheValue, expireSeconds, TimeUnit.SECONDS);
            log.warn("缓存命中空值占位符");
            return null;
        }

        // 解析逻辑过期缓存数据
        LogicExpireCache cacheData = JSONUtil.toBean(cacheValue, LogicExpireCache.class);
        Product product = cacheData.getData();
        long expireTime = cacheData.getExpireTime();

        // 判断是否逻辑过期
        if (System.currentTimeMillis() < expireTime) {
            // 未过期：直接返回数据
            int expireSeconds = 60 + generateRandomExpireSeconds(180);
            localCache.put(cacheKey, cacheValue, expireSeconds, TimeUnit.SECONDS);
            return product;
        } else {
            // 已过期：异步更新缓存，返回旧数据
            asyncUpdateLogicCache(cacheKey, product.getId());
            return product;
        }
    }

    /**
     * 异步更新逻辑过期缓存
     * 加锁避免并发更新
     *
     * @param cacheKey 缓存键
     * @param id 商品id
     */
    @Async // 需要配合@EnableAsync使用
    public void asyncUpdateLogicCache(String cacheKey, Long id) {
        String redisKey = buildRedisKey(id);
        String lockKey = LOCK_PRODUCT_UPDATE_PREFIX + id;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // 尝试获取锁：最多等1秒，持有3秒
            boolean isLocked = lock.tryLock(1, 3, TimeUnit.SECONDS);
            if (isLocked) {
                // 查询最新数据
                Product newProduct = this.getById(id);
                if (newProduct != null) {
                    // 更新逻辑过期缓存
                    LogicExpireCache newCacheData = new LogicExpireCache();
                    newCacheData.setData(newProduct);
                    newCacheData.setExpireTime(System.currentTimeMillis() + LOGIC_EXPIRE_MILLIS);
                    
                    String newJsonValue = JSONUtil.toJsonStr(newCacheData);
                    stringRedisTemplate.opsForValue().set(redisKey, newJsonValue);
                    localCache.put(cacheKey, newJsonValue);
                } else {
                    // 数据已删除，缓存空值
                    stringRedisTemplate.opsForValue().set(redisKey, NULL_PLACEHOLDER, 5, TimeUnit.MINUTES);
                    localCache.put(cacheKey, NULL_PLACEHOLDER);
                }
            }
        } catch (InterruptedException e) {
            log.error("异步更新缓存失败", e);
            Thread.currentThread().interrupt();
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }


    // ==================== 商品更新与缓存同步 ====================

    /**
     * 通过MQ实现商品更新与缓存同步（延时双删策略）
     *
     * @param product 商品信息
     */
    @Override
    public void updateProductByMQ(Product product) {
        // 1. 更新数据库
        this.updateById(product);
        
        Long productId = product.getId();
        String redisKey = buildRedisKey(productId);
        String localCacheKey = buildLocalCacheKey(productId);

        // 2. 立即删除本地缓存
        localCache.invalidate(localCacheKey);
        log.info("本地缓存已删除: {}", localCacheKey);

        // 3. 立即删除Redis缓存
        Boolean deleteSuccess = stringRedisTemplate.delete(redisKey);
        if (deleteSuccess) {
            log.info("Redis缓存已删除: {}", redisKey);
        }

        // 4. 异步执行延时双删
        asyncTaskExecutor.execute(() -> {
            try {
                // 延迟一段时间后再次删除（确保缓存更新）
                TimeUnit.MILLISECONDS.sleep(DELAY_DELETE_MILLIS);
                
                boolean delayDeleted = stringRedisTemplate.delete(redisKey);
                if (delayDeleted) {
                    log.info("延时删除Redis缓存成功: {}", redisKey);
                } else {
                    log.warn("延时删除Redis缓存失败，发送MQ重试: {}", redisKey);
                    // 发送MQ重试删除
                    String routingKey = "product.cache.invalid.retry." + redisKey;
                    rabbitTemplate.convertAndSend(
                            RabbitMQConfig.PRODUCT_EXCHANGE,
                            routingKey,
                            redisKey
                    );
                }
            } catch (Exception e) {
                log.error("延时删除缓存发生异常", e);
            }
        });
    }

    /**
     * 通过Canal实现商品更新（数据库变更同步）
     *
     * @param product 商品信息
     */
    @Override
    public void updateProductByCanal(Product product) {
        // 更新数据库（由Canal监听binlog自动触发）
        this.updateById(product);
    }


    // ==================== 工具方法 ====================

    /**
     * 生成随机过期时间（秒）
     *
     * @param maxRandomSeconds 最大随机秒数
     * @return 0到maxRandomSeconds之间的随机数
     */
    private int generateRandomExpireSeconds(int maxRandomSeconds) {
        return (maxRandomSeconds < 0) ? 0 : RANDOM.nextInt(maxRandomSeconds + 1);
    }

    /**
     * 处理普通缓存命中
     *
     * @param cacheKey 缓存键
     * @param cacheValue 缓存值
     * @return 商品信息
     */
    private Product handleCacheHit(String cacheKey, String cacheValue) {
        if (NULL_PLACEHOLDER.equals(cacheValue)) {
            int expireSeconds = 60 + generateRandomExpireSeconds(180);
            localCache.put(cacheKey, cacheValue, expireSeconds, TimeUnit.SECONDS);
            log.warn("缓存命中空值占位符");
            return null;
        }

        Product product = JSONUtil.toBean(cacheValue, Product.class);
        int expireSeconds = 180 + generateRandomExpireSeconds(180);
        localCache.put(cacheKey, cacheValue, expireSeconds, TimeUnit.SECONDS);
        return product;
    }

    /**
     * 构建Redis缓存键
     *
     * @param id 商品id
     * @return 缓存键
     */
    private String buildRedisKey(Long id) {
        return REDIS_CACHE_PREFIX + DigestUtils.md5DigestAsHex(id.toString().getBytes());
    }

    /**
     * 构建本地缓存键
     *
     * @param id 商品id
     * @return 缓存键
     */
    private String buildLocalCacheKey(Long id) {
        return LOCAL_CACHE_PREFIX + DigestUtils.md5DigestAsHex(id.toString().getBytes());
    }


    /**
     * 带逻辑过期时间的缓存结构
     */
    @Data
    private static class LogicExpireCache {
        private Product data; // 实际业务数据
        private long expireTime; // 逻辑过期时间（毫秒时间戳）
    }
}