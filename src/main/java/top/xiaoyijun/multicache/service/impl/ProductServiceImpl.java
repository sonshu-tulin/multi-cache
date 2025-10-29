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
 * @author 君乐
 * @description 针对表【product(商品信息表)】的数据库操作Service实现
 * @createDate 2025-10-27 11:02:23
 */
@Service
@Slf4j
public class ProductServiceImpl extends ServiceImpl<ProductMapper, Product> implements ProductService {

    // 1. 创建缓存
    // 引入 Redis
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 使用自定义本地缓存工具类创建本地缓存
    public final CaffeineCacheUtil<String, String> LOCAL_CACHE = new CaffeineCacheUtil<>(1024, 10000L);

    // 空值
    private static final String NULL_PLACEHOLDER = "NULL_PLACEHOLDER";

    private static final Random RANDOM = new Random();

    // 注入 Redisson 客户端
    @Resource
    private RedissonClient redissonClient;

    /**
     * 通过分布式锁
     * @param id 商品 id
     * @return
     */
    @Override
    public Product getProductDetailByRedisson(Long id) {

        // 1. 合法性检查
        if (id == null || id < 0) {
            log.error("id:{}，非法", id);
            return null;
        }

        // 2. 构建缓存key
        String redisKey = DigestUtils.md5DigestAsHex(id.toString().getBytes());
        String cacheKey = "cache" + redisKey;

        // 3. 从本地缓存中查询
        String cacheValue = LOCAL_CACHE.getIfPresent(cacheKey);
        if (StrUtil.isNotBlank(cacheValue)) {
            return handleCacheHit(cacheKey, cacheValue);
        }

        // 4. 从 Redis 中查询
        cacheValue = stringRedisTemplate.opsForValue().get(redisKey);
        if (StrUtil.isNotBlank(cacheValue)) {
            return handleCacheHit(cacheKey, cacheValue);
        }

        // 5. 缓存未命中，通过 Redisson 获取分布式锁，限制并发查库
        Product product = getProductByRedisson(id, redisKey, cacheKey);

        return product;
    }


    /**
     * 通过分布式锁查询数据库
     * @param id 查询条件
     */
    private Product getProductByRedisson(Long id, String redisKey, String cacheKey) {
        String cacheValue;
        Product product = null;
        // 定义锁的 key（建议加上业务前缀，避免冲突）
        String lockKey = "lock:product:detail:" + id;
        // 获取锁对象
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // 尝试获取锁：最多等待 100ms，10秒后自动释放（防止死锁）
            // 注意：Redisson 的看门狗机制会自动续期，只要线程未释放锁且未宕机，锁不会过期
            boolean isLocked = lock.tryLock(100, 10, TimeUnit.SECONDS);
            if (isLocked) {
                // 成功获取锁后，再次检查 Redis 缓存（避免其他线程已重建缓存）
                String doubleCheckValue = stringRedisTemplate.opsForValue().get(redisKey);
                if (StrUtil.isNotBlank(doubleCheckValue)) {
                    return handleCacheHit(cacheKey, doubleCheckValue);
                }

                // 真正查询数据库
                product = this.getById(id);

                // 7. 回写缓存（设置随机过期时间，避免缓存雪崩）
                if (product != null) {
                    cacheValue = JSONUtil.toJsonStr(product);
                } else {
                    // 缓存空值，避免缓存穿透
                    cacheValue = NULL_PLACEHOLDER;
                }

                // 过期时间设置为
                int randomExpire = 300 + generateRandomExpireSeconds(300);
                stringRedisTemplate.opsForValue().set(redisKey, cacheValue, randomExpire, TimeUnit.SECONDS);
                // 回写本地缓存
                LOCAL_CACHE.put(cacheKey, cacheValue);
            } else {
                // 未获取到锁，休眠 50-100ms 后重试（避免频繁重试）
                Thread.sleep(50 + new Random().nextInt(51));
                return getProductDetailByRedisson(id);
            }
        } catch (InterruptedException e) {
            log.error("获取锁或重试失败", e);
            Thread.currentThread().interrupt(); // 恢复中断状态
            return null;
        } finally {
            // 释放锁（只有持有锁的线程才能释放）
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }

        return product;
    }

    /**
     * 生成随机过期时间（秒）
     * @param maxRandomSeconds 随机数的最大范围（秒），必须为非负数
     * @return 0 到 maxRandomSeconds（包含）之间的随机秒数
     */
    public static int generateRandomExpireSeconds(int maxRandomSeconds) {
        // 校验参数：若传入负数，默认返回0（避免异常）
        if (maxRandomSeconds < 0) {
            return 0;
        }
        // 生成 0 到 maxRandomSeconds（包含）的随机整数
        return RANDOM.nextInt(maxRandomSeconds + 1);
    }


    // 封装缓存命中处理逻辑（简化代码）
    private Product handleCacheHit(String cacheKey, String cacheValue) {
        if (NULL_PLACEHOLDER.equals(cacheValue)) {
            LOCAL_CACHE.put(cacheKey, cacheValue, 60 + generateRandomExpireSeconds(180),TimeUnit.SECONDS);
            log.warn("该值为空");
            return null;
        }
        Product product = JSONUtil.toBean(cacheValue, Product.class);

        LOCAL_CACHE.put(cacheKey, cacheValue, 180 + generateRandomExpireSeconds(180),TimeUnit.SECONDS);
        return product;
    }



    // 定义带逻辑过期时间的缓存结构
    @Data
    private static class LogicExpireCache {
        private Product data; // 实际业务数据
        private long expireTime; // 逻辑过期时间（毫秒时间戳）
    }

    @Override
    public Product getProductDetailByLogicExpire(Long id) {

        // 1. 合法性检查
        if (id == null || id < 0) {
            log.error("id:{}，非法", id);
            return null;
        }

        // 2. 构建缓存key
        String redisKey = DigestUtils.md5DigestAsHex(id.toString().getBytes());
        String cacheKey = "cache" + redisKey;

        // 3. 从本地缓存中查询（核心：处理逻辑过期）
        String cacheValue = LOCAL_CACHE.getIfPresent(cacheKey);
        if (StrUtil.isNotBlank(cacheValue)) {
            return handleLogicCacheHit(cacheKey, cacheValue);
        }

        // 4. 从 Redis 中查询（核心：处理逻辑过期）
        cacheValue = stringRedisTemplate.opsForValue().get(redisKey);
        if (StrUtil.isNotBlank(cacheValue)) {
            return handleLogicCacheHit(cacheKey, cacheValue);
        }

        // 5. Redis缓存未命中（首次查询或缓存被意外删除）
        // 直接查库并初始化逻辑过期缓存（无需加锁，首次查询压力低）
        Product product = this.getById(id);
        if (product != null) {
            // 封装带逻辑过期时间的缓存数据（设置5分钟后逻辑过期）
            LogicExpireCache cacheData = new LogicExpireCache();
            cacheData.setData(product);
            cacheData.setExpireTime(System.currentTimeMillis() + 5 * 60 * 1000); // 5分钟后过期
            String jsonValue = JSONUtil.toJsonStr(cacheData);
            // Redis存储时不设置物理过期（永不过期）
            stringRedisTemplate.opsForValue().set(redisKey, jsonValue);
            // 回写本地缓存
            LOCAL_CACHE.put(cacheKey, jsonValue);
            return product;
        } else {
            // 缓存空值（避免缓存穿透，设置物理过期，防止长期占用空间）
            stringRedisTemplate.opsForValue().set(redisKey, NULL_PLACEHOLDER, 5, TimeUnit.MINUTES);
            LOCAL_CACHE.put(cacheKey, NULL_PLACEHOLDER);
            return null;
        }
    }

    // 处理缓存命中逻辑（核心：判断逻辑过期并触发异步更新）
    private Product handleLogicCacheHit(String cacheKey, String cacheValue) {
        // 空值处理（保持不变）
        if (NULL_PLACEHOLDER.equals(cacheValue)) {
            LOCAL_CACHE.put(cacheKey, cacheValue, 60 + generateRandomExpireSeconds(180),TimeUnit.SECONDS);
            log.warn("该值为空");
            return null;
        }

        // 解析带逻辑过期时间的缓存数据
        LogicExpireCache cacheData = JSONUtil.toBean(cacheValue, LogicExpireCache.class);
        Product product = cacheData.getData();
        long expireTime = cacheData.getExpireTime();

        // 判断是否逻辑过期
        if (System.currentTimeMillis() < expireTime) {
            // 未过期：直接返回数据，回写本地缓存
            LOCAL_CACHE.put(cacheKey, cacheValue, 60 + generateRandomExpireSeconds(180),TimeUnit.SECONDS);
            return product;
        } else {
            // 已过期：不返回旧数据，异步更新缓存（不阻塞当前请求）
            asyncUpdateLogicCache(cacheKey, cacheData.getData().getId()); // 异步更新
            return product; // 先返回旧数据，保证响应速度
        }
    }

    // 异步更新逻辑过期缓存（加简单锁避免并发更新）
    @Async // 需要开启@EnableAsync注解
    public void asyncUpdateLogicCache(String cacheKey, Long id) {
        String redisKey = DigestUtils.md5DigestAsHex(id.toString().getBytes());
        String lockKey = "lock:product:update:" + id; // 异步更新的锁

        RLock lock = redissonClient.getLock(lockKey);
        try {
            // 尝试获取锁，最多等1秒，持有3秒（防止更新逻辑卡住）
            boolean isLocked = lock.tryLock(1, 3, TimeUnit.SECONDS);
            if (isLocked) {
                // 查询最新数据
                Product newProduct = this.getById(id);
                if (newProduct != null) {
                    // 生成新的逻辑过期时间（续5分钟）
                    LogicExpireCache newCacheData = new LogicExpireCache();
                    newCacheData.setData(newProduct);
                    newCacheData.setExpireTime(System.currentTimeMillis() + 5 * 60 * 1000);
                    String newJsonValue = JSONUtil.toJsonStr(newCacheData);
                    // 更新Redis和本地缓存
                    stringRedisTemplate.opsForValue().set(redisKey, newJsonValue);
                    LOCAL_CACHE.put(cacheKey, newJsonValue);
                } else {
                    // 数据已删除，缓存空值（物理过期）
                    stringRedisTemplate.opsForValue().set(redisKey, NULL_PLACEHOLDER, 5, TimeUnit.MINUTES);
                    LOCAL_CACHE.put(cacheKey, NULL_PLACEHOLDER);
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


    // 引入 RabbitMQ
    @Resource
    private RabbitTemplate rabbitTemplate;

    // 注入异步线程池（避免使用主线程）
    @Resource
    private ThreadPoolTaskExecutor asyncTaskExecutor;

    @Override
    public void updateProductByMQ(Product product) {
        // 1. 更新数据库
        this.updateById(product);

        // 2. 构建缓存Key（与查询时保持一致）
        Long productId = product.getId();
        String redisKey = DigestUtils.md5DigestAsHex(productId.toString().getBytes());
        String cacheKey = "cache" + redisKey;

        // 3. 删除 Caffeine
        LOCAL_CACHE.invalidate(cacheKey);
        System.out.println("本地缓存已删除：" + cacheKey);

        // 4. 删除Redis缓存
        Boolean delete = stringRedisTemplate.delete(redisKey);
        if (delete) {
            System.out.println("Redis缓存已删除：" + redisKey);
        }

        // 5. 异步延时双删除
        asyncTaskExecutor.execute(() -> {
            try {
                TimeUnit.SECONDS.sleep(3000);
                boolean delayDeleted = stringRedisTemplate.delete(redisKey);
                if (delayDeleted) {
                    System.out.println("延时删除Redis缓存成功：" + redisKey);
                } else {
                    System.out.println("延时删除Redis缓存失败，准备放入MQ重试：" + redisKey);
                    // 若延时删除仍失败，放入MQ重试（确保最终一致性）
                    String routingKey = "product.cache.invalid.retry." + redisKey;
                    rabbitTemplate.convertAndSend(
                            RabbitMQConfig.PRODUCT_EXCHANGE, // 交换机
                            routingKey, // 路由键
                            redisKey // 消息
                    );
                }
            } catch (Exception e) {
                System.err.println("延时删除发生异常：" + e.getMessage());
            }
        });

    }

    @Override
    public void updateProductByCanal(Product product) {
        // 1. 更新数据库
        this.updateById(product);
    }

}




