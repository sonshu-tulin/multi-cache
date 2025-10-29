package top.xiaoyijun.multicache.rabbitmq;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import jakarta.annotation.Resource;

@Component
public class ProductCacheInvalidListener {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 监听缓存失效队列
    @RabbitListener(queues = RabbitMQConfig.PRODUCT_CACHE_QUEUE)
    public void handleCacheInvalid(String redisKey) {
        boolean deleted = stringRedisTemplate.delete(redisKey);
        if (deleted) {
            System.out.println("Redis缓存删除成功：" + redisKey);
        } else {
            // Redis 删除失败，抛出异常触发 MQ 重试
            throw new RuntimeException("Redis缓存删除失败，触发MQ重试：" + redisKey);
        }
        // 如果均重试失败，还可以将其放入死信队列中，此处不再赘述
    }

}