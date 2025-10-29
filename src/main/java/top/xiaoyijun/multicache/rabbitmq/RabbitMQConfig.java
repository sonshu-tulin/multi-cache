package top.xiaoyijun.multicache.rabbitmq;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 配置 RabbitMQ
 */
@Configuration
public class RabbitMQConfig {
    // 交换机名称（商品数据变更）
    public static final String PRODUCT_EXCHANGE = "product.exchange";
    // 队列名称（缓存失效通知）
    public static final String PRODUCT_CACHE_QUEUE = "product.cache.queue";
    // 路由键（匹配商品ID的变更通知）
    public static final String PRODUCT_CACHE_ROUTING_KEY = "product.cache.invalid.#";

    // 创建交换机
    @Bean
    public TopicExchange productExchange() {
        return new TopicExchange(PRODUCT_EXCHANGE, true, false);
    }

    // 创建队列（持久化）
    @Bean
    public Queue productCacheQueue() {
        return new Queue(PRODUCT_CACHE_QUEUE, true, false, false);
    }

    // 绑定交换机和队列
    @Bean
    public Binding bindingProductCacheQueue(TopicExchange productExchange, Queue productCacheQueue) {
        return BindingBuilder.bind(productCacheQueue).to(productExchange).with(PRODUCT_CACHE_ROUTING_KEY);
    }
}