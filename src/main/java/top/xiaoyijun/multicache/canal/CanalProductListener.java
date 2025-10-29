package top.xiaoyijun.multicache.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;
import top.xiaoyijun.multicache.rabbitmq.RabbitMQConfig;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class CanalProductListener {

    // 本地缓存（Caffeine），假设已定义为全局常量
    public static final com.github.benmanes.caffeine.cache.Cache<String, Object> LOCAL_CACHE = 
            com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build();

    @Resource
    private CanalConnector canalConnector;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CanalProperties canalProperties;

    @Resource
    private RabbitTemplate rabbitTemplate;

    // 线程池处理Canal事件（避免阻塞）
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    // 记录应用启动时间，用于过滤历史事件
    private final long appStartTimeMillis = System.currentTimeMillis();

    // 初始化时启动监听
    @PostConstruct
    public void startListener() {
        executor.submit(() -> {
            // 持续运行，异常后自动重连
            while (true) {
                try {
                    // 建立连接
                    canalConnector.connect();
                    System.out.println("Canal连接成功");

                    // 从配置中获取监听的数据库和表（动态生成订阅表达式）
                    String database = canalProperties.getListen().getDatabase();
                    String table = canalProperties.getListen().getTable();
                    String subscribeTable = database + "\\." + table;  // 如：your_db.product
                    canalConnector.subscribe(subscribeTable);  // 订阅指定表
                    canalConnector.rollback();

                    // 拉取并处理消息
                    while (true) {
                        Message message = canalConnector.getWithoutAck(100, 5000L, TimeUnit.MILLISECONDS);
                        long batchId = message.getId();
                        int size = message.getEntries().size();

                        if (batchId == -1 || size == 0) {
                            Thread.sleep(1000);
                            continue;
                        }

                        handleEntries(message.getEntries());
                        canalConnector.ack(batchId);
                    }

                } catch (Exception e) {
                    // 打印完整堆栈，便于定位 Read timed out 等问题
                    e.printStackTrace();
                    System.out.println("Canal连接失败或中断，5秒后重试...");
                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException ignored) {
                    }
                } finally {
                    try {
                        canalConnector.disconnect();
                    } catch (Exception ignored) {
                    }
                }
            }
        });
    }


    //处理Canal消息条目
    private void handleEntries(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            // 过滤非事务日志类型
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            // 忽略应用启动前产生的历史事件，避免冷启动时清空缓存
            if (entry.getHeader() != null && entry.getHeader().getExecuteTime() > 0
                    && entry.getHeader().getExecuteTime() < appStartTimeMillis) {
                continue;
            }

            try {
                // 解析binlog日志
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                CanalEntry.EventType eventType = rowChange.getEventType();

                // 只处理UPDATE事件（如果需要处理新增/删除，可添加EventType.INSERT/DELETE）
                if (eventType != CanalEntry.EventType.UPDATE) {
                    continue;
                }

                // 处理每行数据的变更
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    handleProductUpdate(rowData.getAfterColumnsList()); // 取更新后的数据
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    // 处理商品更新事件，删除对应缓存
    private void handleProductUpdate(List<CanalEntry.Column> afterColumns) {
        // 从变更数据中获取productId（假设表的主键为id）
        Long productId = null;
        for (CanalEntry.Column column : afterColumns) {
            if ("id".equals(column.getName())) { // 匹配主键字段名
                productId = Long.parseLong(column.getValue());
                break;
            }
        }

        if (productId == null) {
            System.out.println("未找到productId，跳过缓存删除");
            return;
        }

        // 构建缓存Key（与原逻辑保持一致）
        String redisKey = DigestUtils.md5DigestAsHex(productId.toString().getBytes());
        String cacheKey = "cache" + redisKey;

        // 1. 删除本地缓存Caffeine
        LOCAL_CACHE.invalidate(cacheKey);
        System.out.println("Canal触发本地缓存删除：" + cacheKey);

        // 2. 删除Redis缓存
        Boolean delete = stringRedisTemplate.delete(redisKey);
        if (delete) {
            System.out.println("Canal触发Redis缓存删除：" + redisKey);
        } else {
            System.out.println("Redis缓存不存在或已删除：" + redisKey);
        }

        // 3. （可选）保留延时双删逻辑，进一步确保缓存一致性
        asyncDelayDelete(redisKey);
    }


    // 异步延时删除（与原逻辑一致，可选）
    private void asyncDelayDelete(String redisKey) {
        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                TimeUnit.SECONDS.sleep(3); // 注意：原代码写的3000秒，这里修正为3秒（根据业务调整）
                boolean delayDeleted = stringRedisTemplate.delete(redisKey);
                if (delayDeleted) {
                    System.out.println("Canal延时删除Redis缓存成功：" + redisKey);
                } else {
                    System.out.println("Canal延时删除Redis缓存失败，放入MQ重试：" + redisKey);
                    // 放入MQ时携带时间戳头，供消费者过滤启动前的旧消息
                    String routingKey = "product.cache.invalid.retry." + redisKey;
                    rabbitTemplate.convertAndSend(
                            RabbitMQConfig.PRODUCT_EXCHANGE,
                            routingKey,
                            redisKey,
                            message -> {
                                message.getMessageProperties().setHeader("ts", System.currentTimeMillis());
                                return message;
                            }
                    );
                }
            } catch (Exception e) {
                System.err.println("Canal延时删除异常：" + e.getMessage());
            }
        });
    }
}