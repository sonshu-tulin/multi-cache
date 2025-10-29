-- 商品表：存储商品基本信息（ID、名称、价格、库存等）
CREATE TABLE IF NOT EXISTS product (
   id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '商品ID（主键）',
   name VARCHAR(255) NOT NULL COMMENT '商品名称',
   price DECIMAL(10, 2) NOT NULL COMMENT '商品售价（精确到分）',
   stock INT NOT NULL DEFAULT 0 COMMENT '商品库存数量',
   update_time BIGINT NOT NULL COMMENT '最后更新时间（时间戳，用于版本控制）',
   create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (id),
   KEY idx_update_time (update_time) COMMENT '索引：按更新时间查询（优化缓存同步场景）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品信息表';