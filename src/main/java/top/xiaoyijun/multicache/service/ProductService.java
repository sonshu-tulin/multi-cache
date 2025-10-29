package top.xiaoyijun.multicache.service;

import com.baomidou.mybatisplus.extension.service.IService;
import top.xiaoyijun.multicache.model.Product;

/**
* @author 君乐
* @description 针对表【product(商品信息表)】的数据库操作Service
* @createDate 2025-10-27 11:02:24
*/
public interface ProductService extends IService<Product> {

    /**
     * 获取商品信息(分布式锁)
     * @param id 商品 id
     * @return 商品
     */
    Product getProductDetailByRedisson(Long id);


    /**
     * 获取商品信息(逻辑过期)
     * @param id 商品 id
     * @return 商品
     */
    Product getProductDetailByLogicExpire(Long id);

    /**
     * 修改商品信息(MQ)
     * @param product 商品
     */
    void updateProductByMQ(Product product);

    /**
     * 修改商品信息(Canal)
     * @param product 商品
     */
    void updateProductByCanal(Product product);
}
