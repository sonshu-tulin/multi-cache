package top.xiaoyijun.multicache.controller;

import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.*;
import top.xiaoyijun.multicache.model.Product;
import top.xiaoyijun.multicache.service.ProductService;


@RestController
@RequestMapping("/products")
public class ProductController {

    @Resource
    private ProductService productService;

    /**
     * 获取商品信息
     * @param id 商品 id
     * @return 商品
     */
    @GetMapping("/{id}")
    public Product getProduct(@PathVariable Long id) {
        //return productService.getProductDetailByRedisson(id);
        return productService.getProductDetailByLogicExpire(id);
    }

    /**
     * 修改商品信息
     * @param product 商品
     */
    @PostMapping("/update")
    public void updateProduct( @RequestBody Product product){
        // 通过 MQ 保证
        //productService.updateProductByMQ(product);

        // 通过 canal 保证
        productService.updateProductByCanal(product);
    }


}