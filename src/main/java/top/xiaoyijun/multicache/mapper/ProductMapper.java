package top.xiaoyijun.multicache.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import top.xiaoyijun.multicache.model.Product;

/**
* @author 君乐
* @description 针对表【product(商品信息表)】的数据库操作Mapper
* @createDate 2025-10-27 11:02:24
*/
@Mapper
public interface ProductMapper extends BaseMapper<Product> {

}




