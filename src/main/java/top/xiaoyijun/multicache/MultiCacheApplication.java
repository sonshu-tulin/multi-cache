package top.xiaoyijun.multicache;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
@MapperScan("top.xiaoyijun.multicache.mapper") // 扫描 Mapper 所在包
public class MultiCacheApplication {

    public static void main(String[] args) {
        SpringApplication.run(MultiCacheApplication.class, args);
    }

}
