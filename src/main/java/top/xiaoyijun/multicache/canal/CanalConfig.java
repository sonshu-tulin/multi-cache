package top.xiaoyijun.multicache.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import jakarta.annotation.Resource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

@Configuration
public class CanalConfig {

    @Resource
    private CanalProperties canalProperties;

    @Bean
    public CanalConnector canalConnector() {
        // 从配置类中获取参数
        String host = canalProperties.getServer().getHost();
        int port = canalProperties.getServer().getPort();
        String destination = canalProperties.getDestination();
        String username = canalProperties.getUsername();
        String password = canalProperties.getPassword();

        // Canal 客户端的用户名/密码用于 Canal 服务端鉴权（通常默认不开启）。
        // 若未在 Canal 服务端开启鉴权，应当传 null，否则会因为鉴权失败而无法连接。

        // 创建Canal连接
        return CanalConnectors.newSingleConnector(
                new InetSocketAddress(host, port),
                destination,
                null,
                null
        );
    }
}