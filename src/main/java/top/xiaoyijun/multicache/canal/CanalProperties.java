package top.xiaoyijun.multicache.canal;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "canal")  // 对应YML中的canal前缀
public class CanalProperties {

    private Server server = new Server();
    private String destination;
    private String username;
    private String password;
    private Listen listen = new Listen();

    // 内部类：服务端地址和端口
    @Data
    public static class Server {
        private String host;
        private int port;
    }

    // 内部类：监听的数据库和表
    @Data
    public static class Listen {
        private String database;
        private String table;
    }
}