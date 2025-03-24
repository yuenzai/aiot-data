package cn.ecosync.aiot.data.apiserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "cn.ecosync.aiot")
public class AIoTDataAPIServer {
    public static void main(String[] args) {
        SpringApplication.run(AIoTDataAPIServer.class, args);
    }
}
