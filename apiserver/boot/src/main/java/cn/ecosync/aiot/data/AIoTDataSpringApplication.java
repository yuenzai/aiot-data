package cn.ecosync.aiot.data;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "cn.ecosync.aiot.data")
public class AIoTDataSpringApplication {
    public static void main(String[] args) {
        SpringApplication.run(AIoTDataSpringApplication.class, args);
    }
}
