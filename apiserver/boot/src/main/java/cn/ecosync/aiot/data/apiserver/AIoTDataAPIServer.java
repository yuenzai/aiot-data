package cn.ecosync.aiot.data.apiserver;

import cn.ecosync.aiot.data.apiserver.command.CommandBus;
import cn.ecosync.aiot.data.apiserver.command.CommandHandler;
import cn.ecosync.aiot.data.apiserver.query.QueryBus;
import cn.ecosync.aiot.data.apiserver.query.QueryHandler;
import cn.ecosync.aiot.data.apiserver.serde.JsonSerde;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.List;

@SpringBootApplication(scanBasePackages = "cn.ecosync.aiot")
public class AIoTDataAPIServer {
    @Bean
    public JsonSerde jsonSerde(ObjectMapper objectMapper) {
        return new JsonSerde(objectMapper);
    }

    @Bean
    public CommandBus commandBus(List<CommandHandler<?>> commandHandlers) {
        return new CommandBus(commandHandlers);
    }

    @Bean
    public QueryBus queryBus(List<QueryHandler<?, ?>> queryHandlers) {
        return new QueryBus(queryHandlers);
    }

    public static void main(String[] args) {
        SpringApplication.run(AIoTDataAPIServer.class, args);
    }
}
