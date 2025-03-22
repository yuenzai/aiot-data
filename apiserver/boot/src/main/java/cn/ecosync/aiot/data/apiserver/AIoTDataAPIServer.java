package cn.ecosync.aiot.data.apiserver;

import cn.ecosync.aiot.data.apiserver.command.CommandBus;
import cn.ecosync.aiot.data.apiserver.command.CommandHandler;
import cn.ecosync.aiot.data.apiserver.query.QueryBus;
import cn.ecosync.aiot.data.apiserver.query.QueryHandler;
import cn.ecosync.aiot.data.apiserver.serde.JsonSerde;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.List;

@SpringBootApplication(scanBasePackages = "cn.ecosync.aiot")
public class AIoTDataAPIServer {
    @Bean
    @ConditionalOnMissingBean(JsonSerde.class)
    public JsonSerde jsonSerde(ObjectMapper objectMapper) {
        return new JsonSerde(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean(CommandBus.class)
    public CommandBus commandBus(List<CommandHandler<?>> commandHandlers) {
        return new CommandBus(commandHandlers);
    }

    @Bean
    @ConditionalOnMissingBean(QueryBus.class)
    public QueryBus queryBus(List<QueryHandler<?, ?>> queryHandlers) {
        return new QueryBus(queryHandlers);
    }

    public static void main(String[] args) {
        SpringApplication.run(AIoTDataAPIServer.class, args);
    }
}
