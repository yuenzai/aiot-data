package cn.ecosync.aiot.data.apiserver.edge.gateway;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class EdgeGatewayConfiguration {
    @Bean
    public EdgeGatewayController edgeGatewayController(KafkaTemplate<String, byte[]> kafkaTemplate) {
        return new EdgeGatewayController(kafkaTemplate);
    }
}
