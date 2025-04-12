package cn.ecosync.aiot.data.kafka;

import cn.ecosync.aiot.data.EventBus;
import cn.ecosync.aiot.data.kafka.streams.KafkaStreamsConfiguration;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.stream.Stream;

import static cn.ecosync.aiot.data.prometheus.PrometheusUtils.TOPIC_PROMETHEUS;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "spring.kafka", name = "bootstrap-servers")
@Import({KafkaStreamsConfiguration.class})
public class KafkaConfiguration {
    @Bean
    public KafkaAdmin.NewTopics topics() {
        NewTopic[] newTopics = Stream.of(TOPIC_PROMETHEUS)
                .map(in -> TopicBuilder.name(in).build())
                .toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(newTopics);
    }

    @Bean
    @ConditionalOnMissingBean(EventBus.class)
    public KafkaEventBus kafkaEventBus(KafkaTemplate<String, byte[]> kafkaTemplate) {
        return new KafkaEventBus(kafkaTemplate);
    }
}
