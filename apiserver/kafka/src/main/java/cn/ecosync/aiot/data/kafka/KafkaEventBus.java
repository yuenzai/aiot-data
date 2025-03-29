package cn.ecosync.aiot.data.kafka;

import cn.ecosync.aiot.data.EventBus;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

public class KafkaEventBus implements EventBus {
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public KafkaEventBus(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public CompletableFuture<?> send(String topic, String key, byte[] data) {
        return kafkaTemplate.send(topic, key, data);
    }
}
