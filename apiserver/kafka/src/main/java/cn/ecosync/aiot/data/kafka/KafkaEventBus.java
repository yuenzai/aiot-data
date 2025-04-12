package cn.ecosync.aiot.data.kafka;

import cn.ecosync.aiot.data.EventBus;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class KafkaEventBus implements EventBus {
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public KafkaEventBus(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public CompletableFuture<?> send(String topic, String key, byte[] data, Map<String, String> headers) {
        RecordHeaders recordHeaders = new RecordHeaders();
        headers.forEach((k, v) -> recordHeaders.add(k, v.getBytes(StandardCharsets.UTF_8)));
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, null, key, data, recordHeaders);
        return kafkaTemplate.send(record);
    }
}
