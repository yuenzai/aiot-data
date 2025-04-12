package cn.ecosync.aiot.data;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * @author yan
 * @since 2024
 */
public interface EventBus {
    default CompletableFuture<?> send(String topic, String key, String data) {
        return send(topic, key, data, null);
    }

    default CompletableFuture<?> send(String topic, String key, byte[] data) {
        return send(topic, key, data, null);
    }

    default CompletableFuture<?> send(String topic, String key, String data, Map<String, String> headers) {
        byte[] bytes = Optional.ofNullable(data)
                .map(in -> in.getBytes(StandardCharsets.UTF_8))
                .orElse(null);
        return send(topic, key, bytes, headers);
    }

    CompletableFuture<?> send(String topic, String key, byte[] data, Map<String, String> headers);
}
