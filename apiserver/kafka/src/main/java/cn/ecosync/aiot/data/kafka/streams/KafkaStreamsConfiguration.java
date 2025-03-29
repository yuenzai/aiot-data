package cn.ecosync.aiot.data.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import static org.apache.kafka.common.serialization.Serdes.String;

@EnableKafkaStreams
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(KafkaStreams.class)
@ConditionalOnProperty(prefix = "spring.kafka.streams", name = "bootstrap-servers")
public class KafkaStreamsConfiguration {
    @Bean
    public PrometheusKafkaStreams prometheusKafkaStreams(StreamsBuilder streamsBuilder) {
        return new PrometheusKafkaStreams(streamsBuilder);
    }

    private <V> void addGlobalStore(StreamsBuilder builder, String topic, String storeName, Serde<V> valueSerde, Topology.AutoOffsetReset resetPolicy) {
        addGlobalStore(builder, topic, storeName, String(), valueSerde, resetPolicy);
    }

    private <K, V> void addGlobalStore(StreamsBuilder builder, String topic, String storeName, Serde<K> keySerde, Serde<V> valueSerde, Topology.AutoOffsetReset resetPolicy) {
        KeyValueBytesStoreSupplier keyValueStoreSupplier = Stores.persistentKeyValueStore(storeName);
        StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores
                .keyValueStoreBuilder(keyValueStoreSupplier, keySerde, valueSerde);
        Consumed<K, V> consumed = Consumed.with(keySerde, valueSerde)
                .withOffsetResetPolicy(resetPolicy);
        builder.addGlobalStore(
                storeBuilder,
                topic,
                consumed,
                () -> new GlobalStoreUpdater<>(storeName)
        );
    }

    private void addStore(StreamsBuilder builder, String storeName, Serde<?> valueSerde) {
        KeyValueBytesStoreSupplier keyValueStoreSupplier = Stores.persistentKeyValueStore(storeName);
        builder.addStateStore(Stores.keyValueStoreBuilder(keyValueStoreSupplier, String(), valueSerde));
    }

    // Processor that keeps the global store updated.
    // https://github.com/confluentinc/kafka-streams-examples/blob/master/src/main/java/io/confluent/examples/streams/GlobalStoresExample.java
    public static class GlobalStoreUpdater<K, V> implements Processor<K, V, Void, Void> {
        public static final Logger log = LoggerFactory.getLogger(GlobalStoreUpdater.class);

        private final String storeName;
        private KeyValueStore<K, V> store;

        public GlobalStoreUpdater(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(final ProcessorContext<Void, Void> processorContext) {
            store = processorContext.getStateStore(storeName);
        }

        @Override
        public void process(final Record<K, V> record) {
            store.put(record.key(), record.value());
            log.info("globalStateStore更新[storeName={}, record={}]", storeName, record);
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
