package cn.ecosync.aiot.data.apiserver.edge.gateway.kafka;

import cn.ecosync.aiot.data.apiserver.api.prometheus.Request;
import cn.ecosync.aiot.data.apiserver.edge.gateway.PrometheusRequestDecoder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static cn.ecosync.aiot.data.apiserver.edge.gateway.EdgeGatewayConstants.TOPIC_AIOT_EDGE_GATEWAY_PROMETHEUS;
import static org.apache.kafka.common.serialization.Serdes.ByteArray;
import static org.apache.kafka.common.serialization.Serdes.String;

public class EdgeGatewayKafkaStreams implements InitializingBean {
    public static final Logger log = LoggerFactory.getLogger(EdgeGatewayKafkaStreams.class);

    private final StreamsBuilder streamsBuilder;

    public EdgeGatewayKafkaStreams(StreamsBuilder streamsBuilder) {
        this.streamsBuilder = streamsBuilder;
    }

    @Override
    public void afterPropertiesSet() {
        try {
            afterPropertiesSetImpl();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private void afterPropertiesSetImpl() {
        streamsBuilder.stream(TOPIC_AIOT_EDGE_GATEWAY_PROMETHEUS, Consumed.with(String(), ByteArray()))
                .mapValues(this::toRequest)
                .filter((key, value) -> value != null)
                .flatMapValues(this::flatToTimeSeries)
                .foreach(this::logging);
    }

    private Request toRequest(byte[] bytes) {
        try {
            return PrometheusRequestDecoder.toRequest(bytes);
        } catch (IOException e) {
            log.atError().setCause(e).log("");
            return null;
        }
    }

    private Iterable<String[]> flatToTimeSeries(Request request) {
        return request.getTimeseriesList().stream()
                .map(in -> in.getLabelsRefsList().stream().map(request::getSymbols).toArray(String[]::new))
                .collect(Collectors.toList());
    }

    private void logging(String key, String[] labels) {
        log.atInfo().addKeyValue("key", key).addKeyValue("labels", Arrays.toString(labels)).log("TimeSeries");
    }
}
