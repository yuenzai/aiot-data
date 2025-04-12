package cn.ecosync.aiot.data.kafka.streams;

import cn.ecosync.aiot.data.prometheus.api.Request;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static cn.ecosync.aiot.data.prometheus.PrometheusUtils.TOPIC_PROMETHEUS;
import static org.apache.kafka.common.serialization.Serdes.ByteArray;
import static org.apache.kafka.common.serialization.Serdes.String;

public class PrometheusKafkaStreams implements InitializingBean {
    public static final Logger log = LoggerFactory.getLogger(PrometheusKafkaStreams.class);

    private final StreamsBuilder streamsBuilder;

    public PrometheusKafkaStreams(StreamsBuilder streamsBuilder) {
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
        streamsBuilder.stream(TOPIC_PROMETHEUS, Consumed.with(String(), ByteArray()))
                .mapValues(this::toRequest)
                .filter((key, value) -> value != null)
                .flatMapValues(this::flatToTimeSeries)
                .foreach(this::logging);
    }

    private Request toRequest(byte[] bytes) {
        try {
            byte[] uncompress = Snappy.uncompress(bytes);
            return Request.parseFrom(uncompress);
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
