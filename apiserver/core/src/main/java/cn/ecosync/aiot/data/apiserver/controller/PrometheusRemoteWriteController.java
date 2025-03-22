package cn.ecosync.aiot.data.apiserver.controller;

import cn.ecosync.aiot.data.apiserver.api.prometheus.Request;
import cn.ecosync.aiot.data.apiserver.api.prometheus.Sample;
import cn.ecosync.aiot.data.apiserver.api.prometheus.TimeSeries;
import io.prometheus.metrics.model.snapshots.Labels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import static org.springframework.http.HttpStatus.*;

@RestController
@RequestMapping("/prometheus")
public class PrometheusRemoteWriteController {
    private static final Logger log = LoggerFactory.getLogger(PrometheusRemoteWriteController.class);
    public static final String TOPIC_PROMETHEUS_REMOTE_WRITE = "prometheus-remote-write";

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public PrometheusRemoteWriteController(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping(value = "/api/v1/write")
    public DeferredResult<ResponseEntity<Void>> onMessage(InputStream inputStream, @RequestHeader HttpHeaders requestHeaders) {
        log.atInfo().addKeyValue("requestHeaders", requestHeaders).log("");
        String gatewayCode = requestHeaders.getFirst("Gateway-Code");
        DeferredResult<ResponseEntity<Void>> deferredResult = new DeferredResult<>(null, () -> new ResponseEntity<>(INTERNAL_SERVER_ERROR));
        try {
            byte[] bytes = toByteArray(inputStream);
            byte[] uncompress = Snappy.uncompress(bytes);
            Request request = Request.parseFrom(uncompress);
            CompletableFuture<SendResult<String, byte[]>> completableFuture;
            if (StringUtils.hasText(gatewayCode)) {
                completableFuture = kafkaTemplate.send(TOPIC_PROMETHEUS_REMOTE_WRITE, gatewayCode, bytes);
            } else {
                completableFuture = kafkaTemplate.send(TOPIC_PROMETHEUS_REMOTE_WRITE, bytes);
            }
            completableFuture.whenComplete((result, ex) -> {
                if (ex == null) {
                    int sampleCount = handle(request);
                    MultiValueMap<String, String> responseHeaders = new LinkedMultiValueMap<>(1);
                    responseHeaders.set("X-Prometheus-Remote-Write-Samples-Written", String.valueOf(sampleCount));
                    deferredResult.setResult(new ResponseEntity<>(responseHeaders, NO_CONTENT));
                } else {
                    log.error("", ex);
                    deferredResult.setErrorResult(new ResponseEntity<>(INTERNAL_SERVER_ERROR));
                }
            });
        } catch (IOException e) {
            log.error("", e);
            deferredResult.setErrorResult(new ResponseEntity<>(BAD_REQUEST));
        }
        return deferredResult;
    }

    private int handle(Request request) {
        int sampleCount = 0;
        for (TimeSeries timeSeries : request.getTimeseriesList()) {
            sampleCount += handle(request, timeSeries);
        }
        return sampleCount;
    }

    private int handle(Request request, TimeSeries timeSeries) {
        int sampleCount = 0;
        String metricName = request.getSymbols(timeSeries.getLabelsRefs(1));
        String[] keyValuePairs = timeSeries.getLabelsRefsList().stream()
                .skip(2)
                .map(request::getSymbols)
                .toArray(String[]::new);
        Labels labels = Labels.of(keyValuePairs);
        log.atInfo().addKeyValue("metricName", metricName).addKeyValue("labels", labels).log("TimeSeries");
        for (Sample sample : timeSeries.getSamplesList()) {
            log.atInfo().addKeyValue("value", sample.getValue()).addKeyValue("timestamp", sample.getTimestamp()).log("Sample");
            sampleCount++;
        }
        return sampleCount;
    }

    private static byte[] toByteArray(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int len;
        byte[] data = new byte[1024];
        while ((len = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, len);
        }
        buffer.flush();
        return buffer.toByteArray();
    }
}
