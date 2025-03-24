package cn.ecosync.aiot.data.apiserver.edge.gateway;

import cn.ecosync.aiot.data.apiserver.api.prometheus.Request;
import cn.ecosync.aiot.data.apiserver.api.prometheus.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;

import static cn.ecosync.aiot.data.apiserver.edge.gateway.EdgeGatewayConstants.*;
import static org.springframework.http.HttpStatus.*;

@RestController
@RequestMapping("/edge/gateway")
public class EdgeGatewayRestController {
    private static final Logger log = LoggerFactory.getLogger(EdgeGatewayRestController.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public EdgeGatewayRestController(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping(value = "/prometheus/api/v1/write")
    public DeferredResult<ResponseEntity<Void>> onMessage(InputStream inputStream, @RequestHeader HttpHeaders requestHeaders) {
        DeferredResult<ResponseEntity<Void>> deferredResult = new DeferredResult<>(null, () -> new ResponseEntity<>(INTERNAL_SERVER_ERROR));

        String gatewayCode = requestHeaders.getFirst("Gateway-Code");
        if (!StringUtils.hasText(gatewayCode)) {
            log.atError().log("Missing Gateway-Code header");
            deferredResult.setErrorResult(new ResponseEntity<>(BAD_REQUEST));
            return deferredResult;
        }

        log.atInfo().addKeyValue("gatewayCode", gatewayCode).log("Prometheus remote-write data received from edge-gateway");

        String contentEncoding = requestHeaders.getFirst("Content-Encoding");
        if (!"snappy".equals(contentEncoding)) {
            log.atError().addKeyValue("contentEncoding", contentEncoding).log("Unknown Content-Encoding, only 'snappy' supported");
            deferredResult.setErrorResult(new ResponseEntity<>(UNSUPPORTED_MEDIA_TYPE));
            return deferredResult;
        }

        String contentType = requestHeaders.getFirst(HttpHeaders.CONTENT_TYPE);
        if (!"application/x-protobuf;proto=io.prometheus.write.v2.Request".equals(contentType)) {
            log.atError().addKeyValue("contentType", contentType)
                    .log("Unknown Content-Type, only 'application/x-protobuf;proto=io.prometheus.write.v2.Request' supported");
            deferredResult.setErrorResult(new ResponseEntity<>(UNSUPPORTED_MEDIA_TYPE));
            return deferredResult;
        }

        try {
            byte[] bytes = toByteArray(inputStream);
            Request request = PrometheusRequestDecoder.toRequest(bytes);
            MultiValueMap<String, String> responseHeaders = new LinkedMultiValueMap<>(1);
            handle(request, (responseHeader, writtenCount) -> responseHeaders.set(responseHeader, writtenCount.toString()));
            // Send to kafka
            kafkaTemplate.send(TOPIC_AIOT_EDGE_GATEWAY_PROMETHEUS, gatewayCode, bytes)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
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

    private static void handle(Request request, BiConsumer<String, Integer> written) {
        int sampleCount = request.getTimeseriesList().stream()
                .mapToInt(TimeSeries::getSamplesCount)
                .reduce(0, Integer::sum);
        written.accept(X_PROMETHEUS_REMOTE_WRITE_SAMPLES_WRITTEN, sampleCount);
        written.accept(X_PROMETHEUS_REMOTE_WRITE_HISTOGRAMS_WRITTEN, 0);
        written.accept(X_PROMETHEUS_REMOTE_WRITE_EXEMPLARS_WRITTEN, 0);
    }
}
