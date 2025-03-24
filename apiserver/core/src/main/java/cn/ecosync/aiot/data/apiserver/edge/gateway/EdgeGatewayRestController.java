package cn.ecosync.aiot.data.apiserver.edge.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static cn.ecosync.aiot.data.apiserver.edge.gateway.EdgeGatewayConstants.TOPIC_AIOT_EDGE_GATEWAY_PROMETHEUS;
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

        log.atInfo().addKeyValue("gatewayCode", gatewayCode).log("Prometheus remote-write data received from edge-gateway");

        try {
            byte[] bytes = toByteArray(inputStream);
//            Request request = PrometheusRequestDecoder.toRequest(bytes);
            kafkaTemplate.send(TOPIC_AIOT_EDGE_GATEWAY_PROMETHEUS, gatewayCode, bytes)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
//                    int sampleCount = PrometheusRequestDecoder.getSampleCount(request);
//                    MultiValueMap<String, String> responseHeaders = new LinkedMultiValueMap<>(1);
//                    responseHeaders.set("X-Prometheus-Remote-Write-Samples-Written", String.valueOf(sampleCount));
//                    deferredResult.setResult(new ResponseEntity<>(responseHeaders, NO_CONTENT));
                            deferredResult.setResult(new ResponseEntity<>(NO_CONTENT));
                        } else {
                            log.error("", ex);
                            deferredResult.setErrorResult(new ResponseEntity<>(INTERNAL_SERVER_ERROR));
                        }
                    });
        } catch (Exception e) {
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
}
