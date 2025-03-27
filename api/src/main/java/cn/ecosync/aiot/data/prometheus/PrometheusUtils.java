package cn.ecosync.aiot.data.prometheus;

import cn.ecosync.aiot.data.prometheus.api.Request;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class PrometheusUtils {
    public static final String TOPIC_PROMETHEUS_WRITE_20 = "prometheus-write-2.0";
    public static final String X_PROMETHEUS_REMOTE_WRITE_SAMPLES_WRITTEN = "X-Prometheus-Remote-Write-Samples-Written";
    public static final String X_PROMETHEUS_REMOTE_WRITE_HISTOGRAMS_WRITTEN = "X-Prometheus-Remote-Write-Histograms-Written";
    public static final String X_PROMETHEUS_REMOTE_WRITE_EXEMPLARS_WRITTEN = "X-Prometheus-Remote-Write-Exemplars-Written";

    public static Request toRequest(byte[] bytes) throws IOException {
        byte[] uncompress = Snappy.uncompress(bytes);
        return Request.parseFrom(uncompress);
    }
}
