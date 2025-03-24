package cn.ecosync.aiot.data.apiserver.edge.gateway;

import cn.ecosync.aiot.data.apiserver.api.prometheus.Request;
import cn.ecosync.aiot.data.apiserver.api.prometheus.Sample;
import cn.ecosync.aiot.data.apiserver.api.prometheus.TimeSeries;
import io.prometheus.metrics.model.snapshots.Labels;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class PrometheusRequestDecoder {
    public static Request toRequest(byte[] bytes) throws IOException {
        byte[] uncompress = Snappy.uncompress(bytes);
        return Request.parseFrom(uncompress);
    }

    public static int getSampleCount(Request request) {
        int sampleCount = 0;
        for (TimeSeries timeSeries : request.getTimeseriesList()) {
            sampleCount += handle(request, timeSeries);
        }
        return sampleCount;
    }

    public static int handle(Request request, TimeSeries timeSeries) {
        int sampleCount = 0;
        String metricName = request.getSymbols(timeSeries.getLabelsRefs(1));
        String[] keyValuePairs = timeSeries.getLabelsRefsList().stream()
                .skip(2)
                .map(request::getSymbols)
                .toArray(String[]::new);
        Labels labels = Labels.of(keyValuePairs);
//        log.atInfo().addKeyValue("metricName", metricName).addKeyValue("labels", labels).log("TimeSeries");
        for (Sample sample : timeSeries.getSamplesList()) {
//            log.atInfo().addKeyValue("value", sample.getValue()).addKeyValue("timestamp", sample.getTimestamp()).log("Sample");
            sampleCount++;
        }
        return sampleCount;
    }
}
