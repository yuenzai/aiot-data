package cn.ecosync.aiot.data.job.api;

import java.io.Serializable;
import java.util.List;

public class PrometheusRequest implements Serializable {
    private List<String> symbols;
    private List<TimeSeries> timeseries;

    public List<String> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<String> symbols) {
        this.symbols = symbols;
    }

    public List<TimeSeries> getTimeseries() {
        return timeseries;
    }

    public void setTimeseries(List<TimeSeries> timeseries) {
        this.timeseries = timeseries;
    }

    public static class TimeSeries implements Serializable {
        private List<Integer> labels_refs;
        private List<Sample> samples;
        private Metadata metadata;

        public List<Integer> getLabels_refs() {
            return labels_refs;
        }

        public void setLabels_refs(List<Integer> labels_refs) {
            this.labels_refs = labels_refs;
        }

        public List<Sample> getSamples() {
            return samples;
        }

        public void setSamples(List<Sample> samples) {
            this.samples = samples;
        }

        public Metadata getMetadata() {
            return metadata;
        }

        public void setMetadata(Metadata metadata) {
            this.metadata = metadata;
        }
    }

    public static class Metadata implements Serializable {
        public static final String METRIC_TYPE_GAUGE = "METRIC_TYPE_GAUGE";

        private String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    public static class Sample implements Serializable {
        private Double value;
        private Long timestamp;

        public Double getValue() {
            return value;
        }

        public void setValue(Double value) {
            this.value = value;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
