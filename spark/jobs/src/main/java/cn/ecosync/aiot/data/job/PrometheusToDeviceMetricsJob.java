package cn.ecosync.aiot.data.job;

import cn.ecosync.aiot.data.job.api.DeviceMetrics;
import cn.ecosync.aiot.data.job.api.PrometheusRequest;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.from_protobuf;

public class PrometheusToDeviceMetricsJob {
    private static final Logger log = LoggerFactory.getLogger(PrometheusToDeviceMetricsJob.class);
    private static final String TABLE_NAME_SOURCE = "aiot.bronze.prometheus";
    private static final String TABLE_NAME_TARGET = "aiot.silver.device_metrics";

    public static void main(String[] args) throws NoSuchTableException {
        log.info("args: {}", Arrays.toString(args));
        if (args.length < 3) {
            throw new IllegalArgumentException("args missing, require: mode timestamp offsetHour");
        }
        String mode = args[0];
        if (!"append".equals(mode) && !"createOrReplace".equals(mode)) {
            throw new IllegalArgumentException("mode not supported: " + mode + ", only accepted append or createOrReplace");
        }
        long timestamp = Long.parseLong(args[1]);
        long offsetHour = Long.parseLong(args[2]);
        long timestamp2 = timestamp + offsetHour * 60 * 60 * 1000;
        long startingTimestamp = Math.min(timestamp, timestamp2);
        long endingTimestamp = Math.max(timestamp, timestamp2);

        SparkSession spark = SparkSession.builder().appName("Prometheus to DeviceMetrics").getOrCreate();
        spark.udf().register("snappy_decode", (UDF1<byte[], byte[]>) PrometheusToDeviceMetricsJob::snappyDecode, DataTypes.BinaryType);
        etl(spark, mode, startingTimestamp, endingTimestamp);
        spark.stop();
    }

    private static void etl(SparkSession spark, String mode, long startingTimestamp, long endingTimestamp) throws NoSuchTableException {
        // extract
        Column valueColumn = col("value");
        Dataset<Row> df = spark.table(TABLE_NAME_SOURCE)
                .select(valueColumn)
                .where("timestamp >= timestamp_millis(%d) and timestamp < timestamp_millis(%d)".formatted(startingTimestamp, endingTimestamp));
        df.show();
        // transform
        Column decodedValueColumn = callUDF("snappy_decode", valueColumn);
        Column protobufValueColumn = from_protobuf(decodedValueColumn, "Request", "aiot-data/prometheus.desc");
        Dataset<PrometheusRequest> prometheusDS = df.withColumn("value", protobufValueColumn)
                .select(col("value.symbols"), col("value.timeseries"))
                .as(Encoders.bean(PrometheusRequest.class));
        prometheusDS.show();
        Dataset<DeviceMetrics> deviceMetricsDS = prometheusDS.flatMap(PrometheusToDeviceMetricsJob::flatMap, Encoders.bean(DeviceMetrics.class));
        deviceMetricsDS.show();
        // load
        if (deviceMetricsDS.count() == 0) {
            return;
        }
        switch (mode) {
            case "append":
                deviceMetricsDS.writeTo(TABLE_NAME_TARGET)
                        .append();
                break;
            case "createOrReplace":
                deviceMetricsDS.writeTo(TABLE_NAME_TARGET)
                        .partitionedBy(days(col("timestamp")), col("job"))
                        .createOrReplace();
                break;
            default:
                throw new IllegalStateException("nothing to do");
        }
    }

    private static Iterator<DeviceMetrics> flatMap(PrometheusRequest request) {
        List<String> symbols = request.getSymbols();
        List<DeviceMetrics> list = new ArrayList<>();
        for (PrometheusRequest.TimeSeries ts : request.getTimeseries()) {
            if (!PrometheusRequest.Metadata.METRIC_TYPE_GAUGE.equals(ts.getMetadata().getType())) {
                continue;
            }
            Map<String, String> labels = new HashMap<>();
            for (int i = 0; 2 * i < ts.getLabels_refs().size(); i++) {
                String key = symbols.get(ts.getLabels_refs().get(2 * i));
                String value = symbols.get(ts.getLabels_refs().get(2 * i + 1));
                labels.put(key, value);
            }
            String jobType = labels.get("job_type");
            if (!"device".equals(jobType)) {
                continue;
            }
            String metricName = labels.get("__name__");
            String deviceCode = labels.get("device_code");
            String gatewayCode = labels.get("gateway_code");
            String job = labels.get("job");
            List<PrometheusRequest.Sample> samples = ts.getSamples();
            for (PrometheusRequest.Sample sample : samples) {
                DeviceMetrics deviceMetrics = new DeviceMetrics();
                deviceMetrics.setTimestamp(Instant.ofEpochMilli(sample.getTimestamp()));
                deviceMetrics.setValue(sample.getValue());
                deviceMetrics.setMetricName(metricName);
                deviceMetrics.setDeviceCode(deviceCode);
                deviceMetrics.setGatewayCode(gatewayCode);
                deviceMetrics.setJob(job);
                list.add(deviceMetrics);
            }
        }
        return list.iterator();
    }

    private static byte[] snappyDecode(byte[] bytes) {
        try {
            return Snappy.uncompress(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Snappy decompression failed", e);
        }
    }
}
