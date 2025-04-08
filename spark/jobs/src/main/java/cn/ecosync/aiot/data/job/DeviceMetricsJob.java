package cn.ecosync.aiot.data.job;

import cn.ecosync.aiot.data.job.api.DeviceMetrics;
import cn.ecosync.aiot.data.job.api.PrometheusRequest;
import cn.ecosync.aiot.data.job.util.DurationParser;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.protobuf.functions.from_protobuf;

public class DeviceMetricsJob {
    private static final Logger log = LoggerFactory.getLogger(DeviceMetricsJob.class);
    private static final String TABLE_NAME_SOURCE = "aiot.bronze.prometheus";
    private static final String TABLE_TARGET = "aiot.silver.device_metrics";
    private static final String STATEMENT_CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS %s (
                `timestamp` timestamp,
                `value` double,
                `metricName` string,
                `deviceCode` string,
                `gatewayCode` string,
                `job` string
            )
            USING iceberg
            PARTITIONED BY (day(`timestamp`), `job`)
            TBLPROPERTIES ('write.wap.enabled'='true')
            """.formatted(TABLE_TARGET);

    public static void main(String[] args) throws NoSuchTableException, IOException {
        SparkSession spark = SparkSession.builder().appName("DeviceMetrics").getOrCreate();
        ZoneId zoneId = ZoneId.of(spark.conf().get("spark.sql.session.timeZone"));
        ZonedDateTime startDateTime = LocalDateTime.parse(spark.conf().get("spark.aiot.dateTime"), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .atZone(zoneId);
        Duration duration = DurationParser.parse(spark.conf().get("spark.aiot.timeWindow"));
        ZonedDateTime endDateTime = startDateTime.plus(duration);

        spark.udf().register("snappy_decode", (UDF1<byte[], byte[]>) DeviceMetricsJob::snappyDecode, DataTypes.BinaryType);
        String wapName = "audit";
        spark.sql(STATEMENT_CREATE_TABLE);
        spark.sql("ALTER TABLE aiot.silver.device_metrics CREATE BRANCH IF NOT EXISTS %s RETAIN 7 DAYS".formatted(wapName)).show();
        spark.sql("SET spark.wap.branch = %s".formatted(wapName)).show();
        etl(spark, startDateTime.toInstant(), endDateTime.toInstant());
        spark.stop();
    }

    private static void etl(SparkSession spark, Instant startingTimestamp, Instant endingTimestamp) throws NoSuchTableException, IOException {
        // extract
        Column valueColumn = col("value");
        Dataset<Row> df = spark.table(TABLE_NAME_SOURCE)
                .select(valueColumn)
                .where(col("timestamp").geq(startingTimestamp).and(col("timestamp").lt(endingTimestamp)));
        df.show();
        // transform
        Column decodedValueColumn = callUDF("snappy_decode", valueColumn);
        byte[] descriptorFile = getDescriptorFile();
        Column protobufValueColumn = from_protobuf(decodedValueColumn, "Request", descriptorFile);
        Dataset<PrometheusRequest> prometheusDS = df.withColumn("value", protobufValueColumn)
                .select(col("value.symbols"), col("value.timeseries"))
                .as(Encoders.bean(PrometheusRequest.class));
        prometheusDS.show();
        Dataset<DeviceMetrics> deviceMetricsDS = prometheusDS.flatMap(DeviceMetricsJob::flatMap, Encoders.bean(DeviceMetrics.class));
        deviceMetricsDS.show();
        // load
        if (deviceMetricsDS.count() == 0) {
            return;
        }
        deviceMetricsDS.writeTo(TABLE_TARGET).append();
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
                Double value = sample.getValue();
                deviceMetrics.setValue(value != null ? value : 0D);
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

    private static byte[] getDescriptorFile() throws IOException {
        try (InputStream inputStream = DeviceMetricsJob.class.getResourceAsStream("/prometheus.desc")) {
            if (inputStream == null) throw new IllegalStateException("prometheus.desc not found");
            return toByteArray(inputStream);
        }
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
