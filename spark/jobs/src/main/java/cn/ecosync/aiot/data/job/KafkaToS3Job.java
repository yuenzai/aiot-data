package cn.ecosync.aiot.data.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaToS3Job {
    private static final Logger log = LoggerFactory.getLogger(KafkaToS3Job.class);
    private static final DateTimeFormatter FORMATTER_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("([a-zA-Z0-9_-]+\\.){2}[a-zA-Z0-9_-]+");

    private static final String TEMP_KAFKA = "temp_kafka";
    // 只追加写，因为数据来自 Kafka 属于不可变数据，所以没有 Update 操作
    private static final String STATEMENT_MERGE = """
            MERGE INTO %s target
            USING %s source
            ON target.offset = source.offset
            WHEN NOT MATCHED THEN INSERT *
            """;

    public static void main(String[] args) {
        if (args.length != 5) {
            throw new IllegalArgumentException("topic, timezone, startingTimestamp, endingTimestamp, tableName args are required");
        }
        String topic = args[0];
        String timezone = args[1];
        String startingDateTime = args[2];
        String endingDateTime = args[3];
        String tableName = args[4];
        Matcher matcher = TABLE_NAME_PATTERN.matcher(tableName);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Illegal table name: " + tableName);
        }

        ZoneId zoneId = ZoneId.of(timezone);
        long startingTimestamp = LocalDateTime.parse(startingDateTime, FORMATTER_DATETIME)
                .atZone(zoneId)
                .toInstant()
                .toEpochMilli();
        long endingTimestamp = LocalDateTime.parse(endingDateTime, FORMATTER_DATETIME)
                .atZone(zoneId)
                .toInstant()
                .toEpochMilli();
        log.info("zoneId: {}", zoneId);
        log.info("startingTimestamp: {}", startingTimestamp);
        log.info("endingTimestamp: {}", endingTimestamp);

        SparkSession spark = SparkSession.builder().appName("Kafka to S3").getOrCreate();
        extract(spark, topic, startingTimestamp, endingTimestamp);
        String sql = STATEMENT_MERGE.formatted(tableName, TEMP_KAFKA);
        spark.sql(sql);
        spark.stop();
    }

    private static void extract(SparkSession spark, String topic, long startingTimestamp, long endingTimestamp) {
        Dataset<Row> df = spark.read().format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", topic)
                .option("startingTimestamp", String.valueOf(startingTimestamp))
                .option("endingTimestamp", String.valueOf(endingTimestamp))
                .load();
        df = df.selectExpr("CAST(key AS STRING)", "value", "offset", "timestamp");
        df.createOrReplaceTempView(TEMP_KAFKA);
    }
}
