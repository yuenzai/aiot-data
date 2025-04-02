package cn.ecosync.aiot.data.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaToS3Job {
    private static final Logger log = LoggerFactory.getLogger(KafkaToS3Job.class);
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("([a-zA-Z0-9_-]+\\.){2}[a-zA-Z0-9_-]+");

    private static final String TMP_KAFKA = "tmp_kafka";
    private static final String STATEMENT_MERGE = """
            MERGE INTO %s target
            USING %s source
            ON target.offset = source.offset
            WHEN NOT MATCHED THEN INSERT *
            """;

    public static void main(String[] args) {
        log.info("args: {}", Arrays.toString(args));
        if (args.length < 4) {
            throw new IllegalArgumentException("args missing, require: [sourceTopic, targetTable, timestamp, offsetHour]");
        }
        String sourceTopic = args[0];
        String targetTable = args[1];
        long offsetHour = Long.parseLong(args[3]);
        long endingTimestamp = Long.parseLong(args[2]);
        long startingTimestamp = endingTimestamp + offsetHour * 60 * 60 * 1000;

        Matcher matcher = TABLE_NAME_PATTERN.matcher(targetTable);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Illegal table name: " + targetTable);
        }

        SparkSession spark = SparkSession.builder().appName("Kafka to S3").getOrCreate();
        extract(spark, sourceTopic, startingTimestamp, endingTimestamp);
        String sql = STATEMENT_MERGE.formatted(targetTable, TMP_KAFKA);
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
        df.createOrReplaceTempView(TMP_KAFKA);
    }
}
