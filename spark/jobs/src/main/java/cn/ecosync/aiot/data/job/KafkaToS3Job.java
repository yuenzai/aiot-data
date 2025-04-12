package cn.ecosync.aiot.data.job;

import cn.ecosync.aiot.data.job.util.SparkSessionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;

public class KafkaToS3Job {
    private static final Logger log = LoggerFactory.getLogger(KafkaToS3Job.class);
    private static final String TMP_KAFKA = "tmp_kafka";
    private static final String STATEMENT_CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS %s (
                `offset` long,
                `key` string,
                `value` binary,
                `timestamp` timestamp,
                `headers` array<struct<key:string,value:binary>>
            )
            USING iceberg
            PARTITIONED BY (day(`timestamp`), `key`)
            """;
    private static final String STATEMENT_WRITE = """
            MERGE INTO %s target
            USING %s source
            ON target.offset = source.offset
            WHEN NOT MATCHED THEN INSERT *
            """;

    public static void main(String[] args) {
        String topic;
        if (args.length < 1 || (topic = args[0]) == null) {
            throw new IllegalArgumentException("Argument 'topic' is required");
        }
        SparkSession spark = SparkSession.builder().appName("KafkaToS3Job").getOrCreate();
        String table = "aiot.bronze." + topic.replaceAll("[\\\\.\\-]", "_");
        spark.sql(STATEMENT_CREATE_TABLE.formatted(table));
        etl(spark, topic, table);
        spark.stop();
    }

    private static void etl(SparkSession spark, String topic, String table) {
        SparkSessionUtils.JobConfig jobConfig = SparkSessionUtils.getJobConfig(spark);
        ZonedDateTime startDateTime = jobConfig.getStartDateTime();
        ZonedDateTime endDateTime = jobConfig.getEndDateTime();
        log.info("topic: {}, startDateTime: {}, endDateTime: {}", topic, startDateTime, endDateTime);
        String kafkaBootstrapServers = spark.conf().get("spark.kafka.bootstrap.servers", "kafka:9092");
        // extract
        Dataset<Row> df = spark.read().format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", topic)
                .option("startingTimestamp", String.valueOf(startDateTime.toInstant().toEpochMilli()))
                .option("endingTimestamp", String.valueOf(endDateTime.toInstant().toEpochMilli()))
                .option("includeHeaders", "true")
                .option("fetchOffset.numRetries", 1)
                .load();
        df = df.selectExpr("CAST(key AS STRING)", "value", "offset", "timestamp", "headers");
        df.show();
        // transform
        // load
        df.createOrReplaceTempView(TMP_KAFKA);
        String sql = STATEMENT_WRITE.formatted(table, TMP_KAFKA);
        spark.sql(sql);
    }
}
