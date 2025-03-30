package cn.ecosync.aiot.data.job;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class KafkaToS3BatchJob {
    private static final Logger log = LoggerFactory.getLogger(KafkaToS3BatchJob.class);

    private static final String BRONZE_PROMETHEUS = "aiot.bronze.prometheus";
    private static final String VIEW_PROMETHEUS = "view_prometheus";
    private static final String STATEMENT_MERGE = "MERGE INTO " + BRONZE_PROMETHEUS + " t " +
            "USING " + VIEW_PROMETHEUS + " s " +
            "ON t.offset = s.offset " +
            "WHEN MATCHED THEN UPDATE SET * " +
            "WHEN NOT MATCHED THEN INSERT *";

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("topic, startingTimestamp and endingTimestamp args are required");
        }
        String topic = args[0];
        String startingTimestamp = args[1];
        String endingTimestamp = args[2];

        SparkSession spark = SparkSession.builder().appName("Kafka to S3").getOrCreate();
        spark.udf().register("snappy_decode", (UDF1<byte[], byte[]>) KafkaToS3BatchJob::snappyDecode, DataTypes.BinaryType);
        extract(spark, topic, startingTimestamp, endingTimestamp);
        spark.sql(STATEMENT_MERGE);
        spark.stop();
    }

    private static void extract(SparkSession spark, String topic, String startingTimestamp, String endingTimestamp) {
        Dataset<Row> df = spark.read().format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", topic)
                .option("startingTimestamp", startingTimestamp)
                .option("endingTimestamp", endingTimestamp)
                .load();
        Column keyColumn = col("key").cast(DataTypes.StringType);
        Column valueColumn = callUDF("snappy_decode", col("value")).as("value");
        df = df.select(col("offset"), keyColumn, valueColumn, col("timestamp"));
        df.createOrReplaceTempView(VIEW_PROMETHEUS);
    }

    private static byte[] snappyDecode(byte[] bytes) {
        try {
            return Snappy.uncompress(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Snappy decompression failed", e);
        }
    }

//    private static Dataset<Row> transform(Dataset<Row> df) {
//        Column keyColumn = col("key").cast(DataTypes.StringType);
//        Column valueColumn = from_protobuf(callUDF("snappy_decode", col("value")), "Request", "prometheus.desc").as("value");
//        df = df.select(keyColumn, valueColumn, col("timestamp"));
//        df.show();
//        return df;
//    }
}
