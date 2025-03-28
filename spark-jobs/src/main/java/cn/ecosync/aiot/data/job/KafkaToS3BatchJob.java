package cn.ecosync.aiot.data.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

public class KafkaToS3BatchJob {
    public static void main(String[] args) throws NoSuchTableException {
        if (args.length != 3) {
            throw new IllegalArgumentException("topic, startingTimestamp and endingTimestamp args are required");
        }
        String topic = args[0];
        String startingTimestamp = args[1];
        String endingTimestamp = args[2];

        SparkSession spark = SparkSession.builder().appName("Kafka to S3").getOrCreate();
        load(transform(extract(spark, topic, startingTimestamp, endingTimestamp)));
        spark.stop();
    }

    private static Dataset<Row> extract(SparkSession spark, String topic, String startingTimestamp, String endingTimestamp) {
        Dataset<Row> df = spark.read().format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", topic)
                .option("startingTimestamp", startingTimestamp)
                .option("endingTimestamp", endingTimestamp)
                .load();
        return df.selectExpr("CAST(key AS STRING)", "value", "timestamp");
    }

    private static Dataset<Row> transform(Dataset<Row> df) {
        return df;
    }

    private static void load(Dataset<Row> df) throws NoSuchTableException {
        df.writeTo("aiot.bronze.prometheus").append();
    }

//    private static void udfRegister(SparkSession spark, Dataset<Row> df) {
//        spark.udf().register("snappy_decode", (UDF1<byte[], byte[]>) KafkaToS3BatchJob::snappyDecode, DataTypes.BinaryType);
//        df.withColumn("value", callUDF("snappy_decode", col("value")))
//                .where(col("value").isNotNull())
//                .select(from_protobuf(col("value"), Request.class.getCanonicalName()));
//    }
//
//    private static byte[] snappyDecode(byte[] bytes) {
//        try {
//            return bytes != null ? Snappy.uncompress(bytes) : null;
//        } catch (IOException e) {
//            throw new RuntimeException("Snappy decompression failed", e);
//        }
//    }
}
