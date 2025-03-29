```bash
docker compose exec -it spark-master /opt/spark/bin/spark-sql

docker compose exec -it spark-master /opt/spark/bin/spark-submit \
--class cn.ecosync.aiot.data.job.KafkaToS3BatchJob \
--master local \
aiot-data-spark-jobs-0.0.1-SNAPSHOT.jar \
prometheus-write-2.0 \
1743091200000 \
1743177600000
```
```sparksql
DROP TABLE aiot.bronze.prometheus PURGE;

CREATE TABLE aiot.bronze.prometheus (
    `key` STRING,
    `value` BINARY,
    `timestamp` TIMESTAMP
)
USING iceberg
PARTITIONED BY (day(`timestamp`), `key`);
```
