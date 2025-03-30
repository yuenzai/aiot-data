```bash
docker compose exec -it spark-master /opt/spark/bin/spark-sql

docker compose exec -it spark-master /opt/spark/bin/spark-submit \
--class cn.ecosync.aiot.data.job.KafkaToS3Job \
--master local \
aiot-data-spark-jobs-0.0.1-SNAPSHOT.jar \
prometheus-write-2.0 \
Asia/Shanghai \
"2025-03-28 00:00:00" \
"2025-03-31 00:00:00" \
aiot.bronze.prometheus

docker compose exec -it spark-master /opt/spark/bin/spark-submit \
--class cn.ecosync.aiot.data.job.KafkaToS3Job \
--master spark://spark-master:7077 \
--deploy-mode cluster \
aiot-data-spark-jobs-0.0.1-SNAPSHOT.jar \
prometheus-write-2.0 \
Asia/Shanghai \
"2025-03-29 00:00:00" \
"2025-03-30 00:00:00" \
aiot.bronze.prometheus
```
```sparksql
DROP TABLE aiot.bronze.prometheus PURGE;

CREATE TABLE aiot.bronze.prometheus (
    `offset` long,
    `key` string,
    `value` binary,
    `timestamp` timestamp
)
USING iceberg
PARTITIONED BY (day(`timestamp`), `key`);

SELECT count(*) FROM aiot.bronze.prometheus;
DELETE FROM aiot.bronze.prometheus WHERE timestamp >= '2025-03-28 00:00:00' AND timestamp < '2025-03-29 00:00:00';
```
