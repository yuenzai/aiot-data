```bash
docker compose exec -it spark-master /opt/spark/bin/spark-sql --conf spark.sql.session.timeZone=Asia/Shanghai
docker compose exec -it spark-master /opt/spark/bin/spark-sql --conf spark.sql.session.timeZone=Asia/Shanghai --conf spark.wap.branch=audit
# 提交 Spark 任务
docker compose exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--deploy-mode client \
--conf spark.sql.session.timeZone=Asia/Shanghai \
--conf spark.aiot.dateTime=2025-04-06T00:00:00 \
--conf spark.aiot.timeWindow=24h \
--class cn.ecosync.aiot.data.job.DeviceMetricsJob \
aiot-data/aiot-data-spark-jobs-0.0.1-SNAPSHOT.jar
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
```sparksql
-- 使用当前快照创建分支
ALTER TABLE aiot.silver.device_metrics CREATE BRANCH audit;
ALTER TABLE aiot.silver.device_metrics CREATE BRANCH test AS OF VERSION 3921915776515600024;
-- 删除分支
ALTER TABLE aiot.silver.device_metrics DROP BRANCH audit;

SELECT * FROM aiot.silver.device_metrics;
SELECT * FROM aiot.silver.device_metrics.branch_main;
SELECT * FROM aiot.silver.device_metrics.branch_audit;
SELECT * FROM aiot.silver.device_metrics VERSION AS OF 8742507384435017915;

SELECT * FROM aiot.silver.device_metrics.snapshots;
SELECT * FROM aiot.silver.device_metrics.refs;
SELECT * FROM aiot.silver.device_metrics.history;
SELECT * FROM aiot.silver.device_metrics.files;
SELECT * FROM aiot.silver.device_metrics.partitions;

INSERT INTO aiot.silver.device_metrics.branch_audit VALUES (now(), 1.1, 'RT', 'AHU_1_1', 'qie', 'AHU');
INSERT INTO aiot.silver.device_metrics.branch_audit VALUES (now(), 2.2, 'ST', 'AHU_1_1', 'qie', 'AHU');
INSERT INTO aiot.silver.device_metrics.branch_audit VALUES (now(), 3.3, 'RT', 'AHU_1_1', 'qie', 'AHU');

CALL aiot.system.fast_forward('silver.device_metrics', 'main', 'audit');
CALL aiot.system.set_current_snapshot('silver.device_metrics', 6774156615847425692);
CALL aiot.system.rollback_to_snapshot('silver.device_metrics', 8742507384435017915);

-- Audit workflow
-- 建表并同时开启 write.wap.enabled
CREATE TABLE IF NOT EXISTS aiot.silver.device_metrics (
    `timestamp` timestamp,
    `value` double,
    `metricName` string,
    `deviceCode` string,
    `gatewayCode` string,
    `job` string
)
USING iceberg
PARTITIONED BY (day(`timestamp`), `job`)
TBLPROPERTIES ('write.wap.enabled'='true');
-- ALTER TABLE aiot.silver.device_metrics SET TBLPROPERTIES ('write.wap.enabled'='true');
ALTER TABLE aiot.silver.device_metrics CREATE BRANCH IF NOT EXISTS B20250407 RETAIN 7 DAYS;
-- 将 wap 分支设置为 audit 分支
SET spark.wap.branch = audit;
-- 当 write.wap.enabled 和 spark.wap.branch 同时开启后，写操作只会影响 wap 分支
INSERT INTO aiot.silver.device_metrics VALUES (now(), 1.1, 'RT', 'AHU_1_1', 'qie', 'AHU');
INSERT INTO aiot.silver.device_metrics VALUES (now(), 2.2, 'ST', 'AHU_1_1', 'qie', 'AHU');
INSERT INTO aiot.silver.device_metrics VALUES (now(), 3.3, 'RT', 'AHU_1_1', 'qie', 'AHU');
-- 将 audit 分支合并到 main 分支
CALL aiot.system.fast_forward('silver.device_metrics', 'main', 'audit');
```
