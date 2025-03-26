```bash
docker compose exec -it spark-master /opt/spark/bin/spark-sql
CREATE TABLE aiot.test.test (id bigint, data string) USING iceberg;
```
