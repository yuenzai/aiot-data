from prefect import flow
from prefect.runtime import flow_run
from prefect_shell import ShellOperation


def generate_flow_run_name():
    flow_name = flow_run.flow_name
    timestamp = int(flow_run.scheduled_start_time.timestamp() * 1000)
    return f"{flow_name}-{timestamp}"


@flow(flow_run_name=generate_flow_run_name, log_prints=True)
def kafka_to_s3(offset_hour: int = -24):
    timestamp = int(flow_run.scheduled_start_time.timestamp() * 1000)
    with ShellOperation(
            commands=[
                f"""
                docker compose exec -it spark-master /opt/spark/bin/spark-submit \
                --class cn.ecosync.aiot.data.job.KafkaToS3Job \
                --master local \
                --conf spark.sql.session.timeZone=Asia/Shanghai \
                aiot-data/aiot-data-spark-jobs-0.0.1-SNAPSHOT.jar \
                prometheus-write-2.0 \
                aiot.bronze.prometheus \
                {timestamp} \
                {offset_hour}
                """
            ]
    ) as kafka_to_s3_operation:
        kafka_to_s3_process = kafka_to_s3_operation.trigger()
        kafka_to_s3_process.wait_for_completion()


if __name__ == "__main__":
    kafka_to_s3.serve()
