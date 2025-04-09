import pendulum
import pendulum.datetime as dt
from prefect import flow, task
from prefect.runtime import flow_run
from prefect_shell import ShellOperation


@task
def kafka_to_s3(
        datetime: dt,
        duration: str,
        timezone: str,
):
    with ShellOperation(
            commands=[
                f"""
                docker compose exec -it spark-master \
                /opt/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --conf spark.sql.session.timeZone={timezone} \
                --conf spark.aiot.datetime={datetime.to_iso8601_string()} \
                --conf spark.aiot.duration={duration} \
                --class cn.ecosync.aiot.data.job.KafkaToS3Job \
                aiot-data/aiot-data-spark-jobs-0.0.1-SNAPSHOT.jar \
                prometheus-write-2.0
                """
            ]
    ) as kafka_to_s3_operation:
        kafka_to_s3_process = kafka_to_s3_operation.trigger()
        kafka_to_s3_process.wait_for_completion()


def generate_flow_run_name():
    flow_name = flow_run.flow_name
    timezone = flow_run.parameters['timezone']
    datetime = pendulum.instance(flow_run.scheduled_start_time).in_timezone(timezone)
    return f"{flow_name}-{datetime.to_iso8601_string()}"


@flow(flow_run_name=generate_flow_run_name, log_prints=True)
def my_flow(
        duration: str = "P1D",
        timezone: str = "Asia/Shanghai",
):
    datetime = pendulum.instance(flow_run.scheduled_start_time).in_timezone(timezone)
    kafka_to_s3(datetime, duration, timezone)


if __name__ == "__main__":
    my_flow.serve()
