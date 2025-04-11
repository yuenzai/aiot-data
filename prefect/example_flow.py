import pendulum
import pendulum.datetime as dt
from prefect import flow, task
from prefect.runtime import flow_run
from prefect_shell import ShellOperation


def generate_flow_run_name():
    flow_name = flow_run.flow_name
    timezone = flow_run.parameters['timezone']
    datetime = pendulum.instance(flow_run.scheduled_start_time).in_timezone(timezone)
    return f"{flow_name}-{datetime.to_iso8601_string()}"


@task(name="Kafka to S3 task")
def kafka_to_s3_task(
        datetime: dt,
        duration: str,
        timezone: str,
):
    cmd = f"""
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
    print(cmd)
    with ShellOperation(commands=[cmd]) as operation:
        process = operation.trigger()
        process.wait_for_completion()
        exit_code = process.return_code
        return exit_code


@task(name="Device metrics task")
def device_metrics_task(
        datetime: dt,
        duration: str,
        timezone: str,
):
    cmd = f"""
          docker compose exec -it spark-master \
          /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.sql.session.timeZone={timezone} \
          --conf spark.aiot.datetime={datetime.to_iso8601_string()} \
          --conf spark.aiot.duration={duration} \
          --class cn.ecosync.aiot.data.job.DeviceMetricsJob \
          aiot-data/aiot-data-spark-jobs-0.0.1-SNAPSHOT.jar
          """
    print(cmd)
    with (ShellOperation(commands=[cmd]) as operation):
        process = operation.trigger()
        process.wait_for_completion()
        exit_code = process.return_code
        return exit_code


@flow(flow_run_name=generate_flow_run_name, log_prints=True)
def example_flow(
        duration: str = "P1D",
        timezone: str = "Asia/Shanghai",
):
    state_list = []
    datetime = pendulum.instance(flow_run.scheduled_start_time).in_timezone(timezone)
    kafka_to_s3_state = kafka_to_s3_task(datetime, duration, timezone, return_state=True)
    state_list.append(kafka_to_s3_state)
    if kafka_to_s3_state.is_completed():
        print("Kafka to S3 任务成功")
        device_metrics_state = device_metrics_task(datetime, duration, timezone, return_state=True)
        state_list.append(device_metrics_state)
    else:
        print("Kafka to S3 任务失败")
    return state_list


if __name__ == "__main__":
    example_flow.serve()
