package cn.ecosync.aiot.data.job.util;

import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class SparkSessionUtils {
    public static JobConfig getJobConfig(SparkSession spark) {
        ZoneId zoneId = ZoneId.of(spark.conf().get("spark.sql.session.timeZone"));
        ZonedDateTime startDateTime = LocalDateTime.parse(spark.conf().get("spark.aiot.dateTime"), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .atZone(zoneId);
        Duration duration = DurationParser.parse(spark.conf().get("spark.aiot.timeWindow"));
        return new JobConfig(zoneId, startDateTime, duration);
    }

    public static class JobConfig {
        private final ZoneId zoneId;
        private final ZonedDateTime startDateTime;
        private final Duration duration;

        public JobConfig(ZoneId zoneId, ZonedDateTime startDateTime, Duration duration) {
            this.zoneId = zoneId;
            this.startDateTime = startDateTime;
            this.duration = duration;
        }

        public ZoneId getZoneId() {
            return zoneId;
        }

        public ZonedDateTime getStartDateTime() {
            return startDateTime;
        }

        public Duration getDuration() {
            return duration;
        }

        public ZonedDateTime getEndDateTime() {
            return startDateTime.plus(duration);
        }
    }
}
