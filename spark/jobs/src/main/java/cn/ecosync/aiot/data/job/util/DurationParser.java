package cn.ecosync.aiot.data.job.util;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DurationParser {
    private static final Pattern PATTERN = Pattern.compile("^(\\d+)([dhms])$");

    public static Duration parse(String input) {
        Matcher matcher = PATTERN.matcher(input);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid duration format: " + input);
        }

        long amount = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2);

        switch (unit) {
            case "d":
                return Duration.of(amount, ChronoUnit.DAYS);
            case "h":
                return Duration.of(amount, ChronoUnit.HOURS);
            case "m":
                return Duration.of(amount, ChronoUnit.MINUTES);
            case "s":
                return Duration.of(amount, ChronoUnit.SECONDS);
            default:
                throw new IllegalArgumentException("Unknown unit: " + unit);
        }
    }
}
