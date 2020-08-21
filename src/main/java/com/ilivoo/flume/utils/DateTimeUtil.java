package com.ilivoo.flume.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.TimeZone;

import net.opentsdb.core.Tags;

public class DateTimeUtil {

    public static final HashMap<String, TimeZone> timezones;
    static {
        final String[] tzs = TimeZone.getAvailableIDs();
        timezones = new HashMap<>(tzs.length);
        for (final String tz : tzs) {
            timezones.put(tz, TimeZone.getTimeZone(tz));
        }
    }

    private static ThreadLocal<SimpleDateFormat> sdf10_1 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf10_2 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy/MM/dd");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf16_1 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy/MM/dd-HH:mm");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf16_2 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf16_3 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy/MM/dd HH:mm");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf19_1 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf19_2 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf19_3 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf21_1 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdf23_1 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }
    };


    /**
     * Attempts to parse a timestamp from a given string
     * Formats accepted are:
     * <ul>
     * <li>Relative: {@code 5m-ago}, {@code 1h-ago}, etc. See
     * {@link #parseDuration}</li>
     * <li>Absolute human readable dates:
     * <ul><li>"yyyy/MM/dd-HH:mm:ss"</li>
     * <li>"yyyy/MM/dd HH:mm:ss"</li>
     * <li>"yyyy/MM/dd-HH:mm"</li>
     * <li>"yyyy/MM/dd HH:mm"</li>
     * <li>"yyyy/MM/dd"</li></ul></li>
     * <li>Unix Timestamp in seconds or milliseconds:
     * <ul><li>1355961600</li>
     * <li>1355961600000</li>
     * <li>1355961600.000</li></ul></li>
     * </ul>
     * @param datetime The string to parse a value for
     * @param tz The timezone to use for parsing.
     * @return A Unix epoch timestamp in milliseconds
     * @throws NullPointerException if the timestamp is null
     * @throws IllegalArgumentException if the request was malformed
     */
    public static final long parseDateTimeString(final String datetime,
                                                 final String tz) {
        if (datetime == null || datetime.isEmpty())
            return -1;

        if (datetime.matches("^[0-9]+s$")) {
            return Tags.parseLong(datetime.replaceFirst("^([0-9]+)(s)$", "$1")) * 1000;
        }

        if (datetime.matches("^[0-9]+ms$")) {
            return Tags.parseLong(datetime.replaceFirst("^([0-9]+)(ms)$", "$1"));
        }

        if (datetime.toLowerCase().equals("now")) {
            return System.currentTimeMillis();
        }

        if (datetime.toLowerCase().endsWith("-ago")) {
            long interval = parseDuration(
                    datetime.substring(0, datetime.length() - 4));
            return System.currentTimeMillis() - interval;
        }

        if (datetime.contains("/") || datetime.contains(":") || datetime.contains("-")) {
            try {
                SimpleDateFormat fmt = null;
                switch (datetime.length()) {
                    // these were pulled from cliQuery but don't work as intended since
                    // they assume a date of 1970/01/01. Can be fixed but may not be worth
                    // it
                    // case 5:
                    //   fmt = new SimpleDateFormat("HH:mm");
                    //   break;
                    // case 8:
                    //   fmt = new SimpleDateFormat("HH:mm:ss");
                    //   break;
                    case 10:
                        if (datetime.contains("-"))
                            fmt = sdf10_1.get();
                        else
                            fmt = sdf10_2.get();
                        break;
                    case 16:
                        if (datetime.contains("/") && datetime.contains("-"))
                            fmt = sdf16_1.get();
                        else if (datetime.contains("-"))
                            fmt = sdf16_2.get();
                        else
                            fmt = sdf16_3.get();
                        break;
                    case 19:
                        if (datetime.contains("/") && datetime.contains("-"))
                            fmt = sdf19_1.get();
                        else if (datetime.contains("-"))
                            fmt = sdf19_2.get();
                        else
                            fmt = sdf19_3.get();
                        break;
                    case 21:
                        fmt = sdf21_1.get();
                        break;
                    case 23:
                        fmt = sdf23_1.get();
                        break;
                    default:
                        // todo - deal with internationalization, other time formats
                        throw new IllegalArgumentException("Invalid absolute date: "
                                + datetime);
                }
                if (tz != null && !tz.isEmpty())
                    setTimeZone(fmt, tz);
                return fmt.parse(datetime).getTime();
            } catch (ParseException e) {
                throw new IllegalArgumentException("Invalid date: " + datetime
                        + ". " + e.getMessage());
            }
        } else {
            try {
                long time;
                final boolean contains_dot = datetime.contains(".");
                // [0-9]{10} ten digits
                // \\. a dot
                // [0-9]{1,3} one to three digits
                if (contains_dot) {
                    final boolean valid_dotted_ms = datetime.matches("^[0-9]{10}\\.[0-9]{1,3}$");
                    if (!valid_dotted_ms) {
                        throw new IllegalArgumentException("Invalid time: " + datetime
                                + ". Millisecond timestamps must be in the format "
                                + "<seconds>.<ms> where the milliseconds are limited to 3 digits");
                    }
                    int dotIndex = datetime.indexOf(".");
                    time = Tags.parseLong(datetime.substring(0, dotIndex)) * 1000;
                    long dot_time = Tags.parseLong(datetime.substring(dotIndex + 1));
                    while (dot_time < 99) {
                        dot_time *= 10;
                    }
                    time += dot_time;
                } else {
                    time = Tags.parseLong(datetime);
                }
                if (time < 0) {
                    throw new IllegalArgumentException("Invalid time: " + datetime
                            + ". Negative timestamps are not supported.");
                }
                // this is a nasty hack to determine if the incoming request is
                // in seconds or milliseconds. This will work until November 2286
                if (!contains_dot && datetime.length() <= 10) {
                    time *= 1000;
                }
                return time;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid time: " + datetime
                        + ". " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        String datetime = "1231231ms";
        System.out.println(parseDateTimeString(datetime, null));
    }

    /**
     * Parses a human-readable duration (e.g, "10m", "3h", "14d") into seconds.
     * <p>
     * Formats supported:<ul>
     * <li>{@code ms}: milliseconds</li>
     * <li>{@code s}: seconds</li>
     * <li>{@code m}: minutes</li>
     * <li>{@code h}: hours</li>
     * <li>{@code d}: days</li>
     * <li>{@code w}: weeks</li>
     * <li>{@code n}: month (30 days)</li>
     * <li>{@code y}: years (365 days)</li></ul>
     * @param duration The human-readable duration to parse.
     * @return A strictly positive number of milliseconds.
     * @throws IllegalArgumentException if the interval was malformed.
     */
    public static final long parseDuration(final String duration) {
        long interval;
        long multiplier;
        double temp;
        int unit = 0;
        while (Character.isDigit(duration.charAt(unit))) {
            unit++;
            if (unit >= duration.length()) {
                throw new IllegalArgumentException("Invalid duration, must have an "
                        + "integer and unit: " + duration);
            }
        }
        try {
            interval = Long.parseLong(duration.substring(0, unit));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid duration (number): " + duration);
        }
        if (interval <= 0) {
            throw new IllegalArgumentException("Zero or negative duration: " + duration);
        }
        switch (duration.toLowerCase().charAt(duration.length() - 1)) {
            case 's':
                if (duration.charAt(duration.length() - 2) == 'm') {
                    return interval;
                }
                multiplier = 1; break;                        // seconds
            case 'm': multiplier = 60; break;               // minutes
            case 'h': multiplier = 3600; break;             // hours
            case 'd': multiplier = 3600 * 24; break;        // days
            case 'w': multiplier = 3600 * 24 * 7; break;    // weeks
            case 'n': multiplier = 3600 * 24 * 30; break;   // month (average)
            case 'y': multiplier = 3600 * 24 * 365; break;  // years (screw leap years)
            default: throw new IllegalArgumentException("Invalid duration (suffix): " + duration);
        }
        multiplier *= 1000;
        temp = (double)interval * multiplier;
        if (temp > Long.MAX_VALUE) {
            throw new IllegalArgumentException("Duration must be < Long.MAX_VALUE ms: " + duration);
        }
        return interval * multiplier;
    }

    /**
     * Applies the given timezone to the given date format.
     * @param fmt Date format to apply the timezone to.
     * @param tzname Name of the timezone, or {@code null} in which case this
     * function is a no-op.
     * @throws IllegalArgumentException if tzname isn't a valid timezone name.
     * @throws NullPointerException if the format is null
     */
    public static void setTimeZone(final SimpleDateFormat fmt,
                                   final String tzname) {
        if (tzname == null) {
            return;  // Use the default timezone.
        }
        final TimeZone tz = timezones.get(tzname);
        if (tz != null) {
            fmt.setTimeZone(tz);
        } else {
            throw new IllegalArgumentException("Invalid timezone name: " + tzname);
        }
    }
}
