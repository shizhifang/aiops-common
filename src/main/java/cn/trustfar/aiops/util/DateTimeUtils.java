package cn.trustfar.aiops.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 * 时间工具类
 * @author liy
 */
public class DateTimeUtils {
    final public static String DATA_TIME_FORMAT_MILLI_SECOND_23 = "yyyy-MM-dd HH:mm:ss.SSS";
    final public static String DATA_TIME_FORMAT_SECOND_19 = "yyyy-MM-dd HH:mm:ss";
    final public static String DATA_TIME_FORMAT_MINUTE_16 = "yyyy-MM-dd HH:mm";
    final public static String DATA_TIME_FORMAT_HOUR_13 = "yyyy-MM-dd HH";
    final public static String DATA_TIME_FORMAT_DAY_10 = "yyyy-MM-dd";

    public static DateTime getMinuteStart(DateTime thisTime) {
        return thisTime.withSecondOfMinute(0).withMillisOfSecond(0);
    }

    /**
     * 判断text是否在start-end区间内
     * @param text
     * @param start
     * @param end
     * @return
     */
    public static boolean whetherInThisInterval(DateTime text, DateTime start, DateTime end) {
        if (text.isAfter(start.minus(1)) && text.isBefore(end)) {
            return true;
        }
        return false;
    }

    /**
     * 时间格式化字符串转DateTime对象
     * @param text 要匹配的字符串
     * @param pattern 字符串时间匹配模式
     * @return
     */
    public static DateTime parseDateTime(String text, String pattern) {
        if (text == null || text.length() < DATA_TIME_FORMAT_DAY_10.length()) {
            return null;
        }
        if (pattern != null && pattern.length() > 0) {
            return DateTimeFormat.forPattern(pattern).parseDateTime(text);
        }
        if (text.length() == DATA_TIME_FORMAT_MILLI_SECOND_23.length()) {
            return DateTimeFormat.forPattern(DATA_TIME_FORMAT_MILLI_SECOND_23).parseDateTime(text);
        }
        if (text.length() == DATA_TIME_FORMAT_SECOND_19.length()) {
            return DateTimeFormat.forPattern(DATA_TIME_FORMAT_SECOND_19).parseDateTime(text);
        }
        if (text.length() == DATA_TIME_FORMAT_MINUTE_16.length()) {
            return DateTimeFormat.forPattern(DATA_TIME_FORMAT_MINUTE_16).parseDateTime(text);
        }
        if (text.length() == DATA_TIME_FORMAT_HOUR_13.length()) {
            return DateTimeFormat.forPattern(DATA_TIME_FORMAT_HOUR_13).parseDateTime(text);
        }
        if (text.length() == DATA_TIME_FORMAT_DAY_10.length()) {
            return DateTimeFormat.forPattern(DATA_TIME_FORMAT_DAY_10).parseDateTime(text);
        }
        return null;
    }


}
